# KeyDB node moves Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move a keydb node onto a new disk through a shadow node that the serving node fills on its own, then promote the shadow with one PR, with processors finding the cluster through the `keydb` Service.

**Architecture:**
- **Shadow role:** a node is SHADOW when its advertised address is listed in `shadowNodeAddresses` for its index, and ACTIVE otherwise.
- **Replication:** an ACTIVE node with a shadow runs a replication job inside the node. It sends full, then incremental, passes over its hash ranges using the existing node-to-node streaming code. The job is bounded by a byte budget with an automatic brake, and reports readiness through metrics, logs and an admin HTTP endpoint.
- **Client:** it discovers the cluster through a seed, adopts address lists from failed responses, and looks the list up again when a node is dead.
- **Chart (rudderstack-operator):** renders the slots, the shadow list and the processors' Service URL.

**Tech Stack:** Go 1.27, gRPC/protobuf, Badger v4.9.6, rudder-go-kit (config, logger, stats, httputil, jsonrs), `golang.org/x/time/rate`, dockertest + MinIO for integration tests. Helm, `helm template` + `yq` for chart tests.

**Spec:** `docs/superpowers/specs/2026-10-26-keydb-disk-shrink-design.md`

## Global Constraints

- **Repo rules (CLAUDE.md):**
  - never `go build`;
  - `make lint` must pass before committing;
  - non-sugared logger with `obskit` fields, no `fmt.Sprintf` in logs;
  - `wg.Go`;
  - `errors.AsType`;
  - `httputil.CloseResponse`;
  - `jsonrs` instead of `encoding/json`;
  - 120-column limit (`lll`).
- **Formatting:** plain `gofmt`/`gofumpt` without `-extra`.
- **Proto:** regenerate with `protoc-gen-go@v1.34.2` and `protoc-gen-go-grpc@v1.5.1`, so the diff stays minimal.
- **Public repo:** `rudderlabs/keydb` is public. No customer names in committed files or PR text.
- **Commits:** no Claude attribution in commits, PR bodies or code.
- **Defaults, from the spec:**
  - gap threshold 60s;
  - old-client quiet period 15m;
  - dead-node lookup after 5s of transport errors;
  - zstd level 1 for replication;
  - one stream goroutine;
  - admin port 8080.
- **Every response carries the full list:** every Get, Put and GetNodeInfo response, from every role, carries the real cluster size and the full (non-degraded) address list.
- **Shutdown:** on SIGTERM there is no fence and no final pass. Stop gRPC (cancelling in-flight calls), then close Badger.

## Review Focus

1. **A shadow whose disk holds data from a previous move:** it must refuse to receive, with a log line and a metric, rather than merging stale data. Task 3 tests this.
2. **Clients bouncing between an old list from a not-yet-refreshed node and the new list:** this must never spin without backoff. Task 9 has a unit test where two mock nodes answer with conflicting lists.
3. **A replication pass running while the node shuts down:** shutdown must not wait for the pass. Task 6 tests that cancelling the context ends the job within a second.
4. **`shadowNodeAddresses` shorter than `nodeAddresses`, or empty:** a node must treat a missing entry as "no shadow" and must not panic. Task 2 tests this.
5. **A byte budget of 0 (paused) mid-pass:** the pass must block, not error out, and resume when unpaused. Task 7 tests this.

---

### Task 1: Protocol

**Files:**
- Modify: `proto/keydb.proto`, regenerate `proto/keydb.pb.go`, `proto/keydb_grpc.pb.go`
- Modify: `client/client.go`, adding the constants

**Interfaces:**
- Produces:
  - `pb.ErrorCode_NOT_READY` (value 4);
  - `enum NodeRole { ACTIVE = 0; SHADOW = 1; }`;
  - `GetNodeInfoResponse` fields `role` (6), `all_ranges_synced` (7), `last_legacy_client_request_unix` (8);
  - `SnapshotChunk.source_address` (5);
  - `client.FeaturesHeader = "keydb-client-features"` and `client.FeatureDiscovery = "discovery"`.

- [ ] Edit the proto, then install the pinned plugins and run the two `protoc` commands from `make proto`.
- [ ] `go vet ./...` passes, and the generated diff only adds the new fields.
- [ ] Commit `feat(proto): NOT_READY, node roles and source address on snapshot chunks`.

### Task 2: Node roles, request gating, legacy-client tracking, pod names

**Files:**
- Modify: `node/config.go`: `AdvertisedAddress string`, `ShadowAddresses func() []string`
- Create: `node/role.go`: `func (s *Service) role() pb.NodeRole` and `func (s *Service) shadowAddress() string`, which returns the address listed for this node's index, or ""
- Modify: `node/node.go` Get, Put and GetNodeInfo
- Modify: `cmd/node/main.go`:
  - config keys `advertisedAddress` (string) and `shadowNodeAddresses` (reloadable);
  - a `^keydb-(\d+)-b-0$` regex in `getNodeID`.
- Test: `node/role_test.go`, `cmd/node/main_test.go` (TestGetNodeID cases)

**Interfaces:**
- Produces:
  - `role()`, which returns SHADOW only when `AdvertisedAddress != ""` and the entry at `NodeID` equals it. A missing or short list means ACTIVE.
  - `shadowAddress()`, used by Task 6.
  - Legacy-client tracking: `s.lastLegacyClientRequest atomic.Int64` (unix seconds), set in Get and Put when the incoming metadata lacks `client.FeaturesHeader`, plus the counter `keydb_legacy_client_requests_total`.

- [ ] **Failing tests:**
  - `TestRole`: an empty list, a short list, a matching entry, a non-matching entry, and an advertised address that isn't set.
  - `TestShadowRefusesRequests`: Get and Put on a SHADOW return `ErrorCode_NOT_READY`, `ClusterSize == len(addresses)` and `NodesAddresses == addresses`.
  - `TestLegacyClientTracking`: a Get without the header sets `GetNodeInfo().LastLegacyClientRequestUnix > 0`; with the header, it stays 0.
  - The `TestGetNodeID` case `keydb-3-b-0` gives 3.
- [ ] **Implement.** The SHADOW check goes before the degraded check. GetNodeInfo fills `Role` and `LastLegacyClientRequestUnix`. `AllRangesSynced` is wired in Task 3.
- [ ] Tests pass with `go test ./node/ -run 'TestRole|TestShadowRefuses|TestLegacyClient' ./cmd/node/ -run TestGetNodeID`.
- [ ] Commit `feat(node): shadow role and old-client tracking`.

### Task 3: Since markers, synced check, leftover-disk guard, shadow streaming receive

**Files:**
- Modify: `node/node.go`: `getSnapshotTimestamp`, `setSnapshotTimestamp`, `ReceiveSnapshot`
- Create: `node/markers.go`: `allRangesSynced()`, `shadowSourceKey = "_shadow_source"`
- Modify: `internal/cache/badger/badger.go`: `HasData() (bool, error)`, true when any non-internal key exists (prefix `hr`, excluding `hr-1:`)
- Modify: the `node.Cache` interface, adding `HasData`
- Test: `node/markers_test.go`, `internal/cache/badger/badger_test.go`

**Interfaces:**
- Consumes: `SnapshotChunk.SourceAddress` (Task 1), `role()` (Task 2).
- Produces:
  - `getSnapshotTimestamp(hr) (ts uint64, found bool, err error)`.
  - `ReceiveSnapshot` always stores max(existing, received), including for empty deltas.
  - `allRangesSynced() bool`: every range from `hasher.GetNodeHashRanges(NodeID)` has a marker.
  - `GetNodeInfo.AllRangesSynced`.
  - **Leftover-disk guard:** while SHADOW, the first chunk's `SourceAddress` is checked against the persisted `_shadow_source`. If the key is missing and `HasData()` is true, or the key differs, the node replies `Success:false`, logs an error and increments `keydb_shadow_leftover_disk_total`. Otherwise it persists the source.
  - **Streaming receive:** while SHADOW, chunks are piped into `cache.LoadSnapshot` through `io.Pipe` instead of being buffered, holding the write lock for the duration. ACTIVE destinations keep today's buffered path.

- [ ] **Failing tests:**
  - `TestEmptyDeltaKeepsMarker`: a marker of 100, then an empty delta with timestamp 0, leaves 100.
  - `TestEmptyRangeGetsMarker`: an empty first delta makes `found=true`.
  - `TestAllRangesSynced`.
  - `TestLeftoverDiskGuard`: data present with no source key gives a refusal and the metric; the same source is accepted; a different source is refused.
  - `TestShadowReceiveStreamsWithoutBuffering`: a multi-chunk snapshot loads correctly.
- [ ] Implement.
- [ ] Tests pass, together with the existing `TestStreaming*` tests.
- [ ] Commit `feat(node): monotonic markers, synced check and leftover-disk guard`.

### Task 4: Shutdown order

**Files:**
- Modify: `cmd/node/main.go`, in the termination goroutine: `healthServer.Shutdown()`, then `server.Stop()`, then `service.Close()`, then `healthGrpcServer.GracefulStop()`.
- Modify: `node/node.go`: `Close()` cancels the service's internal context, waits for the wait group, then takes `s.mu.Lock()` before closing the cache, so that handlers still running finish first.
- Test: `cmd/node/main_test.go`

**Interfaces:**
- Produces: `Service` owns an internal `ctx, cancel` derived from the `NewService` ctx, so that `Close()` stops background jobs even if the caller's ctx is still alive.

- [ ] Failing test `TestShutdownCancelsInFlight`: a Get blocked inside the handler (test hook) gets a gRPC error once shutdown starts, and `run()` returns within the shutdown timeout.
- [ ] Implement.
- [ ] The test passes.
- [ ] Commit `fix(node): stop gRPC before closing badger on shutdown`.

### Task 5: Bounded snapshot stream

**Files:**
- Modify: `internal/cache/badger/badger.go`
- Modify: the `node.Cache` interface
- Test: `internal/cache/badger/badger_test.go`

**Interfaces:**
- Produces:

```go
type StreamOptions struct {
	NumGoroutines    int                                    // replication: 1
	CompressionLevel int                                    // replication: 1
	MaxBatchSize     uint64                                 // stream.MaxSize, replication: 4 MiB
	Wait             func(ctx context.Context, n int) error // byte budget; nil means unlimited
}

// StreamSnapshot writes the entries of one hash range with a version above since to w, without building the
// range in memory. Expired keys are skipped. Returns the max version written and whether anything was written.
func (c *Cache) StreamSnapshot(
	ctx context.Context, hashRange int64, since uint64, w io.Writer, opts StreamOptions,
) (uint64, bool, error)
```

  It reuses `createStream`, with `numGo`, a zstd writer at `CompressionLevel`, and `Wait(len(encoded))` before each write. The output stays loadable by `LoadSnapshot`.

- [ ] **Failing tests:**
  - `TestStreamSnapshotRoundTrip`: stream into a buffer, load it into a second cache, and the keys match; an expired key is absent.
  - `TestStreamSnapshotWaitCalled`: the total bytes passed to `Wait` equal the bytes written.
  - `TestStreamSnapshotSince`.
- [ ] Implement.
- [ ] Tests pass.
- [ ] Commit `feat(cache): bounded single-range snapshot stream`.

### Task 6: Replication job, readiness, metrics, GC skip

**Files:**
- Create: `node/replication.go`
- Modify: `node/node.go`: wiring in `NewService`, `garbageCollection` skipping its tick while `s.replicating.Load()`, and Get and Put recording latency in `s.latency`
- Modify: `node/config.go`: `Replication ReplicationConfig`
- Modify: `cmd/node/main.go`: config keys
- Test: `node/replication_test.go`

**Interfaces:**
- Consumes: `StreamOptions`/`StreamSnapshot` (Task 5), `shadowAddress()` and `role()` (Task 2), `AllRangesSynced` (Task 3), `budget` (Task 7).
- Produces:

```go
type ReplicationConfig struct {
	MaxBytesPerSecond       func() int64  // reloadable "replication.maxBytesPerSecond", default 32 MiB
	Paused                  func() bool   // reloadable "replication.paused", default false
	PauseBetweenPasses      time.Duration // "replication.pauseBetweenPasses", default 5s
	GapThreshold            time.Duration // "replication.gapThreshold", default 60s
	LegacyClientQuietPeriod time.Duration // "replication.legacyClientQuietPeriod", default 15m
	BrakeInterval           time.Duration // "replication.brakeInterval", default 5s
	ReconcileInterval       time.Duration // default 1s; how often the shadow list is re-read
	Signals                 BrakeSignals  // nil means runtime signals (Task 7)
}

type ReplicationStatus struct { // JSON for GET /replication/status
	Role                    string    `json:"role"`
	ShadowAddress           string    `json:"shadowAddress"`
	State                   string    `json:"state"` // "idle" | "running"
	PassesCompleted         int64     `json:"passesCompleted"`
	CurrentPassRangesDone   int       `json:"currentPassRangesDone"`
	CurrentPassRangesTotal  int       `json:"currentPassRangesTotal"`
	LastPassStartedAt       time.Time `json:"lastPassStartedAt"`
	LastPassEndedAt         time.Time `json:"lastPassEndedAt"`
	LastPassDuration        string    `json:"lastPassDuration"`
	LastPassBytes           int64     `json:"lastPassBytes"`
	LastPassRanges          int       `json:"lastPassRanges"`
	GapSeconds              float64   `json:"gapSeconds"` // -1 before the first full pass
	ShadowAllRangesSynced   bool      `json:"shadowAllRangesSynced"`
	LastLegacyClientRequest time.Time `json:"lastLegacyClientRequest"`
	BudgetBytesPerSecond    int64     `json:"budgetBytesPerSecond"`
	BrakeEngaged            bool      `json:"brakeEngaged"`
	BrakeReason             string    `json:"brakeReason"`
	Ready                   bool      `json:"ready"`
	NotReadyReasons         []string  `json:"notReadyReasons"`
	LastError               string    `json:"lastError"`
}

func (s *Service) ReplicationStatus() ReplicationStatus
```

- **Reconcile loop**, started in `NewService` with `waitGroup` and the service ctx: every `ReconcileInterval`, if ACTIVE and `shadowAddress() != ""`, ensure a job runs to that address; otherwise stop it.
- **Pass:** for each owned range in sorted order:
  1. `GetSnapshotSince` on the destination;
  2. a `ReceiveSnapshot` client stream;
  3. `StreamSnapshot` into a chunk writer that sends at most 1 MiB `SnapshotChunk{HashRange, Data, SourceAddress: AdvertisedAddress}` per message;
  4. a final chunk `{IsLast:true, Timestamp:maxVersion}`.
- **Readiness after each pass and every `BrakeInterval`:** `fullPassDone && gap < GapThreshold && shadow.GetNodeInfo().AllRangesSynced && (lastLegacy == 0 || now-lastLegacy > LegacyClientQuietPeriod)`.
- **Logs:** on readiness transitions, `"Replication ready to switch"` and `"Replication no longer ready"`, with the field `reasons` (`strings.Join`).
- **Metrics** (tag `nodeID`):
  - `keydb_replication_ready` (gauge);
  - `keydb_replication_gap_seconds` (gauge);
  - `keydb_replication_pass_duration_seconds` (timer);
  - `keydb_replication_bytes_total` (counter);
  - `keydb_replication_errors_total` (counter);
  - `keydb_replication_budget_bytes_per_second` (gauge);
  - `keydb_replication_brake_events_total` (counter, tag `reason`).

- [ ] **Failing tests**, with two in-process services and the second one SHADOW:
  - `TestReplicationStartsFromConfig`: setting the shadow list makes data written to the source appear on the shadow.
  - `TestReplicationIncremental`: the second pass sends only new keys (bytes much smaller).
  - `TestReplicationReadiness`: ready only after the full pass; old-client traffic makes it not ready.
  - `TestReplicationStopsOnClear`.
  - `TestReplicationStopsOnShutdown`: `Close()` returns within 1s mid-pass.
  - `TestGCSkippedDuringPass`.
- [ ] Implement.
- [ ] Tests pass.
- [ ] Commit `feat(node): replication to shadow nodes with readiness reporting`.

### Task 7: Byte budget and automatic brake

**Files:**
- Create: `node/brake.go`
- Test: `node/brake_test.go`

**Interfaces:**
- Produces:

```go
type BrakeSample struct {
	SchedLatencyP99 time.Duration // delta of runtime/metrics "/sched/latencies:seconds" since the last sample
	RequestLatency  time.Duration // mean Get/Put latency since the last sample
	ThrottledRatio  float64       // delta of throttled_usec in /sys/fs/cgroup/cpu.stat divided by the window
}
type BrakeSignals interface{ Sample() BrakeSample }

// budget owns a rate.Limiter in bytes/s. Wait blocks while paused (rate 0) until ctx ends or the budget resumes.
type budget struct{ /* limiter, current, max func, paused func, thresholds */ }
func (b *budget) Wait(ctx context.Context, n int) error
func (b *budget) tick(sample BrakeSample) // AIMD step, called every BrakeInterval by the job
func (b *budget) current() (bytesPerSecond int64, engaged bool, reason string)
```

- **AIMD:**
  - a breach is `SchedLatencyP99 > 10ms`, `RequestLatency > 2 × baseline` (baseline = the first sample taken before the first pass), or `ThrottledRatio > 0.1`;
  - on a breach, halve the budget, and below 1 MiB/s go to 0;
  - after 3 healthy samples in a row, add `max/8` (from 0, start at 1 MiB/s), capped at `MaxBytesPerSecond()`;
  - `Paused()` forces 0.
- **Runtime signals:** a missing cgroup file means a ratio of 0.

- [ ] **Failing tests:**
  - `TestBudgetHalvesOnBreach`;
  - `TestBudgetRecoversAfterHealthySamples`;
  - `TestBudgetPausedBlocksWait`: `Wait` blocks until unpaused;
  - `TestBudgetCappedByMax`;
  - `TestRuntimeSignalsNoCgroup`.
- [ ] Implement.
- [ ] Tests pass.
- [ ] Commit `feat(node): byte budget with automatic brake for replication`.

### Task 8: Admin HTTP endpoint

**Files:**
- Create: `node/admin.go`: `func (s *Service) AdminHandler() http.Handler`, routing `GET /replication/status` to JSON through `jsonrs`
- Modify: `cmd/node/main.go`: an `http.Server` on `adminPort` (default 8080) started with `httputil.ListenAndServe(ctx, srv)` inside `wg.Go`
- Test: `node/admin_test.go`

- [ ] Failing test `TestAdminReplicationStatus`: GET returns 200 with JSON decoding into `ReplicationStatus`, and other methods return 405.
- [ ] Implement.
- [ ] The test passes.
- [ ] Commit `feat(node): admin endpoint for replication status`.

### Task 9: Client

**Files:**
- Modify: `client/client.go`
- Test: `client/client_test.go`, plus the existing node tests that assert cluster size

**Interfaces:**
- Produces:
  - `Config.DeadNodeLookupAfter time.Duration` (default 5s).
  - **Lazy discovery:** before the first Get or Put, the client calls `GetNodeInfo` through the configured addresses, using a fresh `grpc.NewClient` per attempt, closed afterwards, round-robin across addresses, with backoff. The first non-empty `NodesAddresses` is applied.
  - **Failed responses:** `ErrorCode != NO_ERROR` with a non-empty, different list means: swap pools whose address changed, then back off and retry. A cluster size mismatch keeps today's rehash-and-restart path, but swaps by address.
  - **`NOT_READY`:** back off and rotate to the next pooled connection.
  - **Transport errors:** once node *i* has failed for `DeadNodeLookupAfter`, the client looks the list up through the seed addresses and the other pools, and adopts it if it differs.
  - **Locking:** no `c.mu` held across backoff sleeps.
  - **Header:** a unary interceptor adds `FeaturesHeader: FeatureDiscovery`.

- [ ] **Failing tests:**
  - `TestDiscoveryFromSeed`: a client given one address of a 3-node mock cluster routes keys to all 3.
  - `TestAdoptListFromFailedResponse`.
  - `TestNotReadyRotatesConnection`.
  - `TestDeadNodeLookup`.
  - `TestConflictingListsNoHotLoop`: two mocks hand out each other's lists; the request count stays bounded over 2s.
  - `TestNoLockAcrossBackoff`: a request backing off on node 0 doesn't block a request to node 1.
  - `TestFeaturesHeaderSent`.
- [ ] Implement, adjusting existing tests whose expectations change because of discovery (`TestGetPutAddressBroadcast`).
- [ ] `go test ./client/ ./node/` passes.
- [ ] Commit `feat(client): discovery, list adoption and dead-node lookup`.

### Task 10: Integration tests for moves

**Files:**
- Create: `node/move_test.go`, using the dockertest + MinIO harness from `node_test.go`, with helpers `startNode(slot)`, `setTopology(active, shadow []string)` (`conf.Set` on every node's config) and `stopNode()` (`server.Stop()`, then `service.Close()`)

**Cases (spec section 11, task 5):**
- happy path under load, in both orders;
- two moves back to back (a→b→a);
- restart mid-replication;
- idle client;
- stale lists during the refresh;
- seed through a round-robin TCP proxy;
- old clients;
- CPU bounds (fast destination, injected signals, pause and resume, bounded memory);
- leftover disk.

- [ ] Write the tests.
- [ ] Run them: `go test ./node/ -run TestMove -race -count=1 -timeout 15m`. Expect PASS.
- [ ] Commit `test(node): integration tests for node moves`.

### Task 11: README, lint, docs

- [ ] README: a "Moving a node to a new disk" runbook with full values states, readiness checks, `GET /replication/status`, and the PVC fallback. Mention the new config keys and the admin port.
- [ ] Remove customer names from the spec and the plan (public repo).
- [ ] `make lint`, then `make test`. Expect a clean lint and passing tests.
- [ ] Commit, push the branch `feature/pipe-3580-keydb-shadow-nodes-replication-and-client-discovery-for-node`, and open a draft PR.

### Task 12: Chart (rudderstack-operator)

Branch `feature/pipe-3581-rudderstack-operator-chart-support-for-keydb-node-moves`, from `origin/main`. Implement spec section 5, with these exact names:

- **Values:** `global.keydb.nodeMoves`, and `global.keydb.nodes."<i>"` with `active`, `shadow` and `slots.<a|b>.{diskSize,resources,vpaMinCPU}`.
- **Env:** `KEYDB_ADVERTISED_ADDRESS`, built with the same helper as `nodeAddresses`: `<pod>.<fullname>-headless.<ns>.svc.cluster.local:<port>`.
- **Configmap key:** `shadowNodeAddresses` (index-aligned, comma-separated).
- **Admin port:** container port `admin` 8080 (`KEYDB_ADMIN_PORT`).
- **CPU requests file:** a downward-API volume at `/etc/podinfo` with `cpu_requests` from `requests.cpu`, divisor `1m`.
- **Retention policy:** `whenDeleted: Delete` on every StatefulSet of a node with a shadow.
- **Slot `b`:** StatefulSet `keydb-<i>-b`, with label and selector `statefulset-index: "<i>-b"`.
- **Shadow placement:** preferred anti-affinity against active keydb pods.
- **rudderstack-core:** `RSERVER_KEY_DB_DEDUP_ADDRESSES` set to `<fullname>.<ns>.svc.cluster.local:<port>`, unconditionally.
- **Validation:** the `fail` rules from spec section 5.
- **Render tests:** `tests/keydb`, following `tests/ingestion`.

### Task 13: rudder-server

Branch `feature/pipe-3582-rudder-server-bump-the-keydb-client-for-discovery-through`, from `origin/master`. Run `go get github.com/rudderlabs/keydb@<keydb branch head sha>` and `go mod tidy`, then run `go test ./services/dedup/...`. Open a draft PR noting that the pseudo-version must become the release tag.
