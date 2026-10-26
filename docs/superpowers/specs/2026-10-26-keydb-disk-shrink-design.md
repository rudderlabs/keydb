# KeyDB: move a node to a new disk through a shadow node

Date: 2026-10-26 · Status: draft for review

## Why

KeyDB nodes run on EBS-backed PVCs. Neither Kubernetes nor EBS can shrink a volume, and a StatefulSet's `volumeClaimTemplates` cannot be edited in place. A smaller disk therefore always means a new volume and moving the data onto it.

The only way to do that today is to upload snapshots to S3, wipe the disk and download them again. The node is empty while it downloads, which takes hours for big customers. Anything written after the snapshot point is also lost: if the upload takes an hour, some hash range files are an hour old by the time the node is wiped, which is an hour of deduplication data gone.

We want a repeatable way to move any node onto a new disk while the old node keeps serving, then switch over once the new node is ready.

## Goals

- Move one node, or every node at once, to a new disk of a different size (usually smaller, but larger works the same way).
- Keep serving throughout. The only stall is the switch itself, and it must stay well under 10 minutes for any node's keys.
- Copying never makes the serving node slow enough for rudder-server to time out.
- Human steps are two one-line PRs in `rudder-devops`, plus watching a metric or log and checking a status endpoint. No `kubectl` writes, except a fallback `kubectl delete pvc` if Kubernetes doesn't delete the old disk.
- The same routine works for every later move.

## Non-goals

- Changing the cluster size or which node owns which hash range. The existing scaler does that.
- Protecting against PR2 being merged too early. Merging is a human step, and the human checks readiness first.
- Supporting moves while processors still run today's keydb client.
- Automating the PRs themselves.
- Changing the S3 backup flow.

## Anti-goals

These would make the work a failure even if a move technically completes:

- Copying most of the expired data onto the new disk. A few expired keys getting through is fine.
- Losing more than a very small tail of writes: seconds to minutes, never hours.
- Replication starving the serving node of CPU, memory or IOPS so that rudder-server times out.
- Configuration in rudder-server that has to change for each move.

## Constraints from the requester

1. The majority of expired data must not be copied. A few expired keys getting through is acceptable.
2. Data loss is counted from the snapshot point to the switch. Copying a snapshot and switching an hour later is an hour of loss.
3. Downtime must stay under 10 minutes. Replicate while the main node keeps serving, with extra CPU if needed, then switch once the new node is ready.
4. We control both the client and the server, so the gRPC protocol can change.
5. Manual steps are error-prone and must be kept to a minimum. Kubernetes volume surgery was explicitly rejected.
6. No staging rehearsal. Correctness is proven by integration tests on the existing container-based harness.

## What exists today

The design builds on these facts. File references are to `keydb` unless another repo is named.

- **Topology:** each node is its own StatefulSet `keydb-<i>` with one replica, pod `keydb-<i>-0` and PVC `data-keydb-<i>-0`. All nodes share one `persistence.size` (`rudderstack-operator/helm-charts/rudderstack-aux/charts/keydb/templates/statefulset.yaml`).
- **Node identity:** the node ID is parsed from the pod name with `^keydb-(\d+)-0$` (`cmd/node/main.go:46`).
- **Config:** `nodeAddresses` and `degradedNodes` are reloadable. They come from the configmap mounted at `/etc/rudderstack` without `subPath`, so the kubelet refreshes the file and the node reloads it.
- **Streaming:** `SendSnapshot(hashRange, destinationAddress)` on a source node streams one hash range to any address. The destination stores a since marker per hash range (`node/node.go:1528`), so repeated sends are incremental.
- **Empty ranges:** for a range with no new data, `SendSnapshot` sends timestamp 0 (`node/node.go:1326`). `ReceiveSnapshot` returns before storing any marker (`node/node.go:1209`).
- **Expired keys:** snapshots skip keys that have already expired when the range is streamed (`internal/cache/badger/badger.go:435`).
- **Versions:** Badger's `Load` keeps entry versions and moves the destination's clock past them (badger v4.9.6 `backup.go:265`). A `SinceTs` stream skips only tables whose max version is below the marker (badger `iterator.go:333`), so an incremental pass can still scan a lot.
- **Snapshot cost:** snapshot streams use 10 goroutines (`internal/cache/badger/badger.go:404`) and zstd at its default level. A whole range is built in memory before it is sent.
- **Locking:**
  - Get and Put hold the service read lock for their whole duration (`node/node.go:647`, `:698`), with the role and degraded checks at entry.
  - `SendSnapshot` holds the read lock while it builds and streams a whole range (`node/node.go:1253`).
  - `ReceiveSnapshot` holds the write lock for the whole receive, network transfer included (`node/node.go:1163`).
  - Garbage collection takes the write lock every 5 minutes (`node/node.go:288`). Because Go's RWMutex blocks new readers while a writer waits, Get and Put stall on every GC tick until the range currently being sent is done.
- **CPU:** GOMAXPROCS is set once at start from `MAXPROCS_REQUESTS`, as 1.5 × the CPU request, through `rudder-go-kit/maxprocs`. That package can instead watch `/etc/podinfo/cpu_requests` and follow in-place resizes. `GOMEMLIMIT` is the memory request.
- **No background snapshots:** nothing uploads snapshots periodically (`SnapshotInterval` is only logged), and nodes don't download at boot (`initCaches(ctx, false)`).
- **Client:**
  - It retries forever and reacts only to cluster *size* changes (`client/client.go:611`).
  - It picks one pooled connection per request and retries on that same connection (`client/client.go:349`, `:502`).
  - It holds its read lock for a whole Get or Put, backoff sleeps included (`client/client.go:300`, `:465`).
  - When it learns a bigger cluster, it keeps its seed connection as node 0's connection (`client/client.go:637`).
- **Processor bootstrap:** rudder-server is given one seed address, `keydb-0-0.keydb-headless…` (`rudderstack-operator/helm-charts/rudderstack-core/templates/statefulset.yaml:473`), read once at startup (`rudder-server/services/dedup/keydb/keydb.go:30`).
- **Image pins:** some customers pin the keydb image in their customer object.
- **Delivery:** customer objects reach the cluster through ArgoCD auto-sync and a Helm operator.

## Design overview

For a node being moved, PR1 adds a second StatefulSet, the **shadow**, with the new disk size and the same node identity. The serving node sees the shadow in its config and starts copying to it on its own: first a full pass, then incremental passes back to back, for as long as the shadow exists. The copy is capped so that it can't starve the serving node.

When the shadow has caught up, the serving node reports "ready" through a Prometheus metric, a Loki log line and a status endpoint. It keeps syncing after that. It reports again after any restart, once it has caught up again.

A human checks the gap and merges PR2. PR2 makes the shadow the node's active pod and removes the old StatefulSet:

- the old pod gets SIGTERM, stops everything and cancels in-flight requests, which clients retry against the new node;
- Kubernetes then deletes its disk;
- writes the old pod acknowledged after its last completed pass are lost: about one pass of writes, the accepted tail;
- the shadow starts serving as soon as its config refreshes;
- clients see the old node fail and rediscover the cluster through the `keydb` Service.

No processor restarts, and there is no third PR.

Names alternate between two **slots** per node. Slot `a` is today's naming (`keydb-<i>`, pod `keydb-<i>-0`), so existing nodes need no migration. Slot `b` is `keydb-<i>-b`, pod `keydb-<i>-b-0`. A move goes from one slot to the other, and the next move goes back. Node IDs, cluster size and hash range ownership never change.

## 1. Node roles

Two new config inputs, rendered only when the customer has `nodeMoves` on (section 5):

- **`advertisedAddress`** (env `KEYDB_ADVERTISED_ADDRESS`, one per pod): the pod's own address, rendered by the chart with the same helper that renders `nodeAddresses`.
- **`shadowNodeAddresses`** (configmap, reloadable): one entry per node index, empty where a node has no shadow, in the same comma-separated, index-aligned format as `degradedNodes`.

A node works out its role on every request, the same way `isDegraded` is evaluated today:

- **Matches `shadowNodeAddresses[NodeID]`:** SHADOW.
- **Otherwise:** ACTIVE. This includes the case where `advertisedAddress` isn't set, which keeps today's behaviour for existing deployments and local setups.

There are only these two roles. Once PR2 is applied, the old pod is deleted and the shadow becomes ACTIVE.

What each role does:

- **ACTIVE:** serves Get and Put exactly as today, including degraded-mode handling. When its index has a shadow, it runs the replication job (section 2).
- **SHADOW:** refuses Get and Put with the new code `NOT_READY`. It accepts `ReceiveSnapshot` and `GetSnapshotSince`.
- **All roles:** answer `GetNodeInfo`, which reports the role.

**Every response carries the full list.** Every node, whatever its role and whether or not the request succeeded, returns the real cluster size and the full address list from its config, as Get and Put responses do today. A shadow's list doesn't contain the shadow itself until PR2, so clients are never pointed at it before it's active.

**Since markers.** Two fixes to how the destination stores markers:

- **Never move a marker backwards:** store max(existing, received).
- **Mark empty ranges too:** write a marker even when the delta is empty, so "synced, nothing there" can be told apart from "never synced".

Without them, an empty pass resets the marker to 0 and the next pass re-sends the whole range. The serving node's readiness check also relies on the shadow holding a marker for every range.

**Leftover-disk guard.** A shadow persists a "shadowing `<source address>`" marker the first time it receives data. If a shadow starts with data on its disk but no such marker, or a marker for a different source, it refuses to receive and reports this in a log and a metric. The usual cause would be a disk Kubernetes didn't delete after an earlier move. Without the guard, the next move back onto that slot would quietly keep the old size and stale data.

**Pod names.** Accept `keydb-<i>-b-0` in `getNodeID`, alongside today's two formats.

## 2. Replication on the serving node

**Starting and stopping.** An ACTIVE node starts a replication job whenever `shadowNodeAddresses[NodeID]` is set, whether at boot or on a config reload. It stops the job when the entry is cleared, for example when PR1 is reverted. The scaler is not involved. A node that restarts or crashes starts the job again on boot. The job continues incrementally, because the shadow keeps its markers.

**The loop:**

1. A pass sends every hash range the node owns, one range at a time, through the same code path as `SendSnapshot`. Refactor that code into an internal function both can call.
2. After a pass, the job pauses briefly and starts the next one. It never stops while the shadow exists, so the shadow stays in sync after readiness is reported.
3. A failed range is retried with backoff. The job keeps running, and its status shows the last error.

**Readiness.** The node is ready to switch when all of these hold:

- at least one full pass has completed;
- the gap is below a threshold (default 60s). The gap is now minus the start of the last completed pass: the shadow holds every write the node accepted before that moment;
- the shadow reports, through `GetNodeInfo`, that it holds a marker for every range it owns;
- the node has seen no request from an old client for a while (default 15 minutes). An old client is one without the features header (section 3). Old clients send node *i*'s keys to node *i*, so the serving node sees them.

Readiness is reported three ways:

- **Metric:** a `keydb_replication_ready` gauge (0 or 1), plus `keydb_replication_gap_seconds`.
- **Log:** one structured line each time the node becomes ready ("replication ready to switch", with the gap, passes and shadow address), and one each time it stops being ready, with the reason. After a restart the job catches up and logs "ready" again.
- **Status endpoint:** `GET /replication/status` on a small HTTP admin server on the node, on a new port (default 8080). It returns:
  - role and shadow address;
  - state, passes completed, and the current pass's progress (ranges done out of total);
  - the start, end, duration, bytes and range count of the last pass;
  - the gap;
  - whether the shadow confirms every range;
  - when an old client was last seen;
  - the current byte budget and brake state (below);
  - the ready verdict with its reasons, and the last error.

  The human reaches it with their own port-forward.

**Shutdown.** On SIGTERM a node stops everything it is doing: the replication job, any snapshot stream, and in-flight Get and Put calls. In-flight calls are cancelled, not waited for. Clients retry them, and after PR2 the retries land on the new node.

This is close to today's shutdown (`cmd/node/main.go:336`), which already fails in-flight requests by closing Badger before it stops the gRPC server. The order should change: stop the gRPC server first, cancelling in-flight calls, then close Badger. The replication job must stop with everything else, so that a long pass can't hold up shutdown.

There is no fence and no final pass. Writes the old node acknowledged after its last completed pass never reach the new node. That is at most the gap, about one pass of writes, which is the accepted tail.

**Rare orderings, accepted:**

- **The old pod picks up the new config before its SIGTERM.** This is unlikely, because the operator deletes the old StatefulSet in the same apply that changes the config, while the config file takes up to a minute to refresh. If it happens, the old pod keeps serving as ACTIVE for those few seconds, and what it accepts in them is lost with the tail.
- **PR2 is only partly applied:** the config changes but `keydb-<i>` isn't deleted. The old pod keeps serving as a second ACTIVE for node *i* until it's stopped, and the writes it accepts after its last pass are lost. A partial apply shows up as a failed operator reconcile.

**Keep replication from starving the node.** A Badger stream will use every core it's given, and rudder-server sees the result as timeouts. Replication must stay a background task whatever the node's size or traffic.

1. **A hard ceiling on parallelism.** Replication runs its own stream with one iterator goroutine instead of 10, sends one range at a time, and compresses with zstd level 1 instead of the default. Stream, compression and gRPC send together can use at most about two cores, whatever the data size.
2. **A byte budget.** A token bucket in the stream's `Send` callback caps bytes per second. When the bucket is empty, `Send` blocks, and Badger's iterator stops through backpressure. The cap therefore limits CPU and EBS read throughput together, and serving reads keep their IOPS.
3. **An automatic brake.** Every few seconds the job reads three signals:
   - Go scheduler latency (`/sched/latencies:seconds` in `runtime/metrics`): how long runnable goroutines wait for a CPU;
   - the node's own Get and Put latency, compared with a baseline taken before the job started;
   - cgroup CPU throttling (`cpu.stat`).

   If any signal crosses its threshold, the job halves its byte budget, down to a full pause. After a healthy stretch it raises the budget again in steps. Status and metrics show the current budget and why the brake engaged.
4. **Manual limits.** Reloadable config keys, set through the customer object's `configOverrides`, cap the byte budget or pause the job without restarting it. The brake works below that cap.
5. **Bounded memory.** Today a range is built entirely in memory before it is sent. On a big node that pushes the heap toward `GOMEMLIMIT`, where Go's garbage collector runs almost continuously and burns CPU. Replication streams each range in bounded chunks instead. A shadow, which takes no traffic, loads straight from the stream.
6. **GC skips its tick** while a replication pass is running. Otherwise the GC's write lock queues behind a long range send and blocks every new Get and Put meanwhile (`node/node.go:288` has a TODO about the same problem). The builder may pick a different mechanism with the same effect.
7. **Extra CPU that Go can use.** The chart mounts `/etc/podinfo/cpu_requests` through the downward API when `nodeMoves` is on, so `rudder-go-kit/maxprocs` keeps GOMAXPROCS in step with in-place resizes. PR1 can then raise the serving node's CPU through its VPA minimum, without a restart where VPA can resize in place.
8. **Shadows on other machines.** A shadow's loading and compactions are heavy. Shadow pods prefer a different Kubernetes node from any active keydb pod.

Defaults for the budget, the thresholds and the steps are left to the builder. They should start conservative: a slow first copy costs nothing, because the node keeps serving.

Metrics:

- `keydb_replication_ready`
- `keydb_replication_gap_seconds`
- `keydb_replication_pass_duration_seconds`
- `keydb_replication_bytes_total`
- `keydb_replication_errors_total`
- `keydb_replication_budget_bytes_per_second`
- `keydb_replication_brake_events_total`, by reason

While a move is in progress, don't run `/hashRangeMovements` or `/backup`. They compete with replication for the cache's single-snapshot guard (`ErrSnapshotInProgress`) and would keep retrying. The README runbook says so.

## 3. Client changes

Processors must run this client, with the `keydb` Service as their URL, before a customer's first move. Today's client never rediscovers, so it would retry a removed node forever. Readiness stays false while any old client is connected.

- **Discovery.** `Config.Addresses` may be a single seed or a full list, as today. On first use, the client calls `GetNodeInfo` through the configured addresses, opening a fresh connection for each attempt so that a Service seed spreads attempts across pods. Any node's answer will do, because every node returns the full list. This also works against older keydb nodes. The client then builds one pool per node address. Data requests never go through the seed. Discovery stays lazy (the first Get or Put waits for it, with backoff) so that `NewClient` remains non-blocking as today.
- **Failed responses carry the list.** When a request fails with a response (`NOT_READY`, `WRONG_NODE` or any other error code), the client adopts that response's address list if it differs from its own, swaps the changed pools, and retries after the usual backoff. Adopting a list never skips the backoff, so a disagreement between nodes can't turn into a hot loop.
- **Dead nodes give no list.** A node that is gone, like the old pod after PR2, can't answer. After transport errors for a few seconds (default 5s), the client asks another node for the list: through the seed, or any other node it knows.
- **During the config refresh after PR2,** a node that hasn't refreshed yet still hands out the old list. A client may then be sent back to the old address, fail, and get the new list again on the next attempt. This converges once every pod has the new config, within about a minute.
- **`NOT_READY`:** back off and retry, using the next pooled connection rather than the same one.
- **Swap by address.** When the list changes, through a failed response, a lookup after a dead node, or a size change, swap only the pools whose address changed, under the client's write lock, the same way `updateClusterSize` does today. Today, growing from 1 to N keeps the seed connection as node 0's pool, which breaks once the seed is a Service.
- **No lock held while waiting.** The client must not hold its read lock across backoff sleeps. Otherwise one request backing off on node *i*, plus another waiting to swap node *i*'s pool, blocks every new request to every node. rudder-server's dedup call spans a whole batch across all nodes (`rudder-server/processor/processor.go:1992`).
- **Features header:** every request carries the gRPC metadata `keydb-client-features: discovery`. Nodes count Get and Put requests that lack it (`keydb_legacy_client_requests_total`) and record when they last saw one, which feeds readiness.

## 4. Protocol changes

- **`ErrorCode`:** add `NOT_READY`. Old clients retry unknown codes, as long as the response carries the real cluster size.
- **`GetNodeInfoResponse`:** add `role`, `all_ranges_synced` and `last_legacy_client_request_unix`.
- **Header:** the `keydb-client-features` metadata, sent by a client interceptor.

Regenerate with `make proto`. See the memory note on avoiding gofumpt noise in generated files.

## 5. Chart changes (rudderstack-operator)

Values live under `global.keydb`, so the keydb, keydb-scaler and rudderstack-core charts all see them:

```yaml
global:
  keydb:
    replicaCount: 3
    nodeMoves: false             # enables everything in this section
    nodes:                       # optional, keyed by node index; requires nodeMoves
      "0":
        active: a                # a (default) or b
        shadow: b                # optional: slot being filled
        slots:
          b:
            diskSize: 20Gi       # required for slot b; slot a defaults to persistence.size
            resources: {}        # default: statefulset.resources
            vpaMinCPU: ""        # optional minimum CPU for this slot's VPA
```

**`nodeMoves`** renders, on every keydb pod:

- `KEYDB_ADVERTISED_ADDRESS`;
- the admin port;
- the CPU requests file;
- the `shadowNodeAddresses` configmap key.

It also allows `nodes`. Turning it on changes every keydb pod template, so it restarts the keydb pods once. It must only be turned on together with a keydb image that contains this feature. It is a per-customer flag rather than something tied to an operator release, because some customers pin the image. With the flag off, the render is identical to today.

For each node *i*, the chart renders a StatefulSet for its active slot, and one for its shadow slot if set:

- **Slot `a`:** `keydb-<i>`, with names, labels and selector exactly as today, so existing StatefulSets aren't touched.
- **Slot `b`:** `keydb-<i>-b`, pod `keydb-<i>-b-0`, with label and selector `statefulset-index: "<i>-b"`. Slot `a`'s selector is `statefulset-index: "<i>"`, so it doesn't match slot `b` pods and the two StatefulSets never overlap. Slot `b` pods carry the same `keydb.selectorLabels`, so the `keydb` Service, the headless Service, the ServiceMonitor and the Cilium policy all include them. Node-to-node traffic is already allowed by `service: keydb`.
- **Retention policy:** every StatefulSet of a node that currently has a shadow gets `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`; otherwise it keeps the default `Retain`. The field is mutable, so flipping it doesn't restart pods. PR2 removing the old slot then makes Kubernetes delete its disk once the pod has exited. Reverting PR1 deletes the shadow's disk. Our tests can't check this behaviour, so it is verified on the first real move. The fallback is a manual `kubectl delete pvc`.
- **VPA:** one object per StatefulSet name, honouring the slot's `vpaMinCPU`.
- **Shadow placement:** shadow pods get a preferred pod anti-affinity against active keydb pods.

**Disk sizes never change for an existing slot.** A slot's `diskSize` must stay the same for as long as that slot exists, because `volumeClaimTemplates` are immutable. A changed size makes the API server reject the StatefulSet update, and the aux upgrade can fail after the configmap has already been applied. For the same reason, a StatefulSet's spec must not change between PR1 and PR2 except for the retention policy, so that PR2 doesn't restart the shadow it is promoting.

Other rendered pieces:

- **Configmap:** `nodeAddresses` (active slots) and `shadowNodeAddresses`.
- **keydb-scaler env:** the address list from active slots, so scale operations keep working after a move. PR2 restarts the scaler if it's enabled, which is harmless.
- **rudderstack-core:** `RSERVER_KEY_DB_DEDUP_ADDRESSES` becomes `keydb.<ns>.svc.cluster.local:<port>` for every customer, with no flag. It ships in the same operator release that moves the default rudder-server image to the version with the new client (section 6).
- **Validation (`fail`):**
  - `nodes` requires `nodeMoves`;
  - every node index is below `replicaCount`;
  - slots are `a` or `b`;
  - active and shadow are distinct;
  - slot `b`, whenever referenced, has an explicit `diskSize`.

## 6. rudder-server

Bump the keydb client and release. The Service URL (section 5) ships in the same operator release that makes this version the default rudder-server image, so the URL and the client change together, in the restart the upgrade causes anyway.

A customer pinned to an older rudder-server must upgrade before that operator release reaches it. With an old client and a Service seed, node 0's pool stays on the Service, so node 0's keys go to random pods and get retried forever. Today that means an on-prem customer pinning `rudder-server-enterprise` and the staging customers running `develop` images.

## 7. The procedure

Every snippet below is the full `nodes` state after the PR, not a diff. The README runbook must show it the same way, because dropping a slot's `diskSize` breaks the upgrade (section 5).

**PR0, once per customer** (it can ride along with normal upgrades):

- a keydb image that contains this feature;
- `nodeMoves: true`, which restarts the keydb pods once;
- the rudder-server version with the new client, which arrives with the operator release together with the Service URL.

**PR1, add the shadow:**

```yaml
global:
  keydb:
    nodes:
      "0": {active: a, shadow: b, slots: {b: {diskSize: 20Gi}}}
```

Repeat the entry for every node being moved. To give the serving nodes extra CPU for the copy, set their slot's `vpaMinCPU`. A customer without VPA raises CPU requests in PR0 instead, which restarts the node once. Replication doesn't depend on extra CPU to be safe: the brake keeps it in check either way, and extra CPU only makes the copy faster.

`keydb-0-b` starts as a shadow, and `keydb-0` starts copying to it automatically. The first pass is a full copy and takes as long as the data and the byte budget need, hours for big nodes. The node keeps serving throughout.

**Watch and check.** Wait for `keydb_replication_ready` to become 1, or for the "replication ready to switch" log line from `keydb-0`. Then check `GET /replication/status` on `keydb-0` for the gap and the verdict.

**PR2, the switch:**

```yaml
      "0": {active: b, slots: {b: {diskSize: 20Gi}}}
```

What happens, per node *i*:

1. **The operator applies the change.**
   - The configmap gets `nodeAddresses[i] = keydb-<i>-b-0…`, with the shadow entry cleared.
   - `keydb-<i>-b`'s retention policy goes back to `Retain`, in place.
   - StatefulSet `keydb-<i>` is deleted. No other keydb pod restarts.
2. **`keydb-<i>-0` gets SIGTERM.** It stops replicating, cancels in-flight requests (clients retry them) and exits. Kubernetes then deletes its disk.
3. **`keydb-<i>-b-0` picks up the new config within about a minute** and starts serving.
4. **Clients see node *i* fail and rediscover through the Service.** They adopt the new list and send node *i*'s keys to `keydb-<i>-b-0`.

**Afterwards:** confirm that `data-keydb-<i>-0` is gone. If it isn't, run `kubectl delete pvc data-keydb-<i>-0` yourself.

All nodes can move together in the same two PRs. The next move of node *i* goes the other way:

1. `{active: b, shadow: a, slots: {a: {diskSize: 15Gi}, b: {diskSize: 20Gi}}}`
2. `{active: a, slots: {a: {diskSize: 15Gi}}}`

Slot `a` needs its size stated from then on, because it no longer matches `persistence.size`.

## 8. Downtime and loss

- **Downtime:** only node *i*'s keys stall. The stall runs from the old pod's shutdown until clients reach the new pod. That is at most the config refresh on the new pod (about a minute) plus a rediscovery or two, about 1–2 minutes in all. It doesn't depend on how long a pass takes. ArgoCD and operator latency add nothing, because the old pod keeps serving until PR2 is applied.
- **Loss:**
  - Writes the old pod acknowledged after its last completed pass are lost. That is at most the gap, about one pass, since readiness requires a gap under 60s. A few duplicates can get through as a result.
  - Requests in flight at shutdown aren't lost: they're cancelled, and clients retry them against the new node.
  - The requester accepted this tail in exchange for having no fence and no final pass.
- **Expired keys:** the snapshot filter skips every key that has already expired when its range is streamed. A key that expires later is copied with its original expiry and expires on the new node on schedule.

## 9. Rollback

- **Before PR2:** revert PR1. The shadow StatefulSet and its disk are removed, and the serving node stops its job.
- **After PR2:** the old disk is gone. Rolling back means another move, back onto the other slot.

## 10. Testing, and what the tests can't show

There is no staging rehearsal. Correctness is proven by integration tests: in keydb with the existing dockertest harness, and in rudderstack-operator with Helm render tests (section 11, tasks 5 and 8). They can't reproduce:

- the kubelet's configmap refresh;
- ArgoCD and operator apply behaviour;
- the retention policy actually deleting the disk;
- production-sized data.

The numbers below come from the metrics during the first real move:

- **Pass cost and where the brake settles on a busy node.** These set how long the first copy takes and how big the read gap is. Watch them next to rudder-server's keydb latency and retry metrics.
- **Memory headroom** on big nodes during the first full pass.
- **Config refresh gap**, which shows up as the stall in the client retry metrics.

Also check:

- **Dashboards and alerts:** anything keyed on the pod name, the `instance` label or `statefulset-index` sees new values after a move. Anything keyed on the node ID doesn't.
- **Pre-existing, out of scope:** since markers are keyed by hash range, not by source. A marker taken against one node's clock could be reused when the same range later arrives from a different node, and data could be skipped. A separate fix should key markers by source.

## 11. Work breakdown and effort

Estimates are engineer-days for someone who knows this codebase, working by hand.

| # | Task                               | Repo                 | Days     |
|---|------------------------------------|----------------------|----------|
| 1 | Protocol                           | keydb                | 0.25     |
| 2 | Node roles and safety              | keydb                | 0.75     |
| 3 | Replication, CPU bounds, readiness | keydb                | 2.25     |
| 4 | Client                             | keydb                | 1.25     |
| 5 | Integration tests                  | keydb                | 2        |
| 6 | README runbook                     | keydb                | 0.25     |
| 7 | Chart                              | rudderstack-operator | 1.5      |
| 8 | Chart render tests                 | rudderstack-operator | 0.5      |
| 9 | Client bump                        | rudder-server        | 0.25     |
|   | **Total**                          |                      | **9**    |

1. **Protocol:** `NOT_READY`, the `GetNodeInfo` fields and the features header (section 4).
2. **Node roles and safety:** ACTIVE and SHADOW with request gating; marker fixes; the leftover-disk guard; a shutdown that stops replication and cancels in-flight calls; `-b-0` pod names; legacy-client tracking (sections 1 and 2). Unit tests included.
3. **Replication, CPU bounds and readiness:**
   - config-driven start and stop, and the pass loop;
   - the single-goroutine stream with level-1 compression;
   - the byte budget and manual limits;
   - the automatic brake;
   - streaming in bounded chunks;
   - GC skipping during passes;
   - the readiness computation with its metric and log lines;
   - the admin HTTP server with `GET /replication/status`;
   - refactoring `SendSnapshot` into a shared internal function.

   See section 2. Unit tests are included, with the brake's signals injectable.
4. **Client:** discovery through the seed, adopting lists from failed responses, asking another node when one is dead, `NOT_READY` with connection rotation, swapping pools by address, not holding the lock across backoff, the features header (section 3).
5. **Integration tests** with the existing harness: real nodes over real gRPC with Badger on disk and MinIO in a container via dockertest, as in `node/node_test.go` (`TestScaleUpAndDownStreaming`, `TestGetPutAddressBroadcast`). Config reloads are driven with `conf.Set`, the way the existing tests change `nodeAddresses`. Node restarts reopen Badger on the same directory. Shutdown goes through `run()` with a cancelled context, as `cmd/node/main_test.go` does. Cases:
   - **Happy path under load:** two nodes and a client writing and reading continuously. Add a shadow for node 0, wait for readiness through the metric and the status endpoint, then apply the switch: new config, and the old node shut down. Cover both orders of "new pod sees new config" and "old pod shuts down". Assert that:
     - every key acknowledged before the last completed pass exists on the new node;
     - requests in flight at shutdown are retried onto the new node;
     - clients converge without surfacing errors;
     - the stall stays bounded.
   - **Two moves back to back, under continuous load:** move node 0 from slot `a` to slot `b`, so it ends up as `keydb-0-b`, then move it back from `b` to `a`, so it ends up as `keydb-0` again, on a fresh disk. Assert, for each move, everything the happy path asserts. Also assert that:
     - keys written between the two moves survive the second move;
     - the second move starts with a full pass onto the fresh slot-`a` disk, and the markers left on `keydb-0-b` by the first move don't interfere;
     - readiness is reported for both moves;
     - clients end up on slot `a`'s address again.
   - **Restart mid-replication:** the serving node restarts, resumes incrementally, and reports readiness again once caught up.
   - **Idle client:** a client that made no requests during the move rediscovers after the old node is gone.
   - **Stale lists during the refresh:** while the shadow still has the old config, it hands clients the old list. They bounce back to the dead old address at most a few times, always with backoff, and converge once the shadow refreshes.
   - **Seed through a Service:** a client seeded through a small round-robin TCP proxy in front of all nodes, standing in for the `keydb` Service, discovers the cluster even when a shadow answers first.
   - **Old clients:** a request without the features header keeps readiness false. A shadow's response carries the real cluster size, so a header-less caller gets a retryable error rather than a hard one.
   - **CPU bounds:** the byte budget holds under a fast destination; an injected latency or throttling signal engages the brake and a healthy signal releases it; manual pause and resume through config take effect live; memory stays bounded while a large range streams.
   - **Leftover disk:** a shadow starting on a disk with foreign data refuses to receive.
   - **Unit-level checks:** empty deltas don't reset markers, and GC skips during passes.
6. **README runbook:** the procedure with full values states, the checks and the PVC fallback.
7. **Chart:** the `nodeMoves` flag, slots, the configmap shadow list, advertised-address env, admin port, CPU requests file, retention policies, VPA per StatefulSet with a per-slot minimum CPU, shadow anti-affinity, the scaler's address list, the processors' Service URL and validation (section 5).
8. **Chart render tests:** a `tests/keydb` suite in rudderstack-operator following `tests/ingestion` (cases rendered with `helm template` and asserted with `yq`). Cover:
   - the flag off rendering exactly as today;
   - PR1 and PR2 states: StatefulSet names, selectors, retention policies, configmap lists and scaler env;
   - a move back to slot `a`;
   - every validation failure;
   - the processors' Service URL.
9. **Client bump:** bump the keydb client in rudder-server and release. The existing dedup tests, which start keydb in-process, cover the bump.

That is about 9 engineer-days of effort, around 6.75 of them in keydb. My guess is that with AI assistance it shrinks to about 4 days of wall-clock time including review.

Calendar time that is not effort:

- the rudder-server release reaching each customer;
- the keydb image release;
- the first full copy, hours for big nodes, during which nobody needs to watch.

## 12. Decisions

**Decided by the requester:**

- **Approach:** a shadow node filled while the old node serves. Rejected alternatives: swapping disks behind the PVC name (too many manual Kubernetes steps), a second disk inside the same pod, the S3 route (the node sits empty while downloading), and a three-PR, scaler-driven flow with a retired phase (more human steps).
- **Roles:** only ACTIVE and SHADOW.
- **Shutdown:** on SIGTERM the old node stops everything and cancels in-flight requests, which clients retry against the new node. There is no fence and no final pass. The writes acknowledged after the last completed pass are the accepted tail.
- **Two PRs:** PR1 adds the shadow. PR2 promotes it and removes the old node, its disk included. There's no protection against an early PR2, because the human checks readiness first.
- **Automatic start:** the serving node starts copying by itself. It reports readiness through a metric and a log line, again after any restart, and keeps syncing afterwards.
- **Gap check:** the gap is checked through `GET /replication/status` on the serving node.
- **Serving after the switch:** the new node serves as soon as its config refreshes, accepting a short read gap.
- **Processors:** they run the new client and find the cluster through the `keydb` Service. The URL is the chart default for everyone, with no flag. No processor restart and no PR3 per move.
- **Disk deletion:** the old disk is deleted by Kubernetes in PR2 if that is simple. It is chart-only, through the retention policy. Otherwise `kubectl delete pvc` is run by hand.
- **Naming:** after a move node 0 lives under `keydb-0-b`, and names alternate between the two slots.
- **Expired keys:** most expired data must stay behind; a few expired keys may get through.
- **CPU:** replication must never starve the serving node.
- **Testing:** no staging, only integration tests on the existing harness plus chart render tests. The integration tests include two moves back to back: node 0 goes from `keydb-0` to `keydb-0-b`, then back to `keydb-0`.

**Calls made while writing this design**, which the builder may revisit with a reason:

- **Roles come from the shadow list plus the pod's advertised address.** `NOT_READY` carries the real cluster size and the full list.
- **Readiness includes "no old client seen recently",** so the prerequisite on the processors is checked automatically.
- **Leftover-disk guard on shadows**, because disk deletion can't be tested.
- **CPU bounds:**
  - a one-goroutine stream with level-1 compression;
  - a byte budget with an automatic brake on scheduler latency, request latency and cgroup throttling;
  - bounded-chunk streaming;
  - GC skipping during passes;
  - GOMAXPROCS following in-place resizes;
  - shadows on other machines;
  - manual limits through reloadable config.
- **Lists:** clients adopt the list from failed responses, always with backoff. They ask another node after 5s of transport errors.
- **Opt-in:** everything is gated by a per-customer `nodeMoves` flag rather than tied to an operator release, because of image pins.
- **Slot `b`** uses `statefulset-index: "<i>-b"`, and its `diskSize` is required.
- **Discovery is lazy.**

**Left to the builder:**

- names of config keys, fields and metrics;
- the exact list format;
- the admin port number;
- default thresholds: gap 60s, old-client quiet period 15 minutes, dead-node lookup after 5s, brake thresholds and steps, the initial byte budget;
- the mechanism for keeping GC from stalling requests;
- how the streaming code is refactored;
- how the chart helpers are structured;
- how the integration tests are laid out.
