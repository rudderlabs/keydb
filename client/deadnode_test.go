package client

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
)

// TestDeadNodeBehindResolver moves node 1 the way Kubernetes deletes a pod: its name first keeps resolving to an
// address that no longer answers, or stops resolving at once.
func TestDeadNodeBehindResolver(t *testing.T) {
	cases := []struct {
		name string
		// kill makes the old node's name unusable
		kill   func(t *testing.T, name string)
		adjust func(*Config)
		// within is how long the request may take once the old node is gone
		within time.Duration
	}{
		{
			name: "blackholed",
			kill: func(t *testing.T, name string) { testResolvers.set(name, startSilentListener(t)) },
			adjust: func(conf *Config) {
				conf.GrpcConfig.MinConnectTimeout = 2 * time.Second
				conf.GrpcConfig.BackoffBaseDelay = 100 * time.Millisecond
			},
			// the first attempt blocks for MinConnectTimeout: its failure must be enough to discover the cluster again
			within: 3 * time.Second,
		},
		{
			name:   "nxdomain",
			kill:   func(_ *testing.T, name string) { testResolvers.set(name) },
			within: 3 * time.Second,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a0, old1, new1 := freeMockAddress(t), freeMockAddress(t), freeMockAddress(t)
			name1 := testResolvers.name(old1)
			node0 := newMockNode(t, a0)
			node0.setList(a0, name1)
			oldNode1 := newMockNode(t, old1)
			oldNode1.setList(a0, name1)
			newNode1 := newMockNode(t, new1)
			newNode1.setList(a0, new1)

			c := newTestClient(t, tc.adjust, a0)
			keys := keysFor(1, 2, 2)
			_, err := c.Get(context.Background(), keys) // the connections to the old node are in use
			require.NoError(t, err)

			oldNode1.stop()
			tc.kill(t, name1)
			node0.setList(a0, new1)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := time.Now()
			exists, err := c.Get(ctx, keys)
			require.NoError(t, err)
			require.Equal(t, []bool{true, true}, exists)
			require.Less(t, time.Since(start), tc.within, "the first failure must make the client discover again")
			require.Positive(t, newNode1.gets.Load())
		})
	}
}

// testResolverScheme names the addresses that testResolvers resolves
const testResolverScheme = "keydbtest"

// testResolverBuilder resolves names to addresses the test sets, the way the dns resolver turns a pod's name into
// its IP. Setting no address reproduces NXDOMAIN, which the dns resolver reports as an update with no address.
type testResolverBuilder struct {
	mu        sync.Mutex
	addresses map[string][]string
	resolvers map[string]map[*testResolver]struct{}
}

var (
	testResolvers = &testResolverBuilder{
		addresses: map[string][]string{},
		resolvers: map[string]map[*testResolver]struct{}{},
	}
	testResolverNames atomic.Int64
)

func init() { resolver.Register(testResolvers) }

func (b *testResolverBuilder) Scheme() string { return testResolverScheme }

func (b *testResolverBuilder) Build(
	target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions,
) (resolver.Resolver, error) {
	r := &testResolver{builder: b, name: target.Endpoint(), cc: cc}
	b.mu.Lock()
	if b.resolvers[r.name] == nil {
		b.resolvers[r.name] = map[*testResolver]struct{}{}
	}
	b.resolvers[r.name][r] = struct{}{}
	addresses := b.addresses[r.name]
	b.mu.Unlock()
	r.push(addresses)
	return r, nil
}

// name returns a new name that resolves to the given addresses
func (b *testResolverBuilder) name(addresses ...string) string {
	target := testResolverScheme + ":///node-" + strconv.FormatInt(testResolverNames.Add(1), 10)
	b.set(target, addresses...)
	return target
}

// set changes what a name resolves to, for every connection already using it too
func (b *testResolverBuilder) set(target string, addresses ...string) {
	name := target[len(testResolverScheme+":///"):]
	b.mu.Lock()
	b.addresses[name] = addresses
	resolvers := make([]*testResolver, 0, len(b.resolvers[name]))
	for r := range b.resolvers[name] {
		resolvers = append(resolvers, r)
	}
	b.mu.Unlock()
	for _, r := range resolvers {
		r.push(addresses)
	}
}

type testResolver struct {
	builder *testResolverBuilder
	name    string
	cc      resolver.ClientConn
}

func (r *testResolver) push(addresses []string) {
	var state resolver.State
	for _, a := range addresses {
		state.Addresses = append(state.Addresses, resolver.Address{Addr: a})
		state.Endpoints = append(state.Endpoints, resolver.Endpoint{Addresses: []resolver.Address{{Addr: a}}})
	}
	_ = r.cc.UpdateState(state)
}

func (r *testResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.builder.mu.Lock()
	addresses := r.builder.addresses[r.name]
	r.builder.mu.Unlock()
	go r.push(addresses)
}

func (r *testResolver) Close() {
	r.builder.mu.Lock()
	delete(r.builder.resolvers[r.name], r)
	r.builder.mu.Unlock()
}

// startSilentListener accepts connections and never answers them. gRPC waits for the server's HTTP/2 preface until
// its connect timeout, so to a client it behaves like an address that drops every packet.
func startSilentListener(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var (
		mu    sync.Mutex
		conns []net.Conn
	)
	t.Cleanup(func() {
		_ = lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	return lis.Addr().String()
}
