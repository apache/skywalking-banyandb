// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package grpc

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// fakeKeyState backs the GetKeyRevisions fake response for a single key.
// Keys absent from the keyState map default to Present=false (the natural
// "node hasn't seen this key" semantic).
type fakeKeyState struct {
	rev     int64
	present bool
}

// fakeNodeStatusClient is a minimal clusterv1.NodeSchemaStatusServiceClient.
// GetMaxRevision and GetKeyRevisions are implemented; GetAbsentKeys remains
// a panic stub until Phase 2.4 (FD-1/FD-2). Per-call counters and delays let
// tests verify chunking + shared-deadline regression behavior.
type fakeNodeStatusClient struct {
	err          error
	callsRef     *int32
	keyState     map[string]fakeKeyState
	keyRevsCalls *int32
	revs         []int64
	maxRev       int64
	delay        time.Duration
	keyRevsDelay time.Duration
}

func (f *fakeNodeStatusClient) GetMaxRevision(ctx context.Context, _ *clusterv1.GetMaxRevisionRequest, _ ...grpc.CallOption) (*clusterv1.GetMaxRevisionResponse, error) {
	n := int32(0)
	if f.callsRef != nil {
		n = atomic.AddInt32(f.callsRef, 1) - 1
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	if len(f.revs) > 0 {
		idx := int(n)
		if idx >= len(f.revs) {
			idx = len(f.revs) - 1
		}
		return &clusterv1.GetMaxRevisionResponse{MaxModRevision: f.revs[idx]}, nil
	}
	return &clusterv1.GetMaxRevisionResponse{MaxModRevision: f.maxRev}, nil
}

func (f *fakeNodeStatusClient) GetKeyRevisions(
	ctx context.Context, req *clusterv1.GetKeyRevisionsRequest, _ ...grpc.CallOption,
) (*clusterv1.GetKeyRevisionsResponse, error) {
	if f.keyRevsCalls != nil {
		atomic.AddInt32(f.keyRevsCalls, 1)
	}
	if f.keyRevsDelay > 0 {
		select {
		case <-time.After(f.keyRevsDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.err != nil {
		return nil, f.err
	}
	keys := req.GetKeys()
	revs := make([]*clusterv1.KeyRevision, len(keys))
	for i, k := range keys {
		mapKey := k.GetKind() + "|" + k.GetGroup() + "|" + k.GetName()
		if state, ok := f.keyState[mapKey]; ok {
			revs[i] = &clusterv1.KeyRevision{Key: k, Present: state.present, ModRevision: state.rev}
			continue
		}
		revs[i] = &clusterv1.KeyRevision{Key: k}
	}
	return &clusterv1.GetKeyRevisionsResponse{Revisions: revs}, nil
}

// keyStateKey produces the deterministic map key used by fakeNodeStatusClient
// to look up per-(kind,group,name) state. Tests currently exercise only the
// "stream" kind; extend the signature when other kinds need direct state
// injection.
//
//nolint:unparam // kind/group/name kept symmetric with SchemaKey for clarity.
func keyStateKey(kind, group, name string) string {
	return kind + "|" + group + "|" + name
}

func (*fakeNodeStatusClient) GetAbsentKeys(_ context.Context, _ *clusterv1.GetAbsentKeysRequest, _ ...grpc.CallOption) (*clusterv1.GetAbsentKeysResponse, error) {
	panic("GetAbsentKeys: unused in Phase 2.2 tests")
}

// fakeQueueClient embeds queue.Client (a nil interface) so unused methods
// panic at runtime; only the two methods the barrier fan-out actually calls
// are overridden. routeTableFn (when set) overrides routeTable per call so
// frozen-snapshot tests can mutate cluster membership between iterations.
type fakeQueueClient struct {
	queue.Client
	routeTable    *databasev1.RouteTable
	routeTableFn  func() *databasev1.RouteTable
	statusClients map[string]clusterv1.NodeSchemaStatusServiceClient
}

func (f *fakeQueueClient) GetRouteTable() *databasev1.RouteTable {
	if f.routeTableFn != nil {
		return f.routeTableFn()
	}
	if f.routeTable == nil {
		return &databasev1.RouteTable{}
	}
	return f.routeTable
}

func (f *fakeQueueClient) NewNodeSchemaStatusClient(node string) (clusterv1.NodeSchemaStatusServiceClient, error) {
	c, ok := f.statusClients[node]
	if !ok {
		return nil, errors.New("no fake client for node " + node)
	}
	return c, nil
}

func newFakeTier(active []string, clients map[string]clusterv1.NodeSchemaStatusServiceClient) *fakeQueueClient {
	return &fakeQueueClient{
		routeTable:    &databasev1.RouteTable{Active: active},
		statusClients: clients,
	}
}

// clusterFixture wires a barrier service for the cluster fan-out tests.
type clusterFixture struct {
	cache        barrierCacheReader
	tier1, tier2 *fakeQueueClient
	self         string
}

func (f *clusterFixture) build() *barrierService {
	return newBarrierServiceCluster(
		func() barrierCacheReader { return f.cache },
		func() queue.Client { return f.tier1 },
		func() queue.Client { return f.tier2 },
		func() string { return f.self },
	)
}

func laggardNames(laggards []*schemav1.NodeLaggard) []string {
	out := make([]string, len(laggards))
	for i, l := range laggards {
		out[i] = l.GetNode()
	}
	sort.Strings(out)
	return out
}

// TestFanOut_AllNodesReady_ReturnsApplied verifies that when every member
// (self + peer liaisons + data nodes) reports max_rev >= min_rev, the call
// returns Applied=true on the first iteration.
func TestFanOut_AllNodesReady_ReturnsApplied(t *testing.T) {
	const target = int64(100)
	cache := &staticBarrierCache{maxModRevision: 200}
	tier1 := newFakeTier([]string{"liaison-1"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"liaison-1": &fakeNodeStatusClient{maxRev: 150},
	})
	tier2 := newFakeTier([]string{"data-1", "data-2"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"data-1": &fakeNodeStatusClient{maxRev: 110},
		"data-2": &fakeNodeStatusClient{maxRev: target},
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: tier2, self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: target,
		Timeout:     durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied())
	assert.Empty(t, resp.GetLaggards())
}

// TestFanOut_OneLaggard_ReportsOnTimeout verifies that when one peer remains
// behind the target on every iteration, the timeout response names exactly
// that laggard with its addressable role-prefixed identifier.
func TestFanOut_OneLaggard_ReportsOnTimeout(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	tier1 := newFakeTier(nil, nil)
	tier2 := newFakeTier([]string{"data-1", "data-slow"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"data-1":    &fakeNodeStatusClient{maxRev: 100},
		"data-slow": &fakeNodeStatusClient{maxRev: 42}, // never catches up
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: tier2, self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(80 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	require.Len(t, resp.GetLaggards(), 1)
	assert.Equal(t, "data-data-slow", resp.GetLaggards()[0].GetNode())
	assert.Equal(t, int64(42), resp.GetLaggards()[0].GetCurrentModRevision())
}

// TestFanOut_AllLaggards_ListsAll verifies that when every probed peer is
// behind the target, all of them appear in the laggards list with their
// role-prefixed names.
func TestFanOut_AllLaggards_ListsAll(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 0} // self also behind
	tier1 := newFakeTier([]string{"peer-l"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"peer-l": &fakeNodeStatusClient{maxRev: 5},
	})
	tier2 := newFakeTier([]string{"peer-d"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"peer-d": &fakeNodeStatusClient{maxRev: 7},
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: tier2, self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	assert.Equal(t, []string{"data-peer-d", "liaison-peer-l", "liaison-self-liaison"},
		laggardNames(resp.GetLaggards()))
}

// TestFanOut_NodeRPCErrors_CountedAsLaggard verifies that a non-Unimplemented
// gRPC error from a per-member probe makes that member a laggard for the
// iteration but does not fail the whole call. With a generous catch-up
// scenario the loop converges once the error clears.
func TestFanOut_NodeRPCErrors_CountedAsLaggard(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	errClient := &fakeNodeStatusClient{err: status.Errorf(codes.Internal, "boom")}
	tier1 := newFakeTier(nil, nil)
	tier2 := newFakeTier([]string{"sick"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"sick": errClient,
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: tier2, self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	require.Len(t, resp.GetLaggards(), 1)
	assert.Equal(t, "data-sick", resp.GetLaggards()[0].GetNode())
}

// TestFanOut_EmptyActiveSet_ReturnsUnavailable verifies that when the snapshot
// finds zero members (no self, no peers, no data nodes) the call fails fast
// with codes.Unavailable rather than parking on an empty watched set.
func TestFanOut_EmptyActiveSet_ReturnsUnavailable(t *testing.T) {
	svc := (&clusterFixture{
		cache: &staticBarrierCache{},
		tier1: newFakeTier(nil, nil),
		tier2: newFakeTier(nil, nil),
		self:  "", // empty self → no member contributed
	}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 1,
		Timeout:     durationpb.New(20 * time.Millisecond),
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

// TestFanOut_ProbeIsShortUnary_NoServerWait verifies that a slow per-member
// probe (the fake delays past the call deadline) is treated as a transient
// laggard rather than as a successful long-poll. The whole call returns
// applied=false within the timeout instead of blocking on the slow probe.
func TestFanOut_ProbeIsShortUnary_NoServerWait(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	slow := &fakeNodeStatusClient{delay: 200 * time.Millisecond, maxRev: 100}
	tier2 := newFakeTier([]string{"slow"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"slow": slow,
	})
	svc := (&clusterFixture{cache: cache, tier1: newFakeTier(nil, nil), tier2: tier2, self: "self-liaison"}).build()

	start := time.Now()
	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(60 * time.Millisecond),
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	assert.Less(t, elapsed, 500*time.Millisecond,
		"call must respect its own deadline rather than waiting for the slow per-probe response")
	require.Len(t, resp.GetLaggards(), 1)
	assert.Equal(t, "data-slow", resp.GetLaggards()[0].GetNode())
}

// TestFanOut_BackoffBounded verifies that when convergence requires multiple
// iterations the caller observes >= 2 probes per member but the per-iteration
// sleep stays bounded so the overall call respects its timeout. The fake
// peer counts how many times GetMaxRevision was called; the loop should make
// at least 2 probes inside an 80ms budget.
func TestFanOut_BackoffBounded(t *testing.T) {
	var calls int32
	cache := &staticBarrierCache{maxModRevision: 100}
	peer := &fakeNodeStatusClient{maxRev: 50, callsRef: &calls} // permanently behind
	tier2 := newFakeTier([]string{"laggy"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"laggy": peer,
	})
	svc := (&clusterFixture{cache: cache, tier1: newFakeTier(nil, nil), tier2: tier2, self: "self-liaison"}).build()

	start := time.Now()
	_, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(80 * time.Millisecond),
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2),
		"backoff loop must make multiple probes within the timeout window")
	// 80ms budget plus init/grow/cap sleeps; even the worst-case wall-clock
	// stays well under 500ms = barrierMaxInterval.
	assert.Less(t, elapsed, 500*time.Millisecond,
		"timeout must not be exceeded by the backoff schedule")
}

// TestFanOut_NodeReturnsUnimplemented_TreatedAsReady locks the cross-version
// policy: a peer (or data node) that returns codes.Unimplemented from
// NodeSchemaStatusService — i.e. a Phase-1 v0.11/v0.12 node — is treated as
// ready (assume max_revision = ∞) so partial-upgrade clusters do not deadlock
// barrier callers.
func TestFanOut_NodeReturnsUnimplemented_TreatedAsReady(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	legacy := &fakeNodeStatusClient{err: status.Error(codes.Unimplemented, "phase 1 node")}
	tier2 := newFakeTier([]string{"phase1"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"phase1": legacy,
	})
	svc := (&clusterFixture{cache: cache, tier1: newFakeTier(nil, nil), tier2: tier2, self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied(),
		"Unimplemented from a Phase-1 peer must not block a v0.13 barrier caller")
	assert.Empty(t, resp.GetLaggards())
}

// mutatingRouteTable returns a closure that produces `first` for the first
// two GetRouteTable calls and `rest` for every call thereafter. The two-call
// threshold lines up with the production sequence: snapshotMembers consumes
// call 1 (initial freeze), iteration 1's currentMembership consumes call 2,
// and iteration 2 onwards observes the mutated state. This means the first
// probe round runs against `first` (so per-member revs are recorded into
// lastRev) before the transition fires — matching the production assumption
// that eviction follows at least one probe of the member.
func mutatingRouteTable(first, rest *databasev1.RouteTable) func() *databasev1.RouteTable {
	var calls atomic.Int32
	return func() *databasev1.RouteTable {
		if calls.Add(1) <= 2 {
			return first
		}
		return rest
	}
}

// TestFanOut_NodeEvictedMidWait_DropsAndAnnotates verifies that when a member
// transitions Active → Evictable mid-call, the barrier drops it from
// subsequent probes, records exactly one laggard with reason
// "evicted_during_poll" carrying the last-observed mod_revision, and
// converges based on the remaining members.
func TestFanOut_NodeEvictedMidWait_DropsAndAnnotates(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	tier1 := &fakeQueueClient{
		routeTableFn: mutatingRouteTable(
			&databasev1.RouteTable{Active: []string{"peer-A", "peer-B"}},
			&databasev1.RouteTable{Active: []string{"peer-B"}, Evictable: []string{"peer-A"}},
		),
		statusClients: map[string]clusterv1.NodeSchemaStatusServiceClient{
			"peer-A": &fakeNodeStatusClient{maxRev: 50},  // permanently behind
			"peer-B": &fakeNodeStatusClient{maxRev: 100}, // already caught up
		},
	}
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(200 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied(),
		"barrier should converge once peer-A is evicted (peer-B + self both at 100)")
	require.Len(t, resp.GetLaggards(), 1, "exactly one evicted laggard expected")
	assert.Equal(t, "liaison-peer-A", resp.GetLaggards()[0].GetNode())
	assert.Equal(t, "evicted_during_poll", resp.GetLaggards()[0].GetReason())
	assert.Equal(t, int64(50), resp.GetLaggards()[0].GetCurrentModRevision(),
		"laggard should carry the last-observed revision before eviction")
}

// TestFanOut_NodeSetChangesMidWait_SkipsDepartedNodes verifies that when a
// member's name disappears from BOTH active and evictable mid-call (graceful
// leave), the barrier drops it silently — no laggard entry — and converges
// on the remaining members.
func TestFanOut_NodeSetChangesMidWait_SkipsDepartedNodes(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	tier1 := &fakeQueueClient{
		routeTableFn: mutatingRouteTable(
			&databasev1.RouteTable{Active: []string{"peer-A", "peer-B"}},
			&databasev1.RouteTable{Active: []string{"peer-B"}}, // peer-A simply gone
		),
		statusClients: map[string]clusterv1.NodeSchemaStatusServiceClient{
			"peer-A": &fakeNodeStatusClient{maxRev: 50}, // would block if probed
			"peer-B": &fakeNodeStatusClient{maxRev: 100},
		},
	}
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(200 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied(),
		"barrier should converge once peer-A leaves (peer-B + self both at 100)")
	assert.Empty(t, resp.GetLaggards(),
		"a graceful leave (Active → Removed) should not produce a laggard entry")
}

// TestFanOut_LateJoiner_Excluded verifies that nodes entering Active after
// the call's initial snapshot are NOT added to the watched set. A late
// joiner with max_revision=0 must not cause spurious timeouts; barrier
// returns Applied=true once the original watched set is ready.
//
// peer-A starts behind (rev=50) and catches up to 100 on its second probe;
// peer-B is the late joiner — deliberately omitted from statusClients so
// any erroneous probe attempt would surface as a laggard rather than a
// silent pass.
func TestFanOut_LateJoiner_Excluded(t *testing.T) {
	cache := &staticBarrierCache{maxModRevision: 100}
	var peerACalls int32
	tier1 := &fakeQueueClient{
		routeTableFn: mutatingRouteTable(
			&databasev1.RouteTable{Active: []string{"peer-A"}},
			&databasev1.RouteTable{Active: []string{"peer-A", "peer-B"}}, // peer-B joins
		),
		statusClients: map[string]clusterv1.NodeSchemaStatusServiceClient{
			"peer-A": &fakeNodeStatusClient{revs: []int64{50, 100}, callsRef: &peerACalls},
			// peer-B intentionally absent — any probe would error.
		},
	}
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitRevisionApplied(context.Background(), &schemav1.AwaitRevisionAppliedRequest{
		MinRevision: 100,
		Timeout:     durationpb.New(200 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied(),
		"frozen snapshot should converge based on peer-A only; late-joiner peer-B is ignored")
	assert.Empty(t, resp.GetLaggards(),
		"laggards list must not contain the late joiner peer-B")
}

// presentAt builds a fakeKeyState shorthand for "key is present on this
// member at the given revision."
//
//nolint:unparam // tests currently target rev=100 only; the parameter is kept for clarity.
func presentAt(rev int64) fakeKeyState { return fakeKeyState{rev: rev, present: true} }

// streamKey is a SchemaKey constructor for the cluster fan-out tests.
//
//nolint:unparam // tests currently use a single group "g"; group remains in the signature for readability.
func streamKey(group, name string) *schemav1.SchemaKey {
	return &schemav1.SchemaKey{Kind: "stream", Group: group, Name: name}
}

// laggardByNode returns the NodeLaggard whose Node field matches `name`, or
// nil if not found. Used to assert per-node laggard payloads without
// depending on slice ordering.
func laggardByNode(laggards []*schemav1.NodeLaggard, name string) *schemav1.NodeLaggard {
	for _, l := range laggards {
		if l.GetNode() == name {
			return l
		}
	}
	return nil
}

// TestAwaitSchemaApplied_FanOut_PerKeyLaggards verifies that the timeout
// response carries per-node missing_keys with role-prefixed identifiers, so
// the caller can see exactly which keys are outstanding on which member.
func TestAwaitSchemaApplied_FanOut_PerKeyLaggards(t *testing.T) {
	keys := []*schemav1.SchemaKey{
		streamKey("g", "k0"),
		streamKey("g", "k1"),
		streamKey("g", "k2"),
		streamKey("g", "k3"),
		streamKey("g", "k4"),
	}
	cache := &staticBarrierCache{
		maxModRevision: 100,
		keys: map[string]int64{
			"stream_g/k0": 100, "stream_g/k1": 100,
			"stream_g/k2": 100, "stream_g/k3": 100, "stream_g/k4": 100,
		},
	}
	peerA := &fakeNodeStatusClient{keyState: map[string]fakeKeyState{
		keyStateKey("stream", "g", "k0"): presentAt(100),
		keyStateKey("stream", "g", "k1"): presentAt(100),
	}} // missing: k2, k3, k4
	peerB := &fakeNodeStatusClient{keyState: map[string]fakeKeyState{
		keyStateKey("stream", "g", "k0"): presentAt(100),
		keyStateKey("stream", "g", "k1"): presentAt(100),
		keyStateKey("stream", "g", "k2"): presentAt(100),
		keyStateKey("stream", "g", "k3"): presentAt(100),
	}} // missing: k4
	tier1 := newFakeTier([]string{"peer-A", "peer-B"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"peer-A": peerA,
		"peer-B": peerB,
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    keys,
		Timeout: durationpb.New(60 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	require.Len(t, resp.GetLaggards(), 2)

	la := laggardByNode(resp.GetLaggards(), "liaison-peer-A")
	require.NotNil(t, la, "laggard for peer-A must be present")
	assert.Equal(t, []string{"k2", "k3", "k4"}, schemaKeyNames(la.GetMissingKeys()))

	lb := laggardByNode(resp.GetLaggards(), "liaison-peer-B")
	require.NotNil(t, lb, "laggard for peer-B must be present")
	assert.Equal(t, []string{"k4"}, schemaKeyNames(lb.GetMissingKeys()))
}

// schemaKeyNames extracts the Name field from each SchemaKey for assertion
// readability.
func schemaKeyNames(keys []*schemav1.SchemaKey) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = k.GetName()
	}
	sort.Strings(out)
	return out
}

// TestAwaitSchemaApplied_FanOut_ChunkedKeys verifies that a request whose
// key count exceeds barrierKeyChunkSize (1000) triggers multiple per-peer
// RPCs, with the per-chunk responses correctly aggregated into a single
// missing_keys slice for that peer.
func TestAwaitSchemaApplied_FanOut_ChunkedKeys(t *testing.T) {
	const total = 1500
	keys := make([]*schemav1.SchemaKey, total)
	cacheKeys := make(map[string]int64, total)
	for i := range total {
		name := "k" + strconv.Itoa(i)
		keys[i] = streamKey("g", name)
		cacheKeys["stream_g/"+name] = 100 // self has them all
	}
	cache := &staticBarrierCache{maxModRevision: 100, keys: cacheKeys}

	var peerCalls int32
	peer := &fakeNodeStatusClient{
		keyState:     map[string]fakeKeyState{}, // peer has nothing → every key missing
		keyRevsCalls: &peerCalls,
	}
	tier1 := newFakeTier([]string{"peer"}, map[string]clusterv1.NodeSchemaStatusServiceClient{"peer": peer})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    keys,
		Timeout: durationpb.New(40 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())

	// Expect 2 chunks (1000 + 500) per iteration. The loop may run multiple
	// iterations within the timeout; assert at least one full pass happened.
	assert.GreaterOrEqual(t, atomic.LoadInt32(&peerCalls), int32(2),
		"a 1500-key request must produce >= 2 GetKeyRevisions calls per peer (chunked at 1000)")
	assert.Equal(t, int32(0), atomic.LoadInt32(&peerCalls)%2,
		"chunks per iteration is 2 (1000 + 500); call count should be a multiple of 2")

	// All 1500 keys missing from peer; aggregated across both chunks.
	la := laggardByNode(resp.GetLaggards(), "liaison-peer")
	require.NotNil(t, la)
	assert.Len(t, la.GetMissingKeys(), total,
		"per-peer missing_keys aggregates both chunks")
}

// TestAwaitSchemaApplied_FanOut_SharedDeadline regresses the plan §2.3
// contract that each per-node, per-chunk RPC inherits time.Until(deadline)
// rather than req.Timeout / N. With per-chunk delays larger than the call's
// total budget, the call must complete around req.Timeout — NOT N × delay.
func TestAwaitSchemaApplied_FanOut_SharedDeadline(t *testing.T) {
	const total = 1500
	keys := make([]*schemav1.SchemaKey, total)
	for i := range total {
		keys[i] = streamKey("g", "k"+strconv.Itoa(i))
	}
	cache := &staticBarrierCache{maxModRevision: 100} // no keys → self also missing

	peer := &fakeNodeStatusClient{
		keyState:     map[string]fakeKeyState{},
		keyRevsDelay: 100 * time.Millisecond, // each chunk is slow
	}
	tier1 := newFakeTier([]string{"peer"}, map[string]clusterv1.NodeSchemaStatusServiceClient{"peer": peer})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	start := time.Now()
	resp, err := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    keys,
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.False(t, resp.GetApplied())
	// Without shared deadline the call would take 2 × 100ms = 200ms.
	// With shared deadline the entire call respects req.Timeout (50ms)
	// regardless of how many chunks would otherwise be issued.
	assert.Less(t, elapsed, 180*time.Millisecond,
		"shared deadline must bound total wall-clock at req.Timeout, not N × per-chunk delay")
}

// TestAwaitSchemaApplied_FanOut_PeerUnimplemented_TreatedAsReady locks the
// cross-version policy: a peer (or data node) that returns
// codes.Unimplemented from GetKeyRevisions — i.e. a Phase-1 v0.11 / v0.12
// node — is treated as ready (assume every key applied) so partial-upgrade
// clusters do not deadlock barrier callers.
func TestAwaitSchemaApplied_FanOut_PeerUnimplemented_TreatedAsReady(t *testing.T) {
	keys := []*schemav1.SchemaKey{streamKey("g", "k0"), streamKey("g", "k1")}
	cache := &staticBarrierCache{maxModRevision: 100, keys: map[string]int64{
		"stream_g/k0": 100, "stream_g/k1": 100,
	}}
	legacy := &fakeNodeStatusClient{err: status.Error(codes.Unimplemented, "phase 1 node")}
	tier1 := newFakeTier([]string{"phase1"}, map[string]clusterv1.NodeSchemaStatusServiceClient{
		"phase1": legacy,
	})
	svc := (&clusterFixture{cache: cache, tier1: tier1, tier2: newFakeTier(nil, nil), self: "self-liaison"}).build()

	resp, err := svc.AwaitSchemaApplied(context.Background(), &schemav1.AwaitSchemaAppliedRequest{
		Keys:    keys,
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	require.NoError(t, err)
	assert.True(t, resp.GetApplied(),
		"Unimplemented from a Phase-1 peer must not block AwaitSchemaApplied")
	assert.Empty(t, resp.GetLaggards())
}
