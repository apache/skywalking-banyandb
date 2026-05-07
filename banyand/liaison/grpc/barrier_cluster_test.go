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

// fakeNodeStatusClient is a minimal clusterv1.NodeSchemaStatusServiceClient.
// Only GetMaxRevision is implemented for Phase 2.2 tests; the other RPCs
// land in 2.3/2.4 and panic if a test accidentally invokes them.
type fakeNodeStatusClient struct {
	err      error
	callsRef *int32
	maxRev   int64
	delay    time.Duration
}

func (f *fakeNodeStatusClient) GetMaxRevision(ctx context.Context, _ *clusterv1.GetMaxRevisionRequest, _ ...grpc.CallOption) (*clusterv1.GetMaxRevisionResponse, error) {
	if f.callsRef != nil {
		atomic.AddInt32(f.callsRef, 1)
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
	return &clusterv1.GetMaxRevisionResponse{MaxModRevision: f.maxRev}, nil
}

func (*fakeNodeStatusClient) GetKeyRevisions(_ context.Context, _ *clusterv1.GetKeyRevisionsRequest, _ ...grpc.CallOption) (*clusterv1.GetKeyRevisionsResponse, error) {
	panic("GetKeyRevisions: unused in Phase 2.2 tests")
}

func (*fakeNodeStatusClient) GetAbsentKeys(_ context.Context, _ *clusterv1.GetAbsentKeysRequest, _ ...grpc.CallOption) (*clusterv1.GetAbsentKeysResponse, error) {
	panic("GetAbsentKeys: unused in Phase 2.2 tests")
}

// fakeQueueClient embeds queue.Client (a nil interface) so unused methods
// panic at runtime; only the two methods the barrier fan-out actually calls
// are overridden.
type fakeQueueClient struct {
	queue.Client
	routeTable    *databasev1.RouteTable
	statusClients map[string]clusterv1.NodeSchemaStatusServiceClient
}

func (f *fakeQueueClient) GetRouteTable() *databasev1.RouteTable {
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
