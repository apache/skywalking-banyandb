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

package consistency

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
)

// kindStream mirrors schema.KindStream.String(); the checker treats kinds as
// opaque strings, so the test hard-codes the value to avoid importing the
// metadata/schema package (which depends on this one).
const kindStream = "stream"

func nodeState(nodeID string, objs ...*NodeObjectFP) NodeObjects {
	return NodeObjects{Node: nodeID, Objects: objs}
}

// testObjectName is the single object every checker case operates on; the
// checker's behavior does not depend on the name, only on the fingerprints.
const testObjectName = "foo"

func obj(cache, runtime uint64) *NodeObjectFP {
	return &NodeObjectFP{Kind: kindStream, Name: testObjectName, Cache: cache, Runtime: runtime}
}

func streamKey() ObjectKey {
	return ObjectKey{Kind: kindStream, Name: testObjectName}
}

func TestChecker_AllAgreeIsConsistent(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}

	got := c.Check("g", registry,
		[]NodeObjects{nodeState("data-1", obj(100, 100))}, 1)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, got.GetStatus())
	assert.Empty(t, got.GetIssues())
}

func TestChecker_SuppressesFirstOccurrence(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(200, 200))}

	first := c.Check("g", registry, states, 1)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, first.GetStatus(),
		"a first-round divergence may still be in-flight propagation")
	assert.Empty(t, first.GetIssues())
}

func TestChecker_ReportsOnSecondConsecutiveOccurrence(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(200, 200))}

	c.Check("g", registry, states, 1)
	second := c.Check("g", registry, states, 1)

	require.Len(t, second.GetIssues(), 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, second.GetStatus())
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE, second.GetIssues()[0].GetType())
	assert.Equal(t, "data-1", second.GetIssues()[0].GetNode())
}

func TestChecker_RecoveryResetsCounter(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	bad := []NodeObjects{nodeState("data-1", obj(200, 200))}
	good := []NodeObjects{nodeState("data-1", obj(100, 100))}

	c.Check("g", registry, bad, 1)
	c.Check("g", registry, good, 1)
	afterRelapse := c.Check("g", registry, bad, 1)

	assert.Empty(t, afterRelapse.GetIssues(),
		"recovery must reset the counter so the next divergence starts over")
}

func TestChecker_MissingInCache(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1")}

	c.Check("g", registry, states, 1)
	got := c.Check("g", registry, states, 1)

	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_MISSING_IN_CACHE, got.GetIssues()[0].GetType())
	// The registry truth lives in the object-centric table now, not on the issue.
	require.Len(t, got.GetObjects(), 1)
	assert.Equal(t, uint64(100), got.GetObjects()[0].GetRegistryFingerprint())
	assert.Empty(t, got.GetObjects()[0].GetNodeFingerprints(),
		"the node reported nothing, so it contributes no fingerprint")
}

func TestChecker_ObjectsPairRegistryAndNodeFingerprints(t *testing.T) {
	// The object-centric table lists each object once, pairing its registry truth
	// with every node's cache/runtime fingerprints, node fingerprints sorted by id.
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{
		nodeState("liaison-1", obj(100, 100)),
		nodeState("data-1", obj(100, 100)),
	}

	got := c.Check("g", registry, states, 2)

	require.Len(t, got.GetObjects(), 1)
	o := got.GetObjects()[0]
	assert.Equal(t, kindStream, o.GetKind())
	assert.Equal(t, testObjectName, o.GetName())
	assert.Equal(t, uint64(100), o.GetRegistryFingerprint())
	require.Len(t, o.GetNodeFingerprints(), 2)
	assert.Equal(t, "data-1", o.GetNodeFingerprints()[0].GetNode(), "node fingerprints sort by id")
	assert.Equal(t, uint64(100), o.GetNodeFingerprints()[0].GetCacheFingerprint())
	assert.Equal(t, uint64(100), o.GetNodeFingerprints()[0].GetRuntimeFingerprint())
	assert.Equal(t, "liaison-1", o.GetNodeFingerprints()[1].GetNode())
}

func TestChecker_OrphanObjectHasZeroRegistryFingerprint(t *testing.T) {
	// An object only the node holds (registry does not) still appears in the
	// table, with a zero registry fingerprint and the node's fingerprints.
	c := NewChecker()
	states := []NodeObjects{nodeState("data-1", obj(200, 200))}

	got := c.Check("g", map[ObjectKey]uint64{}, states, 1)

	require.Len(t, got.GetObjects(), 1)
	o := got.GetObjects()[0]
	assert.Zero(t, o.GetRegistryFingerprint(), "the registry does not know this orphan")
	require.Len(t, o.GetNodeFingerprints(), 1)
	assert.Equal(t, uint64(200), o.GetNodeFingerprints()[0].GetCacheFingerprint())
}

func TestChecker_Orphan(t *testing.T) {
	c := NewChecker()
	states := []NodeObjects{nodeState("data-1", obj(100, 100))}

	c.Check("g", map[ObjectKey]uint64{}, states, 1)
	got := c.Check("g", map[ObjectKey]uint64{}, states, 1)

	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_ORPHAN, got.GetIssues()[0].GetType())
}

func TestChecker_RuntimeNotApplied(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(100, 999))}

	c.Check("g", registry, states, 1)
	got := c.Check("g", registry, states, 1)

	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_RUNTIME_NOT_APPLIED, got.GetIssues()[0].GetType())
}

func TestChecker_CacheStaleHidesRuntimeIssue(t *testing.T) {
	// Both boundaries diverge; only the upstream one is reported so the root
	// cause is not buried under a derived symptom.
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(200, 999))}

	c.Check("g", registry, states, 1)
	got := c.Check("g", registry, states, 1)

	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE, got.GetIssues()[0].GetType())
}

func TestChecker_IncompleteCollectionIsUnknown(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(100, 100))}

	got := c.Check("g", registry, states, 3)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus(),
		"only 1 of 3 expected nodes answered")
}

func TestChecker_PrunesCountersForVanishedObjects(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	bad := []NodeObjects{nodeState("data-1", obj(200, 200))}

	c.Check("g", registry, bad, 1)
	// The object disappears from both the registry and the node.
	c.Check("g", map[ObjectKey]uint64{}, []NodeObjects{nodeState("data-1")}, 1)

	assert.Zero(t, c.trackedCount(), "counters for vanished objects must be pruned")
}

func TestChecker_PerNodeIndependence(t *testing.T) {
	// One bad node must not suppress or trigger reporting for a healthy peer.
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{
		nodeState("data-1", obj(100, 100)),
		nodeState("data-2", obj(200, 200)),
	}

	c.Check("g", registry, states, 2)
	got := c.Check("g", registry, states, 2)

	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, "data-2", got.GetIssues()[0].GetNode())
}

func TestChecker_ConcurrentGroupsAreSafe(t *testing.T) {
	// InspectAll inspects up to maxInspectGroupConcurrency groups in parallel
	// against one shared checker, so Check must tolerate concurrent callers.
	// Run with -race to make this meaningful.
	const groups = 32
	const rounds = 20
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(200, 200))}

	var wg sync.WaitGroup
	for g := 0; g < groups; g++ {
		wg.Add(1)
		//panicdiag:allow-rawgo test-only contention driver; a panic here must fail the test loudly rather than be recovered and hidden
		go func(idx int) {
			defer wg.Done()
			group := fmt.Sprintf("g%d", idx)
			for r := 0; r < rounds; r++ {
				c.Check(group, registry, states, 1)
			}
		}(g)
	}
	wg.Wait()

	assert.Equal(t, groups, c.trackedCount(),
		"each group must keep exactly one live counter, with no cross-group interference")
}

func TestChecker_NoNodeAnsweredIsUnknown(t *testing.T) {
	// Zero evidence must never read as agreement. This also covers the roster
	// lookup failing and reporting zero expected nodes.
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}

	got := c.Check("g", registry, nil, 0)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus())
	assert.Empty(t, got.GetIssues())
}

func TestChecker_GroupsAreIndependent(t *testing.T) {
	// Pruning one group's counters must not clear another group's streak.
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	bad := []NodeObjects{nodeState("data-1", obj(200, 200))}

	c.Check("g1", registry, bad, 1)
	c.Check("g2", registry, bad, 1)
	got := c.Check("g1", registry, bad, 1)

	require.Len(t, got.GetIssues(), 1, "g2's round must not reset g1's streak")
}

// The tests below give each ConsistencyStatus value dedicated coverage.

// TestChecker_StatusConsistentRequiresFullRoster pins that CONSISTENT is only
// reached when every expected node answered AND nothing diverged; a matching
// but incomplete roster must stay UNKNOWN even though no issue exists.
func TestChecker_StatusConsistentRequiresFullRoster(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	agree := []NodeObjects{
		nodeState("data-1", obj(100, 100)),
		nodeState("liaison-1", obj(100, 100)),
	}

	full := c.Check("g", registry, agree, 2)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, full.GetStatus(),
		"all expected nodes answered and agree")

	short := c.Check("g", registry, agree, 3)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, short.GetStatus(),
		"one rostered node is silent, so agreement cannot be concluded")
	assert.Empty(t, short.GetIssues())
}

// TestChecker_StatusUnknownWhileSuppressing pins that a first-round divergence
// keeps the whole group UNKNOWN (not CONSISTENT) even though it emits no issue
// yet: the suppressed divergence is unresolved, not agreement.
func TestChecker_StatusUnknownWhileSuppressing(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(200, 200))}

	got := c.Check("g", registry, states, 1)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus())
	assert.Empty(t, got.GetIssues(), "the divergence is still suppressed on its first round")
}

// TestChecker_StatusInconsistentThenRecoversToConsistent walks the full status
// lifecycle for one node: UNKNOWN (suppressed) -> INCONSISTENT (reported) ->
// CONSISTENT (healed), proving the rollup tracks the latest evidence and does
// not latch on a past INCONSISTENT verdict.
func TestChecker_StatusInconsistentThenRecoversToConsistent(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	bad := []NodeObjects{nodeState("data-1", obj(200, 200))}
	good := []NodeObjects{nodeState("data-1", obj(100, 100))}

	first := c.Check("g", registry, bad, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, first.GetStatus())

	second := c.Check("g", registry, bad, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, second.GetStatus())

	healed := c.Check("g", registry, good, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, healed.GetStatus(),
		"once the node matches again the group returns to CONSISTENT without latching")
	assert.Empty(t, healed.GetIssues())
}

// TestChecker_StatusInconsistentWinsOverSuppressionAndShortfall pins the switch
// priority: a reported issue makes the group INCONSISTENT even when another
// node is still suppressing and the roster is short. INCONSISTENT is the most
// severe verdict and must not be masked by UNKNOWN conditions.
func TestChecker_StatusInconsistentWinsOverSuppressionAndShortfall(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	// data-1 has diverged for two rounds (will report); data-2 diverges only now
	// (still suppressed). Expected roster is larger than answered.
	round1 := []NodeObjects{nodeState("data-1", obj(200, 200))}
	c.Check("g", registry, round1, 5)

	round2 := []NodeObjects{
		nodeState("data-1", obj(200, 200)),
		nodeState("data-2", obj(300, 300)),
	}
	got := c.Check("g", registry, round2, 5)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, got.GetStatus(),
		"a confirmed issue outranks suppression and roster shortfall")
	require.Len(t, got.GetIssues(), 1)
	assert.Equal(t, "data-1", got.GetIssues()[0].GetNode())
}

// TestChecker_StatusUnknownOnRosterShortfallWithoutIssue pins that a healthy but
// incomplete collection (every answering node agrees, yet fewer answered than
// the roster) is UNKNOWN, never CONSISTENT: a silent node could be diverging.
func TestChecker_StatusUnknownOnRosterShortfallWithoutIssue(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	states := []NodeObjects{nodeState("data-1", obj(100, 100))}

	// Two consecutive rounds so suppression is not the reason for UNKNOWN.
	c.Check("g", registry, states, 2)
	got := c.Check("g", registry, states, 2)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, got.GetStatus(),
		"1 of 2 rostered nodes answered; agreement of the one is not agreement of all")
	assert.Empty(t, got.GetIssues())
}

// TestChecker_StatusUnspecifiedNeverEscapes pins that the checker never returns
// the zero-value UNSPECIFIED status: every code path assigns an explicit
// verdict, so a caller can always trust GetStatus().
func TestChecker_StatusUnspecifiedNeverEscapes(t *testing.T) {
	c := NewChecker()
	registry := map[ObjectKey]uint64{streamKey(): 100}
	cases := []struct {
		name          string
		nodes         []NodeObjects
		expectedNodes int
	}{
		{"empty", nil, 0},
		{"agree-full", []NodeObjects{nodeState("data-1", obj(100, 100))}, 1},
		{"agree-short", []NodeObjects{nodeState("data-1", obj(100, 100))}, 2},
		{"diverge", []NodeObjects{nodeState("data-1", obj(200, 200))}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Check(tc.name, registry, tc.nodes, tc.expectedNodes)
			assert.NotEqual(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNSPECIFIED, got.GetStatus(),
				"every path must assign an explicit status")
		})
	}
}
