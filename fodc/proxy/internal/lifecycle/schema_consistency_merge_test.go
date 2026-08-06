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

package lifecycle

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/internal/consistency"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// testGroup is the single group the assembly cases operate on.
const testGroup = "g"

// nodeObj is one object carrying a single node's cache/runtime fingerprints, as
// that node's agent reports it.
func nodeObj(kind, name, node string, cache, runtime uint64) *fodcv1.ObjectConsistency {
	return &fodcv1.ObjectConsistency{
		Kind: kind, Name: name,
		NodeFingerprints: []*fodcv1.NodeFingerprint{{Node: node, CacheFingerprint: cache, RuntimeFingerprint: runtime}},
	}
}

// agentData is one agent's LifecycleData with a single group whose
// schema_consistency partial carries the given objects.
func agentData(objs ...*fodcv1.ObjectConsistency) *agentLifecycleData {
	return &agentLifecycleData{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{
		{Name: testGroup, SchemaConsistency: &fodcv1.SchemaConsistency{Objects: objs}},
	}}}
}

// registryTruth is what the proxy fetches once from a schema-serving node: the
// group object plus the stream object, both at their canonical fingerprints. A
// node whose stream fingerprint differs from 100 is diverging.
func registryTruth() map[string]map[consistency.ObjectKey]uint64 {
	return map[string]map[consistency.ObjectKey]uint64{
		testGroup: {
			{Kind: "group", Name: testGroup}: 1,
			{Kind: "stream", Name: "foo"}:    100,
		},
	}
}

// nodeContribution is one node's fingerprints for the group and stream objects.
func nodeContribution(node string, cacheFP, runtimeFP uint64) *agentLifecycleData {
	return agentData(
		nodeObj("group", testGroup, node, 1, 1),
		nodeObj("stream", "foo", node, cacheFP, runtimeFP),
	)
}

func TestAssembleSchemaConsistency_AgreementIsConsistent(t *testing.T) {
	data := []*agentLifecycleData{nodeContribution("data-1", 100, 100)}

	verdicts := assembleSchemaConsistency(data, consistency.NewChecker(), false, registryTruth())

	require.Contains(t, verdicts, "g")
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, verdicts["g"].GetStatus())
}

func TestAssembleSchemaConsistency_DivergenceReportedAfterTwoRounds(t *testing.T) {
	// The node's cache (200) disagrees with the registry (100). The checker
	// suppresses the first round, then reports on the second.
	checker := consistency.NewChecker()
	data := []*agentLifecycleData{nodeContribution("data-1", 200, 200)}

	first := assembleSchemaConsistency(data, checker, false, registryTruth())
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, first["g"].GetStatus(),
		"a first-round divergence may still be in-flight propagation")

	second := assembleSchemaConsistency(data, checker, false, registryTruth())
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, second["g"].GetStatus())
	require.Len(t, second["g"].GetIssues(), 1)
	assert.Equal(t, "data-1", second["g"].GetIssues()[0].GetNode())
}

func TestAssembleSchemaConsistency_NoRegistryIsUnknown(t *testing.T) {
	// A node reported, but the proxy fetched no registry truth for the group
	// (fetch failed or omitted it), so the objects cannot be verified.
	data := []*agentLifecycleData{nodeContribution("data-1", 100, 100)}

	verdicts := assembleSchemaConsistency(data, consistency.NewChecker(), false, nil)

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, verdicts["g"].GetStatus())
}

func TestAssembleSchemaConsistency_ShortfallForcesUnknown(t *testing.T) {
	// Every answering node agrees, but an agent went silent this round, so a node
	// could be silently diverging -> UNKNOWN, never CONSISTENT.
	data := []*agentLifecycleData{nodeContribution("data-1", 100, 100)}

	verdicts := assembleSchemaConsistency(data, consistency.NewChecker(), true, registryTruth())

	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN, verdicts["g"].GetStatus())
}

func TestAssembleSchemaConsistency_UnionsNodeFingerprints(t *testing.T) {
	// Each node's fingerprints come from its own agent; the proxy pairs them with
	// the single registry truth it fetched.
	data := []*agentLifecycleData{
		nodeContribution("data-1", 100, 100),
		nodeContribution("liaison-1", 100, 100),
	}

	verdicts := assembleSchemaConsistency(data, consistency.NewChecker(), false, registryTruth())

	objs := verdicts["g"].GetObjects()
	var stream *fodcv1.ObjectConsistency
	for _, o := range objs {
		if o.GetKind() == "stream" {
			stream = o
		}
	}
	require.NotNil(t, stream)
	assert.Equal(t, uint64(100), stream.GetRegistryFingerprint())
	require.Len(t, stream.GetNodeFingerprints(), 2, "both nodes' fingerprints appear")
	assert.Equal(t, "data-1", stream.GetNodeFingerprints()[0].GetNode(), "sorted by node id")
	assert.Equal(t, "liaison-1", stream.GetNodeFingerprints()[1].GetNode())
}

func TestMergeGroups_AppliesVerdictAndUnionsErrors(t *testing.T) {
	// Two agents each observe the group with a different error; the merge unions
	// them and stamps on the proxy's verdict.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{{Name: "g", Errors: []string{"collect: boom"}}}}},
		{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{{Name: "g", Errors: []string{"schema: registry unavailable"}}}}},
	}
	verdicts := map[string]*fodcv1.SchemaConsistency{
		"g": {Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT},
	}

	got := mgr.mergeGroups(data, verdicts, nil)

	require.Len(t, got, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, got[0].GetSchemaConsistency().GetStatus())
	assert.ElementsMatch(t, []string{"collect: boom", "schema: registry unavailable"}, got[0].GetErrors(),
		"collection and schema errors are unioned across agents")
}

func TestMergeGroups_NoVerdictStaysNil(t *testing.T) {
	// A group with no schema reports (e.g. a property group) gets no verdict.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{{Data: &fodcv1.LifecycleData{
		Groups: []*fodcv1.GroupLifecycleInfo{{Name: "g"}},
	}}}

	got := mgr.mergeGroups(data, map[string]*fodcv1.SchemaConsistency{}, nil)

	require.Len(t, got, 1)
	assert.Nil(t, got[0].GetSchemaConsistency())
}

func TestMergeGroups_IsolatesDistinctGroups(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{
		{Name: "a"}, {Name: "b"},
	}}}}
	verdicts := map[string]*fodcv1.SchemaConsistency{
		"a": {Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT},
		"b": {Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT},
	}

	got := mgr.mergeGroups(data, verdicts, nil)

	require.Len(t, got, 2)
	byName := map[string]*fodcv1.GroupLifecycleInfo{got[0].GetName(): got[0], got[1].GetName(): got[1]}
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, byName["a"].GetSchemaConsistency().GetStatus())
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, byName["b"].GetSchemaConsistency().GetStatus())
}

func TestRegistrySchemaErrors_FatalAttachesToEveryVerdictGroup(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-reg-err"))
	verdicts := map[string]*fodcv1.SchemaConsistency{"a": {}, "b": {}}

	got := mgr.registrySchemaErrors(verdicts, map[string]string{"a": "boom-a"}, errors.New("unreachable"))

	assert.Contains(t, got["a"], "registry read failed: boom-a", "a per-group read error attaches to its group")
	assert.Contains(t, got["a"], "registry unavailable: unreachable", "the fatal error also attaches to a")
	assert.Contains(t, got["b"], "registry unavailable: unreachable", "the fatal error reaches every downgraded group")
	assert.NotContains(t, got["b"], "registry read failed: boom-a", "a per-group error does not bleed into other groups")
}

func TestRegistrySchemaErrors_NoErrorIsEmpty(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-reg-err"))

	got := mgr.registrySchemaErrors(map[string]*fodcv1.SchemaConsistency{"a": {}}, nil, nil)

	assert.Empty(t, got["a"], "a clean fetch surfaces no schema errors")
}

func TestMergeGroups_SurfacesRegistryErrors(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge-reg"))
	data := []*agentLifecycleData{{Data: &fodcv1.LifecycleData{
		Groups: []*fodcv1.GroupLifecycleInfo{{Name: "g", Errors: []string{"collect: boom"}}},
	}}}

	got := mgr.mergeGroups(data, map[string]*fodcv1.SchemaConsistency{}, map[string][]string{"g": {"registry unavailable: down"}})

	require.Len(t, got, 1)
	assert.ElementsMatch(t, []string{"collect: boom", "registry unavailable: down"}, got[0].GetErrors(),
		"registry errors are unioned onto the group alongside the agent's own errors")
}

func TestSchemaConsistencySnapshot_CachesVerdictExcludingNilAndCopies(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-snap"))
	assert.Empty(t, mgr.SchemaConsistencySnapshot(), "no collection yet -> empty")

	mgr.cacheSchemaStatus([]*fodcv1.GroupLifecycleInfo{
		{Name: "a", SchemaConsistency: &fodcv1.SchemaConsistency{Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT}},
		{Name: "b", SchemaConsistency: &fodcv1.SchemaConsistency{Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT}},
		{Name: "prop", SchemaConsistency: nil}, // property group: no verdict -> excluded
	})

	snap := mgr.SchemaConsistencySnapshot()
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, snap["a"])
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, snap["b"])
	assert.NotContains(t, snap, "prop", "a group without a verdict exports no metric")

	// The returned map is a copy: mutating it must not corrupt the cache.
	snap["a"] = fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, mgr.SchemaConsistencySnapshot()["a"])
}
