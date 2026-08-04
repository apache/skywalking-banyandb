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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func groupWithVerdict(status fodcv1.ConsistencyStatus, issues ...*fodcv1.SchemaIssue) *agentLifecycleData {
	return &agentLifecycleData{
		Data: &fodcv1.LifecycleData{
			Groups: []*fodcv1.GroupLifecycleInfo{{
				Name: "g",
				SchemaConsistency: &fodcv1.SchemaConsistency{
					Status: status,
					Issues: issues,
				},
			}},
		},
	}
}

func staleIssue(node string) *fodcv1.SchemaIssue {
	return &fodcv1.SchemaIssue{
		Kind: "stream", Name: "foo", Node: node,
		Type: fodcv1.SchemaIssueType_SCHEMA_ISSUE_TYPE_CACHE_STALE,
	}
}

func TestMergeGroups_InconsistentWinsOverConsistent(t *testing.T) {
	// Two liaisons disagree on the same group. The pessimistic verdict must win
	// so one healthy reporter cannot hide another's finding.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT),
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-1")),
	}

	got := mgr.mergeGroups(data)

	require.Len(t, got, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT,
		got[0].GetSchemaConsistency().GetStatus())
	assert.Len(t, got[0].GetSchemaConsistency().GetIssues(), 1)
}

func TestMergeGroups_InconsistentWinsRegardlessOfOrder(t *testing.T) {
	// The same, with the healthy report arriving last: last-wins would lose it.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-1")),
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT),
	}

	got := mgr.mergeGroups(data)

	require.Len(t, got, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT,
		got[0].GetSchemaConsistency().GetStatus())
	require.Len(t, got[0].GetSchemaConsistency().GetIssues(), 1)
}

func TestMergeGroups_UnknownWinsOverConsistent(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT),
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN),
	}

	got := mgr.mergeGroups(data)

	require.Len(t, got, 1)
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_UNKNOWN,
		got[0].GetSchemaConsistency().GetStatus())
}

func TestMergeGroups_DeduplicatesIdenticalIssues(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-1")),
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-1")),
	}

	got := mgr.mergeGroups(data)

	assert.Len(t, got[0].GetSchemaConsistency().GetIssues(), 1,
		"the same issue seen by two liaisons must appear once")
}

func TestMergeGroups_KeepsDistinctIssues(t *testing.T) {
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-1")),
		groupWithVerdict(fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, staleIssue("data-2")),
	}

	got := mgr.mergeGroups(data)

	assert.Len(t, got[0].GetSchemaConsistency().GetIssues(), 2,
		"issues on different nodes are different findings")
}

func TestMergeGroups_UnionsObjectNodeFingerprints(t *testing.T) {
	// Each liaison reports the full object table; the merge dedups objects by
	// kind+name and unions their node fingerprints by node id.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	objFrom := func(node string) *agentLifecycleData {
		return &agentLifecycleData{
			Data: &fodcv1.LifecycleData{
				Groups: []*fodcv1.GroupLifecycleInfo{{
					Name: "g",
					SchemaConsistency: &fodcv1.SchemaConsistency{
						Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT,
						Objects: []*fodcv1.ObjectConsistency{{
							Kind: "stream", Name: "foo", RegistryFingerprint: 1,
							NodeFingerprints: []*fodcv1.NodeFingerprint{{
								Node: node, CacheFingerprint: 1, RuntimeFingerprint: 1,
							}},
						}},
					},
				}},
			},
		}
	}

	got := mgr.mergeGroups([]*agentLifecycleData{objFrom("data-1"), objFrom("liaison-1")})

	require.Len(t, got, 1)
	objs := got[0].GetSchemaConsistency().GetObjects()
	require.Len(t, objs, 1, "the same object from two liaisons must appear once")
	assert.Equal(t, uint64(1), objs[0].GetRegistryFingerprint())
	require.Len(t, objs[0].GetNodeFingerprints(), 2, "node fingerprints from both liaisons union")
	assert.Equal(t, "data-1", objs[0].GetNodeFingerprints()[0].GetNode(), "sorted by node id")
	assert.Equal(t, "liaison-1", objs[0].GetNodeFingerprints()[1].GetNode())
}

func TestMergeGroups_IsolatesDistinctGroups(t *testing.T) {
	// mergeGroups reuses each round's freshly received group in place: the gRPC
	// stream re-unmarshals lifecycle data on every collection and nothing caches
	// it, so there is no cross-round leak to guard against. The property that
	// in-place reuse could still get wrong is cross-group isolation -- merging one
	// group's verdict must never bleed into a different group's.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	healthy := &agentLifecycleData{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{{
		Name:              "a",
		SchemaConsistency: &fodcv1.SchemaConsistency{Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT},
	}}}}
	broken := &agentLifecycleData{Data: &fodcv1.LifecycleData{Groups: []*fodcv1.GroupLifecycleInfo{{
		Name: "b",
		SchemaConsistency: &fodcv1.SchemaConsistency{
			Status: fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT,
			Issues: []*fodcv1.SchemaIssue{staleIssue("data-1")},
		},
	}}}}

	got := mgr.mergeGroups([]*agentLifecycleData{healthy, broken})

	require.Len(t, got, 2)
	byName := map[string]*fodcv1.GroupLifecycleInfo{got[0].GetName(): got[0], got[1].GetName(): got[1]}
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_CONSISTENT, byName["a"].GetSchemaConsistency().GetStatus())
	assert.Empty(t, byName["a"].GetSchemaConsistency().GetIssues(),
		"group b's issue must not bleed into group a")
	assert.Equal(t, fodcv1.ConsistencyStatus_CONSISTENCY_STATUS_INCONSISTENT, byName["b"].GetSchemaConsistency().GetStatus())
}

func TestMergeGroups_NoVerdictStaysNil(t *testing.T) {
	// A group inspected by a liaison without the checker must not gain a verdict.
	mgr := NewManager(nil, nil, logger.GetLogger("test-merge"))
	data := []*agentLifecycleData{{
		Data: &fodcv1.LifecycleData{
			Groups: []*fodcv1.GroupLifecycleInfo{{Name: "g"}},
		},
	}}

	got := mgr.mergeGroups(data)

	require.Len(t, got, 1)
	assert.Nil(t, got[0].GetSchemaConsistency())
}
