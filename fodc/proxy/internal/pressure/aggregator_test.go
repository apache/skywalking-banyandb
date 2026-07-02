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

package pressure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func newTestAggregator(t *testing.T) *Aggregator {
	t.Helper()
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "error"}))
	reg := registry.NewAgentRegistry(logger.GetLogger("test"), time.Minute, 2*time.Minute, 10)
	return NewAggregator(reg, nil, logger.GetLogger("test"))
}

func agentInfo(podName string) *registry.AgentInfo {
	return &registry.AgentInfo{AgentIdentity: registry.AgentIdentity{PodName: podName, Role: "ROLE_DATA"}}
}

func record(profileID string) *fodcv1.PressureProfileRecord {
	return &fodcv1.PressureProfileRecord{ProfileId: profileID}
}

func profileIDs(records []*AggregatedPressureProfile) map[string]struct{} {
	out := make(map[string]struct{}, len(records))
	for _, r := range records {
		out[r.ProfileID] = struct{}{}
	}
	return out
}

func recs(ids ...string) []*fodcv1.PressureProfileRecord {
	out := make([]*fodcv1.PressureProfileRecord, 0, len(ids))
	for _, id := range ids {
		out = append(out, record(id))
	}
	return out
}

// TestReplaceAgentProfilesDropsEvicted pins the stale-cache fix: a later round that no longer
// reports a profile drops it from the cache, and an empty round removes the agent entirely.
func TestReplaceAgentProfilesDropsEvicted(t *testing.T) {
	a := newTestAggregator(t)
	info := agentInfo("pod-1")

	a.ReplaceAgentProfiles("agent-1", info, recs("evt-a", "evt-b"))
	assert.Equal(t, map[string]struct{}{"evt-a": {}, "evt-b": {}}, profileIDs(a.snapshotCache(nil)))

	// evt-a was evicted on the agent's disk, so the next round reports only evt-b.
	a.ReplaceAgentProfiles("agent-1", info, recs("evt-b"))
	assert.Equal(t, map[string]struct{}{"evt-b": {}}, profileIDs(a.snapshotCache(nil)), "evicted evt-a must be dropped")

	// An empty round drops the agent's entry entirely.
	a.ReplaceAgentProfiles("agent-1", info, nil)
	assert.Empty(t, a.snapshotCache(nil))
}

// TestReplaceAgentProfilesIsPerAgent verifies replacing one agent's set does not disturb another's.
func TestReplaceAgentProfilesIsPerAgent(t *testing.T) {
	a := newTestAggregator(t)
	a.ReplaceAgentProfiles("agent-1", agentInfo("pod-1"), recs("a1"))
	a.ReplaceAgentProfiles("agent-2", agentInfo("pod-2"), recs("a2"))
	assert.Equal(t, map[string]struct{}{"a1": {}, "a2": {}}, profileIDs(a.snapshotCache(nil)))

	a.ReplaceAgentProfiles("agent-1", agentInfo("pod-1"), nil)
	assert.Equal(t, map[string]struct{}{"a2": {}}, profileIDs(a.snapshotCache(nil)))
}

// TestRemoveAgent drops the agent's cached set on disconnect.
func TestRemoveAgent(t *testing.T) {
	a := newTestAggregator(t)
	a.ReplaceAgentProfiles("agent-1", agentInfo("pod-1"), recs("a1"))
	assert.NotEmpty(t, a.snapshotCache(nil))
	a.RemoveAgent("agent-1")
	assert.Empty(t, a.snapshotCache(nil))
}
