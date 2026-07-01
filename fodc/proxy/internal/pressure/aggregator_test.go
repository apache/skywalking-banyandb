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

// TestFinalizeAgentListReplacesEvictedEntries pins the stale-cache fix: a second list round
// that no longer reports a profile must drop it from the cache, not keep serving it.
func TestFinalizeAgentListReplacesEvictedEntries(t *testing.T) {
	a := newTestAggregator(t)
	info := agentInfo("pod-1")

	// Round 1: the agent reports two events.
	a.ProcessProfileFromAgent("agent-1", info, record("evt-a"))
	a.ProcessProfileFromAgent("agent-1", info, record("evt-b"))
	// Staged records are not yet visible before the round is finalized.
	assert.Empty(t, a.snapshotCache(nil), "records must not be visible before ListComplete")

	a.FinalizeAgentList("agent-1")
	got := profileIDs(a.snapshotCache(nil))
	assert.Equal(t, map[string]struct{}{"evt-a": {}, "evt-b": {}}, got)

	// Round 2: evt-a was evicted on the agent's disk, so only evt-b is reported.
	a.ProcessProfileFromAgent("agent-1", info, record("evt-b"))
	a.FinalizeAgentList("agent-1")
	got = profileIDs(a.snapshotCache(nil))
	assert.Equal(t, map[string]struct{}{"evt-b": {}}, got, "evicted evt-a must be dropped")
}

// TestFinalizeAgentListIsPerAgent verifies finalizing one agent's round does not disturb
// another agent's cached entries.
func TestFinalizeAgentListIsPerAgent(t *testing.T) {
	a := newTestAggregator(t)
	a.ProcessProfileFromAgent("agent-1", agentInfo("pod-1"), record("a1"))
	a.FinalizeAgentList("agent-1")
	a.ProcessProfileFromAgent("agent-2", agentInfo("pod-2"), record("a2"))
	a.FinalizeAgentList("agent-2")

	assert.Equal(t, map[string]struct{}{"a1": {}, "a2": {}}, profileIDs(a.snapshotCache(nil)))

	// A fresh (empty) round for agent-1 clears only agent-1.
	a.FinalizeAgentList("agent-1")
	assert.Equal(t, map[string]struct{}{"a2": {}}, profileIDs(a.snapshotCache(nil)))
}

// TestRemoveAgentClearsStaging verifies a disconnect drops both cached and in-flight staged
// records for the agent.
func TestRemoveAgentClearsStaging(t *testing.T) {
	a := newTestAggregator(t)
	info := agentInfo("pod-1")
	a.ProcessProfileFromAgent("agent-1", info, record("a1"))
	a.FinalizeAgentList("agent-1")
	a.ProcessProfileFromAgent("agent-1", info, record("a2")) // staged, not yet finalized

	a.RemoveAgent("agent-1")
	assert.Empty(t, a.snapshotCache(nil))
	// A finalize after removal must not resurrect the staged record.
	a.FinalizeAgentList("agent-1")
	assert.Empty(t, a.snapshotCache(nil))
}
