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

package registry

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
}

func newTestRegistry(t *testing.T, heartbeatTimeout, cleanupTimeout time.Duration, maxAgents int) *AgentRegistry {
	t.Helper()
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "registry")
	return NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, maxAgents)
}

func TestNewAgentRegistry(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "registry")
	heartbeatTimeout := 5 * time.Second
	cleanupTimeout := 10 * time.Second
	maxAgents := 100

	ar := NewAgentRegistry(testLogger, heartbeatTimeout, cleanupTimeout, maxAgents)

	assert.NotNil(t, ar)
	assert.Equal(t, heartbeatTimeout, ar.heartbeatTimeout)
	assert.Equal(t, cleanupTimeout, ar.cleanupTimeout)
	assert.Equal(t, maxAgents, ar.maxAgents)
	assert.NotNil(t, ar.agents)
	assert.Equal(t, 0, len(ar.agents))
	assert.NotNil(t, ar.healthCheckStopCh)
	assert.NotNil(t, ar.healthCheckTicker)

	ar.Stop()
}

func TestRegisterAgent_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test"},
		PodName:        "test.pod",
		ContainerNames: []string{"data"},
	}

	agentID, err := ar.RegisterAgent(ctx, identity)

	require.NoError(t, err)
	assert.NotEmpty(t, agentID)
	agentInfo, getErr := ar.GetAgentByID(agentID)
	require.NoError(t, getErr)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.Role, agentInfo.AgentIdentity.Role)
	assert.Equal(t, identity.Labels, agentInfo.Labels)
	assert.Equal(t, identity.PodName, agentInfo.AgentIdentity.PodName)
	assert.Equal(t, identity.ContainerNames, agentInfo.AgentIdentity.ContainerNames)
	assert.Equal(t, AgentStatusOnline, agentInfo.Status)
	assert.WithinDuration(t, time.Now(), agentInfo.RegisteredAt, time.Second)
	assert.WithinDuration(t, time.Now(), agentInfo.LastHeartbeat, time.Second)
}

func TestRegisterAgent_MaxAgentsReached(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 2)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID1, err1 := ar.RegisterAgent(ctx, identity)
	require.NoError(t, err1)
	assert.NotEmpty(t, agentID1)

	identity2 := AgentIdentity{Role: "datanode-cold", PodName: "test", ContainerNames: []string{"lifecycle"}}
	agentID2, err2 := ar.RegisterAgent(ctx, identity2)
	require.NoError(t, err2)
	assert.NotEmpty(t, agentID2)

	identity3 := AgentIdentity{Role: "datanode-warm", PodName: "test", ContainerNames: []string{"liaison"}}
	_, err3 := ar.RegisterAgent(ctx, identity3)
	assert.Error(t, err3)
	assert.Contains(t, err3.Error(), "maximum number of agents")
}

func TestRegisterAgent_EmptyPodName(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "", ContainerNames: []string{"data"}}

	_, err := ar.RegisterAgent(ctx, identity)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pod name cannot be empty")
}

func TestRegisterAgent_EmptyRole(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "", PodName: "test", ContainerNames: []string{}}

	_, err := ar.RegisterAgent(ctx, identity)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node role cannot be empty")
}

func TestUnregisterAgent_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	err := ar.UnregisterAgent(agentID)

	require.NoError(t, err)
	_, getErr := ar.GetAgentByID(agentID)
	assert.Error(t, getErr)
	assert.Contains(t, getErr.Error(), "not found")
}

func TestUnregisterAgent_NotFound(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	err := ar.UnregisterAgent("non-existent-id")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateHeartbeat_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	agentInfoBefore, getErrBefore := ar.GetAgentByID(agentID)
	require.NoError(t, getErrBefore)
	initialHeartbeat := agentInfoBefore.LastHeartbeat

	time.Sleep(10 * time.Millisecond)

	err := ar.UpdateHeartbeat(agentID)
	require.NoError(t, err)

	agentInfoAfter, getErrAfter := ar.GetAgentByID(agentID)
	require.NoError(t, getErrAfter)
	assert.True(t, agentInfoAfter.LastHeartbeat.After(initialHeartbeat))
	assert.Equal(t, AgentStatusOnline, agentInfoAfter.Status)
}

func TestUpdateHeartbeat_NotFound(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	err := ar.UpdateHeartbeat("non-existent-id")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGetAgent_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test", "zone": "us-east"},
		PodName:        "test.pod",
		ContainerNames: []string{"data"},
	}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	agentInfo, err := ar.GetAgent("test.pod", "datanode-hot", map[string]string{"env": "test", "zone": "us-east"})

	require.NoError(t, err)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.PodName, agentInfo.AgentIdentity.PodName)
	assert.Equal(t, identity.ContainerNames, agentInfo.AgentIdentity.ContainerNames)
	assert.Equal(t, identity.Role, agentInfo.AgentIdentity.Role)
}

func TestGetAgent_NotFound(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	_, err := ar.GetAgent("test.pod", "datanode-hot", nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent not found")
}

func TestGetAgent_LabelMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test"},
		PodName:        "test.pod",
		ContainerNames: []string{"data"},
	}

	_, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	_, err := ar.GetAgent("test.pod", "datanode-hot", map[string]string{"env": "prod"})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent not found")
}

func TestGetAgentByID_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	agentInfo, err := ar.GetAgentByID(agentID)

	require.NoError(t, err)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.PodName, agentInfo.AgentIdentity.PodName)
	assert.Equal(t, identity.ContainerNames, agentInfo.AgentIdentity.ContainerNames)
	assert.Equal(t, identity.Role, agentInfo.AgentIdentity.Role)
}

func TestGetAgentByID_NotFound(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	_, err := ar.GetAgentByID("non-existent-id")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestListAgents_Empty(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	agents := ar.ListAgents()

	assert.NotNil(t, agents)
	assert.Equal(t, 0, len(agents))
}

func TestListAgents_Multiple(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity1 := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-cold", PodName: "test", ContainerNames: []string{"lifecycle"}}
	identity3 := AgentIdentity{Role: "datanode-warm", PodName: "test", ContainerNames: []string{"liaison"}}

	agentID1, err1 := ar.RegisterAgent(ctx, identity1)
	require.NoError(t, err1)
	agentID2, err2 := ar.RegisterAgent(ctx, identity2)
	require.NoError(t, err2)
	agentID3, err3 := ar.RegisterAgent(ctx, identity3)
	require.NoError(t, err3)

	agents := ar.ListAgents()

	assert.Equal(t, 3, len(agents))
	agentIDs := make(map[string]bool)
	for _, agentInfo := range agents {
		agentIDs[agentInfo.AgentID] = true
	}
	assert.True(t, agentIDs[agentID1])
	assert.True(t, agentIDs[agentID2])
	assert.True(t, agentIDs[agentID3])
}

func TestListAgentsByRole_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity1 := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-cold", PodName: "test", ContainerNames: []string{"lifecycle"}}
	identity3 := AgentIdentity{Role: "datanode-warm", PodName: "test", ContainerNames: []string{"liaison"}}

	agentID1, err1 := ar.RegisterAgent(ctx, identity1)
	require.NoError(t, err1)
	agentID2, err2 := ar.RegisterAgent(ctx, identity2)
	require.NoError(t, err2)
	agentID3, err3 := ar.RegisterAgent(ctx, identity3)
	require.NoError(t, err3)

	datanodeHotAgents := ar.ListAgentsByRole("datanode-hot")
	assert.Equal(t, 1, len(datanodeHotAgents))
	assert.Equal(t, agentID1, datanodeHotAgents[0].AgentID)
	assert.Equal(t, "datanode-hot", datanodeHotAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity1.PodName, datanodeHotAgents[0].AgentIdentity.PodName)
	assert.Equal(t, identity1.ContainerNames, datanodeHotAgents[0].AgentIdentity.ContainerNames)
	assert.Equal(t, identity1.Role, datanodeHotAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity1.Labels, datanodeHotAgents[0].Labels)
	assert.Equal(t, AgentStatusOnline, datanodeHotAgents[0].Status)
	assert.WithinDuration(t, time.Now(), datanodeHotAgents[0].RegisteredAt, time.Second)
	assert.WithinDuration(t, time.Now(), datanodeHotAgents[0].LastHeartbeat, time.Second)

	datanodeColdAgents := ar.ListAgentsByRole("datanode-cold")
	assert.Equal(t, 1, len(datanodeColdAgents))
	assert.Equal(t, agentID2, datanodeColdAgents[0].AgentID)
	assert.Equal(t, "datanode-cold", datanodeColdAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity2.PodName, datanodeColdAgents[0].AgentIdentity.PodName)
	assert.Equal(t, identity2.ContainerNames, datanodeColdAgents[0].AgentIdentity.ContainerNames)
	assert.Equal(t, identity2.Role, datanodeColdAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity2.Labels, datanodeColdAgents[0].Labels)
	assert.Equal(t, AgentStatusOnline, datanodeColdAgents[0].Status)
	assert.WithinDuration(t, time.Now(), datanodeColdAgents[0].RegisteredAt, time.Second)
	assert.WithinDuration(t, time.Now(), datanodeColdAgents[0].LastHeartbeat, time.Second)

	datanodeWarmAgents := ar.ListAgentsByRole("datanode-warm")
	assert.Equal(t, 1, len(datanodeWarmAgents))
	assert.Equal(t, agentID3, datanodeWarmAgents[0].AgentID)
	assert.Equal(t, "datanode-warm", datanodeWarmAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity3.PodName, datanodeWarmAgents[0].AgentIdentity.PodName)
	assert.Equal(t, identity3.ContainerNames, datanodeWarmAgents[0].AgentIdentity.ContainerNames)
	assert.Equal(t, identity3.Role, datanodeWarmAgents[0].AgentIdentity.Role)
	assert.Equal(t, identity3.Labels, datanodeWarmAgents[0].Labels)
	assert.Equal(t, AgentStatusOnline, datanodeWarmAgents[0].Status)
	assert.WithinDuration(t, time.Now(), datanodeWarmAgents[0].RegisteredAt, time.Second)
	assert.WithinDuration(t, time.Now(), datanodeWarmAgents[0].LastHeartbeat, time.Second)
}

func TestListAgentsByRole_Empty(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	agents := ar.ListAgentsByRole("datanode-hot")

	assert.NotNil(t, agents)
	assert.Equal(t, 0, len(agents))
}

func TestCheckAgentHealth_HeartbeatTimeout(t *testing.T) {
	heartbeatTimeout := 50 * time.Millisecond
	cleanupTimeout := 200 * time.Millisecond
	ar := newTestRegistry(t, heartbeatTimeout, cleanupTimeout, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	agentInfoBefore, getErrBefore := ar.GetAgentByID(agentID)
	require.NoError(t, getErrBefore)
	assert.Equal(t, AgentStatusOnline, agentInfoBefore.Status)

	time.Sleep(heartbeatTimeout + 10*time.Millisecond)

	err := ar.CheckAgentHealth()
	require.NoError(t, err)

	agentInfoAfter, getErrAfter := ar.GetAgentByID(agentID)
	require.NoError(t, getErrAfter)
	assert.Equal(t, AgentStatusOffline, agentInfoAfter.Status)
}

func TestCheckAgentHealth_CleanupTimeout(t *testing.T) {
	heartbeatTimeout := 50 * time.Millisecond
	cleanupTimeout := 100 * time.Millisecond
	ar := newTestRegistry(t, heartbeatTimeout, cleanupTimeout, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-warm", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	time.Sleep(cleanupTimeout + 10*time.Millisecond)

	err := ar.CheckAgentHealth()
	require.NoError(t, err)

	_, getErr := ar.GetAgentByID(agentID)
	assert.Error(t, getErr)
	assert.Contains(t, getErr.Error(), "not found")
}

func TestCheckAgentHealth_HeartbeatKeepsOnline(t *testing.T) {
	heartbeatTimeout := 100 * time.Millisecond
	cleanupTimeout := 200 * time.Millisecond
	ar := newTestRegistry(t, heartbeatTimeout, cleanupTimeout, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	time.Sleep(30 * time.Millisecond)
	updateErr := ar.UpdateHeartbeat(agentID)
	require.NoError(t, updateErr)

	time.Sleep(30 * time.Millisecond)
	updateErr2 := ar.UpdateHeartbeat(agentID)
	require.NoError(t, updateErr2)

	err := ar.CheckAgentHealth()
	require.NoError(t, err)

	agentInfo, getErr := ar.GetAgentByID(agentID)
	require.NoError(t, getErr)
	assert.Equal(t, AgentStatusOnline, agentInfo.Status)
}

func TestStop(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)

	assert.NotNil(t, ar.healthCheckTicker)
	assert.NotNil(t, ar.healthCheckStopCh)

	ar.Stop()

	select {
	case <-ar.healthCheckStopCh:
	default:
		t.Error("healthCheckStopCh should be closed")
	}
}

func TestMatchesIdentity_ExactMatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test", "zone": "us-east"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}
	identity2 := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test", "zone": "us-east"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.True(t, result)
}

func TestMatchesIdentity_IPMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{Role: "datanode-hot", Labels: nil, PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-cold", Labels: nil, PodName: "test", ContainerNames: []string{"lifecycle"}}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_PortMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{Role: "datanode-hot", Labels: nil, PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-cold", Labels: nil, PodName: "test", ContainerNames: []string{"lifecycle"}}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_RoleMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{Role: "datanode-hot", Labels: nil, PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-cold", Labels: nil, PodName: "test", ContainerNames: []string{"liaison"}}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_LabelMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}
	identity2 := AgentIdentity{
		Role:           "datanode-cold",
		Labels:         map[string]string{"env": "prod"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_LabelKeyMissing(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{
		Role:           "datanode-hot",
		Labels:         map[string]string{"env": "test", "zone": "us-east"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}
	identity2 := AgentIdentity{
		Role:           "datanode-cold",
		Labels:         map[string]string{"env": "test"},
		PodName:        "test",
		ContainerNames: []string{"data"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_EmptyLabels(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{Role: "datanode-hot", Labels: nil, PodName: "test", ContainerNames: []string{"data"}}
	identity2 := AgentIdentity{Role: "datanode-hot", Labels: nil, PodName: "test", ContainerNames: []string{"data"}}

	result := ar.matchesIdentity(identity1, identity2)
	assert.True(t, result)
}

func TestConcurrentRegisterAndList(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	numAgents := 10
	doneCh := make(chan bool, numAgents)

	for idx := 0; idx < numAgents; idx++ {
		go func(agentIdx int) {
			identity := AgentIdentity{
				Role:           "datanode-hot",
				Labels:         map[string]string{"idx": "test"},
				PodName:        "test_" + strconv.Itoa(agentIdx),
				ContainerNames: []string{"data"},
			}
			_, registerErr := ar.RegisterAgent(ctx, identity)
			if registerErr != nil {
				t.Errorf("Failed to register agent %d: %v", agentIdx, registerErr)
			}
			doneCh <- true
		}(idx)
	}

	for idx := 0; idx < numAgents; idx++ {
		<-doneCh
	}

	agents := ar.ListAgents()
	assert.Equal(t, numAgents, len(agents))
}

func TestConcurrentUpdateHeartbeat(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{Role: "datanode-hot", PodName: "test", ContainerNames: []string{"data"}}

	agentID, registerErr := ar.RegisterAgent(ctx, identity)
	require.NoError(t, registerErr)

	numUpdates := 20
	doneCh := make(chan bool, numUpdates)

	for idx := 0; idx < numUpdates; idx++ {
		go func() {
			updateErr := ar.UpdateHeartbeat(agentID)
			if updateErr != nil {
				t.Errorf("Failed to update heartbeat: %v", updateErr)
			}
			doneCh <- true
		}()
	}

	for idx := 0; idx < numUpdates; idx++ {
		<-doneCh
	}

	agentInfo, getErr := ar.GetAgentByID(agentID)
	require.NoError(t, getErr)
	assert.Equal(t, AgentStatusOnline, agentInfo.Status)
}
