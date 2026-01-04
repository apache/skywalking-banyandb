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
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test"},
	}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, err := ar.RegisterAgent(ctx, identity, primaryAddr)

	require.NoError(t, err)
	assert.NotEmpty(t, agentID)
	agentInfo, getErr := ar.GetAgentByID(agentID)
	require.NoError(t, getErr)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.IP, agentInfo.AgentIdentity.IP)
	assert.Equal(t, identity.Port, agentInfo.AgentIdentity.Port)
	assert.Equal(t, identity.Role, agentInfo.AgentIdentity.Role)
	assert.Equal(t, identity.Labels, agentInfo.Labels)
	assert.Equal(t, primaryAddr, agentInfo.PrimaryAddress)
	assert.Equal(t, AgentStatusOnline, agentInfo.Status)
	assert.WithinDuration(t, time.Now(), agentInfo.RegisteredAt, time.Second)
	assert.WithinDuration(t, time.Now(), agentInfo.LastHeartbeat, time.Second)
}

func TestRegisterAgent_MaxAgentsReached(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 2)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID1, err1 := ar.RegisterAgent(ctx, identity, primaryAddr)
	require.NoError(t, err1)
	assert.NotEmpty(t, agentID1)

	identity2 := AgentIdentity{IP: "192.168.1.2", Port: 8080, Role: "worker"}
	agentID2, err2 := ar.RegisterAgent(ctx, identity2, Address{IP: "192.168.1.2", Port: 8080})
	require.NoError(t, err2)
	assert.NotEmpty(t, agentID2)

	identity3 := AgentIdentity{IP: "192.168.1.3", Port: 8080, Role: "worker"}
	_, err3 := ar.RegisterAgent(ctx, identity3, Address{IP: "192.168.1.3", Port: 8080})
	assert.Error(t, err3)
	assert.Contains(t, err3.Error(), "maximum number of agents")
}

func TestRegisterAgent_EmptyPrimaryIP(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "", Port: 8080}

	_, err := ar.RegisterAgent(ctx, identity, primaryAddr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary address IP cannot be empty")
}

func TestRegisterAgent_InvalidPort(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 0}

	_, err := ar.RegisterAgent(ctx, identity, primaryAddr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary address port must be greater than 0")
}

func TestRegisterAgent_EmptyRole(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: ""}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	_, err := ar.RegisterAgent(ctx, identity, primaryAddr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "node role cannot be empty")
}

func TestUnregisterAgent_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test", "zone": "us-east"},
	}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
	require.NoError(t, registerErr)

	agentInfo, err := ar.GetAgent("192.168.1.1", 8080, "worker", map[string]string{"env": "test", "zone": "us-east"})

	require.NoError(t, err)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.IP, agentInfo.AgentIdentity.IP)
	assert.Equal(t, identity.Port, agentInfo.AgentIdentity.Port)
	assert.Equal(t, identity.Role, agentInfo.AgentIdentity.Role)
}

func TestGetAgent_NotFound(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	_, err := ar.GetAgent("192.168.1.1", 8080, "worker", nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent not found")
}

func TestGetAgent_LabelMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test"},
	}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	_, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
	require.NoError(t, registerErr)

	_, err := ar.GetAgent("192.168.1.1", 8080, "worker", map[string]string{"env": "prod"})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent not found")
}

func TestGetAgentByID_Success(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
	require.NoError(t, registerErr)

	agentInfo, err := ar.GetAgentByID(agentID)

	require.NoError(t, err)
	assert.Equal(t, agentID, agentInfo.AgentID)
	assert.Equal(t, identity.IP, agentInfo.AgentIdentity.IP)
	assert.Equal(t, identity.Port, agentInfo.AgentIdentity.Port)
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
	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	identity2 := AgentIdentity{IP: "192.168.1.2", Port: 8080, Role: "master"}
	identity3 := AgentIdentity{IP: "192.168.1.3", Port: 8080, Role: "worker"}

	agentID1, err1 := ar.RegisterAgent(ctx, identity1, Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, err1)
	agentID2, err2 := ar.RegisterAgent(ctx, identity2, Address{IP: "192.168.1.2", Port: 8080})
	require.NoError(t, err2)
	agentID3, err3 := ar.RegisterAgent(ctx, identity3, Address{IP: "192.168.1.3", Port: 8080})
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
	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	identity2 := AgentIdentity{IP: "192.168.1.2", Port: 8080, Role: "master"}
	identity3 := AgentIdentity{IP: "192.168.1.3", Port: 8080, Role: "worker"}

	agentID1, err1 := ar.RegisterAgent(ctx, identity1, Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, err1)
	agentID2, err2 := ar.RegisterAgent(ctx, identity2, Address{IP: "192.168.1.2", Port: 8080})
	require.NoError(t, err2)
	agentID3, err3 := ar.RegisterAgent(ctx, identity3, Address{IP: "192.168.1.3", Port: 8080})
	require.NoError(t, err3)

	workerAgents := ar.ListAgentsByRole("worker")
	masterAgents := ar.ListAgentsByRole("master")

	assert.Equal(t, 2, len(workerAgents))
	workerIDs := make(map[string]bool)
	for _, agentInfo := range workerAgents {
		workerIDs[agentInfo.AgentID] = true
		assert.Equal(t, "worker", agentInfo.NodeRole)
	}
	assert.True(t, workerIDs[agentID1])
	assert.True(t, workerIDs[agentID3])

	assert.Equal(t, 1, len(masterAgents))
	assert.Equal(t, agentID2, masterAgents[0].AgentID)
	assert.Equal(t, "master", masterAgents[0].NodeRole)
}

func TestListAgentsByRole_Empty(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	agents := ar.ListAgentsByRole("worker")

	assert.NotNil(t, agents)
	assert.Equal(t, 0, len(agents))
}

func TestCheckAgentHealth_HeartbeatTimeout(t *testing.T) {
	heartbeatTimeout := 50 * time.Millisecond
	cleanupTimeout := 200 * time.Millisecond
	ar := newTestRegistry(t, heartbeatTimeout, cleanupTimeout, 100)
	defer ar.Stop()

	ctx := context.Background()
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test", "zone": "us-east"},
	}
	identity2 := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test", "zone": "us-east"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.True(t, result)
}

func TestMatchesIdentity_IPMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", Labels: nil}
	identity2 := AgentIdentity{IP: "192.168.1.2", Port: 8080, Role: "worker", Labels: nil}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_PortMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", Labels: nil}
	identity2 := AgentIdentity{IP: "192.168.1.1", Port: 8081, Role: "worker", Labels: nil}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_RoleMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", Labels: nil}
	identity2 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "master", Labels: nil}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_LabelMismatch(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test"},
	}
	identity2 := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "prod"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_LabelKeyMissing(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test", "zone": "us-east"},
	}
	identity2 := AgentIdentity{
		IP:     "192.168.1.1",
		Port:   8080,
		Role:   "worker",
		Labels: map[string]string{"env": "test"},
	}

	result := ar.matchesIdentity(identity1, identity2)
	assert.False(t, result)
}

func TestMatchesIdentity_EmptyLabels(t *testing.T) {
	ar := newTestRegistry(t, 5*time.Second, 10*time.Second, 100)
	defer ar.Stop()

	identity1 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", Labels: nil}
	identity2 := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker", Labels: nil}

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
				IP:     "192.168.1.1",
				Port:   8080 + agentIdx,
				Role:   "worker",
				Labels: map[string]string{"idx": "test"},
			}
			primaryAddr := Address{IP: "192.168.1.1", Port: 8080 + agentIdx}
			_, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
	identity := AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	primaryAddr := Address{IP: "192.168.1.1", Port: 8080}

	agentID, registerErr := ar.RegisterAgent(ctx, identity, primaryAddr)
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
