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

// Package registry provides functionality for managing and tracking FODC agents.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// AgentStatus represents the current status of an agent.
type AgentStatus string

const (
	// AgentStatusOnline indicates the agent is online and connected.
	AgentStatusOnline AgentStatus = "online"
	// AgentStatusOffline indicates the agent is offline or unconnected.
	AgentStatusOffline AgentStatus = "unconnected"
)

// Address represents a network address.
type Address struct {
	IP   string
	Port int
}

// AgentIdentity represents the identity of an agent.
type AgentIdentity struct {
	Labels        map[string]string
	IP            string
	Role          string
	Port          int
	PodName       string
	ContainerName string
}

// AgentInfo contains information about a registered agent.
type AgentInfo struct {
	Labels         map[string]string
	RegisteredAt   time.Time
	LastHeartbeat  time.Time
	AgentID        string
	NodeRole       string
	Status         AgentStatus
	AgentIdentity  AgentIdentity
	PrimaryAddress Address
}

// AgentRegistry manages the lifecycle and state of all connected FODC Agents.
type AgentRegistry struct {
	agents            map[string]*AgentInfo
	logger            *logger.Logger
	healthCheckTicker *time.Ticker
	healthCheckStopCh chan struct{}
	mu                sync.RWMutex
	heartbeatTimeout  time.Duration
	cleanupTimeout    time.Duration
	maxAgents         int
}

// NewAgentRegistry creates a new AgentRegistry instance.
func NewAgentRegistry(logger *logger.Logger, heartbeatTimeout, cleanupTimeout time.Duration, maxAgents int) *AgentRegistry {
	ar := &AgentRegistry{
		agents:            make(map[string]*AgentInfo),
		logger:            logger,
		heartbeatTimeout:  heartbeatTimeout,
		maxAgents:         maxAgents,
		cleanupTimeout:    cleanupTimeout,
		healthCheckStopCh: make(chan struct{}),
	}
	ar.startHealthCheck()
	return ar
}

// RegisterAgent registers a new agent or updates existing agent information.
func (ar *AgentRegistry) RegisterAgent(_ context.Context, identity AgentIdentity, primaryAddr Address) (string, error) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if len(ar.agents) >= ar.maxAgents {
		return "", fmt.Errorf("maximum number of agents (%d) reached", ar.maxAgents)
	}

	if primaryAddr.IP == "" {
		return "", fmt.Errorf("primary address IP cannot be empty")
	}
	if primaryAddr.Port <= 0 {
		return "", fmt.Errorf("primary address port must be greater than 0")
	}
	if identity.Role == "" {
		return "", fmt.Errorf("node role cannot be empty")
	}

	agentID := uuid.New().String()
	now := time.Now()

	agentInfo := &AgentInfo{
		AgentID:        agentID,
		AgentIdentity:  identity,
		NodeRole:       identity.Role,
		PrimaryAddress: primaryAddr,
		Labels:         identity.Labels,
		RegisteredAt:   now,
		LastHeartbeat:  now,
		Status:         AgentStatusOnline,
	}

	ar.agents[agentID] = agentInfo

	ar.logger.Info().
		Str("agent_id", agentID).
		Str("ip", primaryAddr.IP).
		Int("port", primaryAddr.Port).
		Str("role", identity.Role).
		Str("pod_name", identity.PodName).
		Str("container_name", identity.ContainerName).
		Msg("Agent registered")

	return agentID, nil
}

// UnregisterAgent removes an agent from the registry.
func (ar *AgentRegistry) UnregisterAgent(agentID string) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	agentInfo, exists := ar.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	delete(ar.agents, agentID)

	ar.logger.Info().
		Str("agent_id", agentID).
		Str("ip", agentInfo.PrimaryAddress.IP).
		Int("port", agentInfo.PrimaryAddress.Port).
		Str("role", agentInfo.NodeRole).
		Str("pod_name", agentInfo.AgentIdentity.PodName).
		Str("container_name", agentInfo.AgentIdentity.ContainerName).
		Msg("Agent unregistered")

	return nil
}

// UpdateHeartbeat updates the last heartbeat timestamp for an agent.
func (ar *AgentRegistry) UpdateHeartbeat(agentID string) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	agentInfo, exists := ar.agents[agentID]
	if !exists {
		return fmt.Errorf("agent %s not found", agentID)
	}

	agentInfo.LastHeartbeat = time.Now()
	agentInfo.Status = AgentStatusOnline

	return nil
}

// GetAgent retrieves agent information by primary IP + port + role + labels.
func (ar *AgentRegistry) GetAgent(ip string, port int, role string, labels map[string]string) (*AgentInfo, error) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	identity := AgentIdentity{
		IP:     ip,
		Port:   port,
		Role:   role,
		Labels: labels,
	}

	for _, agentInfo := range ar.agents {
		if ar.matchesIdentity(agentInfo.AgentIdentity, identity) {
			infoCopy := *agentInfo
			return &infoCopy, nil
		}
	}

	return nil, fmt.Errorf("agent not found")
}

// GetAgentByID retrieves agent information by unique agent ID.
func (ar *AgentRegistry) GetAgentByID(agentID string) (*AgentInfo, error) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agentInfo, exists := ar.agents[agentID]
	if !exists {
		return nil, fmt.Errorf("agent %s not found", agentID)
	}

	infoCopy := *agentInfo
	return &infoCopy, nil
}

// ListAgents returns a list of all registered agents.
func (ar *AgentRegistry) ListAgents() []*AgentInfo {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agents := make([]*AgentInfo, 0, len(ar.agents))
	for _, agentInfo := range ar.agents {
		infoCopy := *agentInfo
		agents = append(agents, &infoCopy)
	}

	return agents
}

// ListAgentsByRole returns agents filtered by role.
func (ar *AgentRegistry) ListAgentsByRole(role string) []*AgentInfo {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	agents := make([]*AgentInfo, 0)
	for _, agentInfo := range ar.agents {
		if agentInfo.NodeRole == role {
			infoCopy := *agentInfo
			agents = append(agents, &infoCopy)
		}
	}

	return agents
}

// CheckAgentHealth periodically checks agent health based on heartbeat timeout.
func (ar *AgentRegistry) CheckAgentHealth() error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	now := time.Now()

	for agentID, agentInfo := range ar.agents {
		timeSinceHeartbeat := now.Sub(agentInfo.LastHeartbeat)

		if timeSinceHeartbeat > ar.heartbeatTimeout {
			if agentInfo.Status == AgentStatusOnline {
				agentInfo.Status = AgentStatusOffline
				ar.logger.Warn().
					Str("agent_id", agentID).
					Dur("time_since_heartbeat", timeSinceHeartbeat).
					Msg("Agent marked as offline due to heartbeat timeout")
			}

			if timeSinceHeartbeat > ar.cleanupTimeout {
				delete(ar.agents, agentID)
				ar.logger.Info().
					Str("agent_id", agentID).
					Str("ip", agentInfo.PrimaryAddress.IP).
					Int("port", agentInfo.PrimaryAddress.Port).
					Str("role", agentInfo.NodeRole).
					Str("pod_name", agentInfo.AgentIdentity.PodName).
					Str("container_name", agentInfo.AgentIdentity.ContainerName).
					Msg("Agent unregistered")
			}
		}
	}

	return nil
}

// Stop stops the health check goroutine.
func (ar *AgentRegistry) Stop() {
	close(ar.healthCheckStopCh)
	if ar.healthCheckTicker != nil {
		ar.healthCheckTicker.Stop()
	}
}

// matchesIdentity checks if two agent identities match.
func (ar *AgentRegistry) matchesIdentity(identity1, identity2 AgentIdentity) bool {
	if identity1.IP != identity2.IP {
		return false
	}
	if identity1.Port != identity2.Port {
		return false
	}
	if identity1.Role != identity2.Role {
		return false
	}

	if len(identity1.Labels) != len(identity2.Labels) {
		return false
	}

	for key, value1 := range identity1.Labels {
		value2, exists := identity2.Labels[key]
		if !exists || value1 != value2 {
			return false
		}
	}

	return true
}

// startHealthCheck starts a background goroutine that periodically checks agent health.
func (ar *AgentRegistry) startHealthCheck() {
	ar.healthCheckTicker = time.NewTicker(ar.heartbeatTimeout / 2)

	go func() {
		for {
			select {
			case <-ar.healthCheckTicker.C:
				if checkErr := ar.CheckAgentHealth(); checkErr != nil {
					ar.logger.Error().Err(checkErr).Msg("Error during agent health check")
				}
			case <-ar.healthCheckStopCh:
				return
			}
		}
	}()
}
