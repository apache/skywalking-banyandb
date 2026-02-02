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

// Package cluster provides cluster state collection functionality for FODC agent.
package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	maxRetries           = 3
	initialBackoff       = 100 * time.Millisecond
	maxBackoff           = 5 * time.Second
	nodeInfoFetchTimeout = 30 * time.Second
)

// TopologyMap represents processed cluster data for a single endpoint.
type TopologyMap struct {
	Nodes []*databasev1.Node `json:"nodes"`
	Calls []*fodcv1.Call     `json:"calls"`
}

// endpointClient holds gRPC connection and clients for a single endpoint.
type endpointClient struct {
	conn            *grpc.ClientConn
	clusterClient   databasev1.ClusterStateServiceClient
	nodeQueryClient databasev1.NodeQueryServiceClient
	addr            string
}

// Collector collects cluster state from BanyanDB nodes via gRPC and stores the data.
type Collector struct {
	log             *logger.Logger
	closer          *run.Closer
	clients         []*endpointClient
	currentNodes    map[string]*databasev1.Node
	clusterTopology TopologyMap
	nodeFetchedCh   chan struct{}
	podName         string
	addrs           []string
	interval        time.Duration
	mu              sync.RWMutex
}

// NewCollector creates a new cluster state collector.
func NewCollector(log *logger.Logger, addrs []string, interval time.Duration, podName string) *Collector {
	return &Collector{
		log:             log,
		addrs:           addrs,
		interval:        interval,
		podName:         podName,
		closer:          run.NewCloser(0),
		nodeFetchedCh:   make(chan struct{}),
		clients:         make([]*endpointClient, 0, len(addrs)),
		currentNodes:    make(map[string]*databasev1.Node),
		clusterTopology: TopologyMap{},
	}
}

// Start starts the cluster state collector.
func (c *Collector) Start(ctx context.Context) error {
	if c.closer.Closed() {
		return fmt.Errorf("cluster state collector has been stopped and cannot be restarted")
	}
	if !c.closer.AddRunning() {
		return fmt.Errorf("cluster state collector is already started and cannot be started again")
	}
	for _, addr := range c.addrs {
		conn, connErr := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if connErr != nil {
			c.closeAllConnections()
			c.closer.Done()
			return fmt.Errorf("failed to create gRPC connection to %s: %w", addr, connErr)
		}
		client := &endpointClient{
			conn:            conn,
			clusterClient:   databasev1.NewClusterStateServiceClient(conn),
			nodeQueryClient: databasev1.NewNodeQueryServiceClient(conn),
			addr:            addr,
		}
		c.clients = append(c.clients, client)
	}
	go c.collectLoop(ctx)
	c.log.Info().Strs("addrs", c.addrs).Dur("interval", c.interval).Msg("Cluster state collector started")
	return nil
}

func (c *Collector) closeAllConnections() {
	for _, client := range c.clients {
		if client.conn != nil {
			if closeErr := client.conn.Close(); closeErr != nil {
				c.log.Warn().Str("addr", client.addr).Err(closeErr).Msg("Error closing gRPC connection")
			}
		}
	}
	c.clients = nil
}

// Stop stops the cluster state collector.
func (c *Collector) Stop() {
	c.closer.CloseThenWait()
	c.mu.Lock()
	c.closeAllConnections()
	c.mu.Unlock()
	c.log.Info().Msg("Cluster state collector stopped")
}

// GetCurrentNodes returns all current node info, keyed by endpoint address.
func (c *Collector) GetCurrentNodes() map[string]*databasev1.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]*databasev1.Node, len(c.currentNodes))
	for addr, node := range c.currentNodes {
		result[addr] = node
	}
	return result
}

// GetClusterTopology returns the aggregated cluster topology across all endpoints.
func (c *Collector) GetClusterTopology() TopologyMap {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterTopology
}

// SetClusterTopology sets the cluster topology data, primarily for tests.
func (c *Collector) SetClusterTopology(topology TopologyMap) {
	c.mu.Lock()
	c.clusterTopology = topology
	c.mu.Unlock()
}

// GetNodeInfo returns the processed node role and labels from the first available current node.
func (c *Collector) GetNodeInfo() (nodeRole string, nodeLabels map[string]string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, node := range c.currentNodes {
		if node != nil {
			return NodeRoleFromNode(node), node.Labels
		}
	}
	return NodeRoleFromNode(nil), nil
}

// WaitForNodeFetched waits until the current node info has been fetched.
func (c *Collector) WaitForNodeFetched(ctx context.Context) error {
	select {
	case <-c.nodeFetchedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Collector) collectLoop(ctx context.Context) {
	defer c.closer.Done()
	c.pollCurrentNode(ctx)
	close(c.nodeFetchedCh)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	c.collectClusterState(ctx)
	for {
		select {
		case <-c.closer.CloseNotify():
			return
		case <-ticker.C:
			c.collectClusterState(ctx)
		}
	}
}

func (c *Collector) pollCurrentNode(ctx context.Context) {
	nodes := c.fetchCurrentNodes(ctx)
	if len(nodes) == 0 {
		c.log.Error().Msg("Failed to fetch current node info from any endpoint")
		return
	}
	c.updateCurrentNodes(nodes)
}

func (c *Collector) updateCurrentNodes(nodes map[string]*databasev1.Node) {
	if len(nodes) == 0 {
		c.log.Warn().Msg("Received empty current nodes map")
		return
	}
	c.mu.Lock()
	c.currentNodes = nodes
	c.mu.Unlock()
	c.log.Info().Int("nodes_count", len(nodes)).Msg("Updated current nodes from all endpoints")
}

func (c *Collector) updateClusterStates(states map[string]*databasev1.GetClusterStateResponse) {
	if len(states) == 0 {
		c.log.Warn().Msg("Received empty cluster states map")
		return
	}
	c.mu.Lock()
	currentNodesCopy := make(map[string]*databasev1.Node, len(c.currentNodes))
	for addr, node := range c.currentNodes {
		currentNodesCopy[addr] = node
	}
	c.mu.Unlock()
	c.processClusterStates(currentNodesCopy, states)
	c.log.Info().Int("states_count", len(states)).Msg("Updated cluster states from all endpoints")
}

func (c *Collector) processClusterStates(currentNodes map[string]*databasev1.Node, clusterStates map[string]*databasev1.GetClusterStateResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	merged := TopologyMap{
		Nodes: make([]*databasev1.Node, 0),
		Calls: make([]*fodcv1.Call, 0),
	}
	nodeMap := make(map[string]*databasev1.Node)
	callMap := make(map[string]*fodcv1.Call)
	allAddrs := make(map[string]bool)

	for addr := range clusterStates {
		if _, hasCurrentNode := currentNodes[addr]; hasCurrentNode {
			allAddrs[addr] = true
		}
	}
	for addrStr := range allAddrs {
		currentNode := currentNodes[addrStr]
		clusterState := clusterStates[addrStr]
		if currentNode == nil || clusterState == nil {
			continue
		}
		currentNodeName := ""
		if currentNode.Metadata != nil && currentNode.Metadata.Name != "" {
			currentNodeName = currentNode.Metadata.Name
			if currentNode.Labels == nil {
				currentNode.Labels = make(map[string]string)
			}
			if c.podName != "" {
				currentNode.Labels["pod_name"] = c.podName
			}
			nodeMap[currentNodeName] = currentNode
		}
		for _, routeTable := range clusterState.RouteTables {
			if routeTable != nil {
				for _, registeredNode := range routeTable.Registered {
					if registeredNode != nil && registeredNode.Metadata != nil && registeredNode.Metadata.Name != "" {
						if registeredNode.Labels == nil {
							registeredNode.Labels = make(map[string]string)
						}
						if c.podName != "" {
							registeredNode.Labels["pod_name"] = c.podName
						}
						nodeMap[registeredNode.Metadata.Name] = registeredNode
					}
				}
			}
		}
		if currentNodeName != "" {
			registeredSet := make(map[string]bool)
			for _, routeTable := range clusterState.RouteTables {
				if routeTable != nil {
					for _, registeredNode := range routeTable.Registered {
						if registeredNode != nil && registeredNode.Metadata != nil && registeredNode.Metadata.Name != "" {
							registeredSet[registeredNode.Metadata.Name] = true
						}
					}
				}
			}
			for registeredName := range registeredSet {
				if registeredName != currentNodeName {
					callID := fmt.Sprintf("%s-%s", currentNodeName, registeredName)
					callMap[callID] = &fodcv1.Call{
						Id:     callID,
						Source: currentNodeName,
						Target: registeredName,
					}
				}
			}
		}
	}
	for _, node := range nodeMap {
		merged.Nodes = append(merged.Nodes, node)
	}
	for _, call := range callMap {
		merged.Calls = append(merged.Calls, call)
	}
	c.clusterTopology = merged
}

func (c *Collector) fetchCurrentNodes(ctx context.Context) map[string]*databasev1.Node {
	if len(c.clients) == 0 {
		return nil
	}
	nodes := make(map[string]*databasev1.Node)
	for _, client := range c.clients {
		node, fetchErr := c.fetchCurrentNodeFromEndpoint(ctx, client)
		if fetchErr != nil {
			c.log.Warn().Str("addr", client.addr).Err(fetchErr).Msg("Failed to fetch current node from endpoint")
			continue
		}
		if node != nil {
			nodes[client.addr] = node
			c.log.Info().Str("addr", client.addr).Msg("Successfully fetched current node from endpoint")
		}
	}
	return nodes
}

func (c *Collector) fetchCurrentNodeFromEndpoint(ctx context.Context, client *endpointClient) (*databasev1.Node, error) {
	if client.nodeQueryClient == nil {
		return nil, fmt.Errorf("node query client not initialized")
	}
	var resp *databasev1.GetCurrentNodeResponse
	var respErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, respErr = client.nodeQueryClient.GetCurrentNode(reqCtx, &databasev1.GetCurrentNodeRequest{})
		cancel()
		if respErr == nil {
			return resp.GetNode(), nil
		}
		if attempt < maxRetries-1 {
			c.log.Warn().Str("addr", client.addr).Int("attempt", attempt+1).Int("max_retries", maxRetries).Err(respErr).Msg("Failed to fetch current node, retrying")
		}
	}
	return nil, fmt.Errorf("failed to fetch current node after %d attempts: %w", maxRetries, respErr)
}

func (c *Collector) collectClusterState(ctx context.Context) {
	states := c.fetchClusterStates(ctx)
	if len(states) == 0 {
		c.log.Error().Msg("Failed to fetch cluster state from any endpoint")
		return
	}
	c.updateClusterStates(states)
}

func (c *Collector) fetchClusterStates(ctx context.Context) map[string]*databasev1.GetClusterStateResponse {
	if len(c.clients) == 0 {
		return nil
	}
	states := make(map[string]*databasev1.GetClusterStateResponse)
	for _, client := range c.clients {
		state, fetchErr := c.fetchClusterStateFromEndpoint(ctx, client)
		if fetchErr != nil {
			c.log.Warn().Str("addr", client.addr).Err(fetchErr).Msg("Failed to fetch cluster state from endpoint")
			continue
		}
		if state != nil {
			states[client.addr] = state
			c.log.Info().Str("addr", client.addr).Msg("Successfully fetched cluster state from endpoint")
		}
	}
	return states
}

func (c *Collector) fetchClusterStateFromEndpoint(ctx context.Context, client *endpointClient) (*databasev1.GetClusterStateResponse, error) {
	if client.clusterClient == nil {
		return nil, fmt.Errorf("gRPC client not initialized")
	}
	var resp *databasev1.GetClusterStateResponse
	var respErr error
	backoff := initialBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, respErr = client.clusterClient.GetClusterState(reqCtx, &databasev1.GetClusterStateRequest{})
		cancel()
		if respErr == nil {
			return resp, nil
		}
		if attempt < maxRetries-1 {
			c.log.Warn().Str("addr", client.addr).Int("attempt", attempt+1).Int("max_retries", maxRetries).Err(respErr).Msg("Failed to fetch cluster state, retrying")
		}
	}
	return nil, fmt.Errorf("failed to fetch cluster state after %d attempts: %w", maxRetries, respErr)
}

// NodeRoleFromNode determines the node role string from the Node's role and labels.
func NodeRoleFromNode(node *databasev1.Node) string {
	if node == nil || len(node.Roles) == 0 {
		return databasev1.Role_name[int32(databasev1.Role_ROLE_UNSPECIFIED)]
	}
	for _, r := range node.Roles {
		switch r {
		case databasev1.Role_ROLE_LIAISON:
			return databasev1.Role_name[int32(databasev1.Role_ROLE_LIAISON)]
		case databasev1.Role_ROLE_DATA:
			return databasev1.Role_name[int32(databasev1.Role_ROLE_DATA)]
		default:
			continue
		}
	}
	return databasev1.Role_name[int32(databasev1.Role_ROLE_UNSPECIFIED)]
}

// GenerateClusterStateAddrs generates cluster state gRPC addresses from the given ports.
func GenerateClusterStateAddrs(ports []string) []string {
	addrs := make([]string, 0, len(ports))
	for _, port := range ports {
		addrs = append(addrs, fmt.Sprintf("localhost:%s", port))
	}
	return addrs
}

// StartCollector creates and starts a cluster state collector.
func StartCollector(ctx context.Context, log *logger.Logger, ports []string, interval time.Duration, podName string) (*Collector, error) {
	if len(ports) == 0 {
		return nil, nil
	}
	addrs := GenerateClusterStateAddrs(ports)
	log.Info().Strs("addrs", addrs).Msg("Starting cluster state collector")
	collector := NewCollector(log, addrs, interval, podName)
	if startErr := collector.Start(ctx); startErr != nil {
		return nil, fmt.Errorf("failed to start cluster collector: %w", startErr)
	}
	waitCtx, cancel := context.WithTimeout(ctx, nodeInfoFetchTimeout)
	defer cancel()
	if waitErr := collector.WaitForNodeFetched(waitCtx); waitErr != nil {
		log.Warn().Err(waitErr).Msg("Failed to fetch node info from lifecycle service; continuing without initial node info")
		return collector, nil
	}
	if len(collector.GetCurrentNodes()) == 0 {
		log.Warn().Msg("No node info received from lifecycle service; continuing without initial node info")
		return collector, nil
	}
	return collector, nil
}

// StopCollector stops the collector if it's not nil.
func StopCollector(collector *Collector) {
	if collector != nil {
		collector.Stop()
	}
}
