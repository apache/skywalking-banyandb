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
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	maxRetries           = 3
	initialBackoff       = 100 * time.Millisecond
	maxBackoff           = 5 * time.Second
	nodeInfoFetchTimeout = 30 * time.Second
)

// Collector collects cluster state from BanyanDB node via gRPC and stores the data.
type Collector struct {
	log             *logger.Logger
	closer          *run.Closer
	conn            *grpc.ClientConn
	clusterClient   databasev1.ClusterStateServiceClient
	nodeQueryClient databasev1.NodeQueryServiceClient
	currentNode     *databasev1.Node
	clusterState    *databasev1.GetClusterStateResponse
	nodeFetchedCh   chan struct{}
	lifecycleAddr   string
	interval        time.Duration
	mu              sync.RWMutex
}

// NewCollector creates a new cluster state collector.
func NewCollector(log *logger.Logger, lifecycleAddr string, interval time.Duration) *Collector {
	return &Collector{
		log:           log,
		lifecycleAddr: lifecycleAddr,
		interval:      interval,
		closer:        run.NewCloser(0),
		nodeFetchedCh: make(chan struct{}),
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
	if c.log == nil {
		c.log = logger.GetLogger("cluster-collector")
	}
	var connErr error
	c.conn, connErr = grpc.NewClient(c.lifecycleAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if connErr != nil {
		c.closer.Done()
		return fmt.Errorf("failed to create gRPC connection: %w", connErr)
	}
	c.clusterClient = databasev1.NewClusterStateServiceClient(c.conn)
	c.nodeQueryClient = databasev1.NewNodeQueryServiceClient(c.conn)
	go c.collectLoop(ctx)
	c.log.Info().Str("lifecycle_addr", c.lifecycleAddr).Dur("interval", c.interval).Msg("Cluster state collector started")
	return nil
}

// Stop stops the cluster state collector.
func (c *Collector) Stop() {
	c.closer.CloseThenWait()
	c.mu.Lock()
	if c.conn != nil {
		if closeErr := c.conn.Close(); closeErr != nil && c.log != nil {
			c.log.Warn().Err(closeErr).Msg("Error closing gRPC connection")
		}
		c.conn = nil
	}
	c.mu.Unlock()
	if c.log != nil {
		c.log.Info().Msg("Cluster state collector stopped")
	}
}

// GetCurrentNode returns the current node info.
func (c *Collector) GetCurrentNode() *databasev1.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentNode
}

// GetClusterState returns the cluster state.
func (c *Collector) GetClusterState() *databasev1.GetClusterStateResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterState
}

// GetNodeInfo returns the processed node role and labels from the current node.
func (c *Collector) GetNodeInfo() (nodeRole string, nodeLabels map[string]string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.currentNode == nil {
		return "", nil
	}
	return NodeRoleFromNode(c.currentNode), c.currentNode.Labels
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
	node, fetchErr := c.fetchCurrentNode(ctx)
	if fetchErr != nil {
		if c.log != nil {
			c.log.Error().Err(fetchErr).Msg("Failed to fetch current node info")
		}
		return
	}
	c.updateCurrentNode(node)
}

func (c *Collector) updateCurrentNode(node *databasev1.Node) {
	if node == nil {
		if c.log != nil {
			c.log.Warn().Msg("Received nil current node")
		}
		return
	}
	c.mu.Lock()
	c.currentNode = node
	c.mu.Unlock()
	if c.log != nil {
		nodeName := ""
		if node.Metadata != nil {
			nodeName = node.Metadata.Name
		}
		c.log.Info().Interface("node", node).Msg("Node details for debugging")
		c.log.Info().Str("node_name", nodeName).Msg("Updated current node info")
	}
}

func (c *Collector) updateClusterState(state *databasev1.GetClusterStateResponse) {
	if state == nil {
		if c.log != nil {
			c.log.Warn().Msg("Received nil cluster state")
		}
		return
	}
	c.mu.Lock()
	c.clusterState = state
	c.mu.Unlock()
	if c.log != nil {
		c.log.Debug().Int("route_tables_count", len(state.RouteTables)).Msg("Updated cluster state")
	}
}

func (c *Collector) fetchCurrentNode(ctx context.Context) (*databasev1.Node, error) {
	if c.nodeQueryClient == nil {
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
		resp, respErr = c.nodeQueryClient.GetCurrentNode(reqCtx, &databasev1.GetCurrentNodeRequest{})
		cancel()
		if respErr == nil {
			return resp.GetNode(), nil
		}
		if attempt < maxRetries-1 && c.log != nil {
			c.log.Warn().Int("attempt", attempt+1).Int("max_retries", maxRetries).Err(respErr).Msg("Failed to fetch current node, retrying")
		}
	}
	return nil, fmt.Errorf("failed to fetch current node after %d attempts: %w", maxRetries, respErr)
}

func (c *Collector) collectClusterState(ctx context.Context) {
	state, collectErr := c.fetchClusterState(ctx)
	c.log.Info().Interface("state", state).Msg("Cluster state details for debugging")
	if collectErr != nil {
		if c.log != nil {
			c.log.Error().Err(collectErr).Msg("Failed to fetch cluster state")
		}
		return
	}
	c.updateClusterState(state)
}

func (c *Collector) fetchClusterState(ctx context.Context) (*databasev1.GetClusterStateResponse, error) {
	if c.clusterClient == nil {
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
		resp, respErr = c.clusterClient.GetClusterState(reqCtx, &databasev1.GetClusterStateRequest{})
		cancel()
		if respErr == nil {
			return resp, nil
		}
		if attempt < maxRetries-1 && c.log != nil {
			c.log.Warn().Int("attempt", attempt+1).Int("max_retries", maxRetries).Err(respErr).Msg("Failed to fetch cluster state, retrying")
		}
	}
	return nil, fmt.Errorf("failed to fetch cluster state after %d attempts: %w", maxRetries, respErr)
}

// NodeRoleFromNode determines the node role string from the Node's role and labels.
func NodeRoleFromNode(node *databasev1.Node) string {
	if node == nil || len(node.Roles) == 0 {
		return "DATA_HOT"
	}
	for _, r := range node.Roles {
		switch r {
		case databasev1.Role_ROLE_LIAISON:
			return "LIAISON"
		case databasev1.Role_ROLE_DATA:
			if node.Labels != nil {
				if tier, ok := node.Labels["tier"]; ok && tier != "" {
					return "DATA_" + strings.ToUpper(tier)
				}
			}
			return "DATA"
		default:
			return "UNKNOWN"
		}
	}

	return "UNKNOWN"
}

// StartCollector creates and starts a cluster state collector.
func StartCollector(ctx context.Context, log *logger.Logger, addr string, interval time.Duration) (*Collector, error) {
	if addr == "" {
		return nil, nil
	}
	log.Info().Str("addr", addr).Msg("Starting cluster state collector")
	collector := NewCollector(log, addr, interval)
	if startErr := collector.Start(ctx); startErr != nil {
		return nil, fmt.Errorf("failed to start cluster collector: %w", startErr)
	}
	waitCtx, cancel := context.WithTimeout(ctx, nodeInfoFetchTimeout)
	defer cancel()
	if waitErr := collector.WaitForNodeFetched(waitCtx); waitErr != nil {
		collector.Stop()
		return nil, fmt.Errorf("failed to fetch node info: %w", waitErr)
	}
	if collector.GetCurrentNode() == nil {
		collector.Stop()
		return nil, fmt.Errorf("no node info received from lifecycle service")
	}
	return collector, nil
}

// StopCollector stops the collector if it's not nil.
func StopCollector(collector *Collector) {
	if collector != nil {
		collector.Stop()
	}
}
