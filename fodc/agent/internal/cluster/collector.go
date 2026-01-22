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
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	maxRetries     = 3
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
)

// StateHandler handles cluster state updates.
type StateHandler interface {
	// OnClusterStateUpdate is called when cluster state is updated.
	OnClusterStateUpdate(state *databasev1.GetClusterStateResponse)
	// OnCurrentNodeUpdate is called when current node info is fetched.
	OnCurrentNodeUpdate(node *databasev1.Node)
}

// Collector collects cluster state from BanyanDB node via gRPC.
type Collector struct {
	log             *logger.Logger
	closer          *run.Closer
	conn            *grpc.ClientConn
	handler         StateHandler
	clusterClient   databasev1.ClusterStateServiceClient
	nodeQueryClient databasev1.NodeQueryServiceClient

	nodeFetchedCh chan struct{}
	lifecycleAddr string
	interval      time.Duration
	mu            sync.RWMutex
}

// NewCollector creates a new cluster state collector.
func NewCollector(handler StateHandler, lifecycleAddr string, interval time.Duration) *Collector {
	return &Collector{
		handler:       handler,
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
	c.conn, connErr = grpc.NewClient(
		c.lifecycleAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if connErr != nil {
		c.closer.Done()
		return fmt.Errorf("failed to create gRPC connection: %w", connErr)
	}

	c.clusterClient = databasev1.NewClusterStateServiceClient(c.conn)
	c.nodeQueryClient = databasev1.NewNodeQueryServiceClient(c.conn)

	go c.collectLoop(ctx)

	c.log.Info().
		Str("lifecycle_addr", c.lifecycleAddr).
		Dur("interval", c.interval).
		Msg("Cluster state collector started")

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

// collectLoop continuously collects cluster state.
func (c *Collector) collectLoop(ctx context.Context) {
	defer c.closer.Done()
	c.fetchCurrentNodeOnce(ctx)
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

// collectClusterState performs a single collection operation.
func (c *Collector) collectClusterState(ctx context.Context) {
	state, collectErr := c.fetchClusterState(ctx)
	if collectErr != nil {
		if c.log != nil {
			c.log.Error().Err(collectErr).Msg("Failed to fetch cluster state")
		}
		return
	}
	if state != nil && c.handler != nil {
		c.handler.OnClusterStateUpdate(state)
	}
}

// fetchCurrentNodeOnce fetches current node info once from BanyanDB node via gRPC.
func (c *Collector) fetchCurrentNodeOnce(ctx context.Context) {
	node, fetchErr := c.fetchCurrentNode(ctx)
	if fetchErr != nil {
		if c.log != nil {
			c.log.Error().Err(fetchErr).Msg("Failed to fetch current node info")
		}
		return
	}
	if node != nil && c.handler != nil {
		c.handler.OnCurrentNodeUpdate(node)
	}
}

// fetchCurrentNode fetches current node from BanyanDB node via gRPC.
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
		if attempt < maxRetries-1 {
			if c.log != nil {
				c.log.Warn().
					Int("attempt", attempt+1).
					Int("max_retries", maxRetries).
					Err(respErr).
					Msg("Failed to fetch current node, retrying")
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch current node after %d attempts: %w", maxRetries, respErr)
}

// fetchClusterState fetches cluster state from BanyanDB lifecycle service via gRPC.
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
		if attempt < maxRetries-1 {
			if c.log != nil {
				c.log.Warn().
					Int("attempt", attempt+1).
					Int("max_retries", maxRetries).
					Err(respErr).
					Msg("Failed to fetch cluster state, retrying")
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch cluster state after %d attempts: %w", maxRetries, respErr)
}
