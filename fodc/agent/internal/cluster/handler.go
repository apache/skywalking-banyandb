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

package cluster

import (
	"sync"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Handler handles cluster state updates and stores cluster data.
type Handler struct {
	log          *logger.Logger
	currentNode  *databasev1.Node
	clusterState *databasev1.GetClusterStateResponse
	mu           sync.RWMutex
}

// NewHandler creates a new cluster state handler.
func NewHandler(log *logger.Logger) *Handler {
	return &Handler{
		log: log,
	}
}

// OnCurrentNodeUpdate is called when current node info is fetched.
func (h *Handler) OnCurrentNodeUpdate(node *databasev1.Node) {
	if node == nil {
		if h.log != nil {
			h.log.Warn().Msg("Received nil current node")
		}
		return
	}
	h.mu.Lock()
	h.currentNode = node
	h.mu.Unlock()
	if h.log != nil {
		nodeName := ""
		if node.Metadata != nil {
			nodeName = node.Metadata.Name
		}
		h.log.Info().Str("node_name", nodeName).Msg("Updated current node info")
	}
}

// OnClusterStateUpdate is called when cluster state is updated.
func (h *Handler) OnClusterStateUpdate(state *databasev1.GetClusterStateResponse) {
	if state == nil {
		if h.log != nil {
			h.log.Warn().Msg("Received nil cluster state")
		}
		return
	}
	h.mu.Lock()
	h.clusterState = state
	h.mu.Unlock()
	if h.log != nil {
		h.log.Debug().
			Int("route_tables_count", len(state.RouteTables)).
			Msg("Updated cluster state")
	}
}

// GetClusterData returns the current node info and cluster state.
func (h *Handler) GetClusterData() (*databasev1.Node, *databasev1.GetClusterStateResponse) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.currentNode, h.clusterState
}
