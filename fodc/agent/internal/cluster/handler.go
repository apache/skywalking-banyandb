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
	"context"
	"sync"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// ProxySender sends cluster state to proxy.
type ProxySender interface {
	// SendClusterState sends cluster state to proxy.
	SendClusterState(ctx context.Context, clusterState *databasev1.GetClusterStateResponse) error
}

// Handler handles cluster state updates and sends them to proxy.
type Handler struct {
	log         *logger.Logger
	proxySender ProxySender
	ctx         context.Context
	mu          sync.RWMutex
}

// NewHandler creates a new cluster state handler.
func NewHandler(ctx context.Context, proxySender ProxySender, log *logger.Logger) *Handler {
	return &Handler{
		log:         log,
		proxySender: proxySender,
		ctx:         ctx,
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

	if h.proxySender == nil {
		if h.log != nil {
			h.log.Warn().Msg("Proxy sender not available, skipping cluster state update")
		}
		return
	}

	if sendErr := h.proxySender.SendClusterState(h.ctx, state); sendErr != nil {
		if h.log != nil {
			h.log.Error().Err(sendErr).Msg("Failed to send cluster state to proxy")
		}
		return
	}

	if h.log != nil {
		h.log.Debug().
			Int("route_tables_count", len(state.RouteTables)).
			Msg("Sent cluster state to proxy")
	}
}
