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

package gossip

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var serverScope = observability.RootScope.SubScope("gossip_messenger_server")

type service struct {
	omr           observability.MetricsRegistry
	local         queue.Queue
	remoteClient  queue.Client
	remoteServer  queue.Server
	log           *logger.Logger
	serverMetrics *serverMetrics
	listeners     map[string][]MessageListener
	closer        *run.Closer
	futures       map[bus.MessageID]*future
	nodeID        string
	listenersLock sync.RWMutex
	futuresLock   sync.Mutex
}

// NewMessenger creates a new instance of Messenger for gossip propagation communication between nodes.
func NewMessenger(omr observability.MetricsRegistry, client queue.Client, server queue.Server) Messenger {
	return &service{
		omr:           omr,
		remoteClient:  client,
		remoteServer:  server,
		local:         queue.Local(),
		closer:        run.NewCloser(1),
		serverMetrics: newServerMetrics(omr.With(serverScope)),
		listeners:     make(map[string][]MessageListener),
		futures:       make(map[bus.MessageID]*future),
	}
}

func (s *service) PreRun(ctx context.Context) error {
	val := ctx.Value(common.ContextNodeKey)
	if val == nil {
		return errors.New("node id is empty")
	}
	node := val.(common.Node)
	s.nodeID = node.NodeID
	s.log = logger.GetLogger("gossip-messenger")
	s.listeners = make(map[string][]MessageListener)
	s.serverMetrics = newServerMetrics(s.omr.With(serverScope))

	propagation := &propagationServer{s: s}
	futureCallback := &futureCallbackServer{s: s}
	return multierr.Combine(
		s.local.Subscribe(data.TopicGossipPropagation, propagation),
		s.remoteServer.Subscribe(data.TopicGossipPropagation, propagation),
		s.remoteServer.Subscribe(data.TopicGossipFutureCallback, futureCallback),
	)
}

func (s *service) Name() string {
	return "gossip-messenger"
}

func (s *service) Role() databasev1.Role {
	return databasev1.Role_ROLE_UNSPECIFIED
}

func (s *service) FlagSet() *run.FlagSet {
	fs := run.NewFlagSet("gossip-messenger")
	return fs
}

func (s *service) Validate() error {
	return nil
}

func (s *service) Serve() run.StopNotify {
	return s.closer.CloseNotify()
}

func (s *service) GracefulStop() {
	s.closer.Done()
	s.closer.CloseThenWait()
}

func (s *service) removeFromWait(f *future) {
	s.futuresLock.Lock()
	defer s.futuresLock.Unlock()
	delete(s.futures, f.messageID)
}

func (s *service) addToWait(f *future) {
	s.futuresLock.Lock()
	defer s.futuresLock.Unlock()
	s.futures[f.messageID] = f
}

func (s *service) getFromWait(msgID bus.MessageID) *future {
	s.futuresLock.Lock()
	defer s.futuresLock.Unlock()
	return s.futures[msgID]
}
