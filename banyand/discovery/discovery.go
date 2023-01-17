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

// Package discovery implements the service discovery.
package discovery

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

// ServiceRepo provides service subscripting and publishing.
//
//go:generate mockgen -destination=./discovery_mock.go -package=discovery github.com/apache/skywalking-banyandb/banyand/discovery ServiceRepo
type ServiceRepo interface {
	NodeID() string
	Name() string
	run.Unit
	bus.Subscriber
	bus.Publisher
	run.Service
}

type repo struct {
	local  *bus.Bus
	stopCh chan struct{}
}

func (r *repo) NodeID() string {
	return "local"
}

func (r *repo) Name() string {
	return "service-discovery"
}

func (r *repo) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	return r.local.Subscribe(topic, listener)
}

func (r *repo) Publish(topic bus.Topic, message ...bus.Message) (bus.Future, error) {
	return r.local.Publish(topic, message...)
}

// NewServiceRepo return a new ServiceRepo.
func NewServiceRepo(_ context.Context) (ServiceRepo, error) {
	return &repo{
		local:  bus.NewBus(),
		stopCh: make(chan struct{}),
	}, nil
}

func (r *repo) Serve() run.StopNotify {
	return r.stopCh
}

func (r *repo) GracefulStop() {
	r.local.Close()
	if r.stopCh != nil {
		close(r.stopCh)
	}
}
