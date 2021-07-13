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

package discovery

import (
	"context"

	bus2 "github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type ServiceRepo interface {
	NodeID() string
	run.Unit
	bus2.Subscriber
	bus2.Publisher
}

type repo struct {
	local *bus2.Bus
}

func (r *repo) NodeID() string {
	return "local"
}

func (r *repo) Name() string {
	return "service-discovery"
}

func (r *repo) Subscribe(topic bus2.Topic, listener bus2.MessageListener) error {
	return r.local.Subscribe(topic, listener)
}

func (r *repo) Publish(topic bus2.Topic, message ...bus2.Message) (bus2.Future, error) {
	return r.local.Publish(topic, message...)
}

func NewServiceRepo(_ context.Context) (ServiceRepo, error) {
	return &repo{
		local: bus2.NewBus(),
	}, nil
}
