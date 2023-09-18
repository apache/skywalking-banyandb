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

// Package queue implements the data transmission queue.
package queue

import (
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ bus.Publisher  = (*local)(nil)
	_ bus.Subscriber = (*local)(nil)
)

type local struct {
	local  *bus.Bus
	stopCh chan struct{}
}

// Local return a new local Queue.
func Local() Queue {
	return &local{
		local:  bus.NewBus(),
		stopCh: make(chan struct{}),
	}
}

// GracefulStop implements Queue.
func (l *local) GracefulStop() {
	l.local.Close()
	if l.stopCh != nil {
		close(l.stopCh)
	}
}

// Serve implements Queue.
func (l *local) Serve() run.StopNotify {
	return l.stopCh
}

func (l *local) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	return l.local.Subscribe(topic, listener)
}

func (l *local) Publish(topic bus.Topic, message ...bus.Message) (bus.Future, error) {
	return l.local.Publish(topic, message...)
}

func (l *local) Broadcast(topic bus.Topic, message bus.Message) ([]bus.Future, error) {
	f, err := l.Publish(topic, message)
	if err != nil {
		return nil, err
	}
	return []bus.Future{f}, nil
}

func (l local) Name() string {
	return "local-pipeline"
}

func (l local) NewBatchPublisher() BatchPublisher {
	return &localBatchPublisher{
		local: l.local,
	}
}

func (*local) GetPort() *uint32 {
	return nil
}

func (*local) Register(schema.EventHandler) {
}

type localBatchPublisher struct {
	local *bus.Bus
}

func (l *localBatchPublisher) Publish(topic bus.Topic, message ...bus.Message) (bus.Future, error) {
	return l.local.Publish(topic, message...)
}

func (l *localBatchPublisher) Close() error {
	return nil
}
