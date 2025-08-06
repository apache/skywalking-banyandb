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
	context "context"
	"errors"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
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

func (l *local) Publish(ctx context.Context, topic bus.Topic, message ...bus.Message) (bus.Future, error) {
	return l.local.Publish(ctx, topic, message...)
}

func (l *local) Broadcast(timeout time.Duration, topic bus.Topic, message bus.Message) ([]bus.Future, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	f, err := l.Publish(ctx, topic, message)
	if err != nil {
		return nil, err
	}
	return []bus.Future{f}, nil
}

func (l *local) OnAddOrUpdate(_ schema.Metadata) {
	panic("unimplemented")
}

func (l local) Name() string {
	return "local-pipeline"
}

func (l local) NewBatchPublisher(_ time.Duration) BatchPublisher {
	return &localBatchPublisher{
		local: l.local,
	}
}

func (*local) GetPort() *uint32 {
	return nil
}

func (*local) Register(bus.Topic, schema.EventHandler) {
}

type localBatchPublisher struct {
	ctx      context.Context
	local    *bus.Bus
	topic    *bus.Topic
	messages []any
}

func (l *localBatchPublisher) Publish(ctx context.Context, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	if l.topic == nil {
		l.topic = &topic
	}
	if l.ctx == nil {
		l.ctx = ctx
	}
	for i := range messages {
		l.messages = append(l.messages, messages[i].Data())
	}
	return nil, nil
}

func (l *localBatchPublisher) Close() (map[string]*common.Error, error) {
	if l.local == nil || len(l.messages) == 0 {
		return nil, nil
	}
	newMessage := bus.NewMessage(1, l.messages)
	f, err := l.local.Publish(l.ctx, *l.topic, newMessage)
	l.messages = nil
	l.topic = nil
	if err != nil {
		var ce *common.Error
		if errors.As(err, &ce) {
			return map[string]*common.Error{"local": ce}, nil
		}
		return nil, err
	}
	if f == nil {
		return nil, nil
	}
	m, err := f.Get()
	if err != nil {
		return nil, err
	}
	if m.Data() != nil {
		if d, ok := m.Data().(*common.Error); ok {
			return map[string]*common.Error{"local": d}, nil
		}
	}
	return nil, nil
}

func (l *local) RegisterChunkedSyncHandler(_ bus.Topic, _ ChunkedSyncHandler) {
}

func (l *local) NewChunkedSyncClient(node string, _ uint32) (ChunkedSyncClient, error) {
	return &localChunkedSyncClient{local: l.local, node: node}, nil
}

type localChunkedSyncClient struct {
	local *bus.Bus
	node  string
}

func (l *localChunkedSyncClient) Close() error {
	return nil
}

func (l *localChunkedSyncClient) SyncStreamingParts(_ context.Context, parts []StreamingPartData) (*SyncResult, error) {
	return &SyncResult{
		Success:     true,
		SessionID:   "local-session",
		TotalBytes:  0,
		DurationMs:  0,
		ChunksCount: 0,
		PartsCount:  uint32(len(parts)),
	}, nil
}
