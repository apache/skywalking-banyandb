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

// Package pub implements the queue client.
package pub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ run.PreRunner = (*pub)(nil)
	_ run.Service   = (*pub)(nil)
)

type pub struct {
	metadata metadata.Repo
	handler  schema.EventHandler
	log      *logger.Logger
	clients  map[string]*client
	closer   *run.Closer
	mu       sync.RWMutex
}

func (p *pub) Register(handler schema.EventHandler) {
	p.handler = handler
}

func (p *pub) GracefulStop() {
	p.closer.Done()
	p.closer.CloseThenWait()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.clients {
		_ = c.conn.Close()
	}
}

// Serve implements run.Service.
func (p *pub) Serve() run.StopNotify {
	return p.closer.CloseNotify()
}

func (p *pub) Broadcast(topic bus.Topic, messages bus.Message) ([]bus.Future, error) {
	var names []string
	p.mu.RLock()
	for k := range p.clients {
		names = append(names, k)
	}
	p.mu.RUnlock()
	var futures []bus.Future
	for _, n := range names {
		f, err := p.Publish(topic, bus.NewMessageWithNode(messages.ID(), n, messages.Data()))
		if err != nil {
			return nil, err
		}
		futures = append(futures, f)
	}
	return futures, nil
}

func (p *pub) Publish(topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	var err error
	f := &future{}
	handleMessage := func(m bus.Message, err error) error {
		r, errSend := messageToRequest(topic, m)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to marshal message %T: %w", m, errSend))
		}
		node := m.Node()
		p.mu.Lock()
		client, ok := p.clients[node]
		p.mu.Unlock()
		if !ok {
			return multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
		}
		stream, errCreateStream := client.client.Send(context.Background())
		if err != nil {
			return multierr.Append(err, fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream))
		}
		errSend = stream.Send(r)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
		}
		f.clients = append(f.clients, stream)
		f.topics = append(f.topics, topic)
		return err
	}
	for _, m := range messages {
		err = handleMessage(m, err)
	}
	return f, err
}

// NewBatchPublisher returns a new batch publisher.
func (p *pub) NewBatchPublisher() queue.BatchPublisher {
	return &batchPublisher{pub: p, streams: make(map[string]writeStream)}
}

// New returns a new queue client.
func New(metadata metadata.Repo) queue.Client {
	return &pub{
		metadata: metadata,
		clients:  make(map[string]*client),
		closer:   run.NewCloser(1),
	}
}

func (*pub) Name() string {
	return "queue-client"
}

func (p *pub) PreRun(context.Context) error {
	p.log = logger.GetLogger("server-queue")
	p.metadata.RegisterHandler("queue-client", schema.KindNode, p)
	return nil
}

type writeStream struct {
	client clusterv1.Service_SendClient
	cancel func()
}

type batchPublisher struct {
	pub     *pub
	streams map[string]writeStream
}

func (bp *batchPublisher) Close() (err error) {
	for _, ws := range bp.streams {
		err = multierr.Append(err, ws.client.CloseSend())
		ws.cancel()
	}
	return err
}

func (bp *batchPublisher) Publish(topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	for _, m := range messages {
		r, err := messageToRequest(topic, m)
		if err != nil {
			err = multierr.Append(err, fmt.Errorf("failed to marshal message %T: %w", m, err))
			continue
		}
		node := m.Node()
		sendData := func() (success bool) {
			if stream, ok := bp.streams[node]; ok {
				defer func() {
					if !success {
						delete(bp.streams, node)
						stream.cancel()
					}
				}()
				select {
				case <-stream.client.Context().Done():
					return false
				default:
				}
				errSend := stream.client.Send(r)
				if errSend != nil {
					err = multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
					return false
				}
				_, errRecv := stream.client.Recv()
				return errRecv == nil
			}
			return false
		}
		if sendData() {
			continue
		}

		bp.pub.mu.Lock()
		client, ok := bp.pub.clients[node]
		bp.pub.mu.Unlock()
		if !ok {
			err = multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
			continue
		}
		//nolint: govet
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, errCreateStream := client.client.Send(ctx)
		if err != nil {
			err = multierr.Append(err, fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream))
			continue
		}
		bp.streams[node] = writeStream{
			client: stream,
			cancel: cancel,
		}
		_ = sendData()
	}
	//nolint: govet
	return nil, nil
}

func messageToRequest(topic bus.Topic, m bus.Message) (*clusterv1.SendRequest, error) {
	r := &clusterv1.SendRequest{
		Topic:     topic.String(),
		MessageId: uint64(m.ID()),
	}
	message, ok := m.Data().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message type %T", m)
	}
	anyMessage, err := anypb.New(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message %T: %w", m, err)
	}
	r.Body = anyMessage
	return r, nil
}

type future struct {
	clients []clusterv1.Service_SendClient
	topics  []bus.Topic
}

func (l *future) Get() (bus.Message, error) {
	if len(l.clients) < 1 {
		return bus.Message{}, io.EOF
	}
	c := l.clients[0]
	t := l.topics[0]
	defer func() {
		l.clients = l.clients[1:]
		l.topics = l.topics[1:]
	}()
	resp, err := c.Recv()
	if err != nil {
		return bus.Message{}, err
	}
	if resp.Error != "" {
		return bus.Message{}, errors.New(resp.Error)
	}
	if resp.Body == nil {
		return bus.NewMessage(bus.MessageID(resp.MessageId), nil), nil
	}
	if messageSupplier, ok := data.TopicResponseMap[t]; ok {
		m := messageSupplier()
		err = resp.Body.UnmarshalTo(m)
		if err != nil {
			return bus.Message{}, err
		}
		return bus.NewMessage(
			bus.MessageID(resp.MessageId),
			m,
		), nil
	}
	return bus.Message{}, fmt.Errorf("invalid topic %s", t)
}

func (l *future) GetAll() ([]bus.Message, error) {
	var globalErr error
	ret := make([]bus.Message, 0, len(l.clients))
	for {
		m, err := l.Get()
		if errors.Is(err, io.EOF) {
			return ret, globalErr
		}
		if err != nil {
			globalErr = multierr.Append(globalErr, err)
			continue
		}
		ret = append(ret, m)
	}
}
