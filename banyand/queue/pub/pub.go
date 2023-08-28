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
	"fmt"
	"sync"
	"time"

	"go.uber.org/multierr"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
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
	log      *logger.Logger
	clients  map[string]*client
	closer   *run.Closer
	mu       sync.RWMutex
}

// GracefulStop implements run.Service.
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

func (*pub) Publish(_ bus.Topic, _ ...bus.Message) (bus.Future, error) {
	panic("unimplemented")
}

// NewBatchPublisher returns a new batch publisher.
func (p *pub) NewBatchPublisher() queue.BatchPublisher {
	return &batchPublisher{pub: p, streams: make(map[string]writeStream)}
}

// New returns a new queue client.
func New(metadata metadata.Repo) queue.Client {
	return &pub{
		metadata: metadata,
		closer:   run.NewCloser(1),
	}
}

// Name implements run.PreRunner.
func (*pub) Name() string {
	return "queue-client"
}

// PreRun implements run.PreRunner.
func (p *pub) PreRun(context.Context) error {
	p.log = logger.GetLogger("server-queue")
	p.metadata.RegisterHandler("queue-client", schema.KindNode, p)
	return nil
}

type writeStream struct {
	client clusterv1.Service_WriteClient
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

func (bp *batchPublisher) Publish(topic bus.Topic, message ...bus.Message) (bus.Future, error) {
	var err error
	for _, m := range message {
		if !m.IsRemote() {
			continue
		}
		r := &clusterv1.WriteRequest{
			Topic:     topic.String(),
			MessageId: uint64(m.ID()),
		}
		switch d := m.Data().(type) {
		case *streamv1.InternalWriteRequest:
			r.Request = &clusterv1.WriteRequest_Stream{Stream: d}
		case *measurev1.InternalWriteRequest:
			r.Request = &clusterv1.WriteRequest_Measure{Measure: d}
		default:
			err = multierr.Append(err, fmt.Errorf("invalid message type %T", d))
			continue
		}
		node := m.Node()

		sendData := func() (success bool) {
			if stream, ok := bp.streams[node]; !ok {
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
					err = multierr.Append(errSend, fmt.Errorf("failed to send message to node %s: %w", node, err))
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
		stream, errCreateStream := client.client.Write(ctx)
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
	return nil, err
}
