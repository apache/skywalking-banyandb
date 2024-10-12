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
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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
	schema.UnimplementedOnInitHandler
	metadata   metadata.Repo
	handler    schema.EventHandler
	log        *logger.Logger
	registered map[string]*databasev1.Node
	active     map[string]*client
	evictable  map[string]evictNode
	closer     *run.Closer
	mu         sync.RWMutex
}

func (p *pub) Register(handler schema.EventHandler) {
	p.handler = handler
}

func (p *pub) GracefulStop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.evictable {
		close(p.evictable[i].c)
	}
	p.evictable = nil
	p.closer.Done()
	p.closer.CloseThenWait()
	for _, c := range p.active {
		_ = c.conn.Close()
	}
	p.active = nil
}

// Serve implements run.Service.
func (p *pub) Serve() run.StopNotify {
	return p.closer.CloseNotify()
}

func (p *pub) Broadcast(timeout time.Duration, topic bus.Topic, messages bus.Message) ([]bus.Future, error) {
	var names []string
	p.mu.RLock()
	for k := range p.active {
		names = append(names, k)
	}
	p.mu.RUnlock()
	futureCh := make(chan publishResult, len(names))
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			f, err := p.publish(timeout, topic, bus.NewMessageWithNode(messages.ID(), n, messages.Data()))
			futureCh <- publishResult{n: n, f: f, e: err}
		}(n)
	}
	go func() {
		wg.Wait()
		close(futureCh)
	}()
	var futures []bus.Future
	var errs error
	for f := range futureCh {
		if f.e != nil {
			errs = multierr.Append(errs, errors.Wrapf(f.e, "failed to publish message to %s", f.n))
			if isFailoverError(f.e) {
				if p.closer.AddRunning() {
					go func() {
						defer p.closer.Done()
						p.failover(f.n)
					}()
				}
			}
			continue
		}
		futures = append(futures, f.f)
	}

	if errs != nil {
		return futures, fmt.Errorf("broadcast errors: %w", errs)
	}
	return futures, nil
}

type publishResult struct {
	f bus.Future
	e error
	n string
}

func (p *pub) publish(timeout time.Duration, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	var err error
	f := &future{}
	handleMessage := func(m bus.Message, err error) error {
		r, errSend := messageToRequest(topic, m)
		if errSend != nil {
			return multierr.Append(err, fmt.Errorf("failed to marshal message[%d]: %w", m.ID(), errSend))
		}
		node := m.Node()
		p.mu.RLock()
		client, ok := p.active[node]
		p.mu.RUnlock()
		if !ok {
			return multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		f.cancelFn = append(f.cancelFn, cancel)
		stream, errCreateStream := client.client.Send(ctx)
		if errCreateStream != nil {
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

func (p *pub) Publish(_ context.Context, _ bus.Topic, _ ...bus.Message) (bus.Future, error) {
	panic("should not be called")
}

// NewBatchPublisher returns a new batch publisher.
func (p *pub) NewBatchPublisher(timeout time.Duration) queue.BatchPublisher {
	return &batchPublisher{
		pub:     p,
		streams: make(map[string]writeStream),
		timeout: timeout,
		f:       batchFuture{errNodes: make(map[string]struct{}), l: p.log},
	}
}

// New returns a new queue client.
func New(metadata metadata.Repo) queue.Client {
	return &pub{
		metadata:   metadata,
		active:     make(map[string]*client),
		evictable:  make(map[string]evictNode),
		registered: make(map[string]*databasev1.Node),
		closer:     run.NewCloser(1),
	}
}

func (*pub) Name() string {
	return "queue-client"
}

func (p *pub) PreRun(context.Context) error {
	p.log = logger.GetLogger("server-queue-pub")
	p.metadata.RegisterHandler("queue-client", schema.KindNode, p)
	return nil
}

type writeStream struct {
	client    clusterv1.Service_SendClient
	ctxDoneCh <-chan struct{}
}

type batchPublisher struct {
	pub     *pub
	streams map[string]writeStream
	f       batchFuture
	timeout time.Duration
}

func (bp *batchPublisher) Close() (err error) {
	for i := range bp.streams {
		err = multierr.Append(err, bp.streams[i].client.CloseSend())
	}
	for i := range bp.streams {
		<-bp.streams[i].ctxDoneCh
	}
	if bp.pub.closer.AddRunning() {
		go func(f *batchFuture) {
			defer bp.pub.closer.Done()
			for _, n := range f.get() {
				bp.pub.failover(n)
			}
		}(&bp.f)
	}
	return err
}

func (bp *batchPublisher) Publish(ctx context.Context, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	var err error
	for _, m := range messages {
		r, errM2R := messageToRequest(topic, m)
		if errM2R != nil {
			err = multierr.Append(err, fmt.Errorf("failed to marshal message %T: %w", m, errM2R))
			continue
		}
		node := m.Node()
		sendData := func() (success bool) {
			if stream, ok := bp.streams[node]; ok {
				defer func() {
					if !success {
						delete(bp.streams, node)
					}
				}()
				select {
				case <-ctx.Done():
					return false
				case <-stream.client.Context().Done():
					return false
				default:
				}
				errSend := stream.client.Send(r)
				if errSend != nil {
					err = multierr.Append(err, fmt.Errorf("failed to send message to node %s: %w", node, errSend))
					return false
				}
				return errSend == nil
			}
			return false
		}
		if sendData() {
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		bp.pub.mu.RLock()
		client, ok := bp.pub.active[node]
		bp.pub.mu.RUnlock()
		if !ok {
			err = multierr.Append(err, fmt.Errorf("failed to get client for node %s", node))
			continue
		}
		streamCtx, cancel := context.WithTimeout(ctx, bp.timeout)
		// this assignment is for getting around the go vet lint
		deferFn := cancel
		stream, errCreateStream := client.client.Send(streamCtx)
		if errCreateStream != nil {
			err = multierr.Append(err, fmt.Errorf("failed to get stream for node %s: %w", node, errCreateStream))
			continue
		}
		bp.streams[node] = writeStream{
			client:    stream,
			ctxDoneCh: streamCtx.Done(),
		}
		bp.f.events = append(bp.f.events, make(chan batchEvent))
		_ = sendData()
		go func(s clusterv1.Service_SendClient, deferFn func(), bc chan batchEvent) {
			defer func() {
				close(bc)
				deferFn()
			}()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, errRecv := s.Recv()
				if errRecv == nil {
					continue
				}
				if errors.Is(errRecv, io.EOF) {
					return
				}
				bc <- batchEvent{n: node, e: errRecv}
				return
			}
		}(stream, deferFn, bp.f.events[len(bp.f.events)-1])
	}
	return nil, err
}

func messageToRequest(topic bus.Topic, m bus.Message) (*clusterv1.SendRequest, error) {
	r := &clusterv1.SendRequest{
		Topic:     topic.String(),
		MessageId: uint64(m.ID()),
		BatchMod:  m.BatchModeEnabled(),
	}
	message, ok := m.Data().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("invalid message type %T", m.Data())
	}
	anyMessage, err := anypb.New(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message %T: %w", m, err)
	}
	r.Body = anyMessage
	return r, nil
}

type future struct {
	clients  []clusterv1.Service_SendClient
	cancelFn []func()
	topics   []bus.Topic
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
		l.cancelFn[0]()
		l.cancelFn = l.cancelFn[1:]
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

type batchEvent struct {
	e error
	n string
}

type batchFuture struct {
	errNodes map[string]struct{}
	l        *logger.Logger
	events   []chan batchEvent
}

func (b *batchFuture) get() []string {
	var wg sync.WaitGroup
	var mux sync.Mutex
	wg.Add(len(b.events))
	for _, e := range b.events {
		go func(e chan batchEvent, mux *sync.Mutex) {
			defer wg.Done()
			for evt := range e {
				func() {
					mux.Lock()
					defer mux.Unlock()
					// only log the error once for each node
					if _, ok := b.errNodes[evt.n]; !ok && isFailoverError(evt.e) {
						b.l.Error().Err(evt.e).Msgf("failed to send message to node %s", evt.n)
						b.errNodes[evt.n] = struct{}{}
						return
					}
					b.l.Error().Err(evt.e).Msgf("failed to send message to node %s", evt.n)
				}()
			}
		}(e, &mux)
	}
	wg.Wait()
	mux.Lock()
	defer mux.Unlock()
	var result []string
	for n := range b.errNodes {
		result = append(result, n)
	}
	return result
}

func isFailoverError(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	return s.Code() == codes.Unavailable || s.Code() == codes.DeadlineExceeded
}
