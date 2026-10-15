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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc/route"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	_ bus.Publisher  = (*local)(nil)
	_ bus.Subscriber = (*local)(nil)
)

type local struct {
	local           *bus.Bus
	exportServer    transferv1.ExportServiceServer
	stopCh          chan struct{}
	streamListeners map[bus.Topic]bus.StreamListener
	streamMu        sync.RWMutex
}

// Local return a new local Queue.
func Local() Queue {
	return &local{
		local:           bus.NewBus(),
		stopCh:          make(chan struct{}),
		streamListeners: make(map[bus.Topic]bus.StreamListener),
	}
}

// GracefulStop implements Queue.
func (l *local) GracefulStop() {
	l.local.Close()
	if l.stopCh != nil {
		close(l.stopCh)
	}
}

// SetSelfNode implements Client. The local pipeline is in-process and has no
// wire / sender stamping, so this is a no-op kept only to satisfy the Client
// interface.
func (l *local) SetSelfNode(_, _, _ string) {}

// Serve implements Queue.
func (l *local) Serve() run.StopNotify {
	return l.stopCh
}

func (l *local) Subscribe(topic bus.Topic, listener bus.MessageListener) error {
	return l.local.Subscribe(topic, listener)
}

// SubscribeStream implements Server.
func (l *local) SubscribeStream(topic bus.Topic, listener bus.StreamListener) error {
	l.streamMu.Lock()
	defer l.streamMu.Unlock()
	if _, ok := l.streamListeners[topic]; ok {
		return fmt.Errorf("stream listener already registered for topic %s", topic)
	}
	l.streamListeners[topic] = listener
	return nil
}

// Scatter implements Client. It runs the topic's stream listener on its own goroutine,
// outside Bus.Publish which would run it synchronously under the bus lock, and hands
// the emitted Go values to the consumer as they are, without encoding.
func (l *local) Scatter(ctx context.Context, topic bus.Topic, message bus.Message, opts ScatterOptions) ([]NodeStream, error) {
	l.streamMu.RLock()
	listener, ok := l.streamListeners[topic]
	l.streamMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no stream listener for topic %s", topic)
	}
	if ce := listener.CheckHealth(); ce != nil {
		return nil, ce
	}
	prefetch := opts.Prefetch
	if prefetch <= 0 {
		prefetch = DefaultScanPrefetch
	}
	runCtx, cancel := context.WithCancel(bus.WithChunkLimits(ctx, bus.ChunkLimits{MaxBytes: opts.MaxChunkBytes, MaxRows: opts.MaxChunkRows}))
	ns := &localNodeStream{
		ch:     make(chan bus.Message, prefetch),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
		cancel: cancel,
	}
	go func() {
		defer close(ns.done)
		err := runStreamListener(runCtx, listener, message, localSink{ch: ns.ch})
		if err == nil {
			err = io.EOF
		}
		ns.err = err
		close(ns.ch)
	}()
	return []NodeStream{ns}, nil
}

// runStreamListener runs the listener and turns a panic into an error. The listener
// runs on its own goroutine, where an unrecovered panic would end the process; the
// remote Scan server gets the same protection from its gRPC recovery interceptor.
func runStreamListener(ctx context.Context, listener bus.StreamListener, message bus.Message, sink bus.Sink) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("stream listener panicked: %v", r)
		}
	}()
	return listener.RevStream(ctx, message, sink)
}

type localSink struct {
	ch chan<- bus.Message
}

func (s localSink) Send(ctx context.Context, chunk bus.Message) error {
	// A canceled context wins even when the buffer has room.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.ch <- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type localNodeStream struct {
	err    error
	ch     chan bus.Message
	done   chan struct{}
	closed chan struct{}
	cancel context.CancelFunc
	once   sync.Once
}

func (n *localNodeStream) Node() string { return "local" }

// Next implements NodeStream.
func (n *localNodeStream) Next(ctx context.Context) (bus.Message, error) {
	select {
	case <-n.closed:
		return bus.Message{}, ErrStreamClosed
	default:
	}
	select {
	case m, ok := <-n.ch:
		if !ok {
			// err is written before ch is closed, so the close orders this read.
			return bus.Message{}, n.err
		}
		return m, nil
	case <-ctx.Done():
		return bus.Message{}, ctx.Err()
	}
}

// Close implements NodeStream. It stops the producer and releases the values still queued.
func (n *localNodeStream) Close() {
	n.once.Do(func() {
		close(n.closed)
		n.cancel()
		// Draining while the producer winds down unblocks a Send that raced the cancel.
		for m := range n.ch {
			releaseChunk(m)
		}
		<-n.done
	})
}

func releaseChunk(m bus.Message) {
	if r, ok := m.Data().(interface{ Release() }); ok {
		r.Release()
	}
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

func (l *local) Name() string {
	return "local-pipeline"
}

func (l *local) NewBatchPublisher(_ time.Duration) BatchPublisher {
	return &localBatchPublisher{
		local: l.local,
	}
}

func (*local) GetPort() *uint32 {
	return nil
}

func (*local) SetRouteProviders(_ map[string]route.TableProvider) {
}

func (*local) SetMetadataRepo(_ metadata.Repo) {
}

func (*local) SetNodeSchemaStatusRepo(_ metadata.Service) {
}

func (*local) Register(bus.Topic, schema.EventHandler) {
}

func (*local) HealthyNodes() []string {
	return nil
}

// GetRouteTable returns an empty route table for local queue.
// Local queue doesn't have distributed nodes, so all fields are empty.
func (*local) GetRouteTable() *databasev1.RouteTable {
	return &databasev1.RouteTable{
		Registered: []*databasev1.Node{},
		Active:     []string{},
		Evictable:  []string{},
	}
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

// NewNodeSchemaStatusClient returns ErrNotImplemented because the standalone
// pipeline has no peer to dial. The cluster barrier fan-out (Step 2.2) treats
// this sentinel as a signal to degrade to in-process self-probing.
func (*local) NewNodeSchemaStatusClient(_ string) (clusterv1.NodeSchemaStatusServiceClient, error) {
	return nil, ErrNotImplemented
}

// NewExportClient returns an in-process client of the ExportService registered with
// SetExportServer, whatever the node name: a standalone process is its own only data
// node. It returns ErrNotImplemented when no server has been registered.
func (l *local) NewExportClient(_ string) (transferv1.ExportServiceClient, error) {
	if l.exportServer == nil {
		return nil, ErrNotImplemented
	}
	return localExportClient{srv: l.exportServer}, nil
}

// SetExportServer implements Server: it keeps the ExportService that NewExportClient
// serves in-process.
func (l *local) SetExportServer(srv transferv1.ExportServiceServer) {
	l.exportServer = srv
}

type localChunkedSyncClient struct {
	local *bus.Bus
	node  string
}

func (l *localChunkedSyncClient) Close() error {
	return nil
}

func (l *localChunkedSyncClient) SyncStreamingParts(_ context.Context, parts []StreamingPartData) (*SyncResult, error) {
	// Check for test failure injector
	if injector := GetChunkedSyncFailureInjector(); injector != nil {
		shouldFail, failedParts, err := injector.BeforeSync(parts)
		if err != nil {
			return nil, err
		}
		if shouldFail {
			return &SyncResult{
				Success:     false,
				SessionID:   "local-session",
				PartsCount:  uint32(len(parts)),
				FailedParts: failedParts,
			}, nil
		}
	}

	return &SyncResult{
		Success:     true,
		SessionID:   "local-session",
		TotalBytes:  0,
		DurationMs:  0,
		ChunksCount: 0,
		PartsCount:  uint32(len(parts)),
	}, nil
}
