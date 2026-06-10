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

package sub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// mockSendServer implements clusterv1.Service_SendServer for sub batch tests.
type mockSendServer struct {
	ctx     context.Context
	sendErr error
	sent    []*clusterv1.SendResponse
	mu      sync.Mutex
}

func newMockSendServer() *mockSendServer {
	return &mockSendServer{
		ctx: metadata.NewIncomingContext(context.Background(), metadata.MD{}),
	}
}

func (m *mockSendServer) Send(resp *clusterv1.SendResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, resp)
	return m.sendErr
}

func (m *mockSendServer) Recv() (*clusterv1.SendRequest, error) { return nil, nil }
func (m *mockSendServer) Context() context.Context              { return m.ctx }
func (m *mockSendServer) SetHeader(metadata.MD) error           { return nil }
func (m *mockSendServer) SendHeader(metadata.MD) error          { return nil }
func (m *mockSendServer) SetTrailer(metadata.MD)                {}
func (m *mockSendServer) SendMsg(_ any) error                   { return nil }
func (m *mockSendServer) RecvMsg(_ any) error                   { return nil }

// echoListener is a healthy MessageListener that echoes messages back.
type echoListener struct {
	bus.UnImplementedHealthyListener
}

func (*echoListener) Rev(_ context.Context, msg bus.Message) bus.Message { return msg }

func newBatchTestServer(m *metrics) *server { //nolint:exhaustruct
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	if logInitErr != nil {
		panic(logInitErr)
	}
	return &server{
		log:           logger.GetLogger("batch-metrics-test"),
		metrics:       m,
		listeners:     make(map[bus.Topic][]bus.MessageListener),
		listenersLock: sync.RWMutex{},
		topicMap:      make(map[string]bus.Topic),
	}
}

func newBatchMetrics() (m *metrics, msgStarted, msgFinished, batchStarted, batchFinished *fakeCounter, batchLatency *fakeHistogram) {
	msgStarted = &fakeCounter{}
	msgFinished = &fakeCounter{}
	batchStarted = &fakeCounter{}
	batchFinished = &fakeCounter{}
	batchLatency = &fakeHistogram{}
	m = &metrics{ //nolint:exhaustruct
		totalStarted:         &fakeCounter{},
		totalFinished:        &fakeCounter{},
		totalLatency:         &fakeHistogram{},
		totalErr:             &fakeCounter{},
		receivedBytes:        &fakeCounter{},
		totalMessageStarted:  msgStarted,
		totalMessageFinished: msgFinished,
		totalBatchStarted:    batchStarted,
		totalBatchFinished:   batchFinished,
		totalBatchLatency:    batchLatency,
	}
	return m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency
}

func makeBatchReq(group, senderNode string) *clusterv1.SendRequest { //nolint:exhaustruct
	return &clusterv1.SendRequest{
		Topic:      data.TopicMeasureWrite.String(),
		Group:      group,
		SenderNode: senderNode,
		SenderRole: "liaison",
		SenderTier: "hot",
		BatchMod:   true,
		Body:       []byte("data"),
	}
}

// TestBatchModNMessageMetrics asserts that for an N-message BatchMod batch:
// total_message_started==N, total_message_finished==N, total_batch_started==1, total_batch_finished==1,
// and total_batch_latency is observed exactly once.
func TestBatchModNMessageMetrics(t *testing.T) {
	const N = 3
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency := newBatchMetrics()
	s := newBatchTestServer(m)

	topic := data.TopicMeasureWrite
	require.NoError(t, s.Subscribe(topic, &echoListener{}))

	stream := newMockSendServer()
	req := makeBatchReq("g1", "liaison-0:17912")

	identity := &streamIdentity{}
	var dataCollection []any
	start := time.Now()

	// Simulate N batch messages (handleBatch called N times with the same identity).
	for range N {
		s.handleBatch(&dataCollection, req, &start, identity)
	}

	// Pin identity as the real Send path does before handleBatch is called.
	s.pinIdentity(identity, req, topic)

	// Simulate EOF: handleEOF dispatches the collected batch.
	s.handleEOF(stream, &topic, dataCollection, req, identity, start)

	require.Equal(t, float64(N), msgStarted.total, "total_message_started must equal N")
	require.Equal(t, float64(N), msgFinished.total, "total_message_finished must equal N")
	require.Equal(t, float64(1), batchStarted.total, "total_batch_started must equal 1")
	require.Equal(t, float64(1), batchFinished.total, "total_batch_finished must equal 1")
	require.Equal(t, 1, batchLatency.count, "total_batch_latency must be observed exactly once")
}

// TestBatchModSendFailureMetrics verifies that a Send failure on the response does NOT affect
// the four metric counts — they are pinned after listener.Rev, independent of Send outcome.
func TestBatchModSendFailureMetrics(t *testing.T) {
	const N = 4
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency := newBatchMetrics()
	s := newBatchTestServer(m)

	topic := data.TopicMeasureWrite
	require.NoError(t, s.Subscribe(topic, &echoListener{}))

	// Wire a Send that always fails (simulates client disconnect before reading EOF response).
	stream := newMockSendServer()
	stream.sendErr = errors.New("client disconnect")

	req := makeBatchReq("g2", "liaison-1:17912")
	identity := &streamIdentity{}
	var dataCollection []any
	start := time.Now()

	for range N {
		s.handleBatch(&dataCollection, req, &start, identity)
	}
	s.pinIdentity(identity, req, topic)
	s.handleEOF(stream, &topic, dataCollection, req, identity, start)

	require.Equal(t, float64(N), msgStarted.total, "total_message_started must equal N even when Send fails")
	require.Equal(t, float64(N), msgFinished.total, "total_message_finished must equal N even when Send fails")
	require.Equal(t, float64(1), batchStarted.total, "total_batch_started must equal 1 even when Send fails")
	require.Equal(t, float64(1), batchFinished.total, "total_batch_finished must equal 1 even when Send fails")
	require.Equal(t, 1, batchLatency.count, "total_batch_latency must be observed once even when Send fails")
}

// TestHandleEOFEmptyCollectionTicksNothing verifies that handleEOF with an empty dataCollection
// returns early without ticking any of the new instruments.
func TestHandleEOFEmptyCollectionTicksNothing(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency := newBatchMetrics()
	s := newBatchTestServer(m)

	topic := data.TopicMeasureWrite
	require.NoError(t, s.Subscribe(topic, &echoListener{}))

	stream := newMockSendServer()
	identity := &streamIdentity{
		senderNode: "liaison-0:17912",
		senderRole: "liaison",
		senderTier: "hot",
		group:      "g1",
		operation:  "batch-write",
		pinned:     true,
	}

	// Call handleEOF with an empty collection — must return immediately without ticking.
	s.handleEOF(stream, &topic, []any{}, nil, identity, time.Now())

	require.Equal(t, float64(0), msgStarted.total, "no message_started on empty collection")
	require.Equal(t, float64(0), msgFinished.total, "no message_finished on empty collection")
	require.Equal(t, float64(0), batchStarted.total, "no batch_started on empty collection")
	require.Equal(t, float64(0), batchFinished.total, "no batch_finished on empty collection")
	require.Equal(t, 0, batchLatency.count, "no batch_latency on empty collection")
}

// TestHandleEOFNoListenerTicksNothing verifies that handleEOF with no registered listener
// returns (via replyWithErrType) without ticking any of the new instruments.
func TestHandleEOFNoListenerTicksNothing(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency := newBatchMetrics()
	s := newBatchTestServer(m)

	// No listener registered for this topic.
	topic := data.TopicMeasureWrite
	stream := newMockSendServer()
	req := makeBatchReq("g1", "liaison-0:17912")
	identity := &streamIdentity{
		senderNode: "liaison-0:17912",
		senderRole: "liaison",
		senderTier: "hot",
		group:      "g1",
		operation:  "batch-write",
		pinned:     true,
	}

	// Non-empty dataCollection but no listener — must not tick finished/batch.
	s.handleEOF(stream, &topic, []any{req.Body}, req, identity, time.Now())

	require.Equal(t, float64(0), msgStarted.total, "no message_started when no listener")
	require.Equal(t, float64(0), msgFinished.total, "no message_finished when no listener")
	require.Equal(t, float64(0), batchStarted.total, "no batch_started when no listener")
	require.Equal(t, float64(0), batchFinished.total, "no batch_finished when no listener")
	require.Equal(t, 0, batchLatency.count, "no batch_latency when no listener")
}

// TestDispatchMessageNonBatchMetrics verifies that a single dispatchMessage call increments
// total_message_started==1 and total_message_finished==1.
func TestDispatchMessageNonBatchMetrics(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	m, msgStarted, msgFinished, batchStarted, batchFinished, batchLatency := newBatchMetrics()
	s := newBatchTestServer(m)

	topic := data.TopicMeasureWrite
	require.NoError(t, s.Subscribe(topic, &echoListener{}))

	stream := newMockSendServer()
	req := &clusterv1.SendRequest{ //nolint:exhaustruct
		Topic:      data.TopicMeasureWrite.String(),
		Group:      "g1",
		SenderNode: "liaison-0:17912",
		SenderRole: "liaison",
		SenderTier: "hot",
		BatchMod:   false,
		Body:       []byte("payload"),
	}
	identity := &streamIdentity{
		senderNode: "liaison-0:17912",
		senderRole: "liaison",
		senderTier: "hot",
		group:      "g1",
		operation:  "batch-write",
		pinned:     true,
	}
	msg := bus.NewMessage(bus.MessageID(1), req.Body)
	start := time.Now()
	s.dispatchMessage(stream, req, topic, msg, identity, &start)

	require.Equal(t, float64(1), msgStarted.total, "total_message_started must be 1 on dispatchMessage")
	require.Equal(t, float64(1), msgFinished.total, "total_message_finished must be 1 on dispatchMessage")
	require.Zero(t, batchStarted.total, "the non-batch path must not tick the batch catalog")
	require.Zero(t, batchFinished.total, "the non-batch path must not tick the batch catalog")
	require.Zero(t, batchLatency.count, "the non-batch path must not observe batch latency")
}
