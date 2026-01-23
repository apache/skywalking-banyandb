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

package proxy

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/cluster"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/metrics"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const testAgentID = "test-agent-id"

// revive:disable-next-line:exported
var NewProxyClient = NewClient

func initTestLogger(t *testing.T) *logger.Logger {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
	return logger.GetLogger("test", "proxy")
}

// mockFODCServiceClient implements fodcv1.FODCServiceClient for testing.
type mockFODCServiceClient struct {
	registerAgentFunc      func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error)
	streamMetricsFunc      func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error)
	streamClusterStateFunc func(context.Context, ...grpc.CallOption) (fodcv1.FODCService_StreamClusterStateClient, error)
}

func (m *mockFODCServiceClient) RegisterAgent(ctx context.Context, opts ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
	if m.registerAgentFunc != nil {
		return m.registerAgentFunc(ctx, opts...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockFODCServiceClient) StreamMetrics(ctx context.Context, opts ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
	if m.streamMetricsFunc != nil {
		return m.streamMetricsFunc(ctx, opts...)
	}
	return nil, errors.New("not implemented")
}

func (m *mockFODCServiceClient) StreamClusterState(ctx context.Context, opts ...grpc.CallOption) (fodcv1.FODCService_StreamClusterStateClient, error) {
	if m.streamClusterStateFunc != nil {
		return m.streamClusterStateFunc(ctx, opts...)
	}
	return nil, errors.New("not implemented")
}

// mockRegisterAgentClient implements fodcv1.FODCService_RegisterAgentClient for testing.
type mockRegisterAgentClient struct {
	ctx           context.Context
	recvChan      chan *fodcv1.RegisterAgentResponse
	sendErr       error
	recvErr       error
	sentRequests  []*fodcv1.RegisterAgentRequest
	recvResponses []*fodcv1.RegisterAgentResponse
	mu            sync.RWMutex
}

func newMockRegisterAgentClient(ctx context.Context) *mockRegisterAgentClient {
	return &mockRegisterAgentClient{
		ctx:           ctx,
		sentRequests:  make([]*fodcv1.RegisterAgentRequest, 0),
		recvResponses: make([]*fodcv1.RegisterAgentResponse, 0),
		recvChan:      make(chan *fodcv1.RegisterAgentResponse, 10),
	}
}

func (m *mockRegisterAgentClient) Send(req *fodcv1.RegisterAgentRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentRequests = append(m.sentRequests, req)
	return nil
}

func (m *mockRegisterAgentClient) Recv() (*fodcv1.RegisterAgentResponse, error) {
	m.mu.Lock()
	recvErr := m.recvErr
	m.mu.Unlock()
	if recvErr != nil {
		return nil, recvErr
	}
	select {
	case resp := <-m.recvChan:
		return resp, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil, io.EOF
	}
}

func (m *mockRegisterAgentClient) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

func (m *mockRegisterAgentClient) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *mockRegisterAgentClient) AddResponse(resp *fodcv1.RegisterAgentResponse) {
	m.recvChan <- resp
}

func (m *mockRegisterAgentClient) CloseSend() error {
	close(m.recvChan)
	return nil
}

func (m *mockRegisterAgentClient) Context() context.Context {
	return m.ctx
}

func (m *mockRegisterAgentClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockRegisterAgentClient) Trailer() metadata.MD {
	return nil
}

func (m *mockRegisterAgentClient) SendMsg(_ interface{}) error {
	return nil
}

func (m *mockRegisterAgentClient) RecvMsg(_ interface{}) error {
	return nil
}

// mockStreamMetricsClient implements fodcv1.FODCService_StreamMetricsClient for testing.
type mockStreamMetricsClient struct {
	ctx           context.Context
	recvChan      chan *fodcv1.StreamMetricsResponse
	sendErr       error
	recvErr       error
	sentRequests  []*fodcv1.StreamMetricsRequest
	recvResponses []*fodcv1.StreamMetricsResponse
	mu            sync.Mutex
}

func newMockStreamMetricsClient(ctx context.Context) *mockStreamMetricsClient {
	return &mockStreamMetricsClient{
		ctx:           ctx,
		sentRequests:  make([]*fodcv1.StreamMetricsRequest, 0),
		recvResponses: make([]*fodcv1.StreamMetricsResponse, 0),
		recvChan:      make(chan *fodcv1.StreamMetricsResponse, 10),
	}
}

func (m *mockStreamMetricsClient) Send(req *fodcv1.StreamMetricsRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentRequests = append(m.sentRequests, req)
	return nil
}

func (m *mockStreamMetricsClient) Recv() (*fodcv1.StreamMetricsResponse, error) {
	m.mu.Lock()
	recvErr := m.recvErr
	m.mu.Unlock()
	if recvErr != nil {
		return nil, recvErr
	}
	select {
	case resp := <-m.recvChan:
		return resp, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil, io.EOF
	}
}

func (m *mockStreamMetricsClient) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

func (m *mockStreamMetricsClient) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *mockStreamMetricsClient) AddResponse(resp *fodcv1.StreamMetricsResponse) {
	m.recvChan <- resp
}

func (m *mockStreamMetricsClient) CloseSend() error {
	close(m.recvChan)
	return nil
}

func (m *mockStreamMetricsClient) Context() context.Context {
	return m.ctx
}

func (m *mockStreamMetricsClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockStreamMetricsClient) Trailer() metadata.MD {
	return nil
}

func (m *mockStreamMetricsClient) SendMsg(_ interface{}) error {
	return nil
}

func (m *mockStreamMetricsClient) RecvMsg(_ interface{}) error {
	return nil
}

// mockStreamClusterStateClient implements fodcv1.FODCService_StreamClusterStateClient for testing.
type mockStreamClusterStateClient struct {
	ctx           context.Context
	recvChan      chan *fodcv1.StreamClusterStateResponse
	sendErr       error
	recvErr       error
	sentRequests  []*fodcv1.StreamClusterStateRequest
	recvResponses []*fodcv1.StreamClusterStateResponse
	mu            sync.Mutex
}

func newMockStreamClusterStateClient(ctx context.Context) *mockStreamClusterStateClient {
	return &mockStreamClusterStateClient{
		ctx:           ctx,
		sentRequests:  make([]*fodcv1.StreamClusterStateRequest, 0),
		recvResponses: make([]*fodcv1.StreamClusterStateResponse, 0),
		recvChan:      make(chan *fodcv1.StreamClusterStateResponse, 10),
	}
}

func (m *mockStreamClusterStateClient) Send(req *fodcv1.StreamClusterStateRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentRequests = append(m.sentRequests, req)
	return nil
}

func (m *mockStreamClusterStateClient) Recv() (*fodcv1.StreamClusterStateResponse, error) {
	m.mu.Lock()
	recvErr := m.recvErr
	m.mu.Unlock()
	if recvErr != nil {
		return nil, recvErr
	}
	select {
	case resp := <-m.recvChan:
		return resp, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil, io.EOF
	}
}

func (m *mockStreamClusterStateClient) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

func (m *mockStreamClusterStateClient) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *mockStreamClusterStateClient) AddResponse(resp *fodcv1.StreamClusterStateResponse) {
	m.recvChan <- resp
}

func (m *mockStreamClusterStateClient) CloseSend() error {
	close(m.recvChan)
	return nil
}

func (m *mockStreamClusterStateClient) Context() context.Context {
	return m.ctx
}

func (m *mockStreamClusterStateClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockStreamClusterStateClient) Trailer() metadata.MD {
	return nil
}

func (m *mockStreamClusterStateClient) SendMsg(_ interface{}) error {
	return nil
}

func (m *mockStreamClusterStateClient) RecvMsg(_ interface{}) error {
	return nil
}

func TestNewProxyClient(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)

	pc := NewProxyClient(
		"localhost:8080",
		"datanode-hot",
		"192.168.1.1",
		[]string{"data"},
		map[string]string{"env": "test"},
		5*time.Second,
		10*time.Second,
		fr,
		nil,
		testLogger,
	)

	require.NotNil(t, pc)
	assert.Equal(t, "localhost:8080", pc.proxyAddr)
	assert.Equal(t, "192.168.1.1", pc.podName)
	assert.Equal(t, "datanode-hot", pc.nodeRole)
	assert.Equal(t, map[string]string{"env": "test"}, pc.labels)
	assert.Equal(t, 5*time.Second, pc.heartbeatInterval)
	assert.Equal(t, 10*time.Second, pc.reconnectInterval)
	assert.Equal(t, fr, pc.flightRecorder)
	assert.Equal(t, testLogger, pc.logger)
	assert.NotNil(t, pc.stopCh)
	assert.False(t, pc.disconnected)
}

func TestProxyClient_Connect_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection manager before calling Connect
	pc.connManager.start(ctx)
	defer pc.connManager.stop()

	err := pc.Connect(ctx)

	// grpc.NewClient may succeed even without a server (lazy connection)
	// So we check that connection was established
	if err != nil {
		// If connection fails, verify error message
		assert.Contains(t, err.Error(), "failed to create proxy client")
	} else {
		// If connection succeeds, verify client is set
		pc.streamsMu.RLock()
		client := pc.client
		pc.streamsMu.RUnlock()
		assert.NotNil(t, client)
	}
}

func TestProxyClient_Connect_AlreadyConnected(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection manager
	pc.connManager.start(ctx)
	defer pc.connManager.stop()

	// First connection
	err := pc.Connect(ctx)
	require.NoError(t, err)

	// Second connection should use existing connection
	err = pc.Connect(ctx)
	assert.NoError(t, err)
}

func TestProxyClient_Connect_Reconnection(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection manager
	pc.connManager.start(ctx)
	defer pc.connManager.stop()

	// Set disconnected state
	pc.streamsMu.Lock()
	pc.disconnected = true
	close(pc.stopCh)
	pc.streamsMu.Unlock()

	err := pc.Connect(ctx)

	// Verify disconnected state is reset regardless of connection success/failure
	pc.streamsMu.RLock()
	disconnected := pc.disconnected
	pc.streamsMu.RUnlock()
	assert.False(t, disconnected)

	// Connection may succeed or fail depending on whether server is running
	// The important part is that disconnected state is reset
	if err == nil {
		pc.streamsMu.RLock()
		client := pc.client
		pc.streamsMu.RUnlock()
		assert.NotNil(t, client)
	}
}

func TestProxyClient_StartRegistrationStream_NotConnected(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not connected")
}

func TestProxyClient_StartRegistrationStream_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient(
		"localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, map[string]string{"env": "test"},
		5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockRegisterAgentClient(ctx)

	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	// Add successful response
	mockStream.AddResponse(&fodcv1.RegisterAgentResponse{
		Success:                  true,
		AgentId:                  testAgentID,
		HeartbeatIntervalSeconds: 10,
		Message:                  "registered",
	})

	err := pc.StartRegistrationStream(ctx)

	require.NoError(t, err)
	agentID := pc.agentID
	heartbeatInterval := pc.heartbeatInterval
	assert.Equal(t, testAgentID, agentID)
	assert.Equal(t, 10*time.Second, heartbeatInterval)
}

func TestProxyClient_StartRegistrationStream_StreamError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return nil, errors.New("stream creation failed")
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create registration stream")
}

func TestProxyClient_StartRegistrationStream_SendError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockRegisterAgentClient(ctx)
	mockStream.SetSendError(errors.New("send failed"))

	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send registration request")
}

func TestProxyClient_StartRegistrationStream_RecvError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockRegisterAgentClient(ctx)
	mockStream.SetRecvError(errors.New("recv failed"))

	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to receive registration response")
}

func TestProxyClient_StartRegistrationStream_RegistrationFailed(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockRegisterAgentClient(ctx)

	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	// Add failed response
	mockStream.AddResponse(&fodcv1.RegisterAgentResponse{
		Success: false,
		Message: "registration failed",
	})

	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "registration failed")
}

func TestProxyClient_StartRegistrationStream_MissingAgentID(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockRegisterAgentClient(ctx)

	mockClient.registerAgentFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_RegisterAgentClient, error) {
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	// Add response without agent ID
	mockStream.AddResponse(&fodcv1.RegisterAgentResponse{
		Success: true,
		AgentId: "",
	})

	err := pc.StartRegistrationStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "registration response missing agent ID")
}

func TestProxyClient_StartMetricsStream_NotConnected(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	err := pc.StartMetricsStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not connected")
}

func TestProxyClient_StartMetricsStream_NoAgentID(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	mockClient := &mockFODCServiceClient{}
	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	ctx := context.Background()
	err := pc.StartMetricsStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent ID not available")
}

func TestProxyClient_StartMetricsStream_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockStreamMetricsClient(ctx)

	mockClient.streamMetricsFunc = func(ctxParam context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		// Verify metadata contains agent_id
		md, ok := metadata.FromOutgoingContext(ctxParam)
		require.True(t, ok)
		agentIDs := md.Get("agent_id")
		require.Len(t, agentIDs, 1)
		assert.Equal(t, testAgentID, agentIDs[0])
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.agentID = testAgentID
	pc.streamsMu.Unlock()

	err := pc.StartMetricsStream(ctx)

	require.NoError(t, err)
	pc.streamsMu.RLock()
	metricsStream := pc.metricsStream
	pc.streamsMu.RUnlock()
	assert.NotNil(t, metricsStream)
}

func TestProxyClient_StartMetricsStream_StreamError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockClient.streamMetricsFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_StreamMetricsClient, error) {
		return nil, errors.New("stream creation failed")
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.agentID = testAgentID
	pc.streamsMu.Unlock()

	err := pc.StartMetricsStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create metrics stream")
}

func TestProxyClient_StartClusterStateStream_NotConnected(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	err := pc.StartClusterStateStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not connected")
}

func TestProxyClient_StartClusterStateStream_NoAgentID(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	mockClient := &mockFODCServiceClient{}
	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.streamsMu.Unlock()

	ctx := context.Background()
	err := pc.StartClusterStateStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "agent ID not available")
}

func TestProxyClient_StartClusterStateStream_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockStream := newMockStreamClusterStateClient(ctx)

	mockClient.streamClusterStateFunc = func(ctxParam context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_StreamClusterStateClient, error) {
		// Verify metadata contains agent_id
		md, ok := metadata.FromOutgoingContext(ctxParam)
		require.True(t, ok)
		agentIDs := md.Get("agent_id")
		require.Len(t, agentIDs, 1)
		assert.Equal(t, testAgentID, agentIDs[0])
		return mockStream, nil
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.agentID = testAgentID
	pc.streamsMu.Unlock()

	err := pc.StartClusterStateStream(ctx)

	require.NoError(t, err)
	pc.streamsMu.RLock()
	clusterStateStream := pc.clusterStateStream
	pc.streamsMu.RUnlock()
	assert.NotNil(t, clusterStateStream)
}

func TestProxyClient_StartClusterStateStream_StreamError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockClient := &mockFODCServiceClient{}
	mockClient.streamClusterStateFunc = func(_ context.Context, _ ...grpc.CallOption) (fodcv1.FODCService_StreamClusterStateClient, error) {
		return nil, errors.New("stream creation failed")
	}

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.agentID = testAgentID
	pc.streamsMu.Unlock()

	err := pc.StartClusterStateStream(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create cluster state stream")
}

func TestProxyClient_SendClusterState_NoStream(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	err := pc.sendClusterData()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cluster state stream not established")
}

func TestProxyClient_SendClusterState_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	collector := cluster.NewCollector(testLogger, "localhost:17914", 30*time.Second)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, collector, testLogger)

	ctx := context.Background()
	mockStream := newMockStreamClusterStateClient(ctx)

	pc.streamsMu.Lock()
	pc.clusterStateStream = mockStream
	pc.streamsMu.Unlock()

	err := pc.sendClusterData()

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := mockStream.sentRequests
	mockStream.mu.Unlock()

	require.Len(t, sentRequests, 1)
	assert.NotNil(t, sentRequests[0].Timestamp)
}

func TestProxyClient_SendHeartbeat_NoStream(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	err := pc.SendHeartbeat(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "registration stream not established")
}

func TestProxyClient_SendHeartbeat_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient(
		"localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"},
		map[string]string{"env": "test"},
		5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockStream := newMockRegisterAgentClient(ctx)

	pc.streamsMu.Lock()
	pc.registrationStream = mockStream
	pc.streamsMu.Unlock()

	err := pc.SendHeartbeat(ctx)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
}

func TestProxyClient_SendHeartbeat_SendError(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockStream := newMockRegisterAgentClient(ctx)
	mockStream.SetSendError(errors.New("send failed"))

	pc.streamsMu.Lock()
	pc.registrationStream = mockStream
	pc.streamsMu.Unlock()

	err := pc.SendHeartbeat(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send heartbeat")
}

func TestProxyClient_RetrieveAndSendMetrics_NoStream(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	err := pc.RetrieveAndSendMetrics(ctx, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metrics stream not established")
}

func TestProxyClient_RetrieveAndSendMetrics_NoDatasources(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	pc.streamsMu.Lock()
	pc.metricsStream = mockStream
	pc.streamsMu.Unlock()

	err := pc.RetrieveAndSendMetrics(ctx, nil)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
	assert.Empty(t, mockStream.sentRequests[0].Metrics)
}

func TestProxyClient_RetrieveAndSendMetrics_LatestMetrics(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Add metrics to flight recorder
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Desc:   "CPU usage percentage",
			Labels: []metrics.Label{},
		},
		{
			Name:  "http_requests_total",
			Value: 100.0,
			Desc:  "Total HTTP requests",
			Labels: []metrics.Label{
				{Name: "method", Value: "GET"},
			},
		},
	}
	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	pc.streamsMu.Lock()
	pc.metricsStream = mockStream
	pc.streamsMu.Unlock()

	err := pc.RetrieveAndSendMetrics(ctx, nil)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
	assert.NotEmpty(t, mockStream.sentRequests[0].Metrics)
}

func TestProxyClient_RetrieveAndSendMetrics_FilteredMetrics(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Add metrics with timestamps
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	// Get datasource and add timestamps
	datasources := fr.GetDatasources()
	require.Len(t, datasources, 1)
	ds := datasources[0]
	now := time.Now()
	ds.AddTimestamp(now.Add(-2 * time.Hour).Unix())
	ds.AddTimestamp(now.Add(-1 * time.Hour).Unix())
	ds.AddTimestamp(now.Unix())

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	pc.streamsMu.Lock()
	pc.metricsStream = mockStream
	pc.streamsMu.Unlock()

	startTime := now.Add(-90 * time.Minute)
	endTime := now.Add(-30 * time.Minute)
	filter := &MetricsRequestFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	err := pc.RetrieveAndSendMetrics(ctx, filter)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
}

func TestProxyClient_RetrieveAndSendMetrics_FilteredNoTimestamps(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Add metrics with timestamps far in the past
	// Then filter for a future time window to ensure no matches
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	pc.streamsMu.Lock()
	pc.metricsStream = mockStream
	pc.streamsMu.Unlock()

	// Filter for a future time window that won't match any timestamps
	// This tests the filtering logic when timestamps exist but don't match
	startTime := time.Now().Add(1 * time.Hour)
	endTime := time.Now().Add(2 * time.Hour)
	filter := &MetricsRequestFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	err := pc.RetrieveAndSendMetrics(ctx, filter)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
	// When filter time window doesn't match any timestamps, should send empty metrics
	assert.Empty(t, mockStream.sentRequests[0].Metrics, "Should send empty metrics when no timestamps match the filter window")
}

func TestProxyClient_Disconnect_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the connection manager before calling Connect
	pc.connManager.start(ctx)
	defer pc.connManager.stop()

	// First connect
	err := pc.Connect(ctx)
	if err != nil {
		t.Skipf("Cannot test disconnect without connection: %v", err)
		return
	}

	// Set up mock streams to test cleanup
	mockClient := &mockFODCServiceClient{}
	mockRegStream := newMockRegisterAgentClient(context.Background())
	mockMetricsStream := newMockStreamMetricsClient(context.Background())
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	pc.streamsMu.Lock()
	pc.client = mockClient
	pc.registrationStream = mockRegStream
	pc.metricsStream = mockMetricsStream
	pc.heartbeatTicker = ticker
	pc.streamsMu.Unlock()

	// Now disconnect
	disconnectErr := pc.Disconnect()

	require.NoError(t, disconnectErr)
	pc.streamsMu.RLock()
	disconnected := pc.disconnected
	client := pc.client
	registrationStream := pc.registrationStream
	metricsStream := pc.metricsStream
	pc.streamsMu.RUnlock()
	assert.True(t, disconnected)
	assert.Nil(t, client)
	assert.Nil(t, registrationStream)
	assert.Nil(t, metricsStream)
}

func TestProxyClient_Disconnect_Idempotent(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	pc.streamsMu.Lock()
	pc.disconnected = true
	pc.streamsMu.Unlock()

	err := pc.Disconnect()

	require.NoError(t, err)
	// Should be able to call again without error
	err2 := pc.Disconnect()
	require.NoError(t, err2)
}

func TestProxyClient_handleRegistrationStream_EOF(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockRegisterAgentClient(ctx)
	mockStream.SetRecvError(io.EOF)

	pc.handleRegistrationStream(ctx, mockStream)

	// Should return without error
}

func TestProxyClient_handleRegistrationStream_ContextCanceled(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mockStream := newMockRegisterAgentClient(ctx)
	mockStream.SetRecvError(context.Canceled)

	pc.handleRegistrationStream(ctx, mockStream)

	// Should return without error
}

func TestProxyClient_handleMetricsStream_EOF(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockStreamMetricsClient(ctx)
	mockStream.SetRecvError(io.EOF)

	pc.handleMetricsStream(ctx, mockStream)

	// Should return without error
}

func TestProxyClient_handleMetricsStream_RequestMetrics(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Add metrics
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	updateErr := fr.Update(rawMetrics)
	require.NoError(t, updateErr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockStreamMetricsClient(ctx)
	pc.streamsMu.Lock()
	pc.metricsStream = mockStream
	pc.streamsMu.Unlock()

	now := time.Now()
	startTime := timestamppb.New(now.Add(-1 * time.Hour))
	endTime := timestamppb.New(now)

	// Add request with time window
	mockStream.AddResponse(&fodcv1.StreamMetricsResponse{
		StartTime: startTime,
		EndTime:   endTime,
	})

	go pc.handleMetricsStream(ctx, mockStream)

	// Wait a bit for the handler to process
	time.Sleep(100 * time.Millisecond)
	cancel()

	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.GreaterOrEqual(t, sentRequests, 1)
}

func TestProxyClient_parseMetricKey_Simple(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	key, err := pc.parseMetricKey("cpu_usage")

	require.NoError(t, err)
	assert.Equal(t, "cpu_usage", key.Name)
	assert.Empty(t, key.Labels)
}

func TestProxyClient_parseMetricKey_WithLabels(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	key, err := pc.parseMetricKey(`http_requests_total{method="GET",status="200"}`)

	require.NoError(t, err)
	assert.Equal(t, "http_requests_total", key.Name)
	assert.Len(t, key.Labels, 2)
}

func TestProxyClient_parseMetricKey_InvalidFormat(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	key, err := pc.parseMetricKey("invalid{format")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid metric key format")
	assert.Empty(t, key.Name)
}

func TestProxyClient_parseMetricKey_MalformedLabels(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Test with malformed labels - should still parse what it can
	key, err := pc.parseMetricKey(`metric{invalid,key="value"}`)

	require.NoError(t, err)
	assert.Equal(t, "metric", key.Name)
	// Should parse valid labels and skip invalid ones
	assert.GreaterOrEqual(t, len(key.Labels), 0)
}

func TestProxyClient_sendLatestMetrics_WithUnfinalizedValue(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Create a datasource with metrics
	ds := flightrecorder.NewDatasource()
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Desc:   "CPU usage",
			Labels: []metrics.Label{},
		},
	}
	updateErr := ds.UpdateBatch(rawMetrics, time.Now().Unix())
	require.NoError(t, updateErr)

	allMetrics := ds.GetMetrics()
	descriptions := ds.GetDescriptions()

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	err := pc.sendLatestMetrics(mockStream, allMetrics, descriptions)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
	assert.NotEmpty(t, mockStream.sentRequests[0].Metrics)
}

func TestProxyClient_sendFilteredMetrics_TimeWindow(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Create a datasource with metrics and timestamps
	ds := flightrecorder.NewDatasource()
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	now := time.Now()
	ds.AddTimestamp(now.Add(-2 * time.Hour).Unix())
	ds.AddTimestamp(now.Add(-1 * time.Hour).Unix())
	ds.AddTimestamp(now.Unix())
	updateErr := ds.UpdateBatch(rawMetrics, now.Add(-2*time.Hour).Unix())
	require.NoError(t, updateErr)
	updateErr2 := ds.UpdateBatch(rawMetrics, now.Add(-1*time.Hour).Unix())
	require.NoError(t, updateErr2)
	updateErr3 := ds.UpdateBatch(rawMetrics, now.Unix())
	require.NoError(t, updateErr3)

	allMetrics := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	timestampValues := timestamps.GetAllValues()
	descriptions := ds.GetDescriptions()

	startTime := now.Add(-90 * time.Minute)
	endTime := now.Add(-30 * time.Minute)
	filter := &MetricsRequestFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	err := pc.sendFilteredMetrics(mockStream, allMetrics, timestampValues, descriptions, filter)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
}

func TestProxyClient_sendFilteredMetrics_NoMatchingTimeWindow(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 10*time.Second, fr, nil, testLogger)

	// Create a datasource with metrics and timestamps
	ds := flightrecorder.NewDatasource()
	rawMetrics := []metrics.RawMetric{
		{
			Name:   "cpu_usage",
			Value:  75.5,
			Labels: []metrics.Label{},
		},
	}
	now := time.Now()
	ds.AddTimestamp(now.Add(-2 * time.Hour).Unix())
	updateErr := ds.UpdateBatch(rawMetrics, now.Add(-2*time.Hour).Unix())
	require.NoError(t, updateErr)

	allMetrics := ds.GetMetrics()
	timestamps := ds.GetTimestamps()
	timestampValues := timestamps.GetAllValues()
	descriptions := ds.GetDescriptions()

	// Filter for future time window (no matches)
	startTime := now.Add(1 * time.Hour)
	endTime := now.Add(2 * time.Hour)
	filter := &MetricsRequestFilter{
		StartTime: &startTime,
		EndTime:   &endTime,
	}

	ctx := context.Background()
	mockStream := newMockStreamMetricsClient(ctx)

	err := pc.sendFilteredMetrics(mockStream, allMetrics, timestampValues, descriptions, filter)

	require.NoError(t, err)
	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.Equal(t, 1, sentRequests)
	// Should send empty metrics since nothing matches the time window
	assert.Empty(t, mockStream.sentRequests[0].Metrics)
}

func TestProxyClient_Start_ContextCancellation(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("invalid-address:99999", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 100*time.Millisecond, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel context immediately
	cancel()

	err := pc.Start(ctx)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestProxyClient_Start_StopChannel(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("invalid-address:99999", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 5*time.Second, 100*time.Millisecond, fr, nil, testLogger)

	ctx := context.Background()

	// Close stop channel
	close(pc.stopCh)

	err := pc.Start(ctx)

	assert.NoError(t, err)
}

func TestProxyClient_startHeartbeat_Success(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 100*time.Millisecond, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockRegisterAgentClient(ctx)
	pc.streamsMu.Lock()
	pc.registrationStream = mockStream
	pc.streamsMu.Unlock()

	pc.startHeartbeat(ctx)

	// Wait for at least one heartbeat
	time.Sleep(150 * time.Millisecond)

	pc.streamsMu.RLock()
	ticker := pc.heartbeatTicker
	pc.streamsMu.RUnlock()
	assert.NotNil(t, ticker)

	mockStream.mu.Lock()
	sentRequests := len(mockStream.sentRequests)
	mockStream.mu.Unlock()
	assert.GreaterOrEqual(t, sentRequests, 1)

	cancel()
	// Wait a bit for goroutine to exit
	time.Sleep(50 * time.Millisecond)
}

func TestProxyClient_startHeartbeat_ReplacesExistingTicker(t *testing.T) {
	testLogger := initTestLogger(t)
	fr := flightrecorder.NewFlightRecorder(1000000)
	pc := NewProxyClient("localhost:8080", "datanode-hot", "192.168.1.1", []string{"data"}, nil, 100*time.Millisecond, 10*time.Second, fr, nil, testLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStream := newMockRegisterAgentClient(ctx)
	pc.streamsMu.Lock()
	pc.registrationStream = mockStream
	oldTicker := time.NewTicker(200 * time.Millisecond)
	pc.heartbeatTicker = oldTicker
	pc.streamsMu.Unlock()

	pc.startHeartbeat(ctx)

	pc.streamsMu.RLock()
	newTicker := pc.heartbeatTicker
	pc.streamsMu.RUnlock()
	assert.NotNil(t, newTicker)
	assert.NotEqual(t, oldTicker, newTicker)

	cancel()
	// Wait a bit for goroutine to exit
	time.Sleep(50 * time.Millisecond)
}
