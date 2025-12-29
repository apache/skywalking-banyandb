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

package grpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	fodcv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/fodc/v1"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func initTestLogger(t *testing.T) {
	t.Helper()
	initErr := logger.Init(logger.Logging{Env: "dev", Level: "debug"})
	require.NoError(t, initErr)
}

// mockRegisterAgentServer implements fodcv1.FODCService_RegisterAgentServer for testing.
type mockRegisterAgentServer struct {
	ctx           context.Context
	recvErr       error
	sendErr       error
	sentResponses []*fodcv1.RegisterAgentResponse
	recvRequests  []*fodcv1.RegisterAgentRequest
	mu            sync.Mutex
}

func newMockRegisterAgentServer(ctx context.Context) *mockRegisterAgentServer {
	return &mockRegisterAgentServer{
		ctx:           ctx,
		sentResponses: make([]*fodcv1.RegisterAgentResponse, 0),
		recvRequests:  make([]*fodcv1.RegisterAgentRequest, 0),
	}
}

func (m *mockRegisterAgentServer) Send(resp *fodcv1.RegisterAgentResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentResponses = append(m.sentResponses, resp)
	return nil
}

func (m *mockRegisterAgentServer) Recv() (*fodcv1.RegisterAgentRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if len(m.recvRequests) == 0 {
		return nil, io.EOF
	}
	req := m.recvRequests[0]
	m.recvRequests = m.recvRequests[1:]
	return req, nil
}

func (m *mockRegisterAgentServer) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

func (m *mockRegisterAgentServer) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *mockRegisterAgentServer) AddRequest(req *fodcv1.RegisterAgentRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvRequests = append(m.recvRequests, req)
}

func (m *mockRegisterAgentServer) Context() context.Context {
	return m.ctx
}

func (m *mockRegisterAgentServer) SendMsg(_ interface{}) error {
	return nil
}

func (m *mockRegisterAgentServer) RecvMsg(_ interface{}) error {
	return nil
}

func (m *mockRegisterAgentServer) SetHeader(_ metadata.MD) error {
	return nil
}

func (m *mockRegisterAgentServer) SendHeader(_ metadata.MD) error {
	return nil
}

func (m *mockRegisterAgentServer) SetTrailer(_ metadata.MD) {}

// mockStreamMetricsServer implements fodcv1.FODCService_StreamMetricsServer for testing.
type mockStreamMetricsServer struct {
	ctx           context.Context
	recvErr       error
	sendErr       error
	recvChan      chan *fodcv1.StreamMetricsRequest
	sentResponses []*fodcv1.StreamMetricsResponse
	recvRequests  []*fodcv1.StreamMetricsRequest
	mu            sync.Mutex
}

func newMockStreamMetricsServer(ctx context.Context) *mockStreamMetricsServer {
	return &mockStreamMetricsServer{
		ctx:           ctx,
		sentResponses: make([]*fodcv1.StreamMetricsResponse, 0),
		recvRequests:  make([]*fodcv1.StreamMetricsRequest, 0),
		recvChan:      make(chan *fodcv1.StreamMetricsRequest, 10),
	}
}

func (m *mockStreamMetricsServer) Send(resp *fodcv1.StreamMetricsResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sentResponses = append(m.sentResponses, resp)
	return nil
}

func (m *mockStreamMetricsServer) Recv() (*fodcv1.StreamMetricsRequest, error) {
	m.mu.Lock()
	recvErr := m.recvErr
	m.mu.Unlock()
	if recvErr != nil {
		return nil, recvErr
	}
	select {
	case req := <-m.recvChan:
		return req, nil
	case <-m.ctx.Done():
		return nil, m.ctx.Err()
	case <-time.After(500 * time.Millisecond):
		return nil, io.EOF
	}
}

func (m *mockStreamMetricsServer) SetRecvError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvErr = err
}

func (m *mockStreamMetricsServer) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendErr = err
}

func (m *mockStreamMetricsServer) AddRequest(req *fodcv1.StreamMetricsRequest) {
	m.recvChan <- req
}

func (m *mockStreamMetricsServer) Context() context.Context {
	return m.ctx
}

func (m *mockStreamMetricsServer) SendMsg(_ interface{}) error {
	return nil
}

func (m *mockStreamMetricsServer) RecvMsg(_ interface{}) error {
	return nil
}

func (m *mockStreamMetricsServer) SetHeader(_ metadata.MD) error {
	return nil
}

func (m *mockStreamMetricsServer) SendHeader(_ metadata.MD) error {
	return nil
}

func (m *mockStreamMetricsServer) SetTrailer(_ metadata.MD) {}

func newTestService(t *testing.T) (*FODCService, *registry.AgentRegistry) {
	t.Helper()
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)
	return service, testRegistry
}

type mockRequestSender struct{}

func (m *mockRequestSender) RequestMetrics(_ context.Context, _ string, _, _ *time.Time) error {
	return nil
}

func TestNewFODCService(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)

	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)

	assert.NotNil(t, service)
	assert.Equal(t, testRegistry, service.registry)
	assert.Equal(t, aggregator, service.metricsAggregator)
	assert.Equal(t, 30*time.Second, service.heartbeatInterval)
	assert.NotNil(t, service.connections)
	assert.Equal(t, 0, len(service.connections))
}

func TestAgentConnection_UpdateActivity(t *testing.T) {
	conn := &AgentConnection{
		LastActivity: time.Now().Add(-1 * time.Hour),
	}

	conn.UpdateActivity()

	lastActivity := conn.GetLastActivity()
	assert.WithinDuration(t, time.Now(), lastActivity, time.Second)
}

func TestAgentConnection_GetLastActivity(t *testing.T) {
	now := time.Now()
	conn := &AgentConnection{
		LastActivity: now,
	}

	result := conn.GetLastActivity()

	assert.Equal(t, now, result)
}

func TestRegisterAgent_Success(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()
	stream := newMockRegisterAgentServer(ctx)

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: "worker",
		PrimaryAddress: &fodcv1.Address{
			Ip:   "192.168.1.1",
			Port: 8080,
		},
		Labels: map[string]string{"env": "test"},
	}
	stream.AddRequest(req)

	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.AddRequest(&fodcv1.RegisterAgentRequest{})
		time.Sleep(10 * time.Millisecond)
		stream.SetRecvError(io.EOF)
	}()

	err := service.RegisterAgent(stream)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(stream.sentResponses))
	assert.True(t, stream.sentResponses[0].Success)
	assert.Contains(t, stream.sentResponses[0].Message, "successfully")
	assert.Greater(t, stream.sentResponses[0].HeartbeatIntervalSeconds, int64(0))
	assert.NotEmpty(t, stream.sentResponses[0].AgentId, "AgentId should be present in registration response")

	service.connectionsMu.RLock()
	assert.Equal(t, 0, len(service.connections))
	service.connectionsMu.RUnlock()
}

func TestRegisterAgent_RegistrationError(t *testing.T) {
	service, testRegistry := newTestService(t)

	testRegistry.Stop()

	ctx := context.Background()
	stream := newMockRegisterAgentServer(ctx)

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: "worker",
		PrimaryAddress: &fodcv1.Address{
			Ip:   "",
			Port: 8080,
		},
	}
	stream.AddRequest(req)

	err := service.RegisterAgent(stream)

	assert.Error(t, err)
	assert.Equal(t, 1, len(stream.sentResponses))
	assert.False(t, stream.sentResponses[0].Success)
}

func TestRegisterAgent_SendError(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	stream := newMockRegisterAgentServer(ctx)
	stream.SetSendError(errors.New("send error"))

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: "worker",
		PrimaryAddress: &fodcv1.Address{
			Ip:   "192.168.1.1",
			Port: 8080,
		},
	}
	stream.AddRequest(req)

	err := service.RegisterAgent(stream)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send error")
	// Verify agent was unregistered after send error
	agents := testRegistry.ListAgents()
	assert.Equal(t, 0, len(agents), "Agent should be unregistered after send error")
	// Verify connection was cleaned up
	service.connectionsMu.RLock()
	assert.Equal(t, 0, len(service.connections))
	service.connectionsMu.RUnlock()
}

func TestRegisterAgent_HeartbeatUpdate(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()
	stream := newMockRegisterAgentServer(ctx)

	req := &fodcv1.RegisterAgentRequest{
		NodeRole: "worker",
		PrimaryAddress: &fodcv1.Address{
			Ip:   "192.168.1.1",
			Port: 8080,
		},
	}
	stream.AddRequest(req)

	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.AddRequest(&fodcv1.RegisterAgentRequest{})
		time.Sleep(10 * time.Millisecond)
		stream.SetRecvError(io.EOF)
	}()

	err := service.RegisterAgent(stream)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(stream.sentResponses), 1)
	if len(stream.sentResponses) > 0 {
		assert.True(t, stream.sentResponses[0].Success)
		assert.NotEmpty(t, stream.sentResponses[0].AgentId, "AgentId should be present in registration response")
	}
}

func TestStreamMetrics_Success(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)

	req := &fodcv1.StreamMetricsRequest{
		Metrics: []*fodcv1.Metric{
			{
				Name:   "cpu_usage",
				Value:  75.5,
				Labels: map[string]string{"cpu": "0"},
			},
		},
		Timestamp: timestamppb.Now(),
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		stream.AddRequest(req)
		time.Sleep(10 * time.Millisecond)
		stream.SetRecvError(io.EOF)
	}()

	err := service.StreamMetrics(stream)

	assert.NoError(t, err)
}

func TestStreamMetrics_AgentIDFromContext(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)

	connectionChecked := make(chan bool, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		service.connectionsMu.RLock()
		conn, exists := service.connections[agentID]
		service.connectionsMu.RUnlock()
		if exists && conn != nil && conn.MetricsStream != nil {
			connectionChecked <- true
		} else {
			connectionChecked <- false
		}
		time.Sleep(50 * time.Millisecond)
		stream.SetRecvError(io.EOF)
	}()

	err := service.StreamMetrics(stream)

	assert.NoError(t, err)
	checked := <-connectionChecked
	assert.True(t, checked, "Connection should exist while stream is active")
}

func TestStreamMetrics_AgentIDFromPeer(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	addr := &mockAddr{addr: "192.168.1.1:8080"}
	peerInfo := &peer.Peer{Addr: addr}
	ctxWithPeer := peer.NewContext(ctx, peerInfo)
	stream := newMockStreamMetricsServer(ctxWithPeer)

	connectionChecked := make(chan bool, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		service.connectionsMu.RLock()
		conn, exists := service.connections[agentID]
		service.connectionsMu.RUnlock()
		if exists && conn != nil && conn.MetricsStream != nil {
			connectionChecked <- true
		} else {
			connectionChecked <- false
		}
		time.Sleep(50 * time.Millisecond)
		stream.SetRecvError(io.EOF)
	}()

	err := service.StreamMetrics(stream)

	assert.NoError(t, err)
	checked := <-connectionChecked
	assert.True(t, checked, "Connection should exist while stream is active")
}

func TestStreamMetrics_NoAgentID(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()
	stream := newMockStreamMetricsServer(ctx)

	err := service.StreamMetrics(stream)

	assert.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
	assert.Contains(t, st.Message(), "agent ID not found")
}

func TestStreamMetrics_ContextCancelled(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx, cancel := context.WithCancel(context.Background())
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := service.StreamMetrics(stream)

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestStreamMetrics_RecvError(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)
	stream.SetRecvError(errors.New("recv error"))

	err := service.StreamMetrics(stream)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recv error")
}

func TestRequestMetrics_Success(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)

	service.connectionsMu.Lock()
	service.connections[agentID] = &AgentConnection{
		AgentID:       agentID,
		MetricsStream: stream,
	}
	service.connectionsMu.Unlock()

	now := time.Now()
	err := service.RequestMetrics(ctx, agentID, &now, nil)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(stream.sentResponses))
	assert.NotNil(t, stream.sentResponses[0].StartTime)
}

func TestRequestMetrics_WithTimeWindow(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)

	service.connectionsMu.Lock()
	service.connections[agentID] = &AgentConnection{
		AgentID:       agentID,
		MetricsStream: stream,
	}
	service.connectionsMu.Unlock()

	startTime := time.Now().Add(-1 * time.Hour)
	endTime := time.Now()
	err := service.RequestMetrics(ctx, agentID, &startTime, &endTime)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(stream.sentResponses))
	assert.NotNil(t, stream.sentResponses[0].StartTime)
	assert.NotNil(t, stream.sentResponses[0].EndTime)
}

func TestRequestMetrics_ConnectionNotFound(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()
	err := service.RequestMetrics(ctx, "non-existent", nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection not found")
}

func TestRequestMetrics_NoMetricsStream(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	service.connectionsMu.Lock()
	service.connections[agentID] = &AgentConnection{
		AgentID:       agentID,
		MetricsStream: nil,
	}
	service.connectionsMu.Unlock()

	err := service.RequestMetrics(ctx, agentID, nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metrics stream not established")
}

func TestRequestMetrics_SendError(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	md := metadata.New(map[string]string{"agent_id": agentID})
	ctxWithMD := metadata.NewIncomingContext(ctx, md)
	stream := newMockStreamMetricsServer(ctxWithMD)
	stream.SetSendError(errors.New("send error"))

	service.connectionsMu.Lock()
	service.connections[agentID] = &AgentConnection{
		AgentID:       agentID,
		MetricsStream: stream,
	}
	service.connectionsMu.Unlock()

	err := service.RequestMetrics(ctx, agentID, nil, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to send metrics request")
}

func TestGetAgentIDFromContext_Success(t *testing.T) {
	service, _ := newTestService(t)

	md := metadata.New(map[string]string{"agent_id": "test-agent-123"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	agentID := service.getAgentIDFromContext(ctx)

	assert.Equal(t, "test-agent-123", agentID)
}

func TestGetAgentIDFromContext_NoMetadata(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()

	agentID := service.getAgentIDFromContext(ctx)

	assert.Empty(t, agentID)
}

func TestGetAgentIDFromContext_NoAgentID(t *testing.T) {
	service, _ := newTestService(t)

	md := metadata.New(map[string]string{"other_key": "value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	agentID := service.getAgentIDFromContext(ctx)

	assert.Empty(t, agentID)
}

func TestGetAgentIDFromPeer_Success(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	addr := &mockAddr{addr: "192.168.1.1:8080"}
	peerInfo := &peer.Peer{Addr: addr}
	ctxWithPeer := peer.NewContext(ctx, peerInfo)

	result := service.getAgentIDFromPeer(ctxWithPeer)

	assert.Equal(t, agentID, result)
}

func TestGetAgentIDFromPeer_SecondaryAddress(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	addr := &mockAddr{addr: "192.168.1.1:8080"}
	peerInfo := &peer.Peer{Addr: addr}
	ctxWithPeer := peer.NewContext(ctx, peerInfo)

	result := service.getAgentIDFromPeer(ctxWithPeer)

	assert.Equal(t, agentID, result)
}

func TestGetAgentIDFromPeer_NoPeer(t *testing.T) {
	service, _ := newTestService(t)

	ctx := context.Background()

	result := service.getAgentIDFromPeer(ctx)

	assert.Empty(t, result)
}

func TestGetAgentIDFromPeer_NoMatch(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	_, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	addr := &mockAddr{addr: "192.168.1.99:8080"}
	peerInfo := &peer.Peer{Addr: addr}
	ctxWithPeer := peer.NewContext(ctx, peerInfo)

	result := service.getAgentIDFromPeer(ctxWithPeer)

	assert.Empty(t, result)
}

func TestCleanupConnection(t *testing.T) {
	service, testRegistry := newTestService(t)

	ctx := context.Background()
	identity := registry.AgentIdentity{IP: "192.168.1.1", Port: 8080, Role: "worker"}
	agentID, registerErr := testRegistry.RegisterAgent(ctx, identity, registry.Address{IP: "192.168.1.1", Port: 8080})
	require.NoError(t, registerErr)

	service.connectionsMu.Lock()
	service.connections[agentID] = &AgentConnection{AgentID: agentID}
	service.connectionsMu.Unlock()

	service.cleanupConnection(agentID)

	service.connectionsMu.RLock()
	_, exists := service.connections[agentID]
	service.connectionsMu.RUnlock()
	assert.False(t, exists)
}

func TestCleanupConnection_EmptyAgentID(t *testing.T) {
	service, _ := newTestService(t)

	service.cleanupConnection("")

	service.connectionsMu.RLock()
	count := len(service.connections)
	service.connectionsMu.RUnlock()
	assert.Equal(t, 0, count)
}

func TestCleanupConnection_NonExistent(t *testing.T) {
	service, _ := newTestService(t)

	service.cleanupConnection("non-existent")

	service.connectionsMu.RLock()
	count := len(service.connections)
	service.connectionsMu.RUnlock()
	assert.Equal(t, 0, count)
}

type mockAddr struct {
	addr string
}

func (m *mockAddr) Network() string {
	return "tcp"
}

func (m *mockAddr) String() string {
	return m.addr
}
