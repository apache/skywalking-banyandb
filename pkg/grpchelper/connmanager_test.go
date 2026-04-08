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

package grpchelper

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func addTestClient(m *ConnManager[*mockClient], node string) {
	m.mu.Lock()
	m.active[node] = &managedNode[*mockClient]{client: &mockClient{}}
	m.mu.Unlock()
}

func removeTestClient(m *ConnManager[*mockClient], node string) {
	m.mu.Lock()
	delete(m.active, node)
	m.mu.Unlock()
}

func TestExecute_Success(t *testing.T) {
	m := newTestConnManager()
	node := "exec-success"
	addTestClient(m, node)
	defer func() {
		removeTestClient(m, node)
		m.GracefulStop()
	}()
	m.RecordSuccess(node)

	called := false
	execErr := m.Execute(node, func(_ *mockClient) error {
		called = true
		return nil
	})
	require.NoError(t, execErr)
	assert.True(t, called)
	assert.True(t, m.IsRequestAllowed(node), "circuit breaker should still allow requests after success")
}

func TestExecute_CallbackFailure(t *testing.T) {
	m := newTestConnManager()
	node := "exec-fail"
	addTestClient(m, node)
	defer func() {
		removeTestClient(m, node)
		m.GracefulStop()
	}()
	m.RecordSuccess(node)

	cbErr := status.Error(codes.Unavailable, "callback error")
	for i := 0; i < defaultCBThreshold; i++ {
		execErr := m.Execute(node, func(_ *mockClient) error {
			return cbErr
		})
		require.Error(t, execErr)
	}
	assert.False(t, m.IsRequestAllowed(node), "circuit breaker should open after repeated failures")
}

func TestExecute_CircuitBreakerOpen(t *testing.T) {
	m := newTestConnManager()
	node := "exec-cb-open"
	addTestClient(m, node)
	defer func() {
		removeTestClient(m, node)
		m.GracefulStop()
	}()

	for i := 0; i < defaultCBThreshold; i++ {
		m.RecordFailure(node, errTransient)
	}

	execErr := m.Execute(node, func(_ *mockClient) error {
		t.Fatal("callback should not be called when circuit breaker is open")
		return nil
	})
	require.Error(t, execErr)
	assert.ErrorIs(t, execErr, ErrCircuitBreakerOpen)
	assert.Contains(t, execErr.Error(), node)
}

func TestExecute_ClientNotFound(t *testing.T) {
	m := newTestConnManager()
	defer m.GracefulStop()
	node := "exec-no-client"

	execErr := m.Execute(node, func(_ *mockClient) error {
		t.Fatal("callback should not be called when client is not found")
		return nil
	})
	require.Error(t, execErr)
	assert.ErrorIs(t, execErr, ErrClientNotFound)
	assert.Contains(t, execErr.Error(), node)
}

func startHealthServerOnAddr(t *testing.T, addr string) (string, func()) {
	t.Helper()
	lis, lisErr := net.Listen("tcp", addr)
	require.NoError(t, lisErr)
	srv := grpc.NewServer()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() { srv.Stop() }
}

func startHealthServer(t *testing.T) (string, func()) {
	t.Helper()
	return startHealthServerOnAddr(t, "127.0.0.1:0")
}

func buildNode(name, addr string) *databasev1.Node {
	return &databasev1.Node{
		Metadata:    &commonv1.Metadata{Name: name},
		GrpcAddress: addr,
	}
}

// mockHandlerWithInsecure extends mockHandler with insecure dial options for real gRPC connections.
type mockHandlerWithInsecure struct {
	mockHandler
}

func (h *mockHandlerWithInsecure) GetDialOptions() ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
}

func newHealthCheckConnManager(t *testing.T, interval time.Duration) (*ConnManager[*mockClient], *mockHandlerWithInsecure) {
	t.Helper()
	handler := &mockHandlerWithInsecure{}
	mgr := NewConnManager(ConnManagerConfig[*mockClient]{
		Handler:             handler,
		Logger:              logger.GetLogger("test-hc"),
		HealthCheckInterval: interval,
		HealthCheckTimeout:  1 * time.Second,
	})
	t.Cleanup(func() { mgr.GracefulStop() })
	return mgr, handler
}

func TestPeriodicHealthCheck(t *testing.T) {
	t.Run("unhealthy_node_moves_to_evictable", func(t *testing.T) {
		// Start a real gRPC health server, register a node, then stop the server.
		// The periodic health check should detect the node is unhealthy
		// and call OnInactive.
		addr, stopServer := startHealthServer(t)

		mgr, handler := newHealthCheckConnManager(t, 200*time.Millisecond)

		// Register the node — health check passes, node becomes active.
		mgr.OnAddOrUpdate(buildNode("hc-node", addr))
		require.Equal(t, 1, mgr.ActiveCount())
		require.True(t, handler.isActive("hc-node"))

		// Stop the server to make the node unhealthy.
		stopServer()

		// Wait for periodic health check to detect the failure.
		require.Eventually(t, func() bool {
			return mgr.ActiveCount() == 0
		}, testflags.EventuallyTimeout, 100*time.Millisecond,
			"node should be moved from active to evictable after health check failure")

		// Verify OnInactive was called.
		assert.True(t, handler.isInactive("hc-node"))
		assert.Equal(t, 1, mgr.EvictableCount())
	})

	t.Run("healthy_node_stays_active", func(t *testing.T) {
		// Start a real gRPC health server that remains running.
		// The periodic health check should keep the node active.
		addr, stopServer := startHealthServer(t)
		defer stopServer()

		mgr, handler := newHealthCheckConnManager(t, 200*time.Millisecond)

		mgr.OnAddOrUpdate(buildNode("hc-stable", addr))
		require.Equal(t, 1, mgr.ActiveCount())

		// Wait for several health check cycles.
		time.Sleep(1 * time.Second)

		// Node should still be active.
		assert.Equal(t, 1, mgr.ActiveCount())
		assert.Equal(t, 0, mgr.EvictableCount())
		assert.True(t, handler.isActive("hc-stable"))
	})

	t.Run("node_recovers_after_server_restart", func(t *testing.T) {
		// Start server, register node, stop server (node becomes evictable),
		// restart server on same address (node should recover to active).
		addr, stopServer := startHealthServer(t)

		mgr, handler := newHealthCheckConnManager(t, 200*time.Millisecond)

		mgr.OnAddOrUpdate(buildNode("hc-restart", addr))
		require.Equal(t, 1, mgr.ActiveCount())

		// Stop server — node should become evictable.
		stopServer()
		require.Eventually(t, func() bool {
			return mgr.ActiveCount() == 0
		}, testflags.EventuallyTimeout, 100*time.Millisecond,
			"node should become evictable after server stop")
		assert.True(t, handler.isInactive("hc-restart"))

		// Restart server on the same address.
		_, stopRestarted := startHealthServerOnAddr(t, addr)
		defer stopRestarted()

		// The eviction retry goroutine should reconnect the node.
		require.Eventually(t, func() bool {
			return mgr.ActiveCount() == 1
		}, testflags.EventuallyTimeout, 100*time.Millisecond,
			"node should recover to active after server restart")
		assert.True(t, handler.isActive("hc-restart"))
	})

	t.Run("disabled_when_interval_is_zero", func(t *testing.T) {
		// With HealthCheckInterval=0, no periodic health check goroutine should run.
		// Start a real server but the health check should never fire.
		addr, stopServer := startHealthServer(t)

		mgr, _ := newHealthCheckConnManager(t, 0)
		mgr.OnAddOrUpdate(buildNode("no-hc-node", addr))
		require.Equal(t, 1, mgr.ActiveCount())

		// Stop the server. Without periodic health check, the node stays active.
		stopServer()
		time.Sleep(500 * time.Millisecond)

		// Node should still be active — no health check removed it.
		assert.Equal(t, 1, mgr.ActiveCount())
	})

	t.Run("graceful_stop_terminates_health_check", func(t *testing.T) {
		// Verify that GracefulStop cleanly shuts down the periodic health check goroutine.
		addr, stopServer := startHealthServer(t)
		defer stopServer()

		handler := &mockHandlerWithInsecure{}
		mgr := NewConnManager(ConnManagerConfig[*mockClient]{
			Handler:             handler,
			Logger:              logger.GetLogger("test-hc-stop"),
			HealthCheckInterval: 100 * time.Millisecond,
			HealthCheckTimeout:  1 * time.Second,
		})

		mgr.OnAddOrUpdate(buildNode("hc-stop", addr))
		require.Equal(t, 1, mgr.ActiveCount())

		// GracefulStop should return without hanging (health check goroutine exits).
		done := make(chan struct{})
		go func() {
			mgr.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
			// Success — GracefulStop returned.
		case <-time.After(5 * time.Second):
			t.Fatal("GracefulStop did not return in time — periodic health check may be leaked")
		}
	})
}

func TestPeriodicHealthCheck_ConnectsViaGRPC(t *testing.T) {
	// Integration test: verify that OnAddOrUpdate creates a real gRPC connection
	// and the periodic health check keeps the node active end-to-end.
	addr, stopServer := startHealthServer(t)

	mgr, handler := newHealthCheckConnManager(t, 200*time.Millisecond)

	// Register the node via ConnManager — should establish a real gRPC connection.
	mgr.OnAddOrUpdate(buildNode("grpc-node", addr))
	require.Equal(t, 1, mgr.ActiveCount())
	require.True(t, handler.isActive("grpc-node"))

	// Wait for several health check cycles — node should stay active.
	time.Sleep(1 * time.Second)
	assert.Equal(t, 1, mgr.ActiveCount())

	// Stop server — periodic health check should detect failure and evict.
	stopServer()
	require.Eventually(t, func() bool {
		return mgr.ActiveCount() == 0
	}, testflags.EventuallyTimeout, 100*time.Millisecond,
		"node should be evicted after server stops")
	assert.True(t, handler.isInactive("grpc-node"))
}
