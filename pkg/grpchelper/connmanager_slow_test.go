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
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type blockedHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
	blocked     atomic.Bool
	unhealthy   atomic.Bool
}

func newBlockedHealthServer(t *testing.T) *blockedHealthServer {
	t.Helper()
	server := &blockedHealthServer{entered: make(chan struct{}, 8), release: make(chan struct{})}
	t.Cleanup(server.unblock)
	return server
}

func (s *blockedHealthServer) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if s.blocked.Load() {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	servingStatus := grpc_health_v1.HealthCheckResponse_SERVING
	if s.unhealthy.Load() {
		servingStatus = grpc_health_v1.HealthCheckResponse_NOT_SERVING
	}
	return &grpc_health_v1.HealthCheckResponse{Status: servingStatus}, nil
}

func (s *blockedHealthServer) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func serveBlockedHealth(t *testing.T, server *blockedHealthServer, options ...grpc.ServerOption) string {
	t.Helper()
	listener, listenErr := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, listenErr)
	grpcServer := grpc.NewServer(options...)
	grpc_health_v1.RegisterHealthServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	return listener.Addr().String()
}

type healthConnectionHandler struct {
	activated []*grpc.ClientConn
	mu        sync.Mutex
}

func (*healthConnectionHandler) AddressOf(node *databasev1.Node) string {
	return node.GetGrpcAddress()
}

func (*healthConnectionHandler) GetDialOptions() ([]grpc.DialOption, error) {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, nil
}

func (*healthConnectionHandler) NewClient(conn *grpc.ClientConn, _ *databasev1.Node) (*grpc.ClientConn, error) {
	return conn, nil
}

func (h *healthConnectionHandler) OnActive(_ string, conn *grpc.ClientConn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.activated = append(h.activated, conn)
}

func (*healthConnectionHandler) OnInactive(string, *grpc.ClientConn) {}

func (h *healthConnectionHandler) activationCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.activated)
}

func newBlockedHealthManager(t *testing.T) (*ConnManager[*grpc.ClientConn], *healthConnectionHandler, func()) {
	t.Helper()
	handler := &healthConnectionHandler{}
	manager := NewConnManager(ConnManagerConfig[*grpc.ClientConn]{
		Handler:            handler,
		Logger:             logger.GetLogger("test-blocked-health"),
		HealthCheckTimeout: 30 * time.Second,
	})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(manager.GracefulStop) }
	t.Cleanup(stop)
	return manager, handler, stop
}

func startHealthOperation(operation func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		operation()
	}()
	return done
}

func awaitHealthOperation(t *testing.T, done <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not finish while the slow health response was held", operation)
	}
}

func checkServing(conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, requestErr := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if requestErr != nil {
		return requestErr
	}
	if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("unexpected health response: %v", response)
	}
	return nil
}

func TestSlowHealthDoesNotBlockHealthyNode(t *testing.T) {
	for _, scenario := range []string{"startup", "maintenance", "deletion"} {
		t.Run(scenario, func(t *testing.T) {
			healthyAddress, stopHealthy := startHealthServer(t)
			t.Cleanup(stopHealthy)
			slow := newBlockedHealthServer(t)
			slowAddress := serveBlockedHealth(t, slow)
			manager, _, _ := newBlockedHealthManager(t)
			// Release the gate before manager cleanup, including on assertion failures.
			t.Cleanup(slow.unblock)
			healthyNode := buildNode("healthy", healthyAddress)
			slowNode := buildNode("slow", slowAddress)
			manager.OnAddOrUpdate(healthyNode)
			require.NoError(t, manager.Execute("healthy", checkServing))
			if scenario != "startup" {
				manager.OnAddOrUpdate(slowNode)
			}
			slow.blocked.Store(true)
			probeDone := startHealthOperation(func() {
				switch scenario {
				case "startup":
					manager.OnAddOrUpdate(slowNode)
				case "maintenance":
					manager.FailoverNode("slow")
				case "deletion":
					manager.OnDelete(slowNode)
				}
			})
			awaitHealthOperation(t, slow.entered, "slow health request arrival")
			// A real RPC to the healthy server proves its response is independent of B.
			direct, dialErr := grpc.NewClient(healthyAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, dialErr)
			t.Cleanup(func() { _ = direct.Close() })
			require.NoError(t, checkServing(direct))

			queryDone := make(chan error, 1)
			go func() { queryDone <- manager.Execute("healthy", checkServing) }()
			routesDone := make(chan []*databasev1.Node, 1)
			go func() { routesDone <- manager.ActiveRegisteredNodes() }()
			select {
			case queryErr := <-queryDone:
				require.NoError(t, queryErr)
			case <-time.After(2 * time.Second):
				t.Fatal("healthy query blocked by another node's health check")
			}
			select {
			case routes := <-routesDone:
				require.Contains(t, routes, healthyNode)
			case <-time.After(2 * time.Second):
				t.Fatal("routing snapshot blocked by another node's health check")
			}
			select {
			case <-probeDone:
				t.Fatal("slow health operation completed before its response was released")
			default:
			}
			slow.unblock()
			awaitHealthOperation(t, probeDone, "released health operation")
		})
	}
}

func TestSlowStartupDiscardsSupersededConnection(t *testing.T) {
	for _, mutation := range []string{"delete", "replace", "stop"} {
		t.Run(mutation, func(t *testing.T) {
			slow := newBlockedHealthServer(t)
			slowAddress := serveBlockedHealth(t, slow)
			manager, handler, stopManager := newBlockedHealthManager(t)
			t.Cleanup(slow.unblock)
			node := buildNode("starting", slowAddress)
			slow.blocked.Store(true)
			startupDone := startHealthOperation(func() { manager.OnAddOrUpdate(node) })
			awaitHealthOperation(t, slow.entered, "startup health request arrival")

			var replacementAddress string
			if mutation == "replace" {
				var stopReplacement func()
				replacementAddress, stopReplacement = startHealthServer(t)
				t.Cleanup(stopReplacement)
			}
			mutationDone := startHealthOperation(func() {
				switch mutation {
				case "delete":
					manager.OnDelete(node)
				case "replace":
					manager.OnAddOrUpdate(buildNode("starting", replacementAddress))
				case "stop":
					stopManager()
				}
			})
			awaitHealthOperation(t, mutationDone, mutation)
			slow.unblock()
			awaitHealthOperation(t, startupDone, "superseded startup")

			if mutation == "replace" {
				client, found := manager.GetClient("starting")
				require.True(t, found)
				require.Equal(t, replacementAddress, client.Target())
				require.NoError(t, manager.Execute("starting", checkServing))
				require.Equal(t, 1, handler.activationCount(), "only the replacement may become active")
			} else {
				require.Zero(t, manager.ActiveCount())
				require.Empty(t, manager.ActiveRegisteredNodes())
				require.Zero(t, handler.activationCount(), "superseded startup must never become active")
			}
			require.Zero(t, manager.EvictableCount(), "superseded startup must not start reconnecting")
		})
	}
}

func TestSlowStartupDuplicateRegistration(t *testing.T) {
	slow := newBlockedHealthServer(t)
	slowAddress := serveBlockedHealth(t, slow)
	manager, handler, _ := newBlockedHealthManager(t)
	t.Cleanup(slow.unblock)
	slow.blocked.Store(true)
	startupDone := startHealthOperation(func() { manager.OnAddOrUpdate(buildNode("starting", slowAddress)) })
	awaitHealthOperation(t, slow.entered, "startup health request arrival")

	// A new node object with the same identity and address must reuse the in-flight attempt.
	duplicateDone := startHealthOperation(func() { manager.OnAddOrUpdate(buildNode("starting", slowAddress)) })
	awaitHealthOperation(t, duplicateDone, "duplicate registration")
	require.Zero(t, handler.activationCount())
	slow.unblock()
	awaitHealthOperation(t, startupDone, "startup health check")
	require.Equal(t, 1, manager.ActiveCount())
	require.Equal(t, 1, handler.activationCount())
	require.NoError(t, manager.Execute("starting", checkServing))
}

func TestSlowHealthResultCannotRemoveReplacement(t *testing.T) {
	for _, scenario := range []string{"maintenance", "deletion"} {
		t.Run(scenario, func(t *testing.T) {
			slow := newBlockedHealthServer(t)
			slowAddress := serveBlockedHealth(t, slow)
			manager, handler, _ := newBlockedHealthManager(t)
			t.Cleanup(slow.unblock)
			node := buildNode("replaced", slowAddress)
			manager.OnAddOrUpdate(node)
			slow.blocked.Store(true)
			probeDone := startHealthOperation(func() {
				if scenario == "maintenance" {
					manager.FailoverNode("replaced")
				} else {
					manager.OnDelete(node)
				}
			})
			awaitHealthOperation(t, slow.entered, "old connection health request arrival")

			replacementAddress, stopReplacement := startHealthServer(t)
			t.Cleanup(stopReplacement)
			replacement := buildNode("replaced", replacementAddress)
			replacementDone := startHealthOperation(func() { manager.OnAddOrUpdate(replacement) })
			awaitHealthOperation(t, replacementDone, "replacement registration")
			// Closing the old connection makes its outstanding health check fail.
			// That failure belongs to the old connection and must not remove the new one.
			slow.unblock()
			awaitHealthOperation(t, probeDone, "old connection health check")

			client, found := manager.GetClient("replaced")
			require.True(t, found)
			require.Equal(t, replacementAddress, client.Target())
			require.NoError(t, manager.Execute("replaced", checkServing))
			require.Equal(t, []*databasev1.Node{replacement}, manager.ActiveRegisteredNodes())
			require.Equal(t, 2, handler.activationCount())
			require.Zero(t, manager.EvictableCount())
		})
	}
}

type healthConnectionStats struct {
	closed chan struct{}
}

func (*healthConnectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (*healthConnectionStats) HandleRPC(context.Context, stats.RPCStats) {}

func (*healthConnectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (s *healthConnectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, closed := event.(*stats.ConnEnd); closed {
		s.closed <- struct{}{}
	}
}

func TestSlowReconnectClosesSupersededConnection(t *testing.T) {
	slow := newBlockedHealthServer(t)
	connections := &healthConnectionStats{closed: make(chan struct{}, 4)}
	slowAddress := serveBlockedHealth(t, slow, grpc.StatsHandler(connections))
	manager, handler, stopManager := newBlockedHealthManager(t)
	t.Cleanup(slow.unblock)
	slow.unhealthy.Store(true)
	manager.OnAddOrUpdate(buildNode("reconnecting", slowAddress))
	require.Equal(t, 1, manager.EvictableCount())
	awaitHealthOperation(t, connections.closed, "failed initial connection shutdown")

	// Wait for the real retry loop to enter its health check, then hold it there.
	slow.unhealthy.Store(false)
	slow.blocked.Store(true)
	awaitHealthOperation(t, slow.entered, "reconnect health request arrival")
	replacementServer := newBlockedHealthServer(t)
	replacementServer.unhealthy.Store(true)
	replacementAddress := serveBlockedHealth(t, replacementServer)
	replacement := buildNode("reconnecting", replacementAddress)
	replacementDone := startHealthOperation(func() { manager.OnAddOrUpdate(replacement) })
	awaitHealthOperation(t, replacementDone, "replacement registration")
	awaitHealthOperation(t, connections.closed, "superseded reconnect connection shutdown")
	slow.unblock()

	// The old attempt must neither activate its connection nor erase the newer
	// eviction entry for the same node name.
	require.Equal(t, []*databasev1.Node{replacement}, manager.GetRouteTable().Registered)
	require.Equal(t, 1, manager.EvictableCount())
	require.Zero(t, manager.ActiveCount())
	require.Zero(t, handler.activationCount())
	stopManager()
	require.Zero(t, handler.activationCount())
}

func TestSlowStartupSupersededByReconnectAll(t *testing.T) {
	slow := newBlockedHealthServer(t)
	slowAddress := serveBlockedHealth(t, slow)
	manager, handler, _ := newBlockedHealthManager(t)
	t.Cleanup(slow.unblock)
	slow.blocked.Store(true)
	startupDone := startHealthOperation(func() { manager.OnAddOrUpdate(buildNode("reconnecting", slowAddress)) })
	awaitHealthOperation(t, slow.entered, "startup health request arrival")

	reconnectDone := startHealthOperation(manager.ReconnectAll)
	awaitHealthOperation(t, startupDone, "superseded startup cancellation")
	awaitHealthOperation(t, slow.entered, "replacement startup health request arrival")
	require.Zero(t, handler.activationCount())
	slow.unblock()
	awaitHealthOperation(t, reconnectDone, "reconnect all")
	require.Equal(t, 1, handler.activationCount())
	require.Equal(t, 1, manager.ActiveCount())
	require.Zero(t, manager.EvictableCount())
	require.NoError(t, manager.Execute("reconnecting", checkServing))
}

func TestSlowReconnectAllStopsReservedAttempts(t *testing.T) {
	first := newBlockedHealthServer(t)
	firstAddress := serveBlockedHealth(t, first)
	second := newBlockedHealthServer(t)
	secondAddress := serveBlockedHealth(t, second)
	manager, handler, stopManager := newBlockedHealthManager(t)
	t.Cleanup(first.unblock)
	t.Cleanup(second.unblock)
	manager.OnAddOrUpdate(buildNode("first", firstAddress))
	manager.OnAddOrUpdate(buildNode("second", secondAddress))
	require.Equal(t, 2, handler.activationCount())
	first.blocked.Store(true)
	second.blocked.Store(true)

	reconnectDone := startHealthOperation(manager.ReconnectAll)
	// ReconnectAll reserves both nodes before probing them one at a time.
	// Whichever map entry comes first holds the other reservation open.
	select {
	case <-first.entered:
	case <-second.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect health request never arrived")
	}
	stopDone := startHealthOperation(stopManager)
	awaitHealthOperation(t, stopDone, "shutdown with reserved reconnect attempts")
	awaitHealthOperation(t, reconnectDone, "canceled reconnect all")
	require.Zero(t, manager.ActiveCount())
	require.Zero(t, manager.EvictableCount())
	require.Equal(t, 2, handler.activationCount(), "shutdown must prevent any reconnect activation")
}

func TestSlowHealthSuppressesDuplicateFailover(t *testing.T) {
	slow := newBlockedHealthServer(t)
	slowAddress := serveBlockedHealth(t, slow)
	manager, handler, _ := newBlockedHealthManager(t)
	t.Cleanup(slow.unblock)
	manager.OnAddOrUpdate(buildNode("slow", slowAddress))
	slow.blocked.Store(true)
	probeDone := startHealthOperation(func() { manager.FailoverNode("slow") })
	awaitHealthOperation(t, slow.entered, "first health request arrival")

	duplicateDone := startHealthOperation(func() { manager.FailoverNode("slow") })
	awaitHealthOperation(t, duplicateDone, "duplicate failover")
	select {
	case <-slow.entered:
		t.Fatal("duplicate failover started another health request")
	default:
	}
	slow.unblock()
	awaitHealthOperation(t, probeDone, "original failover")
	require.Equal(t, 1, manager.ActiveCount())
	require.Equal(t, 1, handler.activationCount())
}
