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
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

var (
	// ErrCircuitBreakerOpen is returned when the circuit breaker is open for a node.
	ErrCircuitBreakerOpen = errors.New("circuit breaker open")
	// ErrClientNotFound is returned when no active client exists for a node.
	ErrClientNotFound = errors.New("client not found")
)

// Client is the minimal interface for a managed gRPC client.
type Client interface {
	Close() error
}

// ConnectionHandler provides upper-layer callbacks for connection lifecycle.
type ConnectionHandler[C Client] interface {
	// AddressOf extracts the gRPC address from a node.
	AddressOf(node *databasev1.Node) string
	// GetDialOptions returns gRPC dial options for the given address.
	GetDialOptions(address string) ([]grpc.DialOption, error)
	// NewClient creates a client from a gRPC connection and node.
	NewClient(conn *grpc.ClientConn, node *databasev1.Node) (C, error)
	// OnActive is called when a node transitions to active.
	OnActive(name string, client C)
	// OnInactive is called when a node leaves active.
	OnInactive(name string, client C)
}

// ConnManagerConfig holds configuration for ConnManager.
type ConnManagerConfig[C Client] struct {
	Handler        ConnectionHandler[C]
	Logger         *logger.Logger
	RetryPolicy    string
	ExtraDialOpts  []grpc.DialOption
	MaxRecvMsgSize int
}

// ConnManager manages gRPC connections with health checking, circuit breaking, and eviction.
type ConnManager[C Client] struct {
	handler    ConnectionHandler[C]
	log        *logger.Logger
	registered map[string]*databasev1.Node
	active     map[string]*managedNode[C]
	evictable  map[string]evictNode
	cbStates   map[string]*circuitState
	closer     *run.Closer
	dialOpts   []grpc.DialOption
	mu         sync.RWMutex
	cbMu       sync.RWMutex
}

type managedNode[C Client] struct {
	client C
	conn   *grpc.ClientConn
	node   *databasev1.Node
}

type evictNode struct {
	n *databasev1.Node
	c chan struct{}
}

// NewConnManager creates a new ConnManager.
func NewConnManager[C Client](cfg ConnManagerConfig[C]) *ConnManager[C] {
	var dialOpts []grpc.DialOption
	if cfg.RetryPolicy != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(cfg.RetryPolicy))
	}
	if cfg.MaxRecvMsgSize > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize)))
	}
	dialOpts = append(dialOpts, cfg.ExtraDialOpts...)
	m := &ConnManager[C]{
		handler:    cfg.Handler,
		log:        cfg.Logger,
		registered: make(map[string]*databasev1.Node),
		active:     make(map[string]*managedNode[C]),
		evictable:  make(map[string]evictNode),
		cbStates:   make(map[string]*circuitState),
		closer:     run.NewCloser(1),
		dialOpts:   dialOpts,
	}
	return m
}

// OnAddOrUpdate registers or updates a node and manages its connection.
func (m *ConnManager[C]) OnAddOrUpdate(node *databasev1.Node) {
	address := m.handler.AddressOf(node)
	if address == "" {
		m.log.Warn().Stringer("node", node).Msg("grpc address is empty")
		return
	}
	name := node.Metadata.GetName()
	if name == "" {
		m.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerNode(node)

	if _, ok := m.active[name]; ok {
		return
	}
	if _, ok := m.evictable[name]; ok {
		return
	}
	credOpts, dialErr := m.handler.GetDialOptions(address)
	if dialErr != nil {
		m.log.Error().Err(dialErr).Msg("failed to load client TLS credentials")
		return
	}
	allOpts := make([]grpc.DialOption, 0, len(credOpts)+len(m.dialOpts))
	allOpts = append(allOpts, credOpts...)
	allOpts = append(allOpts, m.dialOpts...)
	conn, connErr := grpc.NewClient(address, allOpts...)
	if connErr != nil {
		m.log.Error().Err(connErr).Msg("failed to connect to grpc server")
		return
	}

	client, clientErr := m.handler.NewClient(conn, node)
	if clientErr != nil {
		m.log.Error().Err(clientErr).Msg("failed to create client")
		_ = conn.Close()
		return
	}
	if !m.checkHealthAndReconnect(conn, node, client) {
		m.log.Info().Str("status", m.dump()).Stringer("node", node).Msg("node is unhealthy in the register flow, move it to evict queue")
		return
	}
	m.active[name] = &managedNode[C]{conn: conn, client: client, node: node}
	m.handler.OnActive(name, client)
	// Initialize or reset circuit breaker state to closed
	m.RecordSuccess(name)
	m.log.Info().Str("status", m.dump()).Stringer("node", node).Msg("new node is healthy, add it to active queue")
}

// OnDelete removes a node and its connection.
func (m *ConnManager[C]) OnDelete(node *databasev1.Node) {
	name := node.Metadata.GetName()
	if name == "" {
		m.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.registered, name)
	if en, ok := m.evictable[name]; ok {
		close(en.c)
		delete(m.evictable, name)
		m.log.Info().Str("status", m.dump()).Stringer("node", node).Msg("node is removed from evict queue by delete event")
		return
	}
	if mn, ok := m.active[name]; ok {
		if m.removeNodeIfUnhealthy(name, mn) {
			m.log.Info().Str("status", m.dump()).Stringer("node", node).Msg("remove node from active queue by delete event")
			return
		}
		if !m.closer.AddRunning() {
			return
		}
		go func() {
			defer m.closer.Done()
			var elapsed time.Duration
			attempt := 0
			for {
				backoff := JitteredBackoff(InitBackoff, MaxBackoff, attempt, DefaultJitterFactor)
				select {
				case <-time.After(backoff):
					if func() bool {
						elapsed += backoff
						m.mu.Lock()
						defer m.mu.Unlock()
						if _, ok := m.registered[name]; ok {
							return true
						}
						if m.removeNodeIfUnhealthy(name, mn) {
							m.log.Info().Str("status", m.dump()).Stringer("node", node).Dur("after", elapsed).Msg("remove node from active queue by delete event")
							return true
						}
						return false
					}() {
						return
					}
				case <-m.closer.CloseNotify():
					return
				}
				attempt++
			}
		}()
	}
}

// GetClient returns the client for the given node name.
func (m *ConnManager[C]) GetClient(name string) (C, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mn, ok := m.active[name]
	if !ok {
		var zero C
		return zero, false
	}
	return mn.client, true
}

// Execute checks the circuit breaker, gets the client, calls fn, and records success or failure.
func (m *ConnManager[C]) Execute(node string, fn func(C) error) error {
	if !m.IsRequestAllowed(node) {
		return fmt.Errorf("%w for node %s", ErrCircuitBreakerOpen, node)
	}
	c, ok := m.GetClient(node)
	if !ok {
		return fmt.Errorf("%w for node %s", ErrClientNotFound, node)
	}
	if callbackErr := fn(c); callbackErr != nil {
		m.RecordFailure(node, callbackErr)
		return callbackErr
	}
	m.RecordSuccess(node)
	return nil
}

// ActiveNames returns the names of all active nodes.
func (m *ConnManager[C]) ActiveNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.active))
	for name := range m.active {
		names = append(names, name)
	}
	return names
}

// ActiveCount returns the number of active nodes.
func (m *ConnManager[C]) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.active)
}

// EvictableCount returns the number of evictable nodes.
func (m *ConnManager[C]) EvictableCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.evictable)
}

// ActiveRegisteredNodes returns the registered node info for all active nodes.
func (m *ConnManager[C]) ActiveRegisteredNodes() []*databasev1.Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var nodes []*databasev1.Node
	for k := range m.active {
		if n := m.registered[k]; n != nil {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// GetRouteTable returns a snapshot of registered, active, and evictable node info.
func (m *ConnManager[C]) GetRouteTable() *databasev1.RouteTable {
	m.mu.RLock()
	defer m.mu.RUnlock()
	registered := make([]*databasev1.Node, 0, len(m.registered))
	for _, node := range m.registered {
		if node != nil {
			registered = append(registered, node)
		}
	}
	activeNames := make([]string, 0, len(m.active))
	for nodeID := range m.active {
		if node := m.registered[nodeID]; node != nil && node.Metadata != nil {
			activeNames = append(activeNames, node.Metadata.Name)
		}
	}
	evictableNames := make([]string, 0, len(m.evictable))
	for nodeID := range m.evictable {
		if node := m.registered[nodeID]; node != nil && node.Metadata != nil {
			evictableNames = append(evictableNames, node.Metadata.Name)
		}
	}
	return &databasev1.RouteTable{
		Registered: registered,
		Active:     activeNames,
		Evictable:  evictableNames,
	}
}

// FailoverNode checks health for a node and moves it to evictable if unhealthy.
func (m *ConnManager[C]) FailoverNode(node string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if en, ok := m.evictable[node]; ok {
		if _, registered := m.registered[node]; !registered {
			close(en.c)
			delete(m.evictable, node)
			m.log.Info().Str("node", node).Str("status", m.dump()).Msg("node is removed from evict queue by wire event")
		}
		return
	}
	if mn, ok := m.active[node]; ok && !m.checkHealthAndReconnect(mn.conn, mn.node, mn.client) {
		_ = mn.conn.Close()
		delete(m.active, node)
		m.handler.OnInactive(node, mn.client)
		m.log.Info().Str("status", m.dump()).Str("node", node).Msg("node is unhealthy in the failover flow, move it to evict queue")
	}
}

// ReconnectAll closes all active and evictable connections and re-registers all nodes.
func (m *ConnManager[C]) ReconnectAll() {
	m.mu.Lock()
	nodesToReconnect := make([]*databasev1.Node, 0, len(m.registered))
	for name, node := range m.registered {
		if en, ok := m.evictable[name]; ok {
			close(en.c)
			delete(m.evictable, name)
		}
		if mn, ok := m.active[name]; ok {
			_ = mn.conn.Close()
			delete(m.active, name)
			m.handler.OnInactive(name, mn.client)
		}
		nodesToReconnect = append(nodesToReconnect, node)
	}
	m.mu.Unlock()
	for _, node := range nodesToReconnect {
		m.OnAddOrUpdate(node)
	}
}

// GracefulStop closes all connections and stops background goroutines.
func (m *ConnManager[C]) GracefulStop() {
	m.mu.Lock()
	for idx := range m.evictable {
		close(m.evictable[idx].c)
	}
	m.evictable = nil
	m.mu.Unlock()
	m.closer.Done()
	m.closer.CloseThenWait()
	m.mu.Lock()
	for _, mn := range m.active {
		_ = mn.conn.Close()
	}
	m.active = nil
	m.mu.Unlock()
}

func (m *ConnManager[C]) registerNode(node *databasev1.Node) {
	name := node.Metadata.GetName()
	defer func() {
		m.registered[name] = node
	}()

	n, ok := m.registered[name]
	if !ok {
		return
	}
	if m.handler.AddressOf(n) == m.handler.AddressOf(node) {
		return
	}
	if en, ok := m.evictable[name]; ok {
		close(en.c)
		delete(m.evictable, name)
		m.log.Info().Str("node", name).Str("status", m.dump()).Msg("node is removed from evict queue by the new gRPC address updated event")
	}
	if mn, ok := m.active[name]; ok {
		_ = mn.conn.Close()
		delete(m.active, name)
		m.handler.OnInactive(name, mn.client)
		m.log.Info().Str("status", m.dump()).Str("node", name).Msg("node is removed from active queue by the new gRPC address updated event")
	}
}

func (m *ConnManager[C]) removeNodeIfUnhealthy(name string, mn *managedNode[C]) bool {
	if m.healthCheck(mn.node.String(), mn.conn) {
		return false
	}
	_ = mn.conn.Close()
	delete(m.active, name)
	m.handler.OnInactive(name, mn.client)
	return true
}

// checkHealthAndReconnect checks if a node is healthy. If not, closes the conn,
// adds to evictable, calls OnInactive, and starts a retry goroutine.
// Returns true if healthy.
func (m *ConnManager[C]) checkHealthAndReconnect(conn *grpc.ClientConn, node *databasev1.Node, client C) bool {
	if m.healthCheck(node.String(), conn) {
		return true
	}
	_ = conn.Close()
	if !m.closer.AddRunning() {
		return false
	}
	name := node.Metadata.Name
	m.evictable[name] = evictNode{n: node, c: make(chan struct{})}
	m.handler.OnInactive(name, client)
	go func(name string, en evictNode) {
		defer m.closer.Done()
		attempt := 0
		for {
			backoff := JitteredBackoff(InitBackoff, MaxBackoff, attempt, DefaultJitterFactor)
			select {
			case <-time.After(backoff):
				address := m.handler.AddressOf(en.n)
				credOpts, errEvict := m.handler.GetDialOptions(address)
				if errEvict != nil {
					m.log.Error().Err(errEvict).Msg("failed to load client TLS credentials (evict)")
					return
				}
				allOpts := make([]grpc.DialOption, 0, len(credOpts)+len(m.dialOpts))
				allOpts = append(allOpts, credOpts...)
				allOpts = append(allOpts, m.dialOpts...)
				connEvict, errEvict := grpc.NewClient(address, allOpts...)
				if errEvict == nil && m.healthCheck(en.n.String(), connEvict) {
					func() {
						m.mu.Lock()
						defer m.mu.Unlock()
						if _, ok := m.evictable[name]; !ok {
							// The client has been removed from evict clients map, just return
							return
						}
						newClient, clientErr := m.handler.NewClient(connEvict, en.n)
						if clientErr != nil {
							m.log.Error().Err(clientErr).Msg("failed to create client during reconnect")
							_ = connEvict.Close()
							return
						}
						m.active[name] = &managedNode[C]{conn: connEvict, client: newClient, node: en.n}
						m.handler.OnActive(name, newClient)
						delete(m.evictable, name)
						m.RecordSuccess(name)
						m.log.Info().Str("status", m.dump()).Stringer("node", en.n).Msg("node is healthy, move it back to active queue")
					}()
					return
				}
				if connEvict != nil {
					_ = connEvict.Close()
				}
				if func() bool {
					m.mu.RLock()
					defer m.mu.RUnlock()
					_, ok := m.registered[name]
					return !ok
				}() {
					return
				}
				m.log.Error().Err(errEvict).Msgf("failed to re-connect to grpc server %s after waiting for %s", address, backoff)
			case <-en.c:
				return
			case <-m.closer.CloseNotify():
				return
			}
			attempt++
		}
	}(name, m.evictable[name])
	return false
}

func (m *ConnManager[C]) healthCheck(node string, conn *grpc.ClientConn) bool {
	var resp *grpc_health_v1.HealthCheckResponse
	if requestErr := Request(context.Background(), 2*time.Second, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); requestErr != nil {
		if e := m.log.Debug(); e.Enabled() {
			e.Err(requestErr).Str("node", node).Msg("service unhealthy")
		}
		return false
	}
	return resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
}

func (m *ConnManager[C]) dump() string {
	keysRegistered := make([]string, 0, len(m.registered))
	for k := range m.registered {
		keysRegistered = append(keysRegistered, k)
	}
	keysActive := make([]string, 0, len(m.active))
	for k := range m.active {
		keysActive = append(keysActive, k)
	}
	keysEvictable := make([]string, 0, len(m.evictable))
	for k := range m.evictable {
		keysEvictable = append(keysEvictable, k)
	}
	return fmt.Sprintf("registered: %v, active :%v, evictable :%v", keysRegistered, keysActive, keysEvictable)
}
