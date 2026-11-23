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
	GetDialOptions() ([]grpc.DialOption, error)
	// NewClient creates a client from a gRPC connection and node.
	NewClient(conn *grpc.ClientConn, node *databasev1.Node) (C, error)
	// OnActive is called when a node transitions to active.
	OnActive(name string, client C)
	// OnInactive is called when a node leaves active.
	OnInactive(name string, client C)
}

// ConnManagerConfig holds configuration for ConnManager.
type ConnManagerConfig[C Client] struct {
	Handler             ConnectionHandler[C]
	Logger              *logger.Logger
	RetryPolicy         string
	ExtraDialOpts       []grpc.DialOption
	HealthCheckTimeout  time.Duration
	HealthCheckInterval time.Duration
	MaxRecvMsgSize      int
}

// ConnManager manages gRPC connections with health checking, circuit breaking, and eviction.
type ConnManager[C Client] struct {
	handler             ConnectionHandler[C]
	log                 *logger.Logger
	registered          map[string]*databasev1.Node
	active              map[string]*managedNode[C]
	evictable           map[string]*connectionAttempt
	pending             map[string]*connectionAttempt
	cbStates            map[string]*circuitState
	closer              *run.Closer
	dialOpts            []grpc.DialOption
	healthCheckTimeout  time.Duration
	healthCheckInterval time.Duration
	mu                  sync.RWMutex
	cbMu                sync.RWMutex
	stopped             bool
}

type managedNode[C Client] struct {
	client   C
	conn     *grpc.ClientConn
	node     *databasev1.Node
	checking bool
}

// Each attempt owns its cancellation and identity, so an obsolete probe cannot
// publish a connection after a node is deleted, replaced, or reconnected.
type connectionAttempt struct {
	ctx    context.Context
	cancel context.CancelFunc
	node   *databasev1.Node
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
	healthCheckTimeout := cfg.HealthCheckTimeout
	if healthCheckTimeout == 0 {
		healthCheckTimeout = 2 * time.Second
	}
	m := &ConnManager[C]{
		handler:             cfg.Handler,
		log:                 cfg.Logger,
		registered:          make(map[string]*databasev1.Node),
		active:              make(map[string]*managedNode[C]),
		evictable:           make(map[string]*connectionAttempt),
		pending:             make(map[string]*connectionAttempt),
		cbStates:            make(map[string]*circuitState),
		closer:              run.NewCloser(1),
		dialOpts:            dialOpts,
		healthCheckTimeout:  healthCheckTimeout,
		healthCheckInterval: cfg.HealthCheckInterval,
	}
	if m.healthCheckInterval > 0 && m.closer.AddRunning() {
		run.GoOrDie(context.Background(), "grpchelper.connmanager.health-check", m.log, func(_ context.Context) {
			m.periodicHealthCheck() //nolint:contextcheck // probes use the manager's lifetime context
		})
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
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.registerNode(node)

	if _, ok := m.active[name]; ok {
		m.mu.Unlock()
		return
	}
	if _, ok := m.evictable[name]; ok {
		m.mu.Unlock()
		return
	}
	if _, ok := m.pending[name]; ok {
		m.mu.Unlock()
		return
	}
	attempt := m.startConnection(node)
	m.mu.Unlock()
	if attempt != nil {
		m.connect(attempt)
	}
}

// startConnection reserves a node before releasing mu for connection setup.
// The caller must hold mu and call connect even if the attempt is canceled.
func (m *ConnManager[C]) startConnection(node *databasev1.Node) *connectionAttempt {
	if !m.closer.AddRunning() {
		return nil
	}
	ctx, cancel := context.WithCancel(m.closer.Ctx())
	attempt := &connectionAttempt{ctx: ctx, cancel: cancel, node: node}
	m.pending[node.Metadata.Name] = attempt
	return attempt
}

func (m *ConnManager[C]) dial(node *databasev1.Node) (*grpc.ClientConn, error) {
	credOpts, dialErr := m.handler.GetDialOptions()
	if dialErr != nil {
		return nil, fmt.Errorf("failed to load client TLS credentials: %w", dialErr)
	}
	allOpts := make([]grpc.DialOption, 0, len(credOpts)+len(m.dialOpts))
	allOpts = append(allOpts, credOpts...)
	allOpts = append(allOpts, m.dialOpts...)
	return grpc.NewClient(m.handler.AddressOf(node), allOpts...)
}

func (m *ConnManager[C]) connect(attempt *connectionAttempt) {
	defer m.closer.Done()
	defer attempt.cancel()
	name := attempt.node.Metadata.Name
	defer func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.pending[name] == attempt {
			delete(m.pending, name)
		}
	}()
	if attempt.ctx.Err() != nil {
		return
	}
	conn, connErr := m.dial(attempt.node)
	if connErr != nil {
		m.log.Error().Err(connErr).Msg("failed to connect to grpc server")
		return
	}
	healthy := m.healthCheck(attempt.ctx, attempt.node.String(), conn)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.pending[name] != attempt {
		_ = conn.Close()
		return
	}
	client, clientErr := m.handler.NewClient(conn, attempt.node)
	if clientErr != nil {
		m.log.Error().Err(clientErr).Msg("failed to create client")
		_ = conn.Close()
		return
	}
	mn := &managedNode[C]{conn: conn, client: client, node: attempt.node}
	if !healthy {
		m.evict(name, mn)
		m.log.Info().Str("status", m.dump()).Stringer("node", attempt.node).Msg("node is unhealthy in the register flow, move it to evict queue")
		return
	}
	m.active[name] = mn
	m.handler.OnActive(name, client)
	// Initialize or reset circuit breaker state to closed
	m.RecordSuccess(name)
	m.log.Info().Str("status", m.dump()).Stringer("node", attempt.node).Msg("new node is healthy, add it to active queue")
}

// OnDelete removes a node and its connection.
func (m *ConnManager[C]) OnDelete(node *databasev1.Node) {
	name := node.Metadata.GetName()
	if name == "" {
		m.log.Warn().Stringer("node", node).Msg("node name is empty")
		return
	}
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	delete(m.registered, name)
	m.cancelPending(name)
	if en, ok := m.evictable[name]; ok {
		en.cancel()
		delete(m.evictable, name)
		m.log.Info().Str("status", m.dump()).Stringer("node", node).Msg("node is removed from evict queue by delete event")
		m.mu.Unlock()
		return
	}
	mn := m.active[name]
	if mn == nil || !m.closer.AddRunning() {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	defer m.closer.Done()
	if m.removeDeletedNode(name, mn) {
		return
	}
	if !m.closer.AddRunning() {
		return
	}
	run.Go(m.closer.Ctx(), "connmanager-reconnect", m.log, func(_ context.Context) {
		defer m.closer.Done()
		for attempt := 0; ; attempt++ {
			backoff := JitteredBackoff(InitBackoff, MaxBackoff, attempt, DefaultJitterFactor)
			select {
			case <-time.After(backoff):
				if m.removeDeletedNode(name, mn) {
					return
				}
			case <-m.closer.CloseNotify():
				return
			}
		}
	})
}

// removeDeletedNode preserves a healthy connection after a discovery deletion.
// It returns true once this cleanup is obsolete or the unhealthy node is removed.
func (m *ConnManager[C]) removeDeletedNode(name string, mn *managedNode[C]) bool {
	m.mu.RLock()
	obsolete := m.stopped || m.registered[name] != nil || m.active[name] != mn
	m.mu.RUnlock()
	if obsolete {
		return true
	}
	healthy := m.healthCheck(m.closer.Ctx(), mn.node.String(), mn.conn)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.registered[name] != nil || m.active[name] != mn {
		return true
	}
	if healthy {
		return false
	}
	_ = mn.conn.Close()
	delete(m.active, name)
	m.handler.OnInactive(name, mn.client)
	m.log.Info().Str("node", name).Msg("remove unhealthy node after discovery deletion")
	return true
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
	if m.stopped {
		m.mu.Unlock()
		return
	}
	if en, ok := m.evictable[node]; ok {
		if _, registered := m.registered[node]; !registered {
			en.cancel()
			delete(m.evictable, node)
			m.log.Info().Str("node", node).Str("status", m.dump()).Msg("node is removed from evict queue by wire event")
		}
		m.mu.Unlock()
		return
	}
	mn := m.active[node]
	if mn == nil || mn.checking || !m.closer.AddRunning() {
		m.mu.Unlock()
		return
	}
	mn.checking = true
	m.mu.Unlock()
	defer m.closer.Done()
	healthy := m.healthCheck(m.closer.Ctx(), mn.node.String(), mn.conn)
	m.mu.Lock()
	defer m.mu.Unlock()
	mn.checking = false
	if m.stopped || m.active[node] != mn || healthy {
		return
	}
	m.evict(node, mn)
	m.log.Info().Str("status", m.dump()).Str("node", node).Msg("node is unhealthy in the failover flow, move it to evict queue")
}

// ReconnectAll closes all active and evictable connections and re-registers all nodes.
func (m *ConnManager[C]) ReconnectAll() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	attempts := make([]*connectionAttempt, 0, len(m.registered))
	for name, node := range m.registered {
		m.cancelPending(name)
		if en, ok := m.evictable[name]; ok {
			en.cancel()
			delete(m.evictable, name)
		}
		if mn, ok := m.active[name]; ok {
			_ = mn.conn.Close()
			delete(m.active, name)
			m.handler.OnInactive(name, mn.client)
		}
		if attempt := m.startConnection(node); attempt != nil {
			attempts = append(attempts, attempt)
		}
	}
	m.mu.Unlock()
	for _, attempt := range attempts {
		m.connect(attempt)
	}
}

// GracefulStop closes all connections and stops background goroutines.
func (m *ConnManager[C]) GracefulStop() {
	m.mu.Lock()
	m.stopped = true
	for name := range m.pending {
		m.cancelPending(name)
	}
	for _, en := range m.evictable {
		en.cancel()
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

// cancelPending invalidates a connection attempt. The caller must hold mu.
func (m *ConnManager[C]) cancelPending(name string) {
	if attempt := m.pending[name]; attempt != nil {
		attempt.cancel()
		delete(m.pending, name)
	}
}

func (m *ConnManager[C]) registerNode(node *databasev1.Node) {
	name := node.Metadata.GetName()
	defer func() {
		m.registered[name] = node
	}()

	n, ok := m.registered[name]
	if !ok {
		// Discovery deletion retains healthy clients; a new registration must
		// still replace one whose address has changed.
		if mn := m.active[name]; mn != nil {
			n = mn.node
		} else {
			return
		}
	}
	if m.handler.AddressOf(n) == m.handler.AddressOf(node) {
		return
	}
	m.cancelPending(name)
	if en, ok := m.evictable[name]; ok {
		en.cancel()
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

// evict moves a failed connection to the retry queue. The caller must hold mu.
func (m *ConnManager[C]) evict(name string, mn *managedNode[C]) {
	_ = mn.conn.Close()
	delete(m.active, name)
	m.handler.OnInactive(name, mn.client)
	if m.registered[name] == nil || !m.closer.AddRunning() {
		return
	}
	ctx, cancel := context.WithCancel(m.closer.Ctx())
	en := &connectionAttempt{ctx: ctx, cancel: cancel, node: mn.node}
	m.evictable[name] = en
	run.Go(ctx, "connmanager-evict-reconnect", m.log, func(_ context.Context) {
		defer m.closer.Done()
		defer en.cancel()
		for attempt := 0; ; attempt++ {
			backoff := JitteredBackoff(InitBackoff, MaxBackoff, attempt, DefaultJitterFactor)
			select {
			case <-time.After(backoff):
				if m.reconnect(en) {
					return
				}
			case <-en.ctx.Done():
				return
			}
		}
	})
}

// reconnect returns true when the attempt is complete or has been superseded.
func (m *ConnManager[C]) reconnect(en *connectionAttempt) bool {
	if en.ctx.Err() != nil {
		return true
	}
	conn, connErr := m.dial(en.node)
	if connErr != nil {
		m.log.Error().Err(connErr).Msg("failed to reconnect to grpc server")
		return false
	}
	keep := false
	defer func() {
		if !keep {
			_ = conn.Close()
		}
	}()
	healthy := m.healthCheck(en.ctx, en.node.String(), conn)
	name := en.node.Metadata.Name
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.evictable[name] != en {
		return true
	}
	if !healthy {
		return false
	}
	client, clientErr := m.handler.NewClient(conn, en.node)
	if clientErr != nil {
		m.log.Error().Err(clientErr).Msg("failed to create client during reconnect")
		return false
	}
	m.active[name] = &managedNode[C]{conn: conn, client: client, node: en.node}
	m.handler.OnActive(name, client)
	delete(m.evictable, name)
	m.RecordSuccess(name)
	keep = true
	m.log.Info().Str("status", m.dump()).Stringer("node", en.node).Msg("node is healthy, move it back to active queue")
	return true
}

func (m *ConnManager[C]) periodicHealthCheck() {
	defer m.closer.Done()
	ticker := time.NewTicker(m.healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, name := range m.ActiveNames() {
				m.FailoverNode(name)
			}
		case <-m.closer.CloseNotify():
			return
		}
	}
}

func (m *ConnManager[C]) healthCheck(ctx context.Context, node string, conn *grpc.ClientConn) bool {
	var resp *grpc_health_v1.HealthCheckResponse
	if requestErr := Request(ctx, m.healthCheckTimeout, func(rpcCtx context.Context) (err error) {
		resp, err = grpc_health_v1.NewHealthClient(conn).Check(rpcCtx,
			&grpc_health_v1.HealthCheckRequest{
				Service: "",
			})
		return err
	}); requestErr != nil {
		m.log.Info().Err(requestErr).Str("node", node).Msg("healthCheck: service unhealthy, RPC error")
		return false
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		m.log.Info().Str("node", node).Str("status", resp.GetStatus().String()).Msg("healthCheck: service not SERVING")
		return false
	}
	return true
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
