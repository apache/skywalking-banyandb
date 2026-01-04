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

// Package proxy provides connection management for the FODC Proxy client.
package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	connManagerMaxRetryInterval = 30 * time.Second
)

// ConnEventType represents the type of connection event.
type ConnEventType int

// Possible connection event types.
const (
	ConnEventConnect ConnEventType = iota
	ConnEventReconnect
	ConnEventDisconnect
)

// ConnEvent represents a connection event sent to the manager.
type ConnEvent struct {
	ResultCh  chan<- ConnResult
	Context   context.Context
	ErrorCh   chan<- error
	Type      ConnEventType
	Immediate bool // If true, skip backoff and retry immediately
}

// ConnResult represents the result of a connection operation.
type ConnResult struct {
	Conn   *grpc.ClientConn
	Client interface{} // Will be cast to the appropriate client type
	Error  error
}

// ConnState represents the state of the connection.
type ConnState int

const (
	// ConnStateDisconnected indicates the connection is disconnected.
	ConnStateDisconnected ConnState = iota
	// ConnStateConnecting indicates a connection attempt is in progress.
	ConnStateConnecting
	// ConnStateConnected indicates the connection is established.
	ConnStateConnected
	// ConnStateReconnecting indicates a reconnection attempt is in progress.
	ConnStateReconnecting
)

// ConnManager manages connection lifecycle using a single goroutine.
// All connection operations are serialized through an event channel.
type ConnManager struct {
	logger            *logger.Logger
	eventCh           chan ConnEvent
	stopCh            chan struct{}
	currentConn       *grpc.ClientConn
	proxyAddr         string
	reconnectInterval time.Duration
	retryInterval     time.Duration

	stateMu      sync.RWMutex // Protects state, disconnected, currentConn, and retryInterval
	state        ConnState
	disconnected bool
	started      bool
	startedMu    sync.Mutex // Protects started flag
}

// NewConnManager creates a new connection manager.
func NewConnManager(
	proxyAddr string,
	reconnectInterval time.Duration,
	logger *logger.Logger,
) *ConnManager {
	return &ConnManager{
		eventCh:           make(chan ConnEvent, 10),
		stopCh:            make(chan struct{}),
		logger:            logger,
		proxyAddr:         proxyAddr,
		reconnectInterval: reconnectInterval,
		state:             ConnStateDisconnected,
		retryInterval:     reconnectInterval,
	}
}

// EventChannel returns the channel for sending connect/reconnect events.
func (cm *ConnManager) EventChannel() chan<- ConnEvent {
	return cm.eventCh
}

// Start starts the connection manager's event processing goroutine.
func (cm *ConnManager) Start(ctx context.Context) {
	cm.startedMu.Lock()
	defer cm.startedMu.Unlock()

	if cm.started {
		return // Already started
	}

	cm.started = true
	go cm.run(ctx)
}

// Stop stops the connection manager and closes all connections.
func (cm *ConnManager) Stop() {
	close(cm.stopCh)
	// Send disconnect event to clean up
	disconnectEvent := ConnEvent{
		Type:    ConnEventDisconnect,
		Context: context.Background(),
	}
	select {
	case cm.eventCh <- disconnectEvent:
	case <-time.After(1 * time.Second):
		// Channel might be full or manager stopped, force close
		close(cm.eventCh)
	}
}

// RequestConnect requests a connection attempt.
func (cm *ConnManager) RequestConnect(ctx context.Context) <-chan ConnResult {
	resultCh := make(chan ConnResult, 1)
	event := ConnEvent{
		Type:      ConnEventConnect,
		Context:   ctx,
		ResultCh:  resultCh,
		Immediate: true,
	}
	select {
	case cm.eventCh <- event:
	case <-ctx.Done():
		resultCh <- ConnResult{Error: ctx.Err()}
		close(resultCh)
	default:
		resultCh <- ConnResult{Error: fmt.Errorf("connection manager event channel is full")}
		close(resultCh)
	}
	return resultCh
}

// RequestReconnect requests a reconnection attempt.
func (cm *ConnManager) RequestReconnect(ctx context.Context) <-chan ConnResult {
	resultCh := make(chan ConnResult, 1)
	event := ConnEvent{
		Type:      ConnEventReconnect,
		Context:   ctx,
		ResultCh:  resultCh,
		Immediate: false,
	}
	select {
	case cm.eventCh <- event:
	case <-ctx.Done():
		resultCh <- ConnResult{Error: ctx.Err()}
		close(resultCh)
	default:
		resultCh <- ConnResult{Error: fmt.Errorf("connection manager event channel is full")}
		close(resultCh)
	}
	return resultCh
}

// run is the main event processing loop running in a single goroutine.
// All connection state mutations happen here, eliminating the need for locks.
func (cm *ConnManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			cm.cleanup()
			return
		case <-cm.stopCh:
			cm.cleanup()
			return
		case event := <-cm.eventCh:
			cm.handleEvent(ctx, event)
		}
	}
}

// handleEvent runs in the single goroutine and processes a connection event.
func (cm *ConnManager) handleEvent(ctx context.Context, event ConnEvent) {
	switch event.Type {
	case ConnEventDisconnect:
		cm.stateMu.Lock()
		cm.disconnected = true
		cm.state = ConnStateDisconnected
		cm.stateMu.Unlock()

		cm.cleanupConnection()
		if event.ErrorCh != nil {
			event.ErrorCh <- nil
			close(event.ErrorCh)
		}
	case ConnEventConnect:
		cm.stateMu.RLock()
		currentState := cm.state
		currentConn := cm.currentConn
		cm.stateMu.RUnlock()

		if currentState == ConnStateConnected && currentConn != nil {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Conn:   currentConn,
					Client: nil, // Client should be created by caller
				}
				close(event.ResultCh)
			}
			return
		}
		if currentState == ConnStateConnecting {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("connection already in progress"),
				}
				close(event.ResultCh)
			}
			return
		}

		cm.stateMu.Lock()
		cm.state = ConnStateConnecting
		cm.stateMu.Unlock()

		connCtx, connCancel := context.WithCancel(ctx)
		defer connCancel()
		// If event.Context is provided and not done, cancel connCtx when event.Context is done
		if event.Context != nil {
			go func() {
				select {
				case <-event.Context.Done():
					connCancel()
				case <-connCtx.Done():
				}
			}()
		}
		result := cm.doConnect(connCtx)

		cm.stateMu.Lock()
		if result.Error == nil {
			cm.state = ConnStateConnected
			cm.currentConn = result.Conn
			cm.retryInterval = cm.reconnectInterval
		} else {
			cm.state = ConnStateDisconnected
		}
		cm.stateMu.Unlock()

		if event.ResultCh != nil {
			event.ResultCh <- result
			close(event.ResultCh)
		}
	case ConnEventReconnect:
		cm.stateMu.RLock()
		disconnected := cm.disconnected
		currentState := cm.state
		cm.stateMu.RUnlock()

		if disconnected {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("connection manager is disconnected"),
				}
				close(event.ResultCh)
			}
			return
		}
		if currentState == ConnStateReconnecting {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("reconnection already in progress"),
				}
				close(event.ResultCh)
			}
			return
		}

		cm.stateMu.Lock()
		cm.state = ConnStateReconnecting
		cm.stateMu.Unlock()

		cm.cleanupConnection()

		reconnCtx, reconnCancel := context.WithCancel(ctx)
		defer reconnCancel()
		// If event.Context is provided and not done, cancel reconnCtx when event.Context is done
		if event.Context != nil {
			go func() {
				select {
				case <-event.Context.Done():
					reconnCancel()
				case <-reconnCtx.Done():
				}
			}()
		}
		result := cm.doReconnect(reconnCtx, event.Immediate)

		cm.stateMu.Lock()
		if result.Error == nil {
			cm.state = ConnStateConnected
			cm.currentConn = result.Conn
			cm.retryInterval = cm.reconnectInterval
		} else {
			cm.state = ConnStateDisconnected
		}
		cm.stateMu.Unlock()

		if event.ResultCh != nil {
			event.ResultCh <- result
			close(event.ResultCh)
		}
	}
}

// doConnect performs the actual connection attempt.
func (cm *ConnManager) doConnect(ctx context.Context) ConnResult {
	select {
	case <-ctx.Done():
		return ConnResult{Error: ctx.Err()}
	default:
	}

	conn, dialErr := grpc.NewClient(cm.proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if dialErr != nil {
		cm.logger.Error().Err(dialErr).Str("proxy_addr", cm.proxyAddr).Msg("Failed to create proxy client")
		return ConnResult{
			Error: fmt.Errorf("failed to create proxy client: %w", dialErr),
		}
	}
	cm.logger.Info().Str("proxy_addr", cm.proxyAddr).Msg("Connected to FODC Proxy")
	return ConnResult{
		Conn: conn,
	}
}

// doReconnect performs reconnection with exponential backoff.
func (cm *ConnManager) doReconnect(ctx context.Context, immediate bool) ConnResult {
	var retryInterval time.Duration
	if !immediate {
		cm.stateMu.RLock()
		retryInterval = cm.retryInterval
		cm.stateMu.RUnlock()
		// Wait with exponential backoff
		select {
		case <-ctx.Done():
			return ConnResult{Error: ctx.Err()}
		case <-cm.stopCh:
			return ConnResult{Error: fmt.Errorf("connection manager stopped")}
		case <-time.After(retryInterval):
			cm.stateMu.Lock()
			cm.retryInterval *= 2
			if cm.retryInterval > connManagerMaxRetryInterval {
				cm.retryInterval = connManagerMaxRetryInterval
			}
			retryInterval = cm.retryInterval
			cm.stateMu.Unlock()
		}
	}
	cm.logger.Info().Dur("retry_interval", retryInterval).Msg("Attempting to reconnect...")
	return cm.doConnect(ctx)
}

// cleanupConnection closes the current connection and cleans up resources.
func (cm *ConnManager) cleanupConnection() {
	cm.stateMu.Lock()
	currentConn := cm.currentConn
	cm.currentConn = nil
	cm.stateMu.Unlock()

	if currentConn != nil {
		if closeErr := currentConn.Close(); closeErr != nil {
			cm.logger.Warn().Err(closeErr).Msg("Error closing connection")
		}
	}
}

// cleanup performs final cleanup when the manager stops.
func (cm *ConnManager) cleanup() {
	cm.stateMu.Lock()
	cm.disconnected = true
	cm.state = ConnStateDisconnected
	cm.stateMu.Unlock()

	cm.cleanupConnection()
	cm.logger.Info().Msg("Connection manager stopped")
}

// GetState returns the current connection state (thread-safe).
func (cm *ConnManager) GetState() ConnState {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.state
}

// IsDisconnected returns whether the manager is disconnected (thread-safe).
func (cm *ConnManager) IsDisconnected() bool {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.disconnected
}
