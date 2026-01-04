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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	connManagerMaxRetryInterval = 30 * time.Second
)

type ConnEventType int

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

	state        ConnState
	disconnected bool
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
		cm.disconnected = true
		cm.cleanupConnection()
		cm.state = ConnStateDisconnected
		if event.ErrorCh != nil {
			event.ErrorCh <- nil
			close(event.ErrorCh)
		}
	case ConnEventConnect:
		if cm.state == ConnStateConnected && cm.currentConn != nil {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Conn:   cm.currentConn,
					Client: nil, // Client should be created by caller
				}
				close(event.ResultCh)
			}
			return
		}
		if cm.state == ConnStateConnecting {
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("connection already in progress"),
				}
				close(event.ResultCh)
			}
			return
		}
		cm.state = ConnStateConnecting
		connCtx, connCancel := func() (context.Context, context.CancelFunc) {
			if event.Context != nil {
				select {
				case <-event.Context.Done():
					// Event context is done, use parent context
					return context.WithCancel(ctx)
				default:
					return context.WithCancel(event.Context)
				}
			}
			return context.WithCancel(ctx)
		}()
		defer connCancel()
		result := cm.doConnect(connCtx)
		if result.Error == nil {
			cm.state = ConnStateConnected
			cm.currentConn = result.Conn
			cm.retryInterval = cm.reconnectInterval
		} else {
			cm.state = ConnStateDisconnected
		}
		if event.ResultCh != nil {
			event.ResultCh <- result
			close(event.ResultCh)
		}
	case ConnEventReconnect:
		if cm.disconnected {
			// Intentionally disconnected, skip reconnection
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("connection manager is disconnected"),
				}
				close(event.ResultCh)
			}
			return
		}
		if cm.state == ConnStateReconnecting {
			// Reconnection already in progress, return error to avoid blocking
			if event.ResultCh != nil {
				event.ResultCh <- ConnResult{
					Error: fmt.Errorf("reconnection already in progress"),
				}
				close(event.ResultCh)
			}
			return
		}
		cm.state = ConnStateReconnecting
		cm.cleanupConnection()

		reconnCtx, reconnCancel := func() (context.Context, context.CancelFunc) {
			if event.Context != nil {
				select {
				case <-event.Context.Done():
					// Event context is done, use parent context
					return context.WithCancel(ctx)
				default:
					return context.WithCancel(event.Context)
				}
			}
			return context.WithCancel(ctx)
		}()
		defer reconnCancel()
		result := cm.doReconnect(reconnCtx, event.Immediate)
		if result.Error == nil {
			cm.state = ConnStateConnected
			cm.currentConn = result.Conn
			cm.retryInterval = cm.reconnectInterval
		} else {
			cm.state = ConnStateDisconnected
		}
		if event.ResultCh != nil {
			event.ResultCh <- result
			close(event.ResultCh)
		}
	}
}

// doConnect performs the actual connection attempt.
func (cm *ConnManager) doConnect(ctx context.Context) ConnResult {
	// Check if context is already canceled
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
	if !immediate {
		// Wait with exponential backoff
		select {
		case <-ctx.Done():
			return ConnResult{Error: ctx.Err()}
		case <-cm.stopCh:
			return ConnResult{Error: fmt.Errorf("connection manager stopped")}
		case <-time.After(cm.retryInterval):
			cm.retryInterval *= 2
			if cm.retryInterval > connManagerMaxRetryInterval {
				cm.retryInterval = connManagerMaxRetryInterval
			}
		}
	}
	cm.logger.Info().Dur("retry_interval", cm.retryInterval).Msg("Attempting to reconnect...")
	return cm.doConnect(ctx)
}

// cleanupConnection closes the current connection and cleans up resources.
func (cm *ConnManager) cleanupConnection() {
	if cm.currentConn != nil {
		if closeErr := cm.currentConn.Close(); closeErr != nil {
			cm.logger.Warn().Err(closeErr).Msg("Error closing connection")
		}
		cm.currentConn = nil
	}
}

// cleanup performs final cleanup when the manager stops.
func (cm *ConnManager) cleanup() {
	cm.disconnected = true
	cm.cleanupConnection()
	cm.state = ConnStateDisconnected
	cm.logger.Info().Msg("Connection manager stopped")
}
