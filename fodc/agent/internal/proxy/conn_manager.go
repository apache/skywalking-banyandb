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
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	connManagerMaxRetryInterval = 30 * time.Second
	connManagerMaxRetries       = 3
)

// ConnEventType represents the type of connection event.
type ConnEventType int

// Possible connection event types.
const (
	ConnEventConnect ConnEventType = iota
	ConnEventDisconnect
)

// ConnEvent represents a connection event sent to the manager.
type ConnEvent struct {
	ResultCh chan<- ConnResult
	Context  context.Context
	Type     ConnEventType
}

// ConnResult represents the result of a connection operation.
type ConnResult struct {
	Conn  *grpc.ClientConn
	Error error
}

// ConnState represents the state of the connection.
type ConnState int

const (
	// ConnStateDisconnected indicates the connection is disconnected.
	ConnStateDisconnected ConnState = iota
	// ConnStateConnected indicates the connection is established.
	ConnStateConnected
)

// HeartbeatChecker is a function that checks if the connection is healthy.
type HeartbeatChecker func(context.Context) error

// ConnManager manages connection lifecycle using a single goroutine.
type ConnManager struct {
	logger           *logger.Logger
	currentConn      *grpc.ClientConn
	closer           *run.Closer
	heartbeatChecker HeartbeatChecker
	eventCh          chan ConnEvent
	proxyAddr        string
	retryInterval    time.Duration
	state            ConnState
	stateMu          sync.RWMutex
	startOnce        sync.Once
}

// NewConnManager creates a new connection manager.
func NewConnManager(
	proxyAddr string,
	reconnectInterval time.Duration,
	logger *logger.Logger,
) *ConnManager {
	return &ConnManager{
		eventCh:          make(chan ConnEvent, 10),
		closer:           run.NewCloser(1),
		logger:           logger,
		proxyAddr:        proxyAddr,
		state:            ConnStateDisconnected,
		retryInterval:    reconnectInterval,
		heartbeatChecker: nil,
	}
}

// SetHeartbeatChecker sets the heartbeat checker function.
func (cm *ConnManager) SetHeartbeatChecker(checker HeartbeatChecker) {
	cm.stateMu.Lock()
	cm.heartbeatChecker = checker
	cm.stateMu.Unlock()
}

// EventChannel returns the channel for sending connect/reconnect events.
func (cm *ConnManager) EventChannel() chan<- ConnEvent {
	return cm.eventCh
}

// Start starts the connection manager's event processing goroutine.
func (cm *ConnManager) Start(ctx context.Context) {
	cm.startOnce.Do(func() {
		go cm.run(ctx)
	})
}

// Stop stops the connection manager and closes all connections.
func (cm *ConnManager) Stop() {
	cm.closer.CloseThenWait()
}

// requestConnection requests a connection attempt with optional heartbeat check.
func (cm *ConnManager) requestConnection(ctx context.Context) <-chan ConnResult {
	resultCh := make(chan ConnResult, 1)
	event := ConnEvent{
		Type:     ConnEventConnect,
		Context:  ctx,
		ResultCh: resultCh,
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

// RequestConnect requests a connection attempt.
func (cm *ConnManager) RequestConnect(ctx context.Context) <-chan ConnResult {
	return cm.requestConnection(ctx)
}

// RequestReconnect requests a reconnection attempt with exponential backoff.
func (cm *ConnManager) RequestReconnect(ctx context.Context) <-chan ConnResult {
	return cm.requestConnection(ctx)
}

// run is the main event processing loop running in a single goroutine.
func (cm *ConnManager) run(ctx context.Context) {
	defer cm.closer.Done()
	for {
		select {
		case <-ctx.Done():
			cm.cleanup()
			return
		case <-cm.closer.CloseNotify():
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
		cm.state = ConnStateDisconnected
		cm.stateMu.Unlock()

		cm.cleanupConnection()
		if event.ResultCh != nil {
			event.ResultCh <- ConnResult{Error: nil}
			close(event.ResultCh)
		}
	case ConnEventConnect:
		cm.stateMu.RLock()
		currentState := cm.state
		currentConn := cm.currentConn
		heartbeatChecker := cm.heartbeatChecker
		cm.stateMu.RUnlock()

		if currentState == ConnStateConnected && currentConn != nil && heartbeatChecker != nil {
			checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
			heartbeatErr := heartbeatChecker(checkCtx)
			checkCancel()

			if heartbeatErr == nil {
				cm.logger.Debug().Msg("Connection is healthy, reusing existing connection")
				if event.ResultCh != nil {
					event.ResultCh <- ConnResult{Conn: currentConn}
					close(event.ResultCh)
				}
				return
			}
			cm.logger.Warn().Err(heartbeatErr).Msg("Heartbeat check failed, reconnecting")
		}
		// For reconnect cleanup old connection first
		if currentState == ConnStateConnected {
			cm.cleanupConnection()
		}

		connCtx, connCancel := context.WithCancel(ctx)
		defer connCancel()
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

// doConnect performs the connection attempt with retry using exponential backoff.
func (cm *ConnManager) doConnect(ctx context.Context) ConnResult {
	var lastErr error
	retryInterval := cm.retryInterval

	for attempt := 1; attempt <= connManagerMaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return ConnResult{Error: fmt.Errorf("context canceled after %d attempts: %w", attempt-1, ctx.Err())}
			}
			return ConnResult{Error: ctx.Err()}
		default:
		}

		conn, dialErr := grpc.NewClient(cm.proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			cm.logger.Info().Str("proxy_addr", cm.proxyAddr).Int("attempt", attempt).Msg("Connected to FODC Proxy")
			return ConnResult{Conn: conn}
		}

		cm.logger.Error().Err(dialErr).Str("proxy_addr", cm.proxyAddr).Int("attempt", attempt).
			Msg("Failed to create proxy client")
		lastErr = fmt.Errorf("failed to create proxy client: %w", dialErr)

		if attempt >= connManagerMaxRetries {
			return ConnResult{Error: fmt.Errorf("failed to connect after %d attempts: %w", attempt, lastErr)}
		}

		cm.logger.Warn().
			Err(lastErr).
			Dur("retry_interval", retryInterval).
			Int("attempt", attempt).
			Int("remaining", connManagerMaxRetries-attempt).
			Msg("Connection attempt failed, will retry")

		select {
		case <-ctx.Done():
			return ConnResult{Error: ctx.Err()}
		case <-cm.closer.CloseNotify():
			return ConnResult{Error: fmt.Errorf("connection manager stopped")}
		case <-time.After(retryInterval):
		}

		retryInterval *= 2
		if retryInterval > connManagerMaxRetryInterval {
			retryInterval = connManagerMaxRetryInterval
		}
	}
	cm.logger.Error().Err(lastErr).Str("proxy_addr", cm.proxyAddr).Int("attempt", connManagerMaxRetries).
		Msg("Failed to connect to FODC Proxy")
	return ConnResult{Error: fmt.Errorf("failed to connect after %d attempts: %w", connManagerMaxRetries, lastErr)}
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
