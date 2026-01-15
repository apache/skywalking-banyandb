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
	connManagerMaxRetryInterval = 60 * time.Second
	connManagerMaxRetries       = 3
)

// connEventType represents the type of connection event.
type connEventType int

// Possible connection event types.
const (
	connEventConnect connEventType = iota
	connEventDisconnect
)

// connEvent represents a connection event sent to the manager.
type connEvent struct {
	resultCh  chan<- connResult
	context   context.Context
	eventType connEventType
}

// connResult represents the result of a connection operation.
type connResult struct {
	conn *grpc.ClientConn
	err  error
}

// connState represents the state of the connection.
type connState int

const (
	// connStateDisconnected indicates the connection is disconnected.
	connStateDisconnected connState = iota
	// connStateConnected indicates the connection is established.
	connStateConnected
)

// heartbeatCheckerFunc is a function that checks if the connection is healthy.
type heartbeatCheckerFunc func(context.Context) error

// connManager manages connection lifecycle using a single goroutine.
type connManager struct {
	logger           *logger.Logger
	currentConn      *grpc.ClientConn
	closer           *run.Closer
	heartbeatChecker heartbeatCheckerFunc
	eventCh          chan connEvent
	proxyAddr        string
	retryInterval    time.Duration
	state            connState
	stateMu          sync.RWMutex
	startOnce        sync.Once
}

// newConnManager creates a new connection manager.
func newConnManager(
	proxyAddr string,
	reconnectInterval time.Duration,
	logger *logger.Logger,
) *connManager {
	return &connManager{
		eventCh:          make(chan connEvent, 10),
		closer:           run.NewCloser(1),
		logger:           logger,
		proxyAddr:        proxyAddr,
		state:            connStateDisconnected,
		retryInterval:    reconnectInterval,
		heartbeatChecker: nil,
	}
}

// setHeartbeatChecker sets the heartbeat checker function.
func (cm *connManager) setHeartbeatChecker(checker heartbeatCheckerFunc) {
	cm.stateMu.Lock()
	cm.heartbeatChecker = checker
	cm.stateMu.Unlock()
}

// eventChannel returns the channel for sending connect/reconnect events.
func (cm *connManager) eventChannel() chan<- connEvent {
	return cm.eventCh
}

// start starts the connection manager's event processing goroutine.
func (cm *connManager) start(ctx context.Context) {
	cm.startOnce.Do(func() {
		go cm.run(ctx)
	})
}

// stop stops the connection manager and closes all connections.
func (cm *connManager) stop() {
	cm.closer.CloseThenWait()
}

// RequestConnect requests a connection attempt.
func (cm *connManager) RequestConnect(ctx context.Context) <-chan connResult {
	resultCh := make(chan connResult, 1)
	event := connEvent{
		eventType: connEventConnect,
		context:   ctx,
		resultCh:  resultCh,
	}
	select {
	case cm.eventCh <- event:
	case <-ctx.Done():
		resultCh <- connResult{err: ctx.Err()}
		close(resultCh)
	default:
		resultCh <- connResult{err: fmt.Errorf("connection manager event channel is full")}
		close(resultCh)
	}
	return resultCh
}

// run is the main event processing loop running in a single goroutine.
func (cm *connManager) run(ctx context.Context) {
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
func (cm *connManager) handleEvent(ctx context.Context, event connEvent) {
	switch event.eventType {
	case connEventDisconnect:
		cm.stateMu.Lock()
		cm.state = connStateDisconnected
		cm.stateMu.Unlock()

		cm.cleanupConnection()
		if event.resultCh != nil {
			event.resultCh <- connResult{err: nil}
			close(event.resultCh)
		}
	case connEventConnect:
		cm.stateMu.RLock()
		currentState := cm.state
		currentConn := cm.currentConn
		heartbeatChecker := cm.heartbeatChecker
		cm.stateMu.RUnlock()

		if currentState == connStateConnected && currentConn != nil && heartbeatChecker != nil {
			checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
			heartbeatErr := heartbeatChecker(checkCtx)
			checkCancel()

			if heartbeatErr == nil {
				cm.logger.Debug().Msg("Connection is healthy, reusing existing connection")
				if event.resultCh != nil {
					event.resultCh <- connResult{conn: currentConn}
					close(event.resultCh)
				}
				return
			}
			cm.logger.Warn().Err(heartbeatErr).Msg("Heartbeat check failed, reconnecting")
		}
		// For reconnect cleanup old connection first
		if currentState == connStateConnected {
			cm.cleanupConnection()
		}

		connCtx, connCancel := context.WithCancel(ctx)
		defer connCancel()
		if event.context != nil {
			go func() {
				select {
				case <-event.context.Done():
					connCancel()
				case <-connCtx.Done():
				}
			}()
		}

		result := cm.doConnect(connCtx)

		cm.stateMu.Lock()
		if result.err == nil {
			cm.state = connStateConnected
			cm.currentConn = result.conn
		} else {
			cm.state = connStateDisconnected
		}
		cm.stateMu.Unlock()

		if event.resultCh != nil {
			event.resultCh <- result
			close(event.resultCh)
		}
	}
}

// doConnect performs the connection attempt with retry using exponential backoff.
func (cm *connManager) doConnect(ctx context.Context) connResult {
	var lastErr error
	retryInterval := cm.retryInterval

	for attempt := 1; attempt <= connManagerMaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return connResult{err: fmt.Errorf("context canceled after %d attempts: %w", attempt-1, ctx.Err())}
			}
			return connResult{err: ctx.Err()}
		default:
		}

		conn, dialErr := grpc.NewClient(cm.proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr == nil {
			cm.logger.Info().Str("proxy_addr", cm.proxyAddr).Int("attempt", attempt).Msg("Connected to FODC Proxy")
			return connResult{conn: conn}
		}

		cm.logger.Error().Err(dialErr).Str("proxy_addr", cm.proxyAddr).Int("attempt", attempt).
			Msg("Failed to create proxy client")
		lastErr = fmt.Errorf("failed to create proxy client: %w", dialErr)

		if attempt >= connManagerMaxRetries {
			return connResult{err: fmt.Errorf("failed to connect after %d attempts: %w", attempt, lastErr)}
		}

		cm.logger.Warn().
			Err(lastErr).
			Dur("retry_interval", retryInterval).
			Int("attempt", attempt).
			Int("remaining", connManagerMaxRetries-attempt).
			Msg("Connection attempt failed, will retry")

		retryInterval *= 2
		if retryInterval > connManagerMaxRetryInterval {
			retryInterval = connManagerMaxRetryInterval
		}

		select {
		case <-ctx.Done():
			return connResult{err: ctx.Err()}
		case <-cm.closer.CloseNotify():
			return connResult{err: fmt.Errorf("connection manager stopped")}
		case <-time.After(retryInterval):
		}
	}
	cm.logger.Error().Err(lastErr).Str("proxy_addr", cm.proxyAddr).Int("attempt", connManagerMaxRetries).
		Msg("Failed to connect to FODC Proxy")
	return connResult{err: fmt.Errorf("failed to connect after %d attempts: %w", connManagerMaxRetries, lastErr)}
}

// cleanupConnection closes the current connection and cleans up resources.
func (cm *connManager) cleanupConnection() {
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
func (cm *connManager) cleanup() {
	cm.stateMu.Lock()
	cm.state = connStateDisconnected
	cm.stateMu.Unlock()

	cm.cleanupConnection()
	cm.logger.Info().Msg("Connection manager stopped")
}

// getState returns the current connection state (thread-safe).
func (cm *connManager) getState() connState {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.state
}
