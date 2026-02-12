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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
