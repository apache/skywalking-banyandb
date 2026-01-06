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

package server

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/agent/internal/exporter"
	"github.com/apache/skywalking-banyandb/fodc/agent/internal/flightrecorder"
)

func TestNewServer_ValidConfig(t *testing.T) {
	config := Config{
		ListenAddr:        ":9090",
		ReadHeaderTimeout: 5 * time.Second,
		ShutdownTimeout:   10 * time.Second,
	}

	server, err := NewServer(config)

	require.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, ":9090", server.GetListenAddr())
	assert.False(t, server.IsStarted())
	assert.Equal(t, config.ReadHeaderTimeout, server.config.ReadHeaderTimeout)
	assert.Equal(t, config.ShutdownTimeout, server.config.ShutdownTimeout)
}

func TestNewServer_DefaultTimeouts(t *testing.T) {
	config := Config{
		ListenAddr: ":9090",
	}

	server, err := NewServer(config)

	require.NoError(t, err)
	assert.NotNil(t, server)
	assert.Equal(t, defaultReadHeaderTimeout, server.config.ReadHeaderTimeout)
	assert.Equal(t, defaultShutdownTimeout, server.config.ShutdownTimeout)
}

func TestNewServer_EmptyListenAddr(t *testing.T) {
	config := Config{
		ListenAddr: "",
	}

	server, err := NewServer(config)

	assert.Error(t, err)
	assert.Nil(t, server)
	assert.Contains(t, err.Error(), "listen address cannot be empty")
}

func TestServer_StartStop(t *testing.T) {
	config := Config{
		ListenAddr:        "localhost:0", // Use port 0 for automatic assignment
		ReadHeaderTimeout: 1 * time.Second,
		ShutdownTimeout:   1 * time.Second,
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	assert.NotNil(t, server)

	// Create a registry and collector for testing
	registry := prometheus.NewRegistry()
	fr := flightrecorder.NewFlightRecorder(1000)
	datasourceCollector := exporter.NewDatasourceCollector(fr)

	// Start the server
	errCh, startErr := server.Start(registry, datasourceCollector)
	require.NoError(t, startErr)
	assert.True(t, server.IsStarted())

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop the server
	stopErr := server.Stop()
	assert.NoError(t, stopErr)
	assert.False(t, server.IsStarted())

	// Check that no errors were sent on the error channel
	select {
	case err := <-errCh:
		t.Errorf("Unexpected error from server: %v", err)
	default:
		// No error, which is expected
	}
}

func TestServer_Start_AlreadyStarted(t *testing.T) {
	config := Config{
		ListenAddr: "localhost:0",
	}

	server, err := NewServer(config)
	require.NoError(t, err)

	// Create a registry and collector for testing
	registry := prometheus.NewRegistry()
	fr := flightrecorder.NewFlightRecorder(1000)
	datasourceCollector := exporter.NewDatasourceCollector(fr)

	// Start the server first time
	_, startErr := server.Start(registry, datasourceCollector)
	require.NoError(t, startErr)
	assert.True(t, server.IsStarted())

	// Try to start again - should fail
	_, startErr2 := server.Start(registry, datasourceCollector)
	assert.Error(t, startErr2)
	assert.Contains(t, startErr2.Error(), "server is already started")

	// Clean up
	server.Stop()
}

func TestServer_Stop_NotStarted(t *testing.T) {
	config := Config{
		ListenAddr: ":9090",
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	assert.False(t, server.IsStarted())

	// Try to stop without starting - should fail
	stopErr := server.Stop()
	assert.Error(t, stopErr)
	assert.Contains(t, stopErr.Error(), "server is not started")
}

func TestServer_Start_RegistryAlreadyHasCollector(t *testing.T) {
	config := Config{
		ListenAddr: "localhost:0",
	}

	server, err := NewServer(config)
	require.NoError(t, err)

	// Create a registry
	registry := prometheus.NewRegistry()
	fr := flightrecorder.NewFlightRecorder(1000)
	datasourceCollector := exporter.NewDatasourceCollector(fr)

	// First start should work
	errCh, startErr := server.Start(registry, datasourceCollector)
	require.NoError(t, startErr)
	assert.True(t, server.IsStarted())

	// Stop the server
	require.NoError(t, server.Stop())
	assert.False(t, server.IsStarted())

	// Check that no errors were sent on the error channel
	select {
	case err := <-errCh:
		t.Errorf("Unexpected error from server: %v", err)
	default:
		// No error, which is expected
	}
}

func TestServer_GetListenAddr(t *testing.T) {
	config := Config{
		ListenAddr: ":8080",
	}

	server, err := NewServer(config)
	require.NoError(t, err)

	assert.Equal(t, ":8080", server.GetListenAddr())
}

func TestServer_IsStarted(t *testing.T) {
	config := Config{
		ListenAddr: "localhost:0",
	}

	server, err := NewServer(config)
	require.NoError(t, err)

	// Initially not started
	assert.False(t, server.IsStarted())

	// Create a registry and collector for testing
	registry := prometheus.NewRegistry()
	fr := flightrecorder.NewFlightRecorder(1000)
	datasourceCollector := exporter.NewDatasourceCollector(fr)

	// Start the server
	_, startErr := server.Start(registry, datasourceCollector)
	require.NoError(t, startErr)
	assert.True(t, server.IsStarted())

	// Stop the server
	stopErr := server.Stop()
	require.NoError(t, stopErr)
	assert.False(t, server.IsStarted())
}
