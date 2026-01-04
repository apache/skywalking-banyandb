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

package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/metrics"
	"github.com/apache/skywalking-banyandb/fodc/proxy/internal/registry"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestNewServer(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)

	server := NewServer(service, "localhost:0", 1024*1024, testLogger)

	assert.NotNil(t, server)
	assert.Equal(t, service, server.service)
	assert.Equal(t, "localhost:0", server.listenAddr)
	assert.NotNil(t, server.server)
}

func TestServer_StartStop(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)

	server := NewServer(service, "localhost:0", 1024*1024, testLogger)

	err := server.Start()
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	server.Stop()
}

func TestServer_Stop_NotStarted(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)

	server := NewServer(service, "localhost:0", 1024*1024, testLogger)

	server.Stop()
}

func TestServer_Start_InvalidAddress(t *testing.T) {
	initTestLogger(t)
	testLogger := logger.GetLogger("test", "grpc")
	testRegistry := registry.NewAgentRegistry(testLogger, 5*time.Second, 10*time.Second, 100)
	mockSender := &mockRequestSender{}
	aggregator := metrics.NewAggregator(testRegistry, mockSender, testLogger)
	service := NewFODCService(testRegistry, aggregator, testLogger, 30*time.Second)

	server := NewServer(service, "invalid-address", 1024*1024, testLogger)

	err := server.Start()

	assert.Error(t, err)
}
