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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/banyand/internal/test"
	"github.com/apache/skywalking-banyandb/banyand/protector"
)

// TestGrpcBufferMemoryRatioFlagDefault verifies default value.
func TestGrpcBufferMemoryRatioFlagDefault(t *testing.T) {
	s := &server{
		streamSVC:      &streamService{},
		measureSVC:     &measureService{},
		traceSVC:       &traceService{},
		propertyServer: &propertyServer{},
	}
	fs := s.FlagSet()
	flag := fs.Lookup("grpc-buffer-memory-ratio")
	assert.NotNil(t, flag)
	assert.Equal(t, "0.1", flag.DefValue)
}

// TestGrpcBufferMemoryRatioValidation verifies validation logic.
func TestGrpcBufferMemoryRatioValidation(t *testing.T) {
	tests := []struct {
		name      string
		ratio     float64
		expectErr bool
	}{
		{"valid_ratio_0_1", 0.1, false},
		{"valid_ratio_0_5", 0.5, false},
		{"valid_ratio_1_0", 1.0, false},
		{"invalid_ratio_zero", 0.0, true},
		{"invalid_ratio_negative", -0.1, true},
		{"invalid_ratio_too_high", 1.1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				grpcBufferMemoryRatio: tt.ratio,
				host:                  "localhost",
				port:                  17912,
			}
			err := s.Validate()
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// mockProtector implements Service interface for testing.
type mockProtector struct {
	*test.MockMemoryProtector
	state protector.State
}

func (m *mockProtector) State() protector.State {
	return m.state
}

// TestNewServerWithProtector verifies protector injection.
func TestNewServerWithProtector(t *testing.T) {
	// Create a mock protector
	protectorService := &mockProtector{state: protector.StateLow}

	// Create server with protector - should not panic
	server := NewServer(context.Background(), nil, nil, nil, nil, NodeRegistries{}, nil, protectorService)
	assert.NotNil(t, server)
}

// TestNewServerWithoutProtector verifies nil protector handling.
func TestNewServerWithoutProtector(t *testing.T) {
	// Server creation should not fail with nil protector (fail open)
	server := NewServer(context.Background(), nil, nil, nil, nil, NodeRegistries{}, nil, nil)
	assert.NotNil(t, server)
}

// TestProtectorLoadSheddingInterceptorLowState verifies normal operation in low state.
func TestProtectorLoadSheddingInterceptorLowState(t *testing.T) {
	protectorService := &mockProtector{state: protector.StateLow}
	server := &server{protector: protectorService}

	called := false
	handler := func(_ interface{}, _ grpc.ServerStream) error {
		called = true
		return nil
	}

	err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{}, grpc.StreamHandler(handler))

	assert.NoError(t, err)
	assert.True(t, called)
}

// TestProtectorLoadSheddingInterceptorHighState verifies rejection in high state.
func TestProtectorLoadSheddingInterceptorHighState(t *testing.T) {
	protectorService := &mockProtector{state: protector.StateHigh}
	server := &server{protector: protectorService}

	handler := func(_ interface{}, _ grpc.ServerStream) error {
		t.Fatal("handler should not be called")
		return nil
	}

	err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "test"}, grpc.StreamHandler(handler))

	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
}

// TestProtectorLoadSheddingInterceptorNilProtector verifies fail-open behavior.
func TestProtectorLoadSheddingInterceptorNilProtector(t *testing.T) {
	server := &server{protector: nil}

	called := false
	handler := func(_ interface{}, _ grpc.ServerStream) error {
		called = true
		return nil
	}

	err := server.protectorLoadSheddingInterceptor(nil, nil, &grpc.StreamServerInfo{}, grpc.StreamHandler(handler))

	assert.NoError(t, err)
	assert.True(t, called)
}
