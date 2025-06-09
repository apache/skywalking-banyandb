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

// Package test provides mock implementations for testing.
package test

import (
	"context"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// MockMemoryProtector is a mock implementation of a memory protector.
type MockMemoryProtector struct {
	acquireErr          error
	limit               uint64
	ExpectQuotaExceeded bool
}

// AvailableBytes returns a mocked available memory size.
func (f *MockMemoryProtector) AvailableBytes() int64 {
	if f.ExpectQuotaExceeded {
		return 10
	}
	return 10000
}

// GetLimit returns the mocked memory limit.
func (f *MockMemoryProtector) GetLimit() uint64 {
	return f.limit
}

// AcquireResource simulates acquiring memory resources.
func (f *MockMemoryProtector) AcquireResource(_ context.Context, _ uint64) error {
	return f.acquireErr
}

// ShouldApplyFadvis always returns false for testing.
func (f *MockMemoryProtector) ShouldApplyFadvis(_ int64, _ int64) bool {
	return false
}

// ShouldCache always returns true for testing.
func (f *MockMemoryProtector) ShouldCache(_ string) bool {
	return true
}

// Name returns the name of the mock memory protector.
func (f *MockMemoryProtector) Name() string {
	return "mock-memory-protector"
}

// FlagSet returns a new flag set for the mock memory protector.
func (f *MockMemoryProtector) FlagSet() *run.FlagSet {
	return run.NewFlagSet("mock-memory-protector")
}

// Validate validates the mock configuration.
func (f *MockMemoryProtector) Validate() error {
	return nil
}

// PreRun is a no-op setup for the mock.
func (f *MockMemoryProtector) PreRun(_ context.Context) error {
	return nil
}

// GracefulStop simulates graceful shutdown.
func (f *MockMemoryProtector) GracefulStop() {
	// no-op for test
}

// Serve immediately returns a closed stop channel.
func (f *MockMemoryProtector) Serve() run.StopNotify {
	ch := make(chan struct{})
	close(ch)
	return ch
}
