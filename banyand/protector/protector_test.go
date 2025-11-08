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

package protector

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStateConstants verifies state constants are properly defined.
func TestStateConstants(t *testing.T) {
	assert.Equal(t, State(0), StateLow)
	assert.Equal(t, State(1), StateHigh)
}

// TestMemoryStateLow verifies StateLow when memory is plentiful.
func TestMemoryStateLow(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000) // 1KB limit

	// 80% available (800 bytes) > 20% threshold (200 bytes)
	atomic.StoreUint64(&m.usage, 200)
	assert.Equal(t, StateLow, m.State())
}

// TestMemoryStateHigh verifies StateHigh when memory is scarce.
func TestMemoryStateHigh(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000) // 1KB limit

	// 15% available (150 bytes) <= 20% threshold (200 bytes)
	atomic.StoreUint64(&m.usage, 850)
	assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateHighEdgeCase verifies StateHigh when no memory available.
func TestMemoryStateHighEdgeCase(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	m.limit.Store(1000)
	atomic.StoreUint64(&m.usage, 1000) // Exactly at limit
	assert.Equal(t, StateHigh, m.State())
}

// TestMemoryStateNoLimit verifies behavior when no limit is set.
func TestMemoryStateNoLimit(t *testing.T) {
	m := &memory{limit: atomic.Uint64{}}
	// No limit set (0)
	assert.Equal(t, StateLow, m.State()) // Fail open
}
