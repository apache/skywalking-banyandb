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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// fakeMem is a test double that implements the full Memory interface by
// embedding Nop and overriding State() to advance through a script of states.
type fakeMem struct {
	Nop
	states []State
	idx    atomic.Int32
}

// State returns the next scripted state, sticking on the last value.
func (f *fakeMem) State() State {
	i := int(f.idx.Load())
	if i >= len(f.states) {
		i = len(f.states) - 1
	} else {
		f.idx.Add(1)
	}
	return f.states[i]
}

func TestWaitWhileHigh_ReturnsWhenRecovered(t *testing.T) {
	pm := &fakeMem{states: []State{StateHigh, StateHigh, StateLow}}
	err := WaitWhileHigh(context.Background(), pm, time.Second, logger.GetLogger("test"), "test-stage")
	assert.NoError(t, err)
}

func TestWaitWhileHigh_TimesOut(t *testing.T) {
	pm := &fakeMem{states: []State{StateHigh}}
	start := time.Now()
	err := WaitWhileHigh(context.Background(), pm, 50*time.Millisecond, logger.GetLogger("test"), "test-stage")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "timeout test must complete quickly")
}

func TestWaitWhileHigh_FastPathNoTimeoutCtx(t *testing.T) {
	pm := &fakeMem{states: []State{StateLow}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := WaitWhileHigh(ctx, pm, time.Second, logger.GetLogger("test"), "test-stage")
	assert.NoError(t, err)
}
