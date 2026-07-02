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

package proxy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestScheduleReconnect_ArmsRetryWhenHealthy is the regression guard for the reconnect-stall root
// cause: a transient stream-establishment failure must arm another reconnect attempt rather than
// leaving the agent permanently disconnected.
func TestScheduleReconnect_ArmsRetryWhenHealthy(t *testing.T) {
	var calls int32
	c := &Client{
		logger:            initTestLogger(t),
		reconnectInterval: 20 * time.Millisecond,
		reconnectFn:       func(context.Context) { atomic.AddInt32(&calls, 1) },
	}

	c.scheduleReconnect(context.Background())

	assert.Eventually(t, func() bool {
		return atomic.LoadInt32(&calls) == 1
	}, time.Second, 5*time.Millisecond, "scheduleReconnect should arm exactly one retry")
}

// TestScheduleReconnect_SkipsWhenDisconnected confirms an intentional disconnect suppresses retries
// so a shut-down agent does not keep dialing the proxy.
func TestScheduleReconnect_SkipsWhenDisconnected(t *testing.T) {
	var calls int32
	c := &Client{
		logger:            initTestLogger(t),
		reconnectInterval: 10 * time.Millisecond,
		disconnected:      true,
		reconnectFn:       func(context.Context) { atomic.AddInt32(&calls, 1) },
	}

	c.scheduleReconnect(context.Background())

	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&calls), "no retry should be armed when disconnected")
}

// TestScheduleReconnect_SkipsWhenContextDone confirms a canceled context suppresses retries.
func TestScheduleReconnect_SkipsWhenContextDone(t *testing.T) {
	var calls int32
	c := &Client{
		logger:            initTestLogger(t),
		reconnectInterval: 10 * time.Millisecond,
		reconnectFn:       func(context.Context) { atomic.AddInt32(&calls, 1) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.scheduleReconnect(ctx)

	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&calls), "no retry should be armed when context is canceled")
}
