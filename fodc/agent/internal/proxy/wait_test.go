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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitWaitGroupWithTimeout_CompletesBeforeTimeout(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
	}()

	completed := waitWaitGroupWithTimeout(&wg, 500*time.Millisecond)

	assert.True(t, completed, "expected helper to report completion before the timeout fired")
}

func TestWaitWaitGroupWithTimeout_TimesOut(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	start := time.Now()
	completed := waitWaitGroupWithTimeout(&wg, 50*time.Millisecond)
	elapsed := time.Since(start)

	assert.False(t, completed, "expected helper to report timeout when wg never reaches zero")
	assert.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "helper returned earlier than the configured timeout")
	assert.Less(t, elapsed, 500*time.Millisecond, "helper waited significantly past the configured timeout")
}

func TestWaitWaitGroupWithTimeout_AlreadyDone(t *testing.T) {
	var wg sync.WaitGroup

	completed := waitWaitGroupWithTimeout(&wg, 100*time.Millisecond)

	assert.True(t, completed, "expected helper to return immediately for an already-zero WaitGroup")
}
