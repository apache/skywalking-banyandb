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

package measure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSyncRetryDelay_BoundedJitter(t *testing.T) {
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := syncRetryDelay()
		require.GreaterOrEqual(t, d, snapshotSyncRetryDelay, "delay must be >= base delay (2s)")
		require.Less(t, d, snapshotSyncRetryDelay+snapshotSyncRetryJitter, "delay must be < 2.5s")
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "jitter must produce variable delays")
}

func TestWaitSyncRetry_CanceledOnShutdown(t *testing.T) {
	closeCh := make(chan struct{})
	// Simulate shutdown signal received
	close(closeCh)

	start := time.Now()
	exited := waitSyncRetry(closeCh)
	elapsed := time.Since(start)

	require.True(t, exited, "waitSyncRetry must return true when closeNotify is closed")
	// Proves it exits immediately instead of hanging for 2+ seconds
	require.Less(t, elapsed, 100*time.Millisecond, "waitSyncRetry must return immediately on shutdown")
}

func TestWaitSyncRetry_Timeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 2s timeout test in short mode")
	}
	closeCh := make(chan struct{})
	start := time.Now()
	exited := waitSyncRetry(closeCh)
	elapsed := time.Since(start)

	require.False(t, exited, "waitSyncRetry must return false on timeout")
	require.GreaterOrEqual(t, elapsed, snapshotSyncRetryDelay, "must wait at least base delay")
}
