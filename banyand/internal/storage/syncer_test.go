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

package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSyncRetryDelay_DefaultBoundedJitter(t *testing.T) {
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := SyncRetryDelay(0, DefaultSnapshotSyncRetryJitter)
		require.GreaterOrEqual(t, d, DefaultSnapshotSyncRetryDelay, "delay must be >= base delay (2s)")
		require.Less(t, d, DefaultSnapshotSyncRetryDelay+DefaultSnapshotSyncRetryJitter, "delay must be < 2.5s")
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "jitter must produce variable delays")
}

func TestSyncRetryDelay_CustomConfig(t *testing.T) {
	base := 10 * time.Millisecond
	jitter := 5 * time.Millisecond
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		d := SyncRetryDelay(base, jitter)
		require.GreaterOrEqual(t, d, base, "delay must be >= custom base")
		require.Less(t, d, base+jitter, "delay must be < custom base + jitter")
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "jitter must produce variable delays")

	// No jitter test
	dNoJitter := SyncRetryDelay(base, 0)
	require.Equal(t, base, dNoJitter, "zero jitter must return base delay")
}

func TestWaitSyncRetry_CanceledOnShutdown(t *testing.T) {
	closeCh := make(chan struct{})
	// Simulate shutdown signal received
	close(closeCh)

	start := time.Now()
	exited := WaitSyncRetry(closeCh)
	elapsed := time.Since(start)

	require.True(t, exited, "WaitSyncRetry must return true when closeNotify is closed")
	require.Less(t, elapsed, 100*time.Millisecond, "WaitSyncRetry must return immediately on shutdown")
}

func TestWaitSyncRetryWithDelay_CustomTimeout(t *testing.T) {
	closeCh := make(chan struct{})
	start := time.Now()
	exited := WaitSyncRetryWithDelay(closeCh, 10*time.Millisecond, 5*time.Millisecond)
	elapsed := time.Since(start)

	require.False(t, exited, "WaitSyncRetryWithDelay must return false on timeout")
	require.GreaterOrEqual(t, elapsed, 10*time.Millisecond, "must wait at least base delay")
}

func TestWaitSyncRetry_DefaultTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 2s timeout test in short mode")
	}
	closeCh := make(chan struct{})
	start := time.Now()
	exited := WaitSyncRetry(closeCh)
	elapsed := time.Since(start)

	require.False(t, exited, "WaitSyncRetry must return false on timeout")
	require.GreaterOrEqual(t, elapsed, DefaultSnapshotSyncRetryDelay, "must wait at least base delay")
}
