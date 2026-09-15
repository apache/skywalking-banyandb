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
	"math/rand/v2"
	"time"
)

const (
	// DefaultSnapshotSyncRetryDelay is the default base delay between snapshot sync retry attempts.
	DefaultSnapshotSyncRetryDelay = 2 * time.Second
	// DefaultSnapshotSyncRetryJitter is the default max jitter duration added to the retry delay.
	DefaultSnapshotSyncRetryJitter = 500 * time.Millisecond
)

// SyncRetryDelay returns the base delay plus a uniform random jitter up to jitter duration.
// If delay <= 0, DefaultSnapshotSyncRetryDelay is used.
func SyncRetryDelay(delay, jitter time.Duration) time.Duration {
	if delay <= 0 {
		delay = DefaultSnapshotSyncRetryDelay
	}
	if jitter <= 0 {
		return delay
	}
	// #nosec G404 -- not security-critical, just for retry jitter
	return delay + time.Duration(rand.Int64N(int64(jitter)))
}

// WaitSyncRetry waits for the default retry delay (2s base + 500ms jitter) or returns early if closeNotify is triggered.
// It returns true if shutdown was signaled, or false if the retry delay elapsed.
func WaitSyncRetry(closeNotify <-chan struct{}) bool {
	return WaitSyncRetryWithDelay(closeNotify, DefaultSnapshotSyncRetryDelay, DefaultSnapshotSyncRetryJitter)
}

// WaitSyncRetryWithDelay waits for the configured delay with jitter or returns early if closeNotify is triggered.
// It returns true if shutdown was signaled, or false if the retry delay elapsed.
func WaitSyncRetryWithDelay(closeNotify <-chan struct{}, delay, jitter time.Duration) bool {
	d := SyncRetryDelay(delay, jitter)
	select {
	case <-closeNotify:
		return true
	case <-time.After(d):
		return false
	}
}
