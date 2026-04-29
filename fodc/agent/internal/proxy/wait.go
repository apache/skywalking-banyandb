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
	"time"
)

// heartbeatWaitTimeout bounds how long reconnect, cleanupStreams and Disconnect will wait
// for the heartbeat goroutine to exit. It is a package-level variable rather than a const
// so that tests reproducing blocking-Send scenarios can shorten it without dragging real
// production wait times down. The default of 10s tracks the conn_manager's own 5s heartbeat
// check timeout plus enough slack for one Send round-trip.
var heartbeatWaitTimeout = 10 * time.Second

// waitWaitGroupWithTimeout waits for wg to reach zero, but no longer than d. It returns
// true when wg completed before the timeout fired, false when the timeout fired first.
//
// On the timeout branch the inner waiter goroutine remains parked on wg.Wait() until wg
// eventually reaches zero (sync.WaitGroup offers no cancellation). Callers should ensure
// that whatever holds the wg counter up will eventually be released; in this package the
// reconnect / cleanupStreams / Disconnect paths close the registration stream right after
// calling this helper, which unblocks any Send the heartbeat goroutine is parked in and
// lets it run to completion.
func waitWaitGroupWithTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// safeClose closes ch if it is non-nil and has not already been closed. The caller is
// responsible for ensuring no other goroutine closes ch concurrently; this helper only
// guards against the "already closed by an earlier call from this same path" case.
func safeClose(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}
