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

package exporter

import (
	"math"
	"time"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// rateLimitProto converts normalized per-second rates to the Init rate limit; 0 is no limit.
func rateLimitProto(bytesPerSecond, rowsPerSecond float64) *transferv1.RateLimit {
	return &transferv1.RateLimit{
		BytesPerSecond: perSecond(bytesPerSecond),
		RowsPerSecond:  perSecond(rowsPerSecond),
	}
}

// perSecond rounds a normalized per-second rate to the wire value; 0 is no limit.
func perSecond(rate float64) uint64 { return uint64(math.Round(rate)) }

// idleTimer fires when no frame was received for its duration (--node-idle-timeout). The
// caller pauses it only while it backs off before reopening a refused stream. A zero duration
// disables it.
type idleTimer struct {
	t *time.Timer
	d time.Duration
}

func newIdleTimer(d time.Duration, fire func()) *idleTimer {
	if d <= 0 {
		return &idleTimer{}
	}
	return &idleTimer{d: d, t: time.AfterFunc(d, fire)}
}

// touch records progress and restarts the countdown.
func (it *idleTimer) touch() {
	if it.t != nil {
		it.t.Reset(it.d)
	}
}

// pause stops the countdown until resume.
func (it *idleTimer) pause() {
	if it.t != nil {
		it.t.Stop()
	}
}

// resume restarts the countdown after a pause.
func (it *idleTimer) resume() { it.touch() }

// stop releases the timer.
func (it *idleTimer) stop() { it.pause() }
