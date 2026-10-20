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

package grpc

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// relayBurst is the largest burst of the relay limiter: one full chunk.
const relayBurst = 1 << 20

// relayLimiter is the liaison's process-level byte rate shared by every Export relay. The
// stream opened last sets the rate, 0 meaning unlimited, and the rate returns to unlimited
// once no relay is active. One bucket serves every rate, so opening a stream adds no burst.
// The burst is one second's worth of the rate, at most relayBurst, and a chunk larger than
// the burst's share of one relay is relayed in pieces of that share, so a low rate still
// delivers a frame to every relay about every second instead of one whole chunk after a long
// wait.
type relayLimiter struct {
	limiter *rate.Limiter
	mu      sync.Mutex
	active  int
}

func newRelayLimiter() *relayLimiter {
	return &relayLimiter{limiter: rate.NewLimiter(rate.Inf, relayBurst)}
}

// open applies bytesPerSecond for a relay that starts now and returns the function, called once, that
// ends the relay.
func (r *relayLimiter) open(bytesPerSecond uint64) (release func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active++
	limit, burst := rate.Inf, relayBurst
	if bytesPerSecond > 0 {
		limit, burst = rate.Limit(float64(bytesPerSecond)), int(min(bytesPerSecond, relayBurst))
	}
	r.limiter.SetLimit(limit)
	r.limiter.SetBurst(burst)
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.active--
		if r.active == 0 {
			r.limiter.SetLimit(rate.Inf)
			r.limiter.SetBurst(relayBurst)
		}
	}
}

// pieceSize is the largest piece of a chunk relayed at once: the current burst, shared by
// the active relays, so that each of them still delivers a frame about every second.
func (r *relayLimiter) pieceSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pieceSizeLocked()
}

func (r *relayLimiter) pieceSizeLocked() int {
	if r.limiter.Limit() == rate.Inf {
		// Nothing waits: a chunk goes out whole, however many relays are active.
		return relayBurst
	}
	return max(1, r.limiter.Burst()/max(1, r.active))
}

// wait blocks until n bytes may pass, in steps no larger than one relay's share of the burst.
// Every step is reserved under the lock open takes to change the rate, so a relay opening with
// a lower rate never leaves a step larger than the burst. It returns the gRPC status of ctx
// once ctx ends, giving the reserved bytes back.
func (r *relayLimiter) wait(ctx context.Context, n int) error {
	for n > 0 {
		r.mu.Lock()
		step := min(n, r.pieceSizeLocked())
		reservation := r.limiter.ReserveN(time.Now(), step)
		r.mu.Unlock()
		if !reservation.OK() {
			return status.Errorf(codes.Internal, "the relay rate refused %d bytes within its burst", step)
		}
		if delay := reservation.Delay(); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				reservation.Cancel()
				return status.FromContextError(ctx.Err()).Err()
			case <-timer.C:
			}
		} else if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		n -= step
	}
	return nil
}
