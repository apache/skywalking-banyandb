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

package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// MaxBurst is the largest byte burst of a Relay and of a Node: one full chunk.
const MaxBurst = 1 << 20

// New returns a bucket of perSecond tokens a second whose burst is one second's worth, at most
// maxBurst; nil, unlimited, when perSecond is 0.
func New(perSecond uint64, maxBurst int) *rate.Limiter {
	if perSecond == 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), int(min(perSecond, uint64(maxBurst))))
}

// Wait takes n tokens from l in steps of its burst; a nil bucket never waits. A wait that ends
// with ctx returns the gRPC status of ctx, and one that would outlast its deadline
// DeadlineExceeded.
func Wait(ctx context.Context, l *rate.Limiter, n int) error {
	if l == nil {
		return nil
	}
	for n > 0 {
		step := min(n, l.Burst())
		if err := l.WaitN(ctx, step); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return status.FromContextError(ctxErr).Err()
			}
			return status.Error(codes.DeadlineExceeded, err.Error())
		}
		n -= step
	}
	return nil
}

// Relay is a process-level byte rate shared by every stream that relays through it. The
// stream opened last sets the rate, 0 meaning unlimited, and the rate returns to unlimited
// once no relay is active. One bucket serves every rate, so opening a stream adds no burst.
// The burst is one second's worth of the rate, at most MaxBurst, and a chunk larger than
// the burst's share of one relay is relayed in pieces of that share, so a low rate still
// delivers a frame to every relay about every second instead of one whole chunk after a long
// wait.
type Relay struct {
	limiter *rate.Limiter
	mu      sync.Mutex
	active  int
}

// NewRelay returns an unlimited Relay.
func NewRelay() *Relay {
	return &Relay{limiter: rate.NewLimiter(rate.Inf, MaxBurst)}
}

// Open applies bytesPerSecond for a relay that starts now and returns the function, called once, that
// ends the relay.
func (r *Relay) Open(bytesPerSecond uint64) (release func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active++
	limit, burst := rate.Inf, MaxBurst
	if bytesPerSecond > 0 {
		limit, burst = rate.Limit(float64(bytesPerSecond)), int(min(bytesPerSecond, MaxBurst))
	}
	r.setLocked(limit, burst)
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.active--
		if r.active == 0 {
			r.setLocked(rate.Inf, MaxBurst)
		}
	}
}

// setLocked applies a rate and burst. The bucket keeps the bytes it holds, up to the new burst,
// but drops a debt: the pieces that ran into it were paced by the old rate, and paying it back
// at a lower one would hold every relay for the old piece size at the new rate.
func (r *Relay) setLocked(limit rate.Limit, burst int) {
	now := time.Now()
	keep := burst
	if r.limiter.Limit() != rate.Inf {
		keep = int(min(max(r.limiter.TokensAt(now), 0), float64(burst)))
	}
	r.limiter = rate.NewLimiter(limit, burst)
	r.limiter.ReserveN(now, burst-keep)
}

// pieceSizeLocked is the largest piece of a chunk relayed at once: the current burst, shared
// by the active relays, so that each of them still delivers a frame about every second.
func (r *Relay) pieceSizeLocked() int {
	if r.limiter.Limit() == rate.Inf {
		// Nothing waits: a chunk goes out whole, however many relays are active.
		return MaxBurst
	}
	return max(1, r.limiter.Burst()/max(1, r.active))
}

// Take blocks until the next step of n bytes may pass and returns its size: at most one relay's
// share of the burst, sized and reserved under the lock open takes to change the rate, so a
// relay opening with a lower rate never leaves a step larger than the burst and the caller sends
// every step as soon as it passes. It returns the gRPC status of ctx once ctx ends, giving the
// reserved bytes back.
func (r *Relay) Take(ctx context.Context, n int) (int, error) {
	r.mu.Lock()
	step := min(n, r.pieceSizeLocked())
	reservation := r.limiter.ReserveN(time.Now(), step)
	r.mu.Unlock()
	if !reservation.OK() {
		return 0, status.Errorf(codes.Internal, "the relay rate refused %d bytes within its burst", step)
	}
	if delay := reservation.Delay(); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			reservation.Cancel()
			return 0, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	} else if err := ctx.Err(); err != nil {
		return 0, status.FromContextError(err).Err()
	}
	return step, nil
}

// Unlimited tells whether no stream currently limits the rate, so a batch need not be sized
// nor paid.
func (r *Relay) Unlimited() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limiter.Limit() == rate.Inf
}

// Pay blocks until the limiter lets n bytes pass.
func (r *Relay) Pay(ctx context.Context, n int) error {
	for n > 0 {
		step, err := r.Take(ctx, n)
		if err != nil {
			return err
		}
		n -= step
	}
	return nil
}

// NodeSlack is how much longer than its own share a payment to a Node may wait for the
// payments admitted before it. A payment that would wait longer is refused with ErrBusy, so a
// writer that waits for the node's answer bounds the wait by its own share plus NodeSlack.
const NodeSlack = 10 * time.Second

// ErrBusy refuses a payment the node cannot admit in time; nothing was charged for it.
var ErrBusy = errors.New("the node is admitting other imports")

// Node is the rate a data node admits import rows at: one byte pace and one row pace that every
// import writing to the node shares, whichever liaison it comes through. A pace is the time by
// which the node has paid out everything it admitted; each payment moves it on by its cost at the
// rate the payment names, so the last rate applies and a change carries no debt or burst over.
// A payment that names no rate for a pace is not charged to it. Up to one second's worth of a
// rate, at most MaxBurst bytes, passes without waiting on an idle node.
type Node struct {
	bytes time.Time
	rows  time.Time
	now   func() time.Time
	mu    sync.Mutex
}

// NewNode returns a Node that has admitted nothing yet.
func NewNode() *Node {
	return &Node{now: time.Now}
}

// Pay blocks until the node may write rows rows of n bytes at bytesPerSecond and rowsPerSecond,
// 0 meaning unlimited. It returns ErrBusy at once, charging nothing, when the payments admitted
// before it would make it wait more than NodeSlack beyond its own share, or past the deadline
// of ctx. A wait that ends with ctx gives the payment back unless a later one was admitted.
func (l *Node) Pay(ctx context.Context, bytesPerSecond, rowsPerSecond uint64, n, rows int) error {
	wait, release, err := l.admit(ctx, bytesPerSecond, rowsPerSecond, n, rows)
	if err != nil || wait <= 0 {
		return err
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		release()
		return status.FromContextError(ctx.Err()).Err()
	}
}

// charge is what one payment adds to one pace.
type charge struct {
	pace *time.Time
	end  time.Time
	cost time.Duration
}

// admit charges a payment to the paces and returns how long it waits, and how to give it back.
func (l *Node) admit(ctx context.Context, bytesPerSecond, rowsPerSecond uint64, n, rows int) (time.Duration, func(), error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, status.FromContextError(err).Err()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	var (
		charges   []charge
		wait, own time.Duration
	)
	add := func(pace *time.Time, perSecond uint64, amount int, maxBurst int) {
		if perSecond == 0 || amount <= 0 {
			return
		}
		cost := time.Duration(float64(amount) / float64(perSecond) * float64(time.Second))
		burst := time.Duration(float64(min(perSecond, uint64(maxBurst))) / float64(perSecond) * float64(time.Second))
		end := now.Add(cost)
		if pace.After(now) {
			end = pace.Add(cost)
		}
		wait = max(wait, end.Sub(now)-burst)
		own = max(own, cost-burst)
		charges = append(charges, charge{pace: pace, end: end, cost: cost})
	}
	add(&l.bytes, bytesPerSecond, n, MaxBurst)
	add(&l.rows, rowsPerSecond, rows, math.MaxInt)
	if wait > own+NodeSlack {
		return 0, nil, fmt.Errorf("%w: admitting %d rows of %d bytes would take %s, at most %s", ErrBusy, rows, n,
			wait.Round(time.Millisecond), (own + NodeSlack).Round(time.Millisecond))
	}
	if deadline, ok := ctx.Deadline(); ok && now.Add(wait).After(deadline) {
		return 0, nil, fmt.Errorf("%w: admitting %d rows of %d bytes would take %s, past the request's deadline", ErrBusy, rows, n,
			wait.Round(time.Millisecond))
	}
	for _, c := range charges {
		*c.pace = c.end
	}
	release := func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, c := range charges {
			if c.pace.Equal(c.end) {
				*c.pace = c.end.Add(-c.cost)
			}
		}
	}
	return wait, release, nil
}

// Limit is the current rate.
func (r *Relay) Limit() rate.Limit {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limiter.Limit()
}

// Burst is the current burst.
func (r *Relay) Burst() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limiter.Burst()
}

// PieceSize is the size of the next piece a relay would take.
func (r *Relay) PieceSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pieceSizeLocked()
}

// Proto converts normalized per-second rates to the Init rate limit; 0 is no limit.
func Proto(bytesPerSecond, rowsPerSecond float64) *transferv1.RateLimit {
	return &transferv1.RateLimit{
		BytesPerSecond: PerSecond(bytesPerSecond),
		RowsPerSecond:  PerSecond(rowsPerSecond),
	}
}

// PerSecond rounds a normalized per-second rate down to the wire value, so the limit sent never
// exceeds the one configured; 0 is no limit, and the configuration rejects a non-zero rate below
// 1 per second.
func PerSecond(rate float64) uint64 { return uint64(math.Floor(rate)) }
