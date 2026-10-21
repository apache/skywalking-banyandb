// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorized

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

type nativeReceiveScopeKey struct{}

// NativeReceiveScope reserves every pending native receive before streams open.
// The transport owns reservations when present; otherwise Close releases them.
type NativeReceiveScope struct {
	tracker  *MemoryTracker
	ctx      context.Context
	releases []func()
	mu       sync.Mutex
	guarded  atomic.Bool
	closed   bool
}

// WithNativeReceiveScope installs a Go-only guard for measure batch broadcasts.
func WithNativeReceiveScope(ctx context.Context, tracker *MemoryTracker) (context.Context, *NativeReceiveScope) {
	scope := &NativeReceiveScope{ctx: ctx, tracker: tracker}
	return context.WithValue(ctx, nativeReceiveScopeKey{}, scope), scope
}

// NativeReceiveScopeFromContext returns the native measure receive guard, if any.
func NativeReceiveScopeFromContext(ctx context.Context) (*NativeReceiveScope, bool) {
	scope, ok := ctx.Value(nativeReceiveScopeKey{}).(*NativeReceiveScope)
	return scope, ok
}

// ReserveFanout reserves bounded HTTP/2 windows and message workspace before
// opening streams. Fixed windows and parser workspace are reserved first, then a
// quarter of the remaining allowance is partitioned among message copies;
// the rest remains available for decoded batches and final responses. Guarded
// calls must use the native codec's exact-length flattening; the ordinary grpc
// codec's pooled 1MiB materialization tier would exceed this reservation.
// Uneven responses may still fail their per-stream cap.
func (s *NativeReceiveScope) ReserveFanout(streams int, window, maxMessage int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("%w: native receive scope is closed", ErrMemoryBudgetExceeded)
	}
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	if streams <= 0 {
		return 0, fmt.Errorf("native receive requires at least one stream")
	}
	available := s.tracker.limit - s.tracker.Used()
	if lease, ok := query.BudgetLeaseFromContext(s.ctx); ok {
		remaining := lease.Limit() - min(lease.Limit(), lease.Used())
		if remaining < uint64(max(available, 0)) {
			available = int64(remaining)
		}
	}
	if window < 0 || maxMessage <= 0 || available <= 0 {
		return 0, fmt.Errorf("%w: invalid native receive allowance", ErrMemoryBudgetExceeded)
	}
	// Parser/header buffers and queued HTTP/2 frame descriptors are bounded
	// separately from flow-controlled payload and protobuf materialization.
	const parserWorkspace = 256 << 10
	if window > available || parserWorkspace > available-window || int64(streams) > available/(window+parserWorkspace) {
		return 0, fmt.Errorf("%w: pending native transport windows exceed remaining allowance", ErrMemoryBudgetExceeded)
	}
	fixed := (window + parserWorkspace) * int64(streams)
	// Three payload copies cover receive chunks (including the message-read
	// flow-control delta), exact-length flattening and retained protobuf bytes.
	// The static stream window separately covers not-yet-requested data.
	low := min(maxMessage, (available-fixed)/4/3/int64(streams))
	if low < 1024 {
		return 0, fmt.Errorf("%w: native receive message allowance is too small", ErrMemoryBudgetExceeded)
	}
	reservation := fixed + 3*low*int64(streams)
	if err := s.tracker.Reserve(reservation); err != nil {
		return 0, err
	}
	var once sync.Once
	release := query.RegisterTransportRelease(s.ctx, func() { once.Do(func() { s.tracker.Release(reservation) }) })
	s.releases = append(s.releases, release)
	s.guarded.Store(true)
	return int(low), nil
}

// IngressReserved reports that native ingress copies already have reservations.
func (s *NativeReceiveScope) IngressReserved() bool { return s.guarded.Load() }

// Close releases reservations not transferred to the RPC transport scope.
func (s *NativeReceiveScope) Close() {
	s.mu.Lock()
	s.closed = true
	releases := s.releases
	s.releases = nil
	s.mu.Unlock()
	for _, release := range releases {
		release()
	}
}
