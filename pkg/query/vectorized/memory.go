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

package vectorized

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

// ErrMemoryBudgetExceeded indicates exhaustion of the shared query memory allowance.
var ErrMemoryBudgetExceeded = errors.New("memory budget exceeded")

// MemoryTracker is a lock-free per-query memory budget.
//
// Reserve grows used iff used+bytes <= limit; otherwise it returns an error and
// leaves used unchanged. Release shrinks used. Used reads the current value.
type MemoryTracker struct {
	charge func(uint64) error
	// refund hands released bytes back to the lease the charges went to; it is set only for queries
	// that account for live memory (see WithLiveLeaseAccounting).
	refund func(uint64)
	used   atomic.Int64
	limit  int64
}

// NewMemoryTracker returns a tracker with the given byte limit.
func NewMemoryTracker(limit int64) *MemoryTracker {
	return &MemoryTracker{limit: limit}
}

// Reserve attempts to allocate bytes from the budget.
// On success used grows by bytes; on failure used is unchanged.
//
// A negative bytes value indicates a programmer error (Reserve must not be
// used to decrement) and panics. Zero bytes is a valid no-op.
func (m *MemoryTracker) Reserve(bytes int64) error {
	if bytes < 0 {
		panic("vectorized: MemoryTracker.Reserve called with negative bytes")
	}
	if bytes == 0 {
		return nil
	}
	for {
		current := m.used.Load()
		next := current + bytes
		if bytes > m.limit-current {
			return fmt.Errorf("%w: used %d + requested %d > limit %d",
				ErrMemoryBudgetExceeded, current, bytes, m.limit)
		}
		if m.used.CompareAndSwap(current, next) {
			if m.charge != nil {
				if err := m.charge(uint64(bytes)); err != nil {
					m.used.Add(-bytes)
					return err
				}
			}
			recordMemoryReservation(bytes)
			return nil
		}
	}
}

// Release returns bytes to the budget.
// A negative bytes value indicates a programmer error and panics.
func (m *MemoryTracker) Release(bytes int64) {
	if bytes < 0 {
		panic("vectorized: MemoryTracker.Release called with negative bytes")
	}
	if bytes == 0 {
		return
	}
	m.used.Add(-bytes)
	if m.refund != nil {
		m.refund(uint64(bytes))
	}
	recordMemoryRelease(bytes)
}

// Used returns the current outstanding reservation.
func (m *MemoryTracker) Used() int64 {
	return m.used.Load()
}

type memoryTrackerContextKey struct{}

// WithQueryMemoryTracker installs one shared tracker unless the query already owns one.
// Admission lease charging is conservative and monotonic; live tracker reservations are refundable.
func WithQueryMemoryTracker(ctx context.Context, limit int64) context.Context {
	if _, ok := ctx.Value(memoryTrackerContextKey{}).(*MemoryTracker); ok {
		return ctx
	}

	if scope, ok := ctx.Value(queryMemoryScopeKey{}).(*queryMemoryScope); ok {
		return context.WithValue(ctx, memoryTrackerContextKey{}, scope.initialize(ctx, limit))
	}
	return context.WithValue(ctx, memoryTrackerContextKey{}, newQueryMemoryTracker(ctx, limit))
}

type liveLeaseAccountingKey struct{}

// WithLiveLeaseAccounting makes the query tracker created under ctx hand released bytes back to the query lease,
// so the lease tracks what the query holds right now instead of everything it ever reserved. A streaming scan
// needs it: it reserves and releases memory for every block it reads, and a result of any size must run inside
// the lease. Queries that collect their whole result keep the default, where charges only accumulate.
func WithLiveLeaseAccounting(ctx context.Context) context.Context {
	return context.WithValue(ctx, liveLeaseAccountingKey{}, true)
}

func newQueryMemoryTracker(ctx context.Context, limit int64) *MemoryTracker {
	tracker := NewMemoryTracker(limit)
	lease, admitted := query.BudgetLeaseFromContext(ctx)
	if admitted {
		if live, _ := ctx.Value(liveLeaseAccountingKey{}).(bool); live {
			tracker.refund = lease.Refund
		}
		tracker.charge = func(bytes uint64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := lease.Charge(bytes); err != nil {
				return err
			}
			recordLeaseCharge(bytes)
			return nil
		}
	}
	return tracker
}

// QueryMemoryTracker returns the shared query tracker, or a standalone tracker for legacy callers.
func QueryMemoryTracker(ctx context.Context, limit int64) *MemoryTracker {
	if tracker, ok := ctx.Value(memoryTrackerContextKey{}).(*MemoryTracker); ok {
		return tracker
	}
	if scope, ok := ctx.Value(queryMemoryScopeKey{}).(*queryMemoryScope); ok {
		return scope.initialize(ctx, limit)
	}
	return NewMemoryTracker(limit)
}

// QueryMemoryTrackerFromContext returns an installed query tracker without creating one.
func QueryMemoryTrackerFromContext(ctx context.Context) (*MemoryTracker, bool) {
	tracker, ok := ctx.Value(memoryTrackerContextKey{}).(*MemoryTracker)
	return tracker, ok
}
