// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package protector

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	querypkg "github.com/apache/skywalking-banyandb/pkg/query"
)

const (
	queryPoolFraction           = 4
	queryMaxFraction            = 8
	queryHeadroomPercent        = 10
	queryFallbackPool    uint64 = 64 << 20
)

var (
	// ErrQueryTooLarge indicates that a request exceeds the configured hard limit.
	ErrQueryTooLarge = errors.New("query memory request exceeds per-query limit")
	// ErrQueryResourceExhausted indicates temporary contention for the query pool.
	ErrQueryResourceExhausted = errors.New("query memory pool exhausted")
	// ErrQueryWindowPressure indicates a result window exceeding the current assignment.
	ErrQueryWindowPressure = errors.New("query result window exceeds current memory budget")
)

// Admission uses conservative initial estimates, not measured per-record maxima.
// Execution charges and the absolute window are additional bounds; these values
// should be calibrated with representative Property, Stream and Trace workloads.
const (
	queryFixedOverhead       uint64 = 1 << 20
	queryEstimatedEntryBytes uint64 = 4 << 10
	queryAbsoluteWindow      uint64 = 100000
)

// QueryBudget provides an atomic, finite reservation pool for one service.
// Reservations are estimates and must remain held until response materialization
// is complete. It intentionally does not modify Memory.AcquireResource semantics.
type QueryBudget struct {
	protector Memory
	reserved  atomic.Uint64
}

// QueryReservation tracks charged allocations and shared ownership of an admitted query.
type QueryReservation struct {
	owner    *QueryBudget
	assigned uint64
	used     atomic.Uint64
	released atomic.Bool
	mu       sync.Mutex
	refs     int32
}

// Charge accounts bytes before allocation, rejecting charges beyond the assignment.
func (r *QueryReservation) Charge(bytes uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released.Load() {
		return ErrQueryResourceExhausted
	}
	used := r.used.Load()
	if bytes > r.assigned-used {
		return ErrQueryResourceExhausted
	}
	r.used.Store(used + bytes)
	return nil
}

// Limit returns the reservation's assigned byte budget.
func (r *QueryReservation) Limit() uint64 { return r.assigned }

// Used returns the cumulative bytes charged to the reservation.
func (r *QueryReservation) Used() uint64 { return r.used.Load() }

// Owner identifies the shared query pool owning this reservation.
func (r *QueryReservation) Owner() any { return r.owner }

func (r *QueryReservation) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs <= 0 {
		return
	}
	r.refs--
	if r.refs == 0 && r.released.CompareAndSwap(false, true) {
		r.owner.reserved.Add(^(r.assigned - 1))
	}
}

func (r *QueryReservation) retain() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released.Load() || r.refs <= 0 {
		return false
	}
	r.refs++
	return true
}

var fallbackBudget struct {
	budget *QueryBudget
	sync.Once
}
var customBudgets sync.Map

type (
	queryBudgetProvider interface{ QueryBudget() *QueryBudget }
	usageRefresher      interface{ RefreshUsage() }
)

// QueryBudgetFor returns the process-wide budget associated with a protector.
// It avoids independent pools for the liaison's public services.
func QueryBudgetFor(pm Memory) *QueryBudget {
	if pm == nil {
		pm = &Nop{}
	}
	if provider, ok := pm.(queryBudgetProvider); ok {
		return provider.QueryBudget()
	}
	_, pointerNop := pm.(*Nop)
	_, valueNop := pm.(Nop)
	if !pointerNop && !valueNop && reflect.TypeOf(pm).Comparable() {
		budget := NewQueryBudget(pm)
		actual, _ := customBudgets.LoadOrStore(pm, budget)
		return actual.(*QueryBudget)
	}
	fallbackBudget.Do(func() { fallbackBudget.budget = NewQueryBudget(nil) })
	return fallbackBudget.budget
}

// NewQueryBudget derives a bounded query pool from the protector. A disabled or
// unavailable protector uses a finite fallback pool rather than becoming unlimited.
func NewQueryBudget(pm Memory) *QueryBudget {
	return &QueryBudget{protector: pm}
}

// Limit returns the finite pool size available to all admitted queries.
func (b *QueryBudget) Limit() uint64 { return b.poolLimit() }

// Reserved returns the current total of outstanding query reservations.
func (b *QueryBudget) Reserved() uint64 { return b.reserved.Load() }

// MaxQuery returns the hard per-query reservation limit.
func (b *QueryBudget) MaxQuery() uint64 { return b.poolLimit() / queryMaxFraction }

// Reserve atomically reserves bytes, returning a release function. It rejects
// oversized requests and pressure, and honors cancellation before admission.
func (b *QueryBudget) Reserve(ctx context.Context, bytes uint64) (func(), error) {
	if ctx == nil {
		return nil, errors.New("query reservation requires a context")
	}
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("query reservation canceled: %w", ctx.Err())
	default:
	}
	maxQuery := b.MaxQuery()
	if maxQuery == 0 {
		maxQuery = 1
	}
	if bytes > maxQuery {
		return nil, fmt.Errorf("requested %d bytes, maximum %d: %w", bytes, maxQuery, ErrQueryTooLarge)
	}
	for {
		pool := b.poolLimit()
		current := b.reserved.Load()
		if current > pool || bytes > pool-current {
			return nil, ErrQueryResourceExhausted
		}
		available := b.available()
		if available < current || bytes > available-current {
			return nil, ErrQueryResourceExhausted
		}
		if b.reserved.CompareAndSwap(current, current+bytes) {
			var released atomic.Bool
			return func() {
				if bytes > 0 && released.CompareAndSwap(false, true) {
					b.reserved.Add(^(bytes - 1))
				}
			}, nil
		}
	}
}

// Admit reserves the complete dynamic assignment for a query after deriving a
// result-window ceiling from current sampled memory and outstanding reservations.
func (b *QueryBudget) Admit(ctx context.Context, limit, offset, defaultLimit uint32) (func(), error) {
	_, release, err := b.AdmitContext(ctx, limit, offset, defaultLimit)
	return release, err
}

// AdmitContext admits a query and attaches its chargeable reservation to ctx.
func (b *QueryBudget) AdmitContext(ctx context.Context, limit, offset, defaultLimit uint32) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("query admission requires a context")
	}
	if limit == 0 {
		limit = defaultLimit
	}
	window := uint64(limit) + uint64(offset)
	if window > uint64(^uint32(0)) || window > queryAbsoluteWindow {
		return nil, nil, ErrQueryTooLarge
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("query admission canceled: %w", err)
	}
	if existing, ok := querypkg.BudgetLeaseFromContext(ctx); ok && existing.Owner() == b {
		reservation, reservationOk := existing.(*QueryReservation)
		if !reservationOk || reservation.released.Load() {
			return nil, nil, ErrQueryResourceExhausted
		}
		maxWindow := uint64(0)
		if reservation.assigned > queryFixedOverhead {
			maxWindow = (reservation.assigned - queryFixedOverhead) / queryEstimatedEntryBytes
		}
		if window > maxWindow {
			return nil, nil, ErrQueryWindowPressure
		}
		if !reservation.retain() {
			return nil, nil, ErrQueryResourceExhausted
		}
		return querypkg.WithBudgetLease(ctx, reservation), reservationReleaseForTransport(ctx, reservation), nil
	}
	pool := b.poolLimit()
	maxQuery := pool / queryMaxFraction
	if maxQuery == 0 {
		maxQuery = 1
	}
	for {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("query admission canceled: %w", ctx.Err())
		default:
		}
		current := b.reserved.Load()
		available := b.available()
		if current >= pool || current >= available {
			return nil, nil, ErrQueryResourceExhausted
		}
		assignment := minUint64(maxQuery, pool-current)
		assignment = minUint64(assignment, available-current)
		if assignment <= queryFixedOverhead {
			return nil, nil, ErrQueryResourceExhausted
		}
		maxWindow := (assignment - queryFixedOverhead) / queryEstimatedEntryBytes
		if window > maxWindow {
			return nil, nil, ErrQueryWindowPressure
		}
		if b.reserved.CompareAndSwap(current, current+assignment) {
			reservation := &QueryReservation{owner: b, assigned: assignment, refs: 1}
			if chargeErr := reservation.Charge(queryFixedOverhead); chargeErr != nil {
				b.reserved.Add(^(assignment - 1))
				return nil, nil, chargeErr
			}
			return querypkg.WithBudgetLease(ctx, reservation), reservationReleaseForTransport(ctx, reservation), nil
		}
	}
}

func reservationRelease(reservation *QueryReservation) func() {
	var once sync.Once
	return func() { once.Do(reservation.release) }
}

func reservationReleaseForTransport(ctx context.Context, reservation *QueryReservation) func() {
	localRelease := reservationRelease(reservation)
	if _, scoped := querypkg.TransportScopeFromContext(ctx); !scoped {
		return localRelease
	}
	if reservation.retain() {
		querypkg.RegisterTransportRelease(ctx, reservationRelease(reservation))
	}
	return localRelease
}

func (b *QueryBudget) available() uint64 {
	limit := uint64(0)
	if b.protector != nil {
		if refresher, ok := b.protector.(usageRefresher); ok {
			refresher.RefreshUsage()
		}
		limit = b.protector.GetLimit()
	}
	pool := b.poolLimit()
	if limit == 0 || b.protector == nil {
		return pool
	}
	available := b.protector.AvailableBytes()
	if available < 0 {
		return 0
	}
	headroom := limit / queryHeadroomPercent
	if uint64(available) <= headroom {
		return 0
	}
	return minUint64(pool, uint64(available)-headroom)
}

func (b *QueryBudget) poolLimit() uint64 {
	limit := uint64(0)
	if b.protector != nil {
		limit = b.protector.GetLimit()
	}
	if limit == 0 {
		return queryFallbackPool
	}
	pool := limit / queryPoolFraction
	if pool == 0 {
		return 1
	}
	return pool
}

func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}

// MaxWindow computes the policy ceiling without accounting for current contention.
// AdmitContext must be used for admission against available memory.
func (b *QueryBudget) MaxWindow(fixedOverhead, estimatedBytesPerEntry, absoluteCeiling uint64) uint64 {
	maxQuery := b.MaxQuery()
	if estimatedBytesPerEntry == 0 || maxQuery <= fixedOverhead {
		return 0
	}
	window := (maxQuery - fixedOverhead) / estimatedBytesPerEntry
	if window > absoluteCeiling {
		return absoluteCeiling
	}
	return window
}
