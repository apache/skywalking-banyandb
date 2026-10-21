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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestMemoryTracker_Reserve_SucceedsWithinLimit(t *testing.T) {
	m := NewMemoryTracker(1024)
	if err := m.Reserve(512); err != nil {
		t.Fatalf("Reserve(512): %v", err)
	}
	if m.Used() != 512 {
		t.Fatalf("Used after Reserve(512): want 512, got %d", m.Used())
	}
	if err := m.Reserve(256); err != nil {
		t.Fatalf("Reserve(256): %v", err)
	}
	if m.Used() != 768 {
		t.Fatalf("Used after second reserve: want 768, got %d", m.Used())
	}
}

func TestMemoryTracker_Reserve_FailsAtLimit_UsedUnchanged(t *testing.T) {
	m := NewMemoryTracker(100)
	if err := m.Reserve(50); err != nil {
		t.Fatalf("first reserve: %v", err)
	}
	if err := m.Reserve(60); err == nil {
		t.Fatal("Reserve(60) on top of 50/100 must fail")
	}
	if m.Used() != 50 {
		t.Fatalf("failed Reserve must not advance used: got %d", m.Used())
	}
}

func TestMemoryTracker_Release_DecreasesUsed(t *testing.T) {
	m := NewMemoryTracker(1024)
	_ = m.Reserve(512)
	m.Release(200)
	if m.Used() != 312 {
		t.Fatalf("after Release(200): want 312, got %d", m.Used())
	}
}

func TestMemoryTracker_Used_ReturnsCurrent(t *testing.T) {
	m := NewMemoryTracker(1024)
	if m.Used() != 0 {
		t.Fatalf("initial Used: want 0, got %d", m.Used())
	}
	_ = m.Reserve(7)
	if m.Used() != 7 {
		t.Fatalf("after Reserve(7): want 7, got %d", m.Used())
	}
}

func TestMemoryTracker_Reserve_NegativeBytes_Panics(t *testing.T) {
	m := NewMemoryTracker(1024)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Reserve(-1) must panic — programmer error, not a recoverable runtime condition")
		}
	}()
	_ = m.Reserve(-1)
}

func TestMemoryTracker_Release_NegativeBytes_Panics(t *testing.T) {
	m := NewMemoryTracker(1024)
	if err := m.Reserve(100); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Release(-1) must panic — programmer error")
		}
	}()
	m.Release(-1)
}

func TestMemoryTracker_Concurrent_TotalExact(t *testing.T) {
	m := NewMemoryTracker(100000)
	const goroutines = 100
	const perGoroutine = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range perGoroutine {
				if err := m.Reserve(1); err != nil {
					t.Errorf("unexpected reserve failure: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	want := int64(goroutines * perGoroutine)
	if m.Used() != want {
		t.Fatalf("concurrent Used: want %d, got %d", want, m.Used())
	}
}

type trackerTestLease struct {
	used  uint64
	limit uint64
}

func (l *trackerTestLease) Charge(bytes uint64) error {
	if bytes > l.limit-l.used {
		return errTrackerTestLeaseBudget
	}
	l.used += bytes
	return nil
}

func (l *trackerTestLease) Limit() uint64 { return l.limit }
func (l *trackerTestLease) Used() uint64  { return l.used }
func (l *trackerTestLease) Owner() any    { return l }

var errTrackerTestLeaseBudget = errors.New("test lease budget exhausted")

func TestQueryMemoryTrackerCachedLeaseRollbackAndCounters(t *testing.T) {
	before := QueryMemoryStats()
	expectedLeaseCharges := before.LeaseChargedBytes
	if QueryMetricsEnabled {
		expectedLeaseCharges += 6
	}
	lease := &trackerTestLease{limit: 10}
	ctx, cancel := context.WithCancel(query.WithBudgetLease(t.Context(), lease))
	defer cancel()
	tracker := newQueryMemoryTracker(ctx, 100)
	require.NoError(t, tracker.Reserve(6))
	require.Equal(t, uint64(6), lease.Used())
	require.Equal(t, expectedLeaseCharges, QueryMemoryStats().LeaseChargedBytes)
	require.ErrorIs(t, tracker.Reserve(5), errTrackerTestLeaseBudget)
	require.Equal(t, int64(6), tracker.Used())
	cancel()
	require.ErrorIs(t, tracker.Reserve(1), context.Canceled)
	require.Equal(t, int64(6), tracker.Used())
	require.Equal(t, uint64(6), lease.Used())
	require.Equal(t, expectedLeaseCharges, QueryMemoryStats().LeaseChargedBytes)
	tracker.Release(6)
	require.Equal(t, before.LiveReservedBytes, QueryMemoryStats().LiveReservedBytes)
}

func TestQueryMemoryTrackerWithoutLeaseHasNoChargeHook(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tracker := newQueryMemoryTracker(ctx, 100)
	require.Nil(t, tracker.charge)
	require.NoError(t, tracker.Reserve(1), "unadmitted charging remains a no-op after cancellation")
	tracker.Release(1)
}
