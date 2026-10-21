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

package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllocationScopeTracksReusableBufferGrowthAndClose(t *testing.T) {
	var used int64
	failure := errors.New("allocation denied")
	ctx, scope := WithAllocationScope(context.Background(), func(bytes int64) error {
		if bytes > 100-used {
			return failure
		}
		used += bytes
		return nil
	}, func(bytes int64) { used -= bytes })
	defer func() { require.NoError(t, scope.Close()) }()
	key := new(int)
	require.NoError(t, ReserveAllocationOnce(ctx, key, 20))
	require.NoError(t, ReserveAllocationOnce(ctx, key, 20))
	require.Equal(t, int64(20), used)
	require.NoError(t, ReserveAllocationOnce(ctx, key, 50))
	require.Equal(t, int64(50), used)
	require.NoError(t, ReserveAllocation(ctx, 40))
	require.ErrorIs(t, ReserveAllocationOnce(ctx, key, 70), failure)
	require.Equal(t, int64(90), used)
	require.NoError(t, scope.Close())
	require.Zero(t, used)
	require.NoError(t, scope.Close())
	require.Error(t, ReserveAllocation(ctx, 1))
}

func TestAllocationScopeCancellationPreventsReservation(t *testing.T) {
	var called bool
	ctx, scope := WithAllocationScope(context.Background(), func(int64) error { called = true; return nil }, func(int64) {})
	defer func() { require.NoError(t, scope.Close()) }()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, ReserveAllocation(ctx, 100), context.Canceled)
	require.False(t, called)
}

func TestAllocationScopeConcurrentReusableGrowth(t *testing.T) {
	var used int64
	ctx, scope := WithAllocationScope(context.Background(), func(bytes int64) error { used += bytes; return nil }, func(bytes int64) { used -= bytes })
	defer func() { require.NoError(t, scope.Close()) }()
	var workers sync.WaitGroup
	results := make(chan error, 32)
	key := new(int)
	for n := 1; n <= 32; n++ {
		workers.Add(1)
		go func(target uint64) { defer workers.Done(); results <- ReserveAllocationOnce(ctx, key, target) }(uint64(n))
	}
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, int64(32), used)
	require.NoError(t, scope.Close())
	require.Zero(t, used)
}

func TestAllocationScopeOnceRejectsClosedWithoutGrowth(t *testing.T) {
	ctx, scope := WithAllocationScope(t.Context(), func(int64) error { return nil }, func(int64) {})
	key := new(int)
	require.NoError(t, ReserveAllocationOnce(ctx, key, 10))
	require.NoError(t, scope.Close())
	require.ErrorContains(t, ReserveAllocationOnce(ctx, key, 0), "source is closed")
	require.ErrorContains(t, ReserveAllocationOnce(ctx, nil, 0), "source is closed")
	require.Nil(t, scope.lastKey)
	require.Zero(t, scope.lastSize)
}

func TestAllocationScopeOnceInterleavedHighWaterAndFailure(t *testing.T) {
	var used int64
	var charges []int64
	failure := errors.New("denied growth")
	ctx, scope := WithAllocationScope(t.Context(), func(bytes int64) error {
		if bytes > 100-used {
			return failure
		}
		charges = append(charges, bytes)
		used += bytes
		return nil
	}, func(bytes int64) { used -= bytes })
	defer scope.Close()
	first, second := new(int), new(int)
	for _, request := range []struct {
		key   *int
		bytes uint64
	}{{first, 20}, {first, 20}, {first, 5}, {second, 30}, {first, 25}, {second, 10}, {second, 35}} {
		require.NoError(t, ReserveAllocationOnce(ctx, request.key, request.bytes))
	}
	require.Equal(t, []int64{20, 30, 5, 5}, charges)
	require.Equal(t, int64(60), used)
	require.ErrorIs(t, ReserveAllocationOnce(ctx, first, 90), failure)
	require.NoError(t, ReserveAllocationOnce(ctx, first, 30))
	require.Equal(t, int64(65), used)
	require.Equal(t, int64(5), charges[len(charges)-1])
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, ReserveAllocationOnce(canceled, first, 30), context.Canceled)
	require.ErrorIs(t, ReserveAllocationOnce(canceled, first, 31), context.Canceled)
	require.Equal(t, int64(65), used)
	expired, cancelDeadline := context.WithDeadline(ctx, time.Unix(1, 0))
	defer cancelDeadline()
	require.ErrorIs(t, ReserveAllocationOnce(expired, first, 30), context.DeadlineExceeded)
	require.ErrorIs(t, ReserveAllocationOnce(expired, first, 31), context.DeadlineExceeded)
	require.Equal(t, int64(65), used)
	require.NoError(t, scope.Close())
	require.Zero(t, used)
}

func TestAllocationScopeOnceComparableKeysAndPanicUnlock(t *testing.T) {
	ctx, scope := WithAllocationScope(t.Context(), func(int64) error { return nil }, func(int64) {})
	defer scope.Close()
	for _, key := range []any{nil, "reader", 42, struct{ index int }{7}, new(int)} {
		require.NoError(t, ReserveAllocationOnce(ctx, key, 10))
		require.NoError(t, ReserveAllocationOnce(ctx, key, 5))
	}
	require.Panics(t, func() { _ = ReserveAllocationOnce(ctx, []int{1}, 10) })
	require.NoError(t, ReserveAllocationOnce(ctx, "reader", 11), "invalid keys must not leave the lock held")
}

func TestAllocationScopeOnceConcurrentCloseAndGrowth(t *testing.T) {
	var used int64
	ctx, scope := WithAllocationScope(t.Context(), func(bytes int64) error { used += bytes; return nil }, func(bytes int64) { used -= bytes })
	key := new(int)
	require.NoError(t, ReserveAllocationOnce(ctx, key, 1))
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := range 8 {
		workers.Go(func() {
			<-start
			for size := uint64(worker + 1); size < 64; size++ {
				if err := ReserveAllocationOnce(ctx, key, size); err != nil {
					require.ErrorContains(t, err, "source is closed")
					return
				}
			}
		})
	}
	workers.Go(func() { <-start; _ = scope.Close() })
	close(start)
	workers.Wait()
	require.NoError(t, scope.Close())
	require.Zero(t, used)
	require.ErrorContains(t, ReserveAllocationOnce(ctx, key, 0), "source is closed")
}

type allocationBenchmarkContextKey int

type allocationBenchmarkReader struct{ index int }

func BenchmarkReserveAllocationOnceReaders(b *testing.B) {
	for _, readers := range []int{1, 4} {
		b.Run(fmt.Sprintf("readers_%d", readers), func(b *testing.B) {
			ctx, scope := WithAllocationScope(context.Background(), func(int64) error { return nil }, func(int64) {})
			defer scope.Close()
			for depth := range 8 {
				ctx = context.WithValue(ctx, allocationBenchmarkContextKey(depth), depth)
			}
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			keys := make([]*allocationBenchmarkReader, readers)
			for index := range keys {
				keys[index] = &allocationBenchmarkReader{index: index}
				if err := ReserveAllocationOnce(ctx, keys[index], 64<<10); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				if err := ReserveAllocationOnce(ctx, keys[iteration%readers], 64<<10); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
