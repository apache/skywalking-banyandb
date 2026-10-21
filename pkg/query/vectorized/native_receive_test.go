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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestNativeReceiveScopeBoundsAllPendingStreamsBeforeDispatch(t *testing.T) {
	ctx := WithQueryMemoryTracker(t.Context(), 16<<20)
	tracker := QueryMemoryTracker(ctx, 16<<20)
	_, scope := WithNativeReceiveScope(ctx, tracker)
	_, err := scope.ReserveFanout(2, 16<<20, 32<<20)
	require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used())
	require.False(t, scope.IngressReserved())
	scope.Close()
}

func TestNativeReceiveScopeRetainsFanoutBoundUntilTransportCompletion(t *testing.T) {
	ctx, transport := query.NewTransportScope(t.Context())
	ctx = WithQueryMemoryTracker(ctx, 256<<20)
	tracker := QueryMemoryTracker(ctx, 256<<20)
	_, scope := WithNativeReceiveScope(ctx, tracker)
	capBytes, err := scope.ReserveFanout(2, 16<<20, 32<<20)
	require.NoError(t, err)
	require.Positive(t, capBytes)
	require.LessOrEqual(t, capBytes, 16<<20)
	require.Greater(t, tracker.Used(), int64(32<<20))
	require.True(t, scope.IngressReserved())
	scope.Close()
	require.Positive(t, tracker.Used(), "RPC transport owns pending receive reservations")
	transport.Close()
	require.Zero(t, tracker.Used())
	transport.Close()
}

func TestNativeReceiveScopeDirectCallerAndCancellationRelease(t *testing.T) {
	ctx := WithQueryMemoryTracker(t.Context(), 256<<20)
	tracker := QueryMemoryTracker(ctx, 256<<20)
	_, scope := WithNativeReceiveScope(ctx, tracker)
	_, err := scope.ReserveFanout(2, 16<<20, 32<<20)
	require.NoError(t, err)
	scope.Close()
	require.Zero(t, tracker.Used())
	scope.Close()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, scope = WithNativeReceiveScope(canceled, tracker)
	_, err = scope.ReserveFanout(1, 16<<20, 32<<20)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, tracker.Used())
}

func TestNativeReceiveStaticWindowAllowsTinyDefaultQueries(t *testing.T) {
	ctx := WithQueryMemoryTracker(t.Context(), 8<<20)
	tracker := QueryMemoryTracker(ctx, 8<<20)
	_, scope := WithNativeReceiveScope(ctx, tracker)
	capBytes, err := scope.ReserveFanout(2, 64<<10, 32<<20)
	require.NoError(t, err)
	require.Greater(t, capBytes, 32<<10)
	require.LessOrEqual(t, tracker.Used(), int64(3<<20), "reserve only fixed windows plus a quarter of remaining message headroom")
	scope.Close()
	require.Zero(t, tracker.Used())
}

func TestNativeReceiveTierAwareCapHasNoAllowanceCliff(t *testing.T) {
	for _, streams := range []int{1, 2, 17, 23} {
		previous := 0
		for _, limit := range []int64{8 << 20, 16 << 20, 32 << 20, 33 << 20, 48 << 20, 256 << 20} {
			tracker := NewMemoryTracker(limit)
			_, scope := WithNativeReceiveScope(t.Context(), tracker)
			capBytes, err := scope.ReserveFanout(streams, 64<<10, 32<<20)
			if err != nil {
				require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
				require.Zero(t, previous, "increasing allowance must not invalidate a previously valid fanout")
				require.Zero(t, tracker.Used())
				continue
			}
			require.GreaterOrEqual(t, capBytes, previous)
			fixed := int64(streams) * (64<<10 + 256<<10)
			require.LessOrEqual(t, tracker.Used(), fixed+(limit-fixed)/4)
			minimum := int64(streams) * (64<<10 + 256<<10 + 3*int64(capBytes))
			require.Equal(t, minimum, tracker.Used(), "reserve all stream windows, exact-length flatten and payload copies before receiving")
			previous = capBytes
			scope.Close()
			require.Zero(t, tracker.Used())
		}
	}
}

func TestNativeReceiveClosedScopeCannotAcquireReservations(t *testing.T) {
	tracker := NewMemoryTracker(8 << 20)
	_, scope := WithNativeReceiveScope(t.Context(), tracker)
	scope.Close()
	_, err := scope.ReserveFanout(2, 64<<10, 32<<20)
	require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used())
}

func TestNativeReceiveConcurrentCloseCannotLeakReservation(t *testing.T) {
	for i := 0; i < 64; i++ {
		tracker := NewMemoryTracker(8 << 20)
		_, scope := WithNativeReceiveScope(t.Context(), tracker)
		var workers sync.WaitGroup
		var err error
		workers.Go(func() { _, err = scope.ReserveFanout(2, 64<<10, 32<<20) })
		workers.Go(scope.Close)
		workers.Wait()
		if err != nil {
			require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
		}
		require.Zero(t, tracker.Used())
	}
}

func TestNativeReceiveDefaultBudgetSupportsEightSmallStreams(t *testing.T) {
	tracker := NewMemoryTracker(8 << 20)
	require.NoError(t, tracker.Reserve(1<<20))
	defer tracker.Release(1 << 20)
	_, scope := WithNativeReceiveScope(t.Context(), tracker)
	capBytes, err := scope.ReserveFanout(8, 64<<10, 32<<20)
	require.NoError(t, err)
	require.GreaterOrEqual(t, capBytes, 32<<10)
	require.Less(t, tracker.Used(), int64(8<<20))
	scope.Close()
	require.Equal(t, int64(1<<20), tracker.Used())
}
