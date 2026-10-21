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
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

func TestQueryMemoryScopeUsesProcessorLimitAtCaller(t *testing.T) {
	caller := WithQueryMemoryScope(context.Background())
	processor := WithQueryMemoryTracker(caller, 16)
	tracker := QueryMemoryTracker(processor, 256)
	require.Same(t, tracker, QueryMemoryTracker(caller, 256))
	require.ErrorIs(t, tracker.Reserve(17), ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used())
}

func TestResponseEncodingStacksAgainstRetainedOutput(t *testing.T) {
	response := &modelv1.Str{Value: "response"}
	bytes := int64(proto.Size(response) * 2)
	caller := WithQueryMemoryScope(context.Background())
	processor := WithQueryMemoryTracker(caller, 100+bytes-1)
	tracker := QueryMemoryTracker(processor, 256)
	require.NoError(t, tracker.Reserve(100))
	defer tracker.Release(100)
	_, err := ReserveResponseEncoding(caller, response)
	require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
	require.Equal(t, int64(100), tracker.Used())
}

func TestResponseEncodingHeldUntilTransportCompletion(t *testing.T) {
	caller, scope := query.NewTransportScope(WithQueryMemoryScope(context.Background()))
	defer scope.Close()
	processor := WithQueryMemoryTracker(caller, 1024)
	tracker := QueryMemoryTracker(processor, 256)
	response := &modelv1.Str{Value: "response"}
	bytes := int64(proto.Size(response) * 2)
	release, err := ReserveResponseEncoding(caller, response)
	require.NoError(t, err)
	release()
	require.Equal(t, bytes, tracker.Used())
	scope.Close()
	require.Zero(t, tracker.Used())
}

func TestResponseEncodingDirectCallerReleasesLocally(t *testing.T) {
	caller := WithQueryMemoryScope(context.Background())
	processor := WithQueryMemoryTracker(caller, 1024)
	tracker := QueryMemoryTracker(processor, 256)
	release, err := ReserveResponseEncoding(caller, &modelv1.Str{Value: "response"})
	require.NoError(t, err)
	require.Positive(t, tracker.Used())
	release()
	release()
	require.Zero(t, tracker.Used())
}

func encodingScopeFixture(t *testing.T, limit int64) (context.Context, *MemoryTracker) {
	t.Helper()
	caller := WithQueryMemoryScope(t.Context())
	processor := WithQueryMemoryTracker(caller, limit)
	return caller, QueryMemoryTracker(processor, limit)
}

func encodingPoints() ([]*measurev1.InternalDataPoint, *measurev1.QueryResponse) {
	points := []*measurev1.InternalDataPoint{{DataPoint: &measurev1.DataPoint{Version: 7}}, {DataPoint: &measurev1.DataPoint{Version: 9}}}
	return points, &measurev1.QueryResponse{DataPoints: []*measurev1.DataPoint{points[0].DataPoint, points[1].DataPoint}}
}

func TestResponseEncodingBoundCoverageEnvelopeAndOneShot(t *testing.T) {
	ctx, tracker := encodingScopeFixture(t, 1<<20)
	final := BeginResponseEncoding(ctx)
	points, response := encodingPoints()
	response.Trace = &commonv1.Trace{}
	response.GroupStatuses = map[string]modelv1.Status{"group": modelv1.Status(3)}
	response.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	require.NoError(t, RecordResponseEncoding(final, points[:1], 1000, true))
	require.NoError(t, RecordResponseEncoding(final, points[1:], 2000, true))
	FinishResponseEncoding(final, true)
	envelope := &measurev1.QueryResponse{Trace: response.Trace, GroupStatuses: response.GroupStatuses}
	envelope.ProtoReflect().SetUnknown(response.ProtoReflect().GetUnknown())
	expected := int64(2 * (3000 + proto.Size(envelope) + 11))
	release, err := ReserveResponseEncoding(ctx, response)
	require.NoError(t, err)
	require.Equal(t, expected, tracker.Used())
	require.GreaterOrEqual(t, tracker.Used(), int64(2*(proto.Size(response)+11)))
	release()
	require.Zero(t, tracker.Used())
	// A later response of the same shape cannot consume the old bound again.
	release, err = ReserveResponseEncoding(ctx, response)
	require.NoError(t, err)
	require.Equal(t, int64(2*(proto.Size(response)+11)), tracker.Used())
	release()
	require.Zero(t, tracker.Used())
}

func TestResponseEncodingCoverageRejectsUncoveredStaleAndIncomplete(t *testing.T) {
	for _, variant := range []string{"uncovered", "incomplete", "early-close", "identity", "row-count", "replaced"} {
		t.Run(variant, func(t *testing.T) {
			ctx, tracker := encodingScopeFixture(t, 1<<20)
			final := BeginResponseEncoding(ctx)
			points, response := encodingPoints()
			require.NoError(t, RecordResponseEncoding(final, points, 10000, variant != "uncovered"))
			if variant != "incomplete" {
				FinishResponseEncoding(final, true)
			}
			switch variant {
			case "early-close":
				FinishResponseEncoding(final, false)
			case "identity":
				response.DataPoints[1] = &measurev1.DataPoint{Version: 500}
			case "row-count":
				response.DataPoints = response.DataPoints[:1]
			case "replaced":
				fresh := BeginResponseEncoding(ctx)
				require.NoError(t, RecordResponseEncoding(final, points, 10000, true))
				FinishResponseEncoding(fresh, false)
			}
			release, err := ReserveResponseEncoding(ctx, response)
			require.NoError(t, err)
			require.Equal(t, int64(2*(proto.Size(response)+11)), tracker.Used())
			release()
			require.Zero(t, tracker.Used())
		})
	}
}

func TestResponseEncodingBoundBudgetCancelOverflowAndTransport(t *testing.T) {
	ctx, tracker := encodingScopeFixture(t, 100)
	points, response := encodingPoints()
	final := BeginResponseEncoding(ctx)
	require.NoError(t, RecordResponseEncoding(final, points, 1000, true))
	FinishResponseEncoding(final, true)
	release, err := ReserveResponseEncoding(ctx, response)
	require.NoError(t, err)
	require.Equal(t, int64(2*(proto.Size(response)+11)), tracker.Used())
	release()
	// Exact fallback also consumes the estimate.
	release, err = ReserveResponseEncoding(ctx, response)
	require.NoError(t, err)
	release()
	final = BeginResponseEncoding(ctx)
	require.NoError(t, RecordResponseEncoding(final, points, 1000, true))
	FinishResponseEncoding(final, true)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = ReserveResponseEncoding(canceled, response)
	require.ErrorIs(t, err, context.Canceled)
	release, err = ReserveResponseEncoding(ctx, response)
	require.NoError(t, err, "cancellation must consume stale coverage")
	release()
	final = BeginResponseEncoding(ctx)
	require.NoError(t, RecordResponseEncoding(final, points, math.MaxInt64, true))
	require.ErrorIs(t, RecordResponseEncoding(final, nil, 1, true), ErrMemoryBudgetExceeded)
	FinishResponseEncoding(final, true)
	_, err = ReserveResponseEncoding(ctx, response)
	require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
	require.Zero(t, tracker.Used())

	transportCtx, scope := query.NewTransportScope(ctx)
	defer scope.Close()
	final = BeginResponseEncoding(transportCtx)
	require.NoError(t, RecordResponseEncoding(final, points, 20, true))
	FinishResponseEncoding(final, true)
	release, err = ReserveResponseEncoding(transportCtx, response)
	require.NoError(t, err)
	release()
	require.Positive(t, tracker.Used())
	scope.Close()
	require.Zero(t, tracker.Used())
}

func TestResponseEncodingTypedNilAfterEmptySerialization(t *testing.T) {
	ctx, tracker := encodingScopeFixture(t, 100)
	final := BeginResponseEncoding(ctx)
	FinishResponseEncoding(final, true)
	var response *measurev1.QueryResponse
	release, err := ReserveResponseEncoding(ctx, response)
	require.NoError(t, err)
	outer := &bydbqlv1.QueryResponse{Result: &bydbqlv1.QueryResponse_MeasureResult{MeasureResult: response}}
	require.GreaterOrEqual(t, tracker.Used(), int64(proto.Size(outer)*2))
	require.Equal(t, int64(22), tracker.Used())
	release()
	require.Zero(t, tracker.Used())
}

func TestResponseEncodingInterleavedRunsFallBackWithoutFailing(t *testing.T) {
	for _, olderFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(olderFirst), func(t *testing.T) {
			ctx, tracker := encodingScopeFixture(t, 1<<20)
			oldFinal := BeginResponseEncoding(ctx)
			oldPoints, oldResponse := encodingPoints()
			require.NoError(t, RecordResponseEncoding(oldFinal, oldPoints, 10000, true))
			FinishResponseEncoding(oldFinal, true)
			newFinal := BeginResponseEncoding(ctx)
			newPoints, newResponse := encodingPoints()
			require.NoError(t, RecordResponseEncoding(newFinal, newPoints, 20000, true))
			require.NoError(t, RecordResponseEncoding(oldFinal, oldPoints, 10000, true), "superseded bookkeeping cannot fail execution")
			FinishResponseEncoding(oldFinal, false)
			FinishResponseEncoding(newFinal, true)
			first, second := newResponse, oldResponse
			firstExpected := int64(2 * (20000 + 11))
			if olderFirst {
				first, second = oldResponse, newResponse
				firstExpected = int64(2 * (proto.Size(first) + 11))
			}
			release, err := ReserveResponseEncoding(ctx, first)
			require.NoError(t, err)
			require.Equal(t, firstExpected, tracker.Used())
			release()
			release, err = ReserveResponseEncoding(ctx, second)
			require.NoError(t, err)
			require.Equal(t, int64(2*(proto.Size(second)+11)), tracker.Used())
			release()
			require.Zero(t, tracker.Used())
		})
	}
}

func TestResponseEncodingConcurrentDetachAndLateRecord(t *testing.T) {
	ctx, tracker := encodingScopeFixture(t, 1<<20)
	final := BeginResponseEncoding(ctx)
	points, response := encodingPoints()
	require.NoError(t, RecordResponseEncoding(final, points, 10000, true))
	FinishResponseEncoding(final, true)
	var workers sync.WaitGroup
	failures := make(chan error, 2000)
	workers.Go(func() {
		for range 1000 {
			if err := RecordResponseEncoding(final, points, 10000, true); err != nil {
				failures <- err
			}
		}
	})
	workers.Go(func() {
		for range 1000 {
			release, err := ReserveResponseEncoding(ctx, response)
			if err != nil {
				failures <- err
				continue
			}
			release()
		}
	})
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Zero(t, tracker.Used())
}

// encodingCountingLease records every attempted parent charge, including failures.
type encodingCountingLease struct {
	attempts []uint64
	trackerTestLease
}

func (l *encodingCountingLease) Charge(bytes uint64) error {
	l.attempts = append(l.attempts, bytes)
	return l.trackerTestLease.Charge(bytes)
}

func TestResponseEncodingExactFallbackAtRemainingBoundary(t *testing.T) {
	points, response := encodingPoints()
	exact := uint64(2 * (proto.Size(response) + 11))
	for _, parent := range []bool{false, true} {
		for _, deficit := range []uint64{0, 1} {
			t.Run(fmt.Sprintf("parent=%t/deficit=%d", parent, deficit), func(t *testing.T) {
				ctx := t.Context()
				lease := &encodingCountingLease{trackerTestLease: trackerTestLease{limit: 1000 + exact - deficit, used: 1000}}
				limit := int64(exact - deficit)
				if parent {
					ctx = query.WithBudgetLease(ctx, lease)
					limit = 1 << 20
				}
				ctx = WithQueryMemoryScope(ctx)
				processor := WithQueryMemoryTracker(ctx, limit)
				tracker := QueryMemoryTracker(processor, limit)
				final := BeginResponseEncoding(ctx)
				require.NoError(t, RecordResponseEncoding(final, points, 10000, true))
				FinishResponseEncoding(final, true)
				release, err := ReserveResponseEncoding(ctx, response)
				switch {
				case deficit == 0:
					require.NoError(t, err)
					require.Equal(t, int64(exact), tracker.Used())
					release()
				case parent:
					require.ErrorIs(t, err, errTrackerTestLeaseBudget)
				default:
					require.ErrorIs(t, err, ErrMemoryBudgetExceeded)
				}
				require.Zero(t, tracker.Used())
				if parent {
					require.Equal(t, []uint64{exact}, lease.attempts, "never attempt a speculative conservative parent charge")
					wantUsed := uint64(1000)
					if deficit == 0 {
						wantUsed += exact
					}
					require.Equal(t, wantUsed, lease.Used(), "release must not refund the monotonic parent")
				}
			})
		}
	}
}
