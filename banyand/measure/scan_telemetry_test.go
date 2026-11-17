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

package measure

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// tracedResult is liveResult with a tracer in the query context.
func tracedResult(t *testing.T, fx *fxFixture, q *fxQuery, limit uint64) (*queryResult, *query.Tracer, func()) {
	t.Helper()
	tracer, ctx := query.NewTracer(t.Context(), "scan-telemetry")
	lease := &liveScanLease{scanReuseLease{limit: limit}}
	ctx = vectorized.WithLiveLeaseAccounting(query.WithBudgetLease(ctx, lease))
	ctx = vectorized.WithQueryMemoryTracker(ctx, int64(limit))
	result := fx.newResult(t, q, q.schema(t))
	result.ctx = ctx
	result.budget = newScanBudget(1<<30, result.dp)
	require.NoError(t, result.budget.attachShared(ctx, len(result.batchSchema.Columns), 1024, len(result.data)))
	result.budget.rowBytes = projectedRowBytes(result.batchSchema) // as searchBlocks does
	return result, tracer, sync.OnceFunc(result.Release)
}

func scanSpanTags(t *testing.T, tracer *query.Tracer) map[string]string {
	t.Helper()
	span := findScanSpan(t, tracer)
	tags := make(map[string]string, len(span.Tags))
	for _, tag := range span.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

func findScanSpan(t *testing.T, tracer *query.Tracer) *commonv1.Span {
	t.Helper()
	var find func(spans []*commonv1.Span) *commonv1.Span
	find = func(spans []*commonv1.Span) *commonv1.Span {
		for _, sp := range spans {
			if sp.Message == scanSpanName {
				return sp
			}
			if found := find(sp.Children); found != nil {
				return found
			}
		}
		return nil
	}
	span := find(tracer.ToProto().Spans)
	require.NotNil(t, span, "a traced scan records a scan-storage span")
	return span
}

func tagInt(t *testing.T, tags map[string]string, key string) int64 {
	t.Helper()
	raw, ok := tags[key]
	require.Truef(t, ok, "tag %s missing from %v", key, tags)
	v, err := strconv.ParseInt(raw, 10, 64)
	require.NoError(t, err, key)
	return v
}

func TestTracedScanRecordsPoolAndBudgetTags(t *testing.T) {
	var part fxPart
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 240, version: 1})
	}
	// Series 41's block spans the window by its metadata but has no row in it.
	part = append(part, fxRange{sid: 41, from: 0, to: 0, version: 1}, fxRange{sid: 41, from: 30, to: 40, version: 1})
	fx := newFixture(t, part)
	q := prefetchQuery(1, 20, true, true) // narrow window over wide blocks: the cursors compact
	result, tracer, closeResult := tracedResult(t, fx, q, 96<<20)
	rows := drainRows(t, result)
	closeResult()
	require.Len(t, rows, 40*20)

	tags := scanSpanTags(t, tracer)
	st, tel := &result.scan.stats, result.budget.telemetry()
	lease, ok := query.BudgetLeaseFromContext(result.ctx)
	require.True(t, ok)
	peaker, ok := lease.(leasePeaker)
	require.True(t, ok, "the test lease tracks its peak, as the production lease does")
	tracker, ok := vectorized.QueryMemoryTrackerFromContext(result.ctx)
	require.True(t, ok)
	// Every tag the span carries, from the measurement it reports.
	want := map[string]int64{
		tracelabels.TagScanBlocks:         st.blocks,
		tracelabels.TagScanBlocksDecoded:  tel.decodes.Load() - tel.decodesEmpty.Load(),
		tracelabels.TagScanBlocksNoRows:   tel.decodesEmpty.Load(),
		tracelabels.TagScanWindows:        st.windows,
		tracelabels.TagMergeUnits:         st.mergeUnits,
		tracelabels.TagMergeCursors:       st.mergeCursors,
		tracelabels.TagMergePeakLive:      st.mergePeakLive,
		tracelabels.TagPrefetchPeak:       st.prefetchPeak,
		tracelabels.TagPrefetchHits:       tel.hits.Load(),
		tracelabels.TagPrefetchWaits:      tel.waits.Load(),
		tracelabels.TagDecodeWaitNS:       tel.waitNS.Load(),
		tracelabels.TagDecodeNSTotal:      tel.decodeNS.Load(),
		tracelabels.TagDecodeNSMax:        tel.decodeNSMax.Load(),
		tracelabels.TagDecodeQueueNS:      tel.queueNS.Load(),
		tracelabels.TagPlanNS:             tel.planNS.Load(),
		tracelabels.TagScanNS:             tel.scanNS.Load(),
		tracelabels.TagMergeNS:            max(tel.scanNS.Load()-tel.waitNS.Load(), 0),
		tracelabels.TagPoolWorkers:        int64(sharedFixturePool().workers),
		tracelabels.TagPoolQueued:         int64(tel.poolQueued),
		tracelabels.TagCompactedCursors:   tel.compacted.Load(),
		tracelabels.TagBudgetLeaseLimit:   int64(lease.Limit()),
		tracelabels.TagBudgetLeasePeak:    int64(peaker.Peak()),
		tracelabels.TagBudgetSharedLimit:  tracker.Limit(),
		tracelabels.TagBudgetSharedPeak:   tracker.Peak(),
		tracelabels.TagBudgetEstimate:     tel.estimate.Load(),
		tracelabels.TagBudgetEstimateKept: tel.estimateKept.Load(),
		tracelabels.TagBudgetRetained:     tel.retained.Load(),
		tracelabels.TagSliceNeed:          result.budget.sliceNeed,
		tracelabels.TagSliceTarget:        result.budget.sliceBudget,
		tracelabels.TagSlices:             st.slices,
		tracelabels.TagSlicedUnits:        st.slicedUnits,
	}
	got := make(map[string]int64, len(tags))
	for key := range tags {
		got[key] = tagInt(t, tags, key)
	}
	require.Equal(t, want, got)

	// What the scan of this fixture measures, and the invariants between the tags.
	require.EqualValues(t, 41, got[tracelabels.TagScanBlocks])
	require.EqualValues(t, 40, got[tracelabels.TagScanBlocksDecoded])
	require.EqualValues(t, 1, got[tracelabels.TagScanBlocksNoRows])
	require.Equal(t, got[tracelabels.TagScanBlocks], got[tracelabels.TagScanBlocksDecoded]+got[tracelabels.TagScanBlocksNoRows])
	require.EqualValues(t, 40, got[tracelabels.TagMergeCursors], "the cursor with no row in range is not merged")
	require.EqualValues(t, 40, got[tracelabels.TagMergePeakLive])
	require.EqualValues(t, 40, got[tracelabels.TagCompactedCursors])
	require.EqualValues(t, 41, got[tracelabels.TagPrefetchHits]+got[tracelabels.TagPrefetchWaits])
	require.Positive(t, got[tracelabels.TagScanWindows])
	require.Positive(t, got[tracelabels.TagMergeUnits])
	require.Positive(t, got[tracelabels.TagDecodeNSTotal])
	require.Positive(t, got[tracelabels.TagDecodeNSMax])
	require.LessOrEqual(t, got[tracelabels.TagDecodeNSMax], got[tracelabels.TagDecodeNSTotal])
	require.Positive(t, got[tracelabels.TagPlanNS])
	require.Positive(t, got[tracelabels.TagScanNS])
	require.LessOrEqual(t, got[tracelabels.TagDecodeWaitNS], got[tracelabels.TagScanNS])
	require.Equal(t, got[tracelabels.TagScanNS]-got[tracelabels.TagDecodeWaitNS], got[tracelabels.TagMergeNS])
	require.EqualValues(t, 96<<20, got[tracelabels.TagBudgetLeaseLimit])
	require.Positive(t, got[tracelabels.TagBudgetLeasePeak], "the scan charged the lease")
	require.LessOrEqual(t, got[tracelabels.TagBudgetLeasePeak], got[tracelabels.TagBudgetLeaseLimit])
	require.EqualValues(t, 96<<20, got[tracelabels.TagBudgetSharedLimit])
	require.Positive(t, got[tracelabels.TagBudgetSharedPeak])
	require.LessOrEqual(t, got[tracelabels.TagBudgetSharedPeak], got[tracelabels.TagBudgetSharedLimit])
	estimate := got[tracelabels.TagBudgetEstimate]
	kept := got[tracelabels.TagBudgetEstimateKept]
	retained := got[tracelabels.TagBudgetRetained]
	require.Greater(t, estimate, kept, "the refund after decode lowers the reservation")
	require.Positive(t, retained, "a traced scan measures what its cursors retained")
	require.GreaterOrEqual(t, kept, retained, "the envelope covers what the cursors retained")
	require.Zero(t, got[tracelabels.TagSlices], "a merge that fits is not sliced")
	require.Zero(t, got[tracelabels.TagSlicedUnits])
	require.NotContains(t, tags, tracelabels.TagBudgetRejects)
}

// A traced merge cut into time slices reports the slicing and the budgets that decided it.
// Closing a scan again, as its documentation allows, does not finish its span a second time.
func TestWindowScanCloseIsIdempotent(t *testing.T) {
	var part fxPart
	for sid := common.SeriesID(1); sid <= 4; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 240, version: 1})
	}
	fx := newFixture(t, part)
	q := prefetchQuery(1, 20, true, true)
	result, tracer, closeResult := tracedResult(t, fx, q, 96<<20)
	drainRows(t, result)
	scan := result.scan
	require.NotNil(t, scan)
	closeResult()
	scan.close()
	var n int
	for _, tag := range findScanSpan(t, tracer).Tags {
		if tag.Key == tracelabels.TagScanBlocks {
			n++
		}
	}
	require.Equal(t, 1, n, "the scan-storage span is tagged once")
}

func TestTracedSlicedScanRecordsSliceTags(t *testing.T) {
	var older, newer fxPart
	for sid := common.SeriesID(1); sid <= 40; sid++ {
		older = append(older, fxRange{sid: sid, from: 1, to: 240, version: 1})
		newer = append(newer, fxRange{sid: sid, from: 200, to: 300, version: 2})
	}
	fx := newFixture(t, older, newer)
	q := prefetchQuery(0, 400, true, true)
	result, tracer, closeResult := tracedResult(t, fx, q, 64<<20)
	// A budget that holds a few slices' worth of cursors, so the unit is cut.
	result.budget.sliceNeed, result.budget.sliceBudget = 2<<20, 2<<20
	drainRows(t, result)
	closeResult()

	tags := scanSpanTags(t, tracer)
	st := &result.scan.stats
	require.EqualValues(t, 2<<20, tagInt(t, tags, tracelabels.TagSliceNeed))
	require.EqualValues(t, 2<<20, tagInt(t, tags, tracelabels.TagSliceTarget))
	slices, units := tagInt(t, tags, tracelabels.TagSlices), tagInt(t, tags, tracelabels.TagSlicedUnits)
	require.Equal(t, st.slices, slices)
	require.Equal(t, st.slicedUnits, units)
	require.GreaterOrEqual(t, units, int64(1))
	require.Greater(t, slices, units, "each sliced unit is cut into two slices or more")
	// A block that spans several slices is decoded once per slice.
	decoded, noRows := tagInt(t, tags, tracelabels.TagScanBlocksDecoded), tagInt(t, tags, tracelabels.TagScanBlocksNoRows)
	require.Greater(t, decoded+noRows, tagInt(t, tags, tracelabels.TagScanBlocks))
}

func TestUntracedScanRecordsNoSpan(t *testing.T) {
	fx := manySeries(t, 8, 64)
	q := prefetchQuery(1, 64, true, true)
	result, closeResult := liveResult(t, fx, q, 96<<20)
	drainRows(t, result)
	require.Zero(t, result.budget.telemetry().retained.Load(), "the retained size is only measured for traced queries")
	require.Positive(t, result.budget.telemetry().decodes.Load(), "the cheap counters are always kept")
	closeResult()
}

func TestRejectedReservationIsReportedWithItsReason(t *testing.T) {
	var part fxPart
	for sid := common.SeriesID(1); sid <= 400; sid++ {
		part = append(part, fxRange{sid: sid, from: 1, to: 240, version: 1})
	}
	fx := newFixture(t, part)
	q := prefetchQuery(1, 240, true, true) // every series in range: nothing compacts
	result, tracer, closeResult := tracedResult(t, fx, q, 4<<20)
	_, err := drainRowsErr(result)
	require.Error(t, err)
	closeResult()

	tags := scanSpanTags(t, tracer)
	require.Contains(t, tags, tracelabels.TagBudgetRejects)
	require.Contains(t, tags[tracelabels.TagBudgetRejects], "shared_tracker=")
	require.Contains(t, tags[tracelabels.TagBudgetLastReject], "memory budget exceeded")
}

func TestScanTelemetryRejectClassifiesReasons(t *testing.T) {
	var tel scanTelemetry
	exhausted := fmt.Errorf("reserve: %w", &protector.ExhaustedError{Reason: protector.ReasonPerQueryLimit, Limit: 10, Requested: 20})
	require.Equal(t, protector.ReasonPerQueryLimit, tel.reject(exhausted, rejectOther), "an exhaustion error names its reason")
	shared := fmt.Errorf("decode: %w", vectorized.ErrMemoryBudgetExceeded)
	require.Equal(t, rejectSharedTracker, tel.reject(shared, rejectOther), "the shared tracker's error")
	require.Equal(t, rejectRawQuota, tel.reject(errors.New("block scan quota exceeded"), rejectRawQuota), "an error naming no reason")
	// A protector AcquireResource failure wraps the context's error and names no reason.
	acquire := fmt.Errorf("context canceled: memory acquisition failed: %w", context.DeadlineExceeded)
	require.Equal(t, rejectAcquire, tel.reject(acquire, rejectAcquire), "a refused admission")

	counts, last := tel.rejectSummary()
	require.Equal(t, "acquire=1,per_query_limit=1,raw_quota=1,shared_tracker=1", counts)
	require.Equal(t, acquire.Error(), last)
}
