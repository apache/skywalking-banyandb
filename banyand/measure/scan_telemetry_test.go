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
	tags := make(map[string]string, len(span.Tags))
	for _, tag := range span.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
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
	fx := newFixture(t, part)
	q := prefetchQuery(1, 20, true, true) // narrow window over wide blocks: the cursors compact
	result, tracer, closeResult := tracedResult(t, fx, q, 96<<20)
	rows := drainRows(t, result)
	closeResult()
	require.Len(t, rows, 40*20)

	tags := scanSpanTags(t, tracer)
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagScanBlocks))
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagScanBlocksDecoded))
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagMergeCursors))
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagMergePeakLive))
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagCompactedCursors))
	require.Positive(t, tagInt(t, tags, tracelabels.TagDecodeNSTotal))
	require.GreaterOrEqual(t, tagInt(t, tags, tracelabels.TagDecodeNSMax), int64(1))
	require.EqualValues(t, 40, tagInt(t, tags, tracelabels.TagPrefetchHits)+tagInt(t, tags, tracelabels.TagPrefetchWaits))
	require.Positive(t, tagInt(t, tags, tracelabels.TagPlanNS))
	require.Positive(t, tagInt(t, tags, tracelabels.TagScanNS))
	require.EqualValues(t, 96<<20, tagInt(t, tags, tracelabels.TagBudgetLeaseLimit))
	require.EqualValues(t, 96<<20, tagInt(t, tags, tracelabels.TagBudgetSharedLimit))
	require.Positive(t, tagInt(t, tags, tracelabels.TagBudgetSharedPeak))
	estimate := tagInt(t, tags, tracelabels.TagBudgetEstimate)
	kept := tagInt(t, tags, tracelabels.TagBudgetEstimateKept)
	retained := tagInt(t, tags, tracelabels.TagBudgetRetained)
	require.Greater(t, estimate, kept, "the refund after decode lowers the reservation")
	require.Positive(t, retained, "a traced scan measures what its cursors retained")
	require.GreaterOrEqual(t, kept, retained, "the envelope covers what the cursors retained")
	require.NotContains(t, tags, tracelabels.TagBudgetRejects)
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
	require.Contains(t, tags[tracelabels.TagBudgetRejectReason], "memory budget exceeded")
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
