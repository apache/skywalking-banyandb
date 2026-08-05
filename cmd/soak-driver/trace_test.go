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

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

const traceCatalogTestPath = "catalog/trace.json"

func loadTestCatalog(t *testing.T) map[string]traceCatalogEntry {
	t.Helper()
	entries, loadErr := loadTraceCatalog(traceCatalogTestPath)
	require.NoError(t, loadErr)
	byID := make(map[string]traceCatalogEntry, len(entries))
	for _, entry := range entries {
		byID[entry.ID] = entry
	}
	return byID
}

func TestLoadTraceCatalogParsesAllShapes(t *testing.T) {
	entries, loadErr := loadTraceCatalog(traceCatalogTestPath)
	require.NoError(t, loadErr)
	require.Len(t, entries, 5)
	wantIDs := []string{"by_id_single", "by_id_batch", "tag_newest", "tag_slowest", "tag_complex"}
	gotIDs := make([]string, 0, len(entries))
	for _, entry := range entries {
		gotIDs = append(gotIDs, entry.ID)
		assert.Equal(t, traceName, entry.Request.GetName(), "entry %s name", entry.ID)
		assert.Equal(t, []string{traceFixtureGroup}, entry.Request.GetGroups(), "entry %s groups", entry.ID)
		assert.NotEmpty(t, entry.Request.GetTagProjection(), "entry %s tag projection", entry.ID)
	}
	assert.ElementsMatch(t, wantIDs, gotIDs)
}

func TestTraceCatalogByIDSingleShape(t *testing.T) {
	entry := loadTestCatalog(t)["by_id_single"]
	cond := entry.Request.GetCriteria().GetCondition()
	require.NotNil(t, cond)
	assert.Equal(t, traceTagTraceID, cond.GetName())
	assert.Equal(t, modelv1.Condition_BINARY_OP_EQ, cond.GetOp())
	assert.Equal(t, "trace-0000000100", cond.GetValue().GetStr().GetValue())
	assert.Equal(t, uint32(1), entry.Request.GetLimit())
	assert.Nil(t, entry.Request.GetOrderBy())
	assert.False(t, traceCatalogEntryOrdered(entry))
}

func TestTraceCatalogByIDBatchShape(t *testing.T) {
	entry := loadTestCatalog(t)["by_id_batch"]
	cond := entry.Request.GetCriteria().GetCondition()
	require.NotNil(t, cond)
	assert.Equal(t, traceTagTraceID, cond.GetName())
	assert.Equal(t, modelv1.Condition_BINARY_OP_IN, cond.GetOp())
	ids := cond.GetValue().GetStrArray().GetValue()
	require.Len(t, ids, 5)
	assert.Contains(t, ids, "trace-0000000100")
	assert.GreaterOrEqual(t, int(entry.Request.GetLimit()), len(ids), "limit must cover the batch")
	assert.Nil(t, entry.Request.GetOrderBy())
}

func TestTraceCatalogTagNewestShape(t *testing.T) {
	entry := loadTestCatalog(t)["tag_newest"]
	cond := entry.Request.GetCriteria().GetCondition()
	require.NotNil(t, cond)
	assert.Equal(t, traceTagServiceID, cond.GetName())
	assert.Equal(t, modelv1.Condition_BINARY_OP_EQ, cond.GetOp())
	assert.Equal(t, "svc-0", cond.GetValue().GetStr().GetValue())
	order := entry.Request.GetOrderBy()
	require.NotNil(t, order)
	assert.Equal(t, traceIndexTimestamp, order.GetIndexRuleName())
	assert.Equal(t, modelv1.Sort_SORT_DESC, order.GetSort())
	assert.Equal(t, uint32(50), entry.Request.GetLimit())
	assert.True(t, traceCatalogEntryOrdered(entry))
}

func TestTraceCatalogTagSlowestShape(t *testing.T) {
	entry := loadTestCatalog(t)["tag_slowest"]
	le := entry.Request.GetCriteria().GetLe()
	require.NotNil(t, le)
	assert.Equal(t, modelv1.LogicalExpression_LOGICAL_OP_AND, le.GetOp())
	assert.Equal(t, traceTagServiceID, le.GetLeft().GetCondition().GetName())
	assert.Equal(t, "svc-0", le.GetLeft().GetCondition().GetValue().GetStr().GetValue())
	assert.Equal(t, traceTagState, le.GetRight().GetCondition().GetName())
	assert.Equal(t, int64(0), le.GetRight().GetCondition().GetValue().GetInt().GetValue())
	order := entry.Request.GetOrderBy()
	require.NotNil(t, order)
	assert.Equal(t, traceIndexDuration, order.GetIndexRuleName())
	assert.Equal(t, modelv1.Sort_SORT_DESC, order.GetSort())
	assert.Equal(t, uint32(50), entry.Request.GetLimit())
}

func TestTraceCatalogTagComplexShape(t *testing.T) {
	entry := loadTestCatalog(t)["tag_complex"]
	// state == 0 AND service_id == svc-0 AND duration in [lo, hi].
	criteria := entry.Request.GetCriteria()
	conditions := collectConditions(criteria)
	require.GreaterOrEqual(t, len(conditions), 4)
	names := map[string]int{}
	for _, cond := range conditions {
		names[cond.GetName()]++
	}
	assert.Equal(t, 1, names[traceTagState])
	assert.Equal(t, 1, names[traceTagServiceID])
	assert.Equal(t, 2, names[traceTagDuration], "duration must have a lo and hi bound")
	var lo, hi int64
	var sawGE, sawLE bool
	for _, cond := range conditions {
		if cond.GetName() != traceTagDuration {
			continue
		}
		switch cond.GetOp() {
		case modelv1.Condition_BINARY_OP_GE:
			lo = cond.GetValue().GetInt().GetValue()
			sawGE = true
		case modelv1.Condition_BINARY_OP_LE:
			hi = cond.GetValue().GetInt().GetValue()
			sawLE = true
		}
	}
	assert.True(t, sawGE && sawLE, "both duration bounds present")
	assert.Less(t, lo, hi, "duration range must be non-empty")
	order := entry.Request.GetOrderBy()
	require.NotNil(t, order)
	assert.Equal(t, traceIndexDuration, order.GetIndexRuleName())
	assert.Equal(t, modelv1.Sort_SORT_DESC, order.GetSort())
	assert.Equal(t, uint32(20), entry.Request.GetLimit())
}

// collectConditions flattens a criteria tree into its leaf conditions.
func collectConditions(criteria *modelv1.Criteria) []*modelv1.Condition {
	if criteria == nil {
		return nil
	}
	if cond := criteria.GetCondition(); cond != nil {
		return []*modelv1.Condition{cond}
	}
	le := criteria.GetLe()
	if le == nil {
		return nil
	}
	out := collectConditions(le.GetLeft())
	out = append(out, collectConditions(le.GetRight())...)
	return out
}

func TestBuildTraceQueryRequestInjectsTimeRangeKeepsLimit(t *testing.T) {
	entry := loadTestCatalog(t)["tag_complex"]
	untilMs := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	window := 7 * 24 * time.Hour
	req := buildTraceQueryRequest(entry, untilMs, window)
	require.NotNil(t, req.GetTimeRange())
	assert.Equal(t, untilMs, req.GetTimeRange().GetEnd().AsTime().UnixMilli())
	assert.Equal(t, time.UnixMilli(untilMs).Add(-window).UnixMilli(), req.GetTimeRange().GetBegin().AsTime().UnixMilli())
	// Catalog's pinned limit must be preserved, not overridden.
	assert.Equal(t, uint32(20), req.GetLimit())
	// The original catalog entry must be untouched (clone semantics).
	assert.Nil(t, entry.Request.GetTimeRange())
}

func TestTraceFixtureGenerationIsReproducible(t *testing.T) {
	// A fixed base keeps this in-memory reproducibility check deterministic;
	// the production base (traceFixtureBaseTime) is now wall-clock-anchored,
	// which is irrelevant here since no live cluster / TTL is involved.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	first := generateFixtureTraces(t, 20, 5, base)
	second := generateFixtureTraces(t, 20, 5, base)
	require.Len(t, first, len(second))
	for idx := range first {
		assert.True(t, proto.Equal(first[idx], second[idx]), "trace %d must be byte-identical across runs", idx)
	}
}

// generateFixtureTraces builds the in-memory trace set the seed path would
// write, without any cluster, so generation determinism can be asserted.
func generateFixtureTraces(t *testing.T, traces, spans int, base time.Time) []*tracev1.Trace {
	t.Helper()
	out := make([]*tracev1.Trace, 0, traces)
	for traceIdx := 0; traceIdx < traces; traceIdx++ {
		traceID := traceIDForIndex(traceIdx)
		serviceID := traceServiceIDForIndex(traceIdx, traces)
		trace := &tracev1.Trace{TraceId: traceID}
		for spanIdx := 0; spanIdx < spans; spanIdx++ {
			globalSpanIdx := traceIdx*spans + spanIdx
			version := uint64(globalSpanIdx) + 1
			tags := traceSpanTags(base, version, traceIdx, spanIdx, traceID, serviceID)
			modelTags := make([]*modelv1.Tag, 0, len(tags))
			for tagIdx, value := range tags {
				modelTags = append(modelTags, &modelv1.Tag{Key: traceSchemaTagName(tagIdx), Value: value})
			}
			trace.Spans = append(trace.Spans, &tracev1.Span{
				SpanId: traceID + "-span-" + padSpanIdx(spanIdx),
				Tags:   modelTags,
				Span:   traceSpanPayload(traceSpanBytes, traceID, spanIdx),
			})
		}
		out = append(out, trace)
	}
	return out
}

func traceSchemaTagName(idx int) string {
	names := []string{
		traceTagTraceID, traceTagState, traceTagServiceID, traceTagInstanceID,
		traceTagEndpointID, traceTagDuration, traceTagSpanID, traceTagTimestamp,
	}
	return names[idx]
}

func padSpanIdx(spanIdx int) string {
	return string([]byte{
		byte('0' + (spanIdx/1000)%10),
		byte('0' + (spanIdx/100)%10),
		byte('0' + (spanIdx/10)%10),
		byte('0' + spanIdx%10),
	})
}

func TestTraceServiceIDExactFraction(t *testing.T) {
	const traces = 200
	svcZero := 0
	for traceIdx := 0; traceIdx < traces; traceIdx++ {
		if traceServiceIDForIndex(traceIdx, traces) == "svc-0" {
			svcZero++
		}
	}
	assert.Equal(t, int(float64(traces)*traceSvcZeroFraction), svcZero)
}

func TestCompareTraceResultsEqualMatch(t *testing.T) {
	baseline := []*tracev1.Trace{makeTrace("trace-a", 2), makeTrace("trace-b", 3)}
	replay := []*tracev1.Trace{makeTrace("trace-a", 2), makeTrace("trace-b", 3)}
	_, matched := compareTraceResults("eq", baseline, replay, false)
	assert.True(t, matched)
}

func TestCompareTraceResultsReorderedSpansMatch(t *testing.T) {
	baseline := []*tracev1.Trace{makeTrace("trace-a", 4)}
	replay := []*tracev1.Trace{makeTrace("trace-a", 4)}
	// Reverse the replay span order — unordered span comparison must still match.
	spans := replay[0].Spans
	for left, right := 0, len(spans)-1; left < right; left, right = left+1, right-1 {
		spans[left], spans[right] = spans[right], spans[left]
	}
	_, matched := compareTraceResults("reorder", baseline, replay, false)
	assert.True(t, matched)
}

func TestCompareTraceResultsChangedTagNoMatch(t *testing.T) {
	baseline := []*tracev1.Trace{makeTrace("trace-a", 2)}
	replay := []*tracev1.Trace{makeTrace("trace-a", 2)}
	replay[0].Spans[0].Tags[0].Value = traceStrTagValue("mutated")
	div, matched := compareTraceResults("changed", baseline, replay, false)
	assert.False(t, matched)
	assert.NotEmpty(t, div.FirstDiffs)
}

func TestCompareTraceResultsOrderedVsUnordered(t *testing.T) {
	baseline := []*tracev1.Trace{makeTrace("trace-a", 1), makeTrace("trace-b", 1)}
	replay := []*tracev1.Trace{makeTrace("trace-b", 1), makeTrace("trace-a", 1)}
	// Unordered: trace order is normalized away, so it matches.
	_, unorderedMatch := compareTraceResults("set", baseline, replay, false)
	assert.True(t, unorderedMatch)
	// Ordered: position matters, so the swapped order diverges.
	_, orderedMatch := compareTraceResults("seq", baseline, replay, true)
	assert.False(t, orderedMatch)
}

func TestCompareTraceResultsLengthMismatch(t *testing.T) {
	baseline := []*tracev1.Trace{makeTrace("trace-a", 1)}
	replay := []*tracev1.Trace{makeTrace("trace-a", 1), makeTrace("trace-b", 1)}
	div, matched := compareTraceResults("len", baseline, replay, false)
	assert.False(t, matched)
	assert.Equal(t, 1, div.BaselineLen)
	assert.Equal(t, 2, div.ReplayLen)
}

func makeTrace(traceID string, spans int) *tracev1.Trace {
	trace := &tracev1.Trace{TraceId: traceID}
	for spanIdx := 0; spanIdx < spans; spanIdx++ {
		trace.Spans = append(trace.Spans, &tracev1.Span{
			SpanId: traceID + "-span-" + padSpanIdx(spanIdx),
			Tags:   []*modelv1.Tag{{Key: traceTagServiceID, Value: traceStrTagValue("svc-0")}},
			Span:   []byte(traceID),
		})
	}
	return trace
}

func TestDecodeBaselineTracesRoundTrip(t *testing.T) {
	original := makeTrace("trace-rt", 3)
	rawTrace, marshalErr := protojson.Marshal(original)
	require.NoError(t, marshalErr)
	decoded, decodeErr := decodeBaselineTraces([]json.RawMessage{json.RawMessage(rawTrace)}, "rt")
	require.NoError(t, decodeErr)
	require.Len(t, decoded, 1)
	assert.True(t, proto.Equal(original, decoded[0]))
}

func TestTraceReplayAndDiffEmptyBaselineFailsOnDivergence(t *testing.T) {
	// Mirror record-baseline's empty-result guard: an empty baseline record
	// is unusable. compareTraceResults against a non-empty replay must diverge.
	baseline := []*tracev1.Trace(nil)
	replay := []*tracev1.Trace{makeTrace("trace-a", 1)}
	_, matched := compareTraceResults("empty", baseline, replay, false)
	assert.False(t, matched, "empty baseline vs non-empty replay must diverge")
}

func TestTraceBaselineRecordEmptyDetection(t *testing.T) {
	// A baseline record with zero traces must be flagged as empty so the
	// record-baseline path can exit non-zero. This asserts the serialized
	// shape round-trips and the empty count is observable.
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	records := []traceBaselineRecord{
		{QueryName: "full", Traces: []json.RawMessage{json.RawMessage(`{"traceId":"x"}`)}},
		{QueryName: "empty", Traces: nil},
	}
	data, marshalErr := json.Marshal(records)
	require.NoError(t, marshalErr)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	raw, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	var decoded []traceBaselineRecord
	require.NoError(t, json.Unmarshal(raw, &decoded))
	empty := 0
	for _, rec := range decoded {
		if len(rec.Traces) == 0 {
			empty++
		}
	}
	assert.Equal(t, 1, empty)
}

func TestValidateEngine(t *testing.T) {
	assert.NoError(t, validateEngine(engineMeasure))
	assert.NoError(t, validateEngine(engineTrace))
	assert.Error(t, validateEngine("bogus"))
}
