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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const (
	mean  = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN
	sum   = modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
	maxFn = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX
	minFn = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN
	count = modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT
	none  = modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED
	desc  = modelv1.Sort_SORT_DESC
	asc   = modelv1.Sort_SORT_ASC
)

type entityValue struct {
	entity string
	value  int64
}

// encodeTopNWindow encodes the stored TopN list of one window the way the TopN result measure holds it.
func encodeTopNWindow(t *testing.T, window []entityValue) []byte {
	t.Helper()
	values := make([]int64, 0, len(window))
	entities := make([][]byte, 0, len(window))
	for _, ev := range window {
		values = append(values, ev.value)
		encoded, err := pbv1.MarshalTagValues(nil, []*modelv1.TagValue{strTagValue(ev.entity)})
		require.NoError(t, err)
		entities = append(entities, encoded)
	}
	dst := encoding.EncodeBytes(nil, []byte("value"))
	dst = encoding.VarUint64ToBytes(dst, 1)
	dst = encoding.EncodeBytes(dst, []byte("entity"))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(values)))
	buf, encodeType, firstValue := encoding.Int64ListToBytes(nil, values)
	dst = append(dst, byte(encodeType))
	dst = encoding.VarInt64ToBytes(dst, firstValue)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(buf)))
	dst = append(dst, buf...)
	return encoding.EncodeBytesBlock(dst, entities)
}

// fakeTopNContext serves one stored TopN row per window, in time order. Like the storage merge, it keeps
// only the best Number entries of each window.
type fakeTopNContext struct {
	t        *testing.T
	err      error
	windows  [][]entityValue
	shards   []uint32 // the shard of every window, 0 when nil
	released int
}

func (f *fakeTopNContext) Query(_ context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	encoded := make([][]byte, 0, len(f.windows))
	for _, window := range f.windows {
		kept := append([]entityValue(nil), window...)
		sort.SliceStable(kept, func(i, j int) bool {
			if opts.Sort == modelv1.Sort_SORT_ASC {
				return kept[i].value < kept[j].value
			}
			return kept[i].value > kept[j].value
		})
		if opts.Number > 0 && len(kept) > int(opts.Number) {
			kept = kept[:opts.Number]
		}
		encoded = append(encoded, encodeTopNWindow(f.t, kept))
	}
	return &fakeTopNResult{windows: encoded, shards: f.shards, onRelease: func() { f.released++ }}, nil
}

type fakeTopNResult struct {
	onRelease func()
	windows   [][]byte
	shards    []uint32
	next      int
}

func (r *fakeTopNResult) Pull() *model.MeasureResult {
	if r.next >= len(r.windows) {
		return nil
	}
	idx := r.next
	r.next++
	var shardID common.ShardID
	if idx < len(r.shards) {
		shardID = common.ShardID(r.shards[idx])
	}
	return &model.MeasureResult{
		Timestamps: []int64{int64(idx+1) * 60_000_000_000},
		Versions:   []int64{1},
		ShardIDs:   []common.ShardID{shardID},
		Fields: []model.Field{{
			Name: "value",
			Values: []*modelv1.FieldValue{{
				Value: &modelv1.FieldValue_BinaryData{BinaryData: r.windows[idx]},
			}},
		}},
	}
}

func (r *fakeTopNResult) Release() { r.onRelease() }

func topNTestSchemas() (*databasev1.Measure, *databasev1.TopNAggregation) {
	source := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "source", Group: "g"},
		Entity:   &databasev1.Entity{TagNames: []string{"entity"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "entity", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	return source, &databasev1.TopNAggregation{FieldName: "value", GroupByTagNames: []string{"entity"}}
}

func executeTopNPlan(t *testing.T, ec executor.MeasureExecutionContext, agg modelv1.AggregationFunction, topN, partialLimit int32,
	sortDirection modelv1.Sort,
) []*measurev1.InternalDataPoint {
	t.Helper()
	source, rule := topNTestSchemas()
	request := &measurev1.TopNRequest{
		Name:           "rule",
		TopN:           topN,
		Agg:            agg,
		FieldValueSort: sortDirection,
		TimeRange:      &modelv1.TimeRange{Begin: timestamppb.New(time.Now().Add(-24 * time.Hour)), End: timestamppb.New(time.Now())},
	}
	plan, err := TopNAnalyze(request, partialLimit, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule},
		[]executor.MeasureExecutionContext{ec})
	require.NoError(t, err)
	it, err := plan.(executor.MeasureExecutable).Execute(context.Background())
	require.NoError(t, err)
	var points []*measurev1.InternalDataPoint
	for it.Next() {
		points = append(points, it.Current()...)
	}
	require.NoError(t, it.Close())
	return points
}

func runTopNPoints(t *testing.T, windows [][]entityValue, agg modelv1.AggregationFunction, topN int32,
	sortDirection modelv1.Sort,
) []*measurev1.DataPoint {
	t.Helper()
	var points []*measurev1.DataPoint
	for _, idp := range executeTopNPlan(t, &fakeTopNContext{t: t, windows: windows}, agg, topN, 0, sortDirection) {
		points = append(points, idp.GetDataPoint())
	}
	return points
}

func runTopN(t *testing.T, windows [][]entityValue, agg modelv1.AggregationFunction, topN int32, sortDirection modelv1.Sort) []entityValue {
	t.Helper()
	var got []entityValue
	for _, dp := range runTopNPoints(t, windows, agg, topN, sortDirection) {
		got = append(got, entityValue{
			entity: dp.GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue(),
			value:  dp.GetFields()[0].GetValue().GetInt().GetValue(),
		})
	}
	return got
}

func TestTopNAggregateOpAggregatesEveryWindow(t *testing.T) {
	// Two windows (T, T+1m) of the issue: e0=10,90; e1=60,60; e2=5,15.
	issue := [][]entityValue{
		{{"e1", 60}, {"e0", 10}, {"e2", 5}},
		{{"e0", 90}, {"e1", 60}, {"e2", 15}},
	}
	// Six minutes of the TopN integration dataset: svc_1=100,200,151; svc_2=300,50; svc_3=180.
	dataset := [][]entityValue{
		{{"svc_1", 100}},
		{{"svc_1", 200}},
		{{"svc_1", 151}},
		{{"svc_2", 300}},
		{{"svc_2", 50}},
		{{"svc_3", 180}},
	}
	tests := []struct {
		name    string
		windows [][]entityValue
		want    []entityValue
		topN    int32
		agg     modelv1.AggregationFunction
		sort    modelv1.Sort
	}{
		{"issue mean", issue, []entityValue{{"e1", 60}, {"e0", 50}, {"e2", 10}}, 10, mean, desc},
		{"issue sum", issue, []entityValue{{"e1", 120}, {"e0", 100}, {"e2", 20}}, 10, sum, desc},
		{"issue max", issue, []entityValue{{"e0", 90}, {"e1", 60}, {"e2", 15}}, 10, maxFn, desc},
		{"issue min", issue, []entityValue{{"e1", 60}, {"e0", 10}, {"e2", 5}}, 10, minFn, desc},
		{"issue count", issue, []entityValue{{"e0", 2}, {"e1", 2}, {"e2", 2}}, 10, count, desc},
		{"issue without aggregation", issue, []entityValue{{"e0", 90}, {"e1", 60}, {"e2", 15}}, 10, none, desc},
		// N is smaller than the stored window width: the storage must not trim the windows before they are aggregated.
		{"issue sum top 1", issue, []entityValue{{"e1", 120}}, 1, sum, desc},
		{"issue count top 2", issue, []entityValue{{"e0", 2}, {"e1", 2}}, 2, count, desc},
		{"issue mean top 1", issue, []entityValue{{"e1", 60}}, 1, mean, desc},
		{"issue min asc top 1", issue, []entityValue{{"e2", 5}}, 1, minFn, asc},
		{"dataset count", dataset, []entityValue{{"svc_1", 3}, {"svc_2", 2}, {"svc_3", 1}}, 3, count, desc},
		{"dataset sum", dataset, []entityValue{{"svc_1", 451}, {"svc_2", 350}, {"svc_3", 180}}, 3, sum, desc},
		{"dataset mean", dataset, []entityValue{{"svc_3", 180}, {"svc_2", 175}, {"svc_1", 150}}, 3, mean, desc},
		{"dataset mean top 2", dataset, []entityValue{{"svc_3", 180}, {"svc_2", 175}}, 2, mean, desc},
		{"dataset mean asc", dataset, []entityValue{{"svc_1", 150}, {"svc_2", 175}}, 2, mean, asc},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, runTopN(t, tt.windows, tt.agg, tt.topN, tt.sort))
		})
	}
}

// shardPartial is the partial aggregate of one item in a data node's answer.
type shardPartial struct {
	entity  string
	partial []int64
	shardID uint32
}

func runTopNPartials(t *testing.T, ec *fakeTopNContext, agg modelv1.AggregationFunction, partialLimit int32, sortDirection modelv1.Sort) []shardPartial {
	t.Helper()
	resp := ToInternalTopNResponse(executeTopNPlan(t, ec, agg, 1, partialLimit, sortDirection))
	var got []shardPartial
	for _, shard := range resp.GetShards() {
		for _, p := range shard.GetPartials() {
			values := make([]int64, 0, len(p.GetPartial()))
			for _, v := range p.GetPartial() {
				values = append(values, v.GetInt().GetValue())
			}
			got = append(got, shardPartial{shardID: shard.GetShardId(), entity: p.GetEntity()[0].GetValue().GetStr().GetValue(), partial: values})
		}
	}
	return got
}

func TestTopNAggregateOpAnswersPartialsPerShard(t *testing.T) {
	// The issue's windows on shard 3, and two windows of other items on shard 1.
	ec := func() *fakeTopNContext {
		return &fakeTopNContext{
			t: t,
			windows: [][]entityValue{
				{{"e1", 60}, {"e0", 10}, {"e2", 5}},
				{{"x", 7}, {"y", 1}},
				{{"e0", 90}, {"e1", 60}, {"e2", 15}},
				{{"x", 8}},
			},
			shards: []uint32{3, 1, 3, 1},
		}
	}
	tests := []struct {
		name  string
		want  []shardPartial
		limit int32
		agg   modelv1.AggregationFunction
		sort  modelv1.Sort
	}{
		{"mean is a sum and a count", []shardPartial{
			{"x", []int64{15, 2}, 1},
			{"y", []int64{1, 1}, 1},
			{"e1", []int64{120, 2}, 3},
			{"e0", []int64{100, 2}, 3},
			{"e2", []int64{20, 2}, 3},
		}, 10, mean, desc},
		{"sum", []shardPartial{{"x", []int64{15}, 1}, {"y", []int64{1}, 1}, {"e1", []int64{120}, 3}, {"e0", []int64{100}, 3}, {"e2", []int64{20}, 3}}, 10, sum, desc},
		{"count", []shardPartial{{"x", []int64{2}, 1}, {"y", []int64{1}, 1}, {"e0", []int64{2}, 3}, {"e1", []int64{2}, 3}, {"e2", []int64{2}, 3}}, 10, count, desc},
		{"max", []shardPartial{{"x", []int64{8}, 1}, {"y", []int64{1}, 1}, {"e0", []int64{90}, 3}, {"e1", []int64{60}, 3}, {"e2", []int64{15}, 3}}, 10, maxFn, desc},
		{"min", []shardPartial{{"y", []int64{1}, 1}, {"x", []int64{7}, 1}, {"e2", []int64{5}, 3}, {"e0", []int64{10}, 3}, {"e1", []int64{60}, 3}}, 10, minFn, asc},
		// The limit applies to every shard, ranked by the aggregate of the shard's windows: the mean for MEAN.
		{"limit per shard ranked by the mean", []shardPartial{{"x", []int64{15, 2}, 1}, {"e1", []int64{120, 2}, 3}}, 1, mean, desc},
		{"limit per shard", []shardPartial{{"x", []int64{15}, 1}, {"y", []int64{1}, 1}, {"e1", []int64{120}, 3}, {"e0", []int64{100}, 3}}, 2, sum, desc},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, runTopNPartials(t, ec(), tt.agg, tt.limit, tt.sort))
		})
	}
}

func TestTopNAggregateOpFinalAnswerMergesShards(t *testing.T) {
	// A standalone server holds every shard: an item is aggregated over all of them.
	ec := &fakeTopNContext{t: t, windows: [][]entityValue{{{"e0", 10}}, {{"e0", 90}, {"e1", 60}}}, shards: []uint32{0, 1}}
	var got []entityValue
	for _, idp := range executeTopNPlan(t, ec, modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, 10, 0, modelv1.Sort_SORT_DESC) {
		dp := idp.GetDataPoint()
		got = append(got, entityValue{dp.GetTagFamilies()[0].GetTags()[0].GetValue().GetStr().GetValue(), dp.GetFields()[0].GetValue().GetInt().GetValue()})
		assert.Nil(t, dp.GetTimestamp(), "a final answer has no per-item timestamp")
	}
	assert.Equal(t, []entityValue{{"e0", 100}, {"e1", 60}}, got)
}

func TestTopNAggregateOpReleasesTheScanOnError(t *testing.T) {
	source, rule := topNTestSchemas()
	// An unknown aggregation function fails while the first entity is registered, in the middle of the scan.
	request := &measurev1.TopNRequest{
		Name: "rule", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC,
		Agg: modelv1.AggregationFunction(99),
	}
	ec := &fakeTopNContext{t: t, windows: [][]entityValue{{{"e1", 60}}}}
	plan, err := TopNAnalyze(request, 0, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule}, []executor.MeasureExecutionContext{ec})
	require.NoError(t, err)
	_, err = plan.(executor.MeasureExecutable).Execute(context.Background())
	require.Error(t, err)
	assert.Equal(t, 1, ec.released, "the scan result must be released when the aggregation fails")
}

func TestTopNAggregateOpQueryError(t *testing.T) {
	source, rule := topNTestSchemas()
	request := &measurev1.TopNRequest{
		Name: "rule", TopN: 3, FieldValueSort: modelv1.Sort_SORT_DESC,
		Agg: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
	}
	plan, err := TopNAnalyze(request, 0, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule},
		[]executor.MeasureExecutionContext{&fakeTopNContext{t: t, err: errors.New("boom")}})
	require.NoError(t, err)
	_, err = plan.(executor.MeasureExecutable).Execute(context.Background())
	require.Error(t, err)
}

func TestTopNAggregationStateBudget(t *testing.T) {
	saved := topNAggregateStateBudget
	t.Cleanup(func() { topNAggregateStateBudget = saved })
	window := []entityValue{{"e1", 1}, {"e2", 1}, {"e3", 1}, {"e4", 1}, {"e5", 1}, {"e6", 1}, {"e7", 1}, {"e8", 1}}
	execute := func(ec *fakeTopNContext) error {
		source, rule := topNTestSchemas()
		request := &measurev1.TopNRequest{
			Name: "rule", TopN: 1, FieldValueSort: modelv1.Sort_SORT_DESC,
			Agg: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		}
		plan, err := TopNAnalyze(request, 0, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule}, []executor.MeasureExecutionContext{ec})
		require.NoError(t, err)
		it, err := plan.(executor.MeasureExecutable).Execute(context.Background())
		if err == nil {
			require.NoError(t, it.Close())
		}
		return err
	}

	topNAggregateStateBudget = 1 << 20
	within := &fakeTopNContext{t: t, windows: [][]entityValue{window}}
	require.NoError(t, execute(within))
	assert.Equal(t, 1, within.released)

	topNAggregateStateBudget = 512
	over := &fakeTopNContext{t: t, windows: [][]entityValue{window}}
	err := execute(over)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "per-item state")
	assert.Equal(t, 1, over.released, "the scan result must be released when the budget is exceeded")
}
