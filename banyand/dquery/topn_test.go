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

package dquery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

const (
	sumAgg   = modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM
	countAgg = modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT
	meanAgg  = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN
	maxAgg   = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX
	minAgg   = modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN
	desc     = modelv1.Sort_SORT_DESC
	asc      = modelv1.Sort_SORT_ASC
)

type fakeTopNFuture struct {
	err  error
	resp *measurev1.InternalTopNResponse
}

func (f fakeTopNFuture) Get() (bus.Message, error) {
	if f.err != nil {
		return bus.Message{}, f.err
	}
	return bus.NewMessage(1, f.resp), nil
}

func (f fakeTopNFuture) GetAll() ([]bus.Message, error) {
	m, err := f.Get()
	return []bus.Message{m}, err
}

func intValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: v}}}
}

func floatValue(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: v}}}
}

func entityTags(entity string) []*modelv1.Tag {
	return []*modelv1.Tag{{Key: "service_id", Value: pbv1.EntityStrValue(entity)}}
}

// partial is an item's partial aggregate: one value, or the sum and the count for MEAN.
func partial(entity string, values ...int64) *measurev1.InternalTopNPartial {
	p := &measurev1.InternalTopNPartial{Entity: entityTags(entity)}
	for _, v := range values {
		p.Partial = append(p.Partial, intValue(v))
	}
	return p
}

func shard(id uint32, partials ...*measurev1.InternalTopNPartial) *measurev1.InternalTopNShard {
	return &measurev1.InternalTopNShard{ShardId: id, Partials: partials}
}

// node is the answer of one data node.
func node(shards ...*measurev1.InternalTopNShard) bus.Future {
	return fakeTopNFuture{resp: &measurev1.InternalTopNResponse{Shards: shards}}
}

type rankedItem struct {
	entity string
	value  int64
}

// ranked returns the items of the only list in the order the liaison answers them.
func ranked(t *testing.T, lists []*measurev1.TopNList) []rankedItem {
	t.Helper()
	require.Len(t, lists, 1)
	got := make([]rankedItem, 0, len(lists[0].Items))
	for _, item := range lists[0].Items {
		got = append(got, rankedItem{item.Entity[0].Value.GetStr().GetValue(), item.Value.GetInt().GetValue()})
	}
	return got
}

func merge(t *testing.T, topN int32, sortDirection modelv1.Sort, agg modelv1.AggregationFunction, answers ...[]bus.Future) []rankedItem {
	t.Helper()
	lists, _, err := mergeTopNPartials[int64](answers, topN, sortDirection, agg)
	require.NoError(t, err)
	return ranked(t, lists)
}

func TestMergeTopNPartialsCountsReplicasOnce(t *testing.T) {
	// Nodes A and B hold replicas of shard 1; node C holds shard 2. Replicas count once whatever their values:
	// a lagging replica of shard 1 that misses a window is not added to the other.
	nodeA := node(shard(1, partial("e1", 120), partial("e0", 100)))
	nodeB := node(shard(1, partial("e1", 60), partial("e0", 10)), shard(2, partial("e3", 70)))
	nodeC := node(shard(2, partial("e3", 70), partial("e2", 20)))
	assert.Equal(t, []rankedItem{{"e1", 120}, {"e0", 100}, {"e3", 70}},
		merge(t, 3, desc, sumAgg, []bus.Future{nodeA, nodeB, nodeC}))
}

func TestMergeTopNPartialsCombinesShardsOfDifferentTargets(t *testing.T) {
	// Shard 0 of the hot stage and shard 0 of the warm stage hold different windows of the same group: they are
	// different shards and both count.
	hot := []bus.Future{node(shard(0, partial("e0", 90), partial("e1", 60)))}
	warm := []bus.Future{node(shard(0, partial("e1", 60), partial("e0", 10)))}
	assert.Equal(t, []rankedItem{{"e1", 120}, {"e0", 100}}, merge(t, 10, desc, sumAgg, hot, warm))
	assert.Equal(t, []rankedItem{{"e0", 2}, {"e1", 2}}, merge(t, 10, desc, countAgg, []bus.Future{
		node(shard(0, partial("e0", 1), partial("e1", 1))),
	}, []bus.Future{
		node(shard(0, partial("e1", 1), partial("e0", 1))),
	}))
	assert.Equal(t, []rankedItem{{"e0", 90}, {"e1", 60}}, merge(t, 10, desc, maxAgg, hot, warm))
	assert.Equal(t, []rankedItem{{"e0", 10}, {"e1", 60}}, merge(t, 10, asc, minAgg, hot, warm))
}

func TestMergeTopNPartialsMeanFromSumsAndCounts(t *testing.T) {
	// Node A holds the integer values [1,2] of e0, node B holds [3]: the mean is 6/3=2. A node's mean (1) times
	// its count would lose the remainder and give 5/3=1.
	assert.Equal(t, []rankedItem{{"e0", 2}}, merge(t, 10, desc, meanAgg,
		[]bus.Future{node(shard(0, partial("e0", 3, 2)))},
		[]bus.Future{node(shard(0, partial("e0", 3, 1)))}))

	lists, _, err := mergeTopNPartials[float64]([][]bus.Future{
		{fakeTopNFuture{resp: &measurev1.InternalTopNResponse{Shards: []*measurev1.InternalTopNShard{{ShardId: 0, Partials: []*measurev1.InternalTopNPartial{
			{Entity: entityTags("e0"), Partial: []*modelv1.FieldValue{floatValue(1.5), floatValue(2)}},
		}}}}}},
		{fakeTopNFuture{resp: &measurev1.InternalTopNResponse{Shards: []*measurev1.InternalTopNShard{{ShardId: 0, Partials: []*measurev1.InternalTopNPartial{
			{Entity: entityTags("e0"), Partial: []*modelv1.FieldValue{floatValue(3), floatValue(1)}},
		}}}}}},
	}, 10, desc, meanAgg)
	require.NoError(t, err)
	require.Len(t, lists, 1)
	assert.InDelta(t, 1.5, lists[0].Items[0].Value.GetFloat().GetValue(), 1e-9)
}

func TestMergeTopNPartialsOrdersAndBreaksTiesAtTheCutoff(t *testing.T) {
	nodeA := node(shard(1, partial("e1", 5)), shard(3, partial("e3", 50)))
	nodeB := node(shard(2, partial("e2", 5), partial("e0", 1)))
	for _, answers := range [][]bus.Future{{nodeA, nodeB}, {nodeB, nodeA}} {
		assert.Equal(t, []rankedItem{{"e3", 50}, {"e1", 5}}, merge(t, 2, desc, sumAgg, answers))
		assert.Equal(t, []rankedItem{{"e0", 1}, {"e1", 5}}, merge(t, 2, asc, sumAgg, answers))
	}
}

func TestMergeTopNPartialsEmptyAndErrors(t *testing.T) {
	lists, count, err := mergeTopNPartials[int64]([][]bus.Future{{node(), node()}}, 3, desc, sumAgg)
	require.NoError(t, err)
	assert.Nil(t, lists)
	assert.Equal(t, 2, count)

	_, _, err = mergeTopNPartials[int64]([][]bus.Future{{node(shard(0, partial("e0", 1))), fakeTopNFuture{err: errors.New("node down")}}}, 3, desc, sumAgg)
	require.Error(t, err)
}

func TestTopNTargets(t *testing.T) {
	assert.Equal(t, []topNTarget{{group: "g"}}, topNTargets([]string{"g"}, nil))
	targets := topNTargets([]string{"g", "h"}, map[string][]string{"g": {"type=hot", "type=warm"}})
	assert.Equal(t, []topNTarget{{group: "g", selector: "type=hot"}, {group: "g", selector: "type=warm"}, {group: "h"}}, targets)
	assert.Equal(t, map[string][]string{"g": {"type=warm"}}, targets[1].nodeSelectors())
	assert.Nil(t, targets[2].nodeSelectors())
	r := targets[1].request(&measurev1.TopNRequest{Groups: []string{"g", "h"}, Name: "n", TopN: 3})
	assert.Equal(t, []string{"g"}, r.GetGroups())
	assert.Equal(t, "n", r.GetName())
}

func TestTopNPartialLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := schema.NewMockTopNAggregation(ctrl)
	registry.EXPECT().GetTopNAggregation(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, md *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
			return &databasev1.TopNAggregation{Metadata: md, CountersNumber: map[string]int32{"g": 1000, "h": 5}[md.GetGroup()]}, nil
		}).AnyTimes()
	repo := metadata.NewMockRepo(ctrl)
	repo.EXPECT().TopNAggregationRegistry().Return(registry).AnyTimes()
	p := &topNQueryProcessor{queryService: &queryService{metaService: repo}}

	limit, err := p.partialLimit(context.Background(), &measurev1.TopNRequest{Groups: []string{"g"}, Name: "n", TopN: 10}, 1)
	require.NoError(t, err)
	assert.EqualValues(t, 10, limit, "a query of one stage of one group returns the local top N")
	limit, err = p.partialLimit(context.Background(), &measurev1.TopNRequest{Groups: []string{"g"}, Name: "n", TopN: 10}, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 1000, limit, "a query spanning stages returns the counters number of the TopN")
	limit, err = p.partialLimit(context.Background(), &measurev1.TopNRequest{Groups: []string{"h"}, Name: "n", TopN: 10}, 2)
	require.NoError(t, err)
	assert.EqualValues(t, 10, limit, "never less than N")
}

// windowValue is one item's value in a stored TopN window.
type windowValue struct {
	entity string
	value  int64
}

// storedWindow is one stored TopN row: its minute and its shard.
type storedWindow struct {
	values  []windowValue
	minute  int64
	shardID uint32
}

func encodeTopNWindow(t *testing.T, window []windowValue) []byte {
	t.Helper()
	values := make([]int64, 0, len(window))
	entities := make([][]byte, 0, len(window))
	for _, wv := range window {
		values = append(values, wv.value)
		encoded, err := pbv1.MarshalTagValues(nil, []*modelv1.TagValue{pbv1.EntityStrValue(wv.entity)})
		require.NoError(t, err)
		entities = append(entities, encoded)
	}
	dst := encoding.EncodeBytes(nil, []byte("value"))
	dst = encoding.VarUint64ToBytes(dst, 1)
	dst = encoding.EncodeBytes(dst, []byte("service_id"))
	dst = encoding.VarUint64ToBytes(dst, uint64(len(values)))
	buf, encodeType, firstValue := encoding.Int64ListToBytes(nil, values)
	dst = append(dst, byte(encodeType))
	dst = encoding.VarInt64ToBytes(dst, firstValue)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(buf)))
	dst = append(dst, buf...)
	return encoding.EncodeBytesBlock(dst, entities)
}

// dataNodeStore serves the stored TopN rows of a data node.
type dataNodeStore struct {
	t       *testing.T
	windows []storedWindow
}

func (s *dataNodeStore) Query(context.Context, model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	return &dataNodeResult{t: s.t, windows: s.windows}, nil
}

type dataNodeResult struct {
	t       *testing.T
	windows []storedWindow
	next    int
}

func (r *dataNodeResult) Pull() *model.MeasureResult {
	if r.next >= len(r.windows) {
		return nil
	}
	w := r.windows[r.next]
	r.next++
	return &model.MeasureResult{
		Timestamps: []int64{w.minute * int64(time.Minute)},
		Versions:   []int64{1},
		ShardIDs:   []common.ShardID{common.ShardID(w.shardID)},
		Fields: []model.Field{{Name: "value", Values: []*modelv1.FieldValue{{
			Value: &modelv1.FieldValue_BinaryData{BinaryData: encodeTopNWindow(r.t, w.values)},
		}}}},
	}
}

func (r *dataNodeResult) Release() {}

// answerAsDataNode runs the data node's TopN plan for an internal request over the node's stored rows.
func answerAsDataNode(t *testing.T, windows []storedWindow, agg modelv1.AggregationFunction, sortDirection modelv1.Sort, partialLimit int32) bus.Future {
	t.Helper()
	source := &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: "service_cpm", Group: "g"},
		Entity:   &databasev1.Entity{TagNames: []string{"service_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Fields: []*databasev1.FieldSpec{{Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
	}
	rule := &databasev1.TopNAggregation{FieldName: "value", CountersNumber: 1000}
	request := &measurev1.TopNRequest{
		Groups: []string{"g"}, Name: "topn", TopN: 1, Agg: agg, FieldValueSort: sortDirection,
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(3600, 0))},
	}
	plan, err := logical_measure.TopNAnalyze(request, partialLimit, []*databasev1.Measure{source}, []*databasev1.TopNAggregation{rule},
		[]executor.MeasureExecutionContext{&dataNodeStore{t: t, windows: windows}})
	require.NoError(t, err)
	it, err := plan.(executor.MeasureExecutable).Execute(context.Background())
	require.NoError(t, err)
	var result []*measurev1.InternalDataPoint
	for it.Next() {
		result = append(result, it.Current()...)
	}
	require.NoError(t, it.Close())
	return fakeTopNFuture{resp: logical_measure.ToInternalTopNResponse(result)}
}

// TestDistributedTopNAggregatesOneStage reproduces the issue on the TopN integration dataset: six one-minute
// windows, svc_1=100,200,151; svc_2=300,50; svc_3=180. The group of svc_1 is stored on shard 0 of node A and the
// group of svc_2 and svc_3 on shard 1 of node B, which node C replicates. Every node answers its local top N.
func TestDistributedTopNAggregatesOneStage(t *testing.T) {
	nodeA := []storedWindow{
		{minute: 1, shardID: 0, values: []windowValue{{"svc_1", 100}}},
		{minute: 2, shardID: 0, values: []windowValue{{"svc_1", 200}}},
		{minute: 3, shardID: 0, values: []windowValue{{"svc_1", 151}}},
	}
	nodeB := []storedWindow{
		{minute: 4, shardID: 1, values: []windowValue{{"svc_2", 300}}},
		{minute: 5, shardID: 1, values: []windowValue{{"svc_2", 50}}},
		{minute: 6, shardID: 1, values: []windowValue{{"svc_3", 180}}},
	}
	tests := []struct {
		name string
		want []rankedItem
		agg  modelv1.AggregationFunction
		sort modelv1.Sort
	}{
		{"count", []rankedItem{{"svc_1", 3}, {"svc_2", 2}, {"svc_3", 1}}, countAgg, desc},
		{"sum", []rankedItem{{"svc_1", 451}, {"svc_2", 350}, {"svc_3", 180}}, sumAgg, desc},
		{"mean", []rankedItem{{"svc_3", 180}, {"svc_2", 175}, {"svc_1", 150}}, meanAgg, desc},
		{"max", []rankedItem{{"svc_2", 300}, {"svc_1", 200}, {"svc_3", 180}}, maxAgg, desc},
		{"min", []rankedItem{{"svc_2", 50}, {"svc_1", 100}, {"svc_3", 180}}, minAgg, asc},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const topN = 3
			answers := []bus.Future{
				answerAsDataNode(t, nodeA, tt.agg, tt.sort, topN),
				answerAsDataNode(t, nodeB, tt.agg, tt.sort, topN),
				answerAsDataNode(t, nodeB, tt.agg, tt.sort, topN), // node C, a replica of shard 1
			}
			assert.Equal(t, tt.want, merge(t, topN, tt.sort, tt.agg, answers))
		})
	}
}

// TestDistributedTopNAggregatesAcrossStages splits one group's windows between the hot and the warm stage, on
// shard 0 of each. Item a ranks second on both nodes but first overall.
func TestDistributedTopNAggregatesAcrossStages(t *testing.T) {
	hot := []storedWindow{
		{minute: 3, shardID: 0, values: []windowValue{{"b", 60}, {"a", 50}}},
	}
	warm := []storedWindow{
		{minute: 1, shardID: 0, values: []windowValue{{"c", 60}, {"a", 30}}},
		{minute: 2, shardID: 0, values: []windowValue{{"a", 20}}},
	}
	query := func(partialLimit int32) []rankedItem {
		return merge(t, 1, desc, sumAgg,
			[]bus.Future{answerAsDataNode(t, hot, sumAgg, desc, partialLimit)},
			[]bus.Future{answerAsDataNode(t, warm, sumAgg, desc, partialLimit)})
	}
	// With K at least the group's item count every partial reaches the liaison and the result is exact.
	assert.Equal(t, []rankedItem{{"a", 100}}, query(3))
	// The local top N alone misses it: this is why a query spanning stages asks for K items.
	assert.Equal(t, []rankedItem{{"b", 60}}, query(1))

	mean := merge(t, 3, desc, meanAgg,
		[]bus.Future{answerAsDataNode(t, hot, meanAgg, desc, 3)},
		[]bus.Future{answerAsDataNode(t, warm, meanAgg, desc, 3)})
	assert.Equal(t, []rankedItem{{"b", 60}, {"c", 60}, {"a", 33}}, mean, "the mean of a is 100/3 over its three windows")
}
