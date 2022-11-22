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

package query

import (
	"bytes"
	"container/heap"
	"context"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type topNQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (t *topNQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	if request.GetFieldValueSort() == modelv1.Sort_SORT_UNSPECIFIED {
		t.log.Warn().Msg("invalid requested sort direction")
		return
	}
	if e := t.log.Debug(); e.Enabled() {
		e.Stringer("req", request).Msg("received a topN query event")
	}
	topNMetadata := request.GetMetadata()
	topNSchema, err := t.metaService.TopNAggregationRegistry().GetTopNAggregation(context.TODO(), topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to get execution context")
		return
	}
	if topNSchema.GetFieldValueSort() != modelv1.Sort_SORT_UNSPECIFIED &&
		topNSchema.GetFieldValueSort() != request.GetFieldValueSort() {
		t.log.Warn().Msg("unmatched sort direction")
		return
	}
	sourceMeasure, err := t.measureService.Measure(topNSchema.GetSourceMeasure())
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to find source measure")
		return
	}
	shards, err := sourceMeasure.CompanionShards(topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to list shards")
		return
	}
	aggregator := createTopNPostAggregator(request.GetTopN(),
		request.GetAgg(), request.GetFieldValueSort())
	entity, err := locateEntity(topNSchema, request.GetFieldValueSort(), request.GetConditions())
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to parse entity")
		return
	}
	for _, shard := range shards {
		// TODO: support condition
		sl, innerErr := shard.Series().List(context.WithValue(
			context.Background(),
			logger.ContextKey,
			t.log,
		), tsdb.NewPath(entity))
		if innerErr != nil {
			t.log.Error().Err(innerErr).
				Str("topN", topNMetadata.GetName()).
				Msg("fail to list series")
			return
		}
		for _, series := range sl {
			iters, scanErr := t.scanSeries(series, request)
			if scanErr != nil {
				t.log.Error().Err(innerErr).
					Str("topN", topNMetadata.GetName()).
					Msg("fail to scan series")
				return
			}
			if len(iters) < 1 {
				continue
			}
			for _, iter := range iters {
				for iter.Next() {
					tuple, parseErr := parseTopNFamily(iter.Val(), sourceMeasure.GetInterval())
					if parseErr != nil {
						t.log.Error().Err(parseErr).
							Str("topN", topNMetadata.GetName()).
							Msg("fail to parse topN family")
						return
					}
					_ = aggregator.put(tuple.V1.(string), tuple.V2.(int64), iter.Val().Time())
				}
				_ = iter.Close()
			}
		}
	}

	now := time.Now().UnixNano()
	resp = bus.NewMessage(bus.MessageID(now), aggregator.val())

	return
}

func locateEntity(topNSchema *databasev1.TopNAggregation, sortDirection modelv1.Sort, conditions []*modelv1.Condition) (tsdb.Entity, error) {
	entityMap := make(map[string]int)
	entity := make([]tsdb.Entry, 1+1+len(topNSchema.GetGroupByTagNames()))
	// sortDirection
	entity[0] = convert.Int64ToBytes(int64(sortDirection.Number()))
	// rankNumber
	entity[1] = tsdb.AnyEntry
	for idx, tagName := range topNSchema.GetGroupByTagNames() {
		entityMap[tagName] = idx + 2
		// fill AnyEntry by default
		entity[idx+2] = tsdb.AnyEntry
	}
	for _, pairQuery := range conditions {
		if pairQuery.GetOp() != modelv1.Condition_BINARY_OP_EQ {
			return nil, errors.New("op other than EQ is not supported")
		}
		if entityIdx, ok := entityMap[pairQuery.GetName()]; ok {
			switch v := pairQuery.GetValue().GetValue().(type) {
			case *modelv1.TagValue_Str:
				entity[entityIdx] = []byte(v.Str.GetValue())
			case *modelv1.TagValue_Id:
				entity[entityIdx] = []byte(v.Id.GetValue())
			case *modelv1.TagValue_Int:
				entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
			default:
				return nil, errors.New("unsupported condition tag type for entity")
			}
			continue
		}
		return nil, errors.New("only groupBy tag name is supported")
	}
	return entity, nil
}

func parseTopNFamily(item tsdb.Item, interval time.Duration) (*streaming.Tuple2, error) {
	familyRawBytes, err := item.Family(familyIdentity(measure.TopNTagFamily, pbv1.TagFlag))
	if err != nil {
		return nil, err
	}
	tagFamily := &modelv1.TagFamilyForWrite{}
	err = proto.Unmarshal(familyRawBytes, tagFamily)
	if err != nil {
		return nil, err
	}
	fieldBytes, err := item.Family(familyIdentity(measure.TopNValueFieldSpec.GetName(),
		pbv1.EncoderFieldFlag(measure.TopNValueFieldSpec, interval)))
	if err != nil {
		return nil, err
	}
	fieldValue, err := pbv1.DecodeFieldValue(fieldBytes, measure.TopNValueFieldSpec)
	if err != nil {
		return nil, err
	}
	return &streaming.Tuple2{
		// GroupValues
		V1: tagFamily.GetTags()[1].GetStr().GetValue(),
		// FieldValue
		V2: fieldValue.GetInt().GetValue(),
	}, nil
}

func familyIdentity(name string, flag []byte) []byte {
	return bytes.Join([][]byte{tsdb.Hash([]byte(name)), flag}, nil)
}

func (t *topNQueryProcessor) scanSeries(series tsdb.Series, request *measurev1.TopNRequest) ([]tsdb.Iterator, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seriesSpan, err := series.Span(ctx, timestamp.NewInclusiveTimeRange(
		request.GetTimeRange().GetBegin().AsTime(),
		request.GetTimeRange().GetEnd().AsTime()),
	)
	if err != nil {
		if errors.Is(err, tsdb.ErrEmptySeriesSpan) {
			return nil, nil
		}
		return nil, err
	}
	defer func(seriesSpan tsdb.SeriesSpan) {
		if seriesSpan != nil {
			_ = seriesSpan.Close()
		}
	}(seriesSpan)
	seeker, err := seriesSpan.SeekerBuilder().OrderByTime(modelv1.Sort_SORT_ASC).Build()
	if err != nil {
		return nil, err
	}
	return seeker.Seek()
}

var _ heap.Interface = (*postAggregationProcessor)(nil)

type aggregatorItem struct {
	key       string
	int64Func aggregation.Int64Func
	index     int
}

// postProcessor defines necessary methods for Top-N post processor with or without aggregation
type postProcessor interface {
	put(key string, val int64, timestampMillis uint64) error
	val() []*measurev1.TopNList
}

func createTopNPostAggregator(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) postProcessor {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		// if aggregation is not specified, we have to keep all timelines
		return &postNonAggregationProcessor{
			topN:      topN,
			sort:      sort,
			timelines: make(map[uint64]*flow.DedupPriorityQueue),
		}
	}
	aggregator := &postAggregationProcessor{
		topN:     topN,
		sort:     sort,
		aggrFunc: aggrFunc,
		cache:    make(map[string]*aggregatorItem),
	}
	heap.Init(aggregator)
	return aggregator
}

// postAggregationProcessor is an implementation of postProcessor with aggregation
type postAggregationProcessor struct {
	topN            int32
	sort            modelv1.Sort
	aggrFunc        modelv1.AggregationFunction
	items           []*aggregatorItem
	cache           map[string]*aggregatorItem
	latestTimestamp uint64
}

func (aggr postAggregationProcessor) Len() int {
	return len(aggr.items)
}

// Less reports whether min/max heap has to be built.
// For DESC, a min heap has to be built,
// while for ASC, a max heap has to be built.
func (aggr postAggregationProcessor) Less(i, j int) bool {
	if aggr.sort == modelv1.Sort_SORT_DESC {
		return aggr.items[i].int64Func.Val() < aggr.items[j].int64Func.Val()
	}
	return aggr.items[i].int64Func.Val() > aggr.items[j].int64Func.Val()
}

func (aggr *postAggregationProcessor) Swap(i, j int) {
	aggr.items[i], aggr.items[j] = aggr.items[j], aggr.items[i]
	aggr.items[i].index = i
	aggr.items[j].index = j
}

func (aggr *postAggregationProcessor) Push(x any) {
	n := len(aggr.items)
	item := x.(*aggregatorItem)
	item.index = n
	aggr.items = append(aggr.items, item)
}

func (aggr *postAggregationProcessor) Pop() any {
	old := aggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	aggr.items = old[0 : n-1]
	return item
}

func (aggr *postAggregationProcessor) put(key string, val int64, timestampMillis uint64) error {
	// update latest ts
	if aggr.latestTimestamp < timestampMillis {
		aggr.latestTimestamp = timestampMillis
	}
	if item, found := aggr.cache[key]; found {
		item.int64Func.In(val)
		heap.Fix(aggr, item.index)
		return nil
	}

	aggrFunc, err := aggregation.NewInt64Func(aggr.aggrFunc)
	if err != nil {
		return err
	}
	item := &aggregatorItem{
		key:       key,
		int64Func: aggrFunc,
	}
	item.int64Func.In(val)

	if aggr.Len() < int(aggr.topN) {
		aggr.cache[key] = item
		heap.Push(aggr, item)
	} else {
		if lowest := aggr.items[0]; lowest != nil {
			if aggr.sort == modelv1.Sort_SORT_DESC && lowest.int64Func.Val() < val {
				aggr.cache[key] = item
				aggr.items[0] = item
				heap.Fix(aggr, 0)
			} else if aggr.sort != modelv1.Sort_SORT_DESC && lowest.int64Func.Val() > val {
				aggr.cache[key] = item
				aggr.items[0] = item
				heap.Fix(aggr, 0)
			}
		}
	}

	return nil
}

func (aggr *postAggregationProcessor) val() []*measurev1.TopNList {
	topNItems := make([]*measurev1.TopNList_Item, aggr.Len())

	for aggr.Len() > 0 {
		item := heap.Pop(aggr).(*aggregatorItem)
		topNItems[aggr.Len()] = &measurev1.TopNList_Item{
			Name: item.key,
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{Value: item.int64Func.Val()},
				},
			},
		}
	}
	return []*measurev1.TopNList{
		{
			Timestamp: timestamppb.New(time.Unix(0, int64(aggr.latestTimestamp))),
			Items:     topNItems,
		},
	}
}

var _ flow.Element = (*nonAggregatorItem)(nil)

type nonAggregatorItem struct {
	key   string
	val   int64
	index int
}

func (n *nonAggregatorItem) GetIndex() int {
	return n.index
}

func (n *nonAggregatorItem) SetIndex(i int) {
	n.index = i
}

type postNonAggregationProcessor struct {
	topN      int32
	sort      modelv1.Sort
	timelines map[uint64]*flow.DedupPriorityQueue
}

func (naggr *postNonAggregationProcessor) val() []*measurev1.TopNList {
	topNLists := make([]*measurev1.TopNList, 0, len(naggr.timelines))
	for ts, timeline := range naggr.timelines {
		items := make([]*measurev1.TopNList_Item, timeline.Len())
		for idx, elem := range timeline.Values() {
			items[idx] = &measurev1.TopNList_Item{
				Name: elem.(*nonAggregatorItem).key,
				Value: &modelv1.FieldValue{
					Value: &modelv1.FieldValue_Int{
						Int: &modelv1.Int{Value: elem.(*nonAggregatorItem).val},
					},
				},
			}
		}
		topNLists = append(topNLists, &measurev1.TopNList{
			Timestamp: timestamppb.New(time.Unix(0, int64(ts))),
			Items:     items,
		})
	}

	slices.SortStableFunc(topNLists, func(a, b *measurev1.TopNList) bool {
		if a.GetTimestamp().GetSeconds() < b.GetTimestamp().GetSeconds() {
			return true
		} else if a.GetTimestamp().GetSeconds() == b.GetTimestamp().GetSeconds() {
			return a.GetTimestamp().GetNanos() < b.GetTimestamp().GetNanos()
		}
		return false
	})

	return topNLists
}

func (naggr *postNonAggregationProcessor) put(key string, val int64, timestampMillis uint64) error {
	if timeline, ok := naggr.timelines[timestampMillis]; ok {
		if timeline.Len() < int(naggr.topN) {
			heap.Push(timeline, &nonAggregatorItem{val: val, key: key})
		} else {
			if lowest := timeline.Peek(); lowest != nil {
				if naggr.sort == modelv1.Sort_SORT_DESC && lowest.(*nonAggregatorItem).val < val {
					timeline.ReplaceLowest(&nonAggregatorItem{val: val, key: key})
				} else if naggr.sort != modelv1.Sort_SORT_DESC && lowest.(*nonAggregatorItem).val > val {
					timeline.ReplaceLowest(&nonAggregatorItem{val: val, key: key})
				}
			}
		}
		return nil
	}

	timeline := flow.NewPriorityQueue(func(a, b interface{}) int {
		if naggr.sort == modelv1.Sort_SORT_DESC {
			if a.(*nonAggregatorItem).val < b.(*nonAggregatorItem).val {
				return -1
			} else if a.(*nonAggregatorItem).val == b.(*nonAggregatorItem).val {
				return 0
			} else {
				return 1
			}
		}
		if a.(*nonAggregatorItem).val < b.(*nonAggregatorItem).val {
			return 1
		} else if a.(*nonAggregatorItem).val == b.(*nonAggregatorItem).val {
			return 0
		} else {
			return -1
		}
	}, false)
	naggr.timelines[timestampMillis] = timeline
	heap.Push(timeline, &nonAggregatorItem{val: val, key: key})

	return nil
}
