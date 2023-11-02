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
	"container/heap"
	"context"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
)

type topNQueryProcessor struct {
	measureService measure.Service
	*queryService
}

func (t *topNQueryProcessor) Rev(message bus.Message) (resp bus.Message) {
	request, ok := message.Data().(*measurev1.TopNRequest)
	now := time.Now().UnixNano()
	if !ok {
		t.log.Warn().Msg("invalid event data type")
		return
	}
	ml := t.log.Named("topn", request.Metadata.Group, request.Metadata.Name)
	if e := ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(request)).Msg("received a topn event")
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

	schema, err := t.metaService.MeasureRegistry().GetMeasure(context.TODO(), topNMetadata)
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to find topN measure")
		return
	}

	sourceMeasure.SetSchema(schema)
	s, err := logical_measure.BuildTopNSchema(schema, topNSchema.GetGroupByTagNames())
	if err != nil {
		t.log.Error().Err(err).
			Str("topN", topNMetadata.GetName()).
			Msg("fail to build schema")
	}
	plan, err := logical_measure.TopNAnalyze(context.TODO(), request, schema, topNSchema, s)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for topn %s: %v", topNMetadata.GetName(), err))
		return
	}

	if e := ml.Debug(); e.Enabled() {
		e.Str("plan", plan.String()).Msg("topn plan")
	}

	mIterator, err := plan.(executor.MeasureExecutable).Execute(executor.WithMeasureExecutionContext(context.Background(), sourceMeasure))
	if err != nil {
		ml.Error().Err(err).RawJSON("req", logger.Proto(request)).Msg("fail to close the topn plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to execute the topn plan for measure %s: %v", topNMetadata.GetName(), err))
	}
	defer func() {
		if err = mIterator.Close(); err != nil {
			ml.Error().Err(err).RawJSON("req", logger.Proto(request))
		}
	}()

	result := make([]*measurev1.DataPoint, 0)
	for mIterator.Next() {
		current := mIterator.Current()
		if len(current) > 0 {
			result = append(result, current[0])
		}
	}

	resp = bus.NewMessage(bus.MessageID(now), toTopNResponse(result))
	return
}

func toTopNResponse(dps []*measurev1.DataPoint) *measurev1.TopNResponse {
	topNList := make([]*measurev1.TopNList, 0)
	topNItems := make([]*measurev1.TopNList_Item, len(dps))
	for i, dp := range dps {
		topNItems[i] = &measurev1.TopNList_Item{
			Entity: dp.GetTagFamilies()[0].GetTags(),
			Value:  dp.GetFields()[0].GetValue(),
		}
	}
	topNList = append(topNList, &measurev1.TopNList{
		Items: topNItems,
	})
	return &measurev1.TopNResponse{Lists: topNList}
}

var _ heap.Interface = (*postAggregationProcessor)(nil)

type aggregatorItem struct {
	int64Func aggregation.Func[int64]
	key       string
	values    tsdb.EntityValues
	index     int
}

func (n *aggregatorItem) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}

// PostProcessor defines necessary methods for Top-N post processor with or without aggregation.
type PostProcessor interface {
	Put(entityValues tsdb.EntityValues, val int64, timestampMillis uint64) error
	Val([]string) []*measurev1.TopNList
}

// CreateTopNPostAggregator creates a Top-N post processor with or without aggregation.
func CreateTopNPostAggregator(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor {
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
		items:    make([]*aggregatorItem, 0, topN),
	}
	heap.Init(aggregator)
	return aggregator
}

// postAggregationProcessor is an implementation of postProcessor with aggregation.
type postAggregationProcessor struct {
	cache           map[string]*aggregatorItem
	items           []*aggregatorItem
	latestTimestamp uint64
	topN            int32
	sort            modelv1.Sort
	aggrFunc        modelv1.AggregationFunction
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

func (aggr *postAggregationProcessor) Put(entityValues tsdb.EntityValues, val int64, timestampMillis uint64) error {
	// update latest ts
	if aggr.latestTimestamp < timestampMillis {
		aggr.latestTimestamp = timestampMillis
	}
	key := entityValues.String()
	if item, found := aggr.cache[key]; found {
		item.int64Func.In(val)
		aggr.tryEnqueue(key, item)
		return nil
	}

	aggrFunc, err := aggregation.NewFunc[int64](aggr.aggrFunc)
	if err != nil {
		return err
	}
	item := &aggregatorItem{
		key:       key,
		int64Func: aggrFunc,
		values:    entityValues,
	}
	item.int64Func.In(val)

	if aggr.Len() < int(aggr.topN) {
		aggr.cache[key] = item
		heap.Push(aggr, item)
	} else {
		aggr.tryEnqueue(key, item)
	}

	return nil
}

func (aggr *postAggregationProcessor) tryEnqueue(key string, item *aggregatorItem) {
	if lowest := aggr.items[0]; lowest != nil {
		if aggr.sort == modelv1.Sort_SORT_DESC && lowest.int64Func.Val() < item.int64Func.Val() {
			aggr.cache[key] = item
			aggr.items[0] = item
			heap.Fix(aggr, 0)
		} else if aggr.sort != modelv1.Sort_SORT_DESC && lowest.int64Func.Val() > item.int64Func.Val() {
			aggr.cache[key] = item
			aggr.items[0] = item
			heap.Fix(aggr, 0)
		}
	}
}

func (aggr *postAggregationProcessor) Val(tagNames []string) []*measurev1.TopNList {
	topNItems := make([]*measurev1.TopNList_Item, aggr.Len())

	for aggr.Len() > 0 {
		item := heap.Pop(aggr).(*aggregatorItem)
		topNItems[aggr.Len()] = &measurev1.TopNList_Item{
			Entity: item.GetTags(tagNames),
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
	key    string
	values tsdb.EntityValues
	val    int64
	index  int
}

func (n *nonAggregatorItem) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}

func (n *nonAggregatorItem) GetIndex() int {
	return n.index
}

func (n *nonAggregatorItem) SetIndex(i int) {
	n.index = i
}

type postNonAggregationProcessor struct {
	timelines map[uint64]*flow.DedupPriorityQueue
	topN      int32
	sort      modelv1.Sort
}

func (naggr *postNonAggregationProcessor) Val(tagNames []string) []*measurev1.TopNList {
	topNLists := make([]*measurev1.TopNList, 0, len(naggr.timelines))
	for ts, timeline := range naggr.timelines {
		items := make([]*measurev1.TopNList_Item, timeline.Len())
		for idx, elem := range timeline.Values() {
			items[idx] = &measurev1.TopNList_Item{
				Entity: elem.(*nonAggregatorItem).GetTags(tagNames),
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

	slices.SortStableFunc(topNLists, func(a, b *measurev1.TopNList) int {
		r := int(a.GetTimestamp().GetSeconds() - b.GetTimestamp().GetSeconds())
		if r != 0 {
			return r
		}
		return int(a.GetTimestamp().GetNanos() - b.GetTimestamp().GetNanos())
	})

	return topNLists
}

func (naggr *postNonAggregationProcessor) Put(entityValues tsdb.EntityValues, val int64, timestampMillis uint64) error {
	key := entityValues.String()
	if timeline, ok := naggr.timelines[timestampMillis]; ok {
		if timeline.Len() < int(naggr.topN) {
			heap.Push(timeline, &nonAggregatorItem{val: val, key: key, values: entityValues})
		} else {
			if lowest := timeline.Peek(); lowest != nil {
				if naggr.sort == modelv1.Sort_SORT_DESC && lowest.(*nonAggregatorItem).val < val {
					timeline.ReplaceLowest(&nonAggregatorItem{val: val, key: key, values: entityValues})
				} else if naggr.sort != modelv1.Sort_SORT_DESC && lowest.(*nonAggregatorItem).val > val {
					timeline.ReplaceLowest(&nonAggregatorItem{val: val, key: key, values: entityValues})
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
			}
			return 1
		}
		if a.(*nonAggregatorItem).val < b.(*nonAggregatorItem).val {
			return 1
		} else if a.(*nonAggregatorItem).val == b.(*nonAggregatorItem).val {
			return 0
		}
		return -1
	}, false)
	naggr.timelines[timestampMillis] = timeline
	heap.Push(timeline, &nonAggregatorItem{val: val, key: key, values: entityValues})

	return nil
}
