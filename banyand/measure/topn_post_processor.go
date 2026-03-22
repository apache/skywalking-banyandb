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
	"container/heap"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
)

// FieldValueToInt converts a FieldValue to int64.
func FieldValueToInt(fv *modelv1.FieldValue) int64 {
	return fv.GetInt().GetValue()
}

// FieldValueToFloat converts a FieldValue to float64.
func FieldValueToFloat(fv *modelv1.FieldValue) float64 {
	return fv.GetFloat().GetValue()
}

// IntToFieldValue converts an int64 to a FieldValue.
func IntToFieldValue(v int64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Int{
			Int: &modelv1.Int{Value: v},
		},
	}
}

// FloatToFieldValue converts a float64 to a FieldValue.
func FloatToFieldValue(v float64) *modelv1.FieldValue {
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Float{
			Float: &modelv1.Float{Value: v},
		},
	}
}

// PostProcessor defines necessary methods for Top-N post processor with or without aggregation.
type PostProcessor[N aggregation.Number] interface {
	Put(entityValues pbv1.EntityValues, val N, timestampMillis uint64, version int64)
	Flush() ([]*topNAggregatorItem[N], error)
	Val([]string) ([]*measurev1.TopNList, error)
	Reset()
}

// CreateTopNPostProcessorInt creates a Top-N post processor for int64 values.
func CreateTopNPostProcessorInt(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor[int64] {
	return createTopNPostProcessor[int64](topN, aggrFunc, sort)
}

// CreateTopNPostProcessorFloat creates a Top-N post processor for float64 values.
func CreateTopNPostProcessorFloat(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor[float64] {
	return createTopNPostProcessor[float64](topN, aggrFunc, sort)
}

func createTopNPostProcessor[N aggregation.Number](topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor[N] {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		// if aggregation is not specified, we have to keep all timelines
		return &topNPostProcessor[N]{
			topN:      topN,
			sort:      sort,
			timelines: make(map[uint64]*topNTimelineItem[N]),
		}
	}
	aggregator := &topNPostProcessor[N]{
		topN:      topN,
		sort:      sort,
		aggrFunc:  aggrFunc,
		cache:     make(map[string]*topNAggregatorItem[N]),
		timelines: make(map[uint64]*topNTimelineItem[N]),
		items:     make([]*topNAggregatorItem[N], 0, topN),
	}
	heap.Init(aggregator)
	return aggregator
}

func (taggr *topNPostProcessor[N]) Len() int {
	return len(taggr.items)
}

// Less reports whether min/max heap has to be built.
// For DESC, a min heap has to be built,
// while for ASC, a max heap has to be built.
func (taggr *topNPostProcessor[N]) Less(i, j int) bool {
	if taggr.sort == modelv1.Sort_SORT_DESC {
		return taggr.items[i].mapFunc.Val() < taggr.items[j].mapFunc.Val()
	}
	return taggr.items[i].mapFunc.Val() > taggr.items[j].mapFunc.Val()
}

func (taggr *topNPostProcessor[N]) Swap(i, j int) {
	taggr.items[i], taggr.items[j] = taggr.items[j], taggr.items[i]
	taggr.items[i].index = i
	taggr.items[j].index = j
}

func (taggr *topNPostProcessor[N]) Push(x any) {
	n := len(taggr.items)
	item := x.(*topNAggregatorItem[N])
	item.index = n
	taggr.items = append(taggr.items, item)
}

func (taggr *topNPostProcessor[N]) Pop() any {
	old := taggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	taggr.items = old[0 : n-1]
	return item
}

func (taggr *topNPostProcessor[N]) tryEnqueue(key string, item *topNAggregatorItem[N]) {
	if len(taggr.items) == 0 {
		return
	}
	if lowest := taggr.items[0]; lowest != nil {
		var shouldReplace bool
		if taggr.sort == modelv1.Sort_SORT_DESC {
			shouldReplace = lowest.mapFunc.Val() < item.mapFunc.Val()
		} else {
			shouldReplace = lowest.mapFunc.Val() > item.mapFunc.Val()
		}

		if shouldReplace {
			delete(taggr.cache, lowest.key)
			taggr.cache[key] = item
			taggr.items[0] = item
			item.index = 0
			heap.Fix(taggr, 0)
		}
	}
}

var (
	_ flow.Element = (*topNAggregatorItem[int64])(nil)
	_ flow.Element = (*topNAggregatorItem[float64])(nil)
)

type topNAggregatorItem[N aggregation.Number] struct {
	mapFunc aggregation.Map[N]
	val     N
	key     string
	values  pbv1.EntityValues
	version int64
	index   int
}

func (n *topNAggregatorItem[N]) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}

func (n *topNAggregatorItem[N]) GetIndex() int {
	return n.index
}

func (n *topNAggregatorItem[N]) SetIndex(i int) {
	n.index = i
}

type topNTimelineItem[N aggregation.Number] struct {
	queue *flow.DedupPriorityQueue
	items map[string]*topNAggregatorItem[N]
}

type topNPostProcessor[N aggregation.Number] struct {
	cache     map[string]*topNAggregatorItem[N]
	timelines map[uint64]*topNTimelineItem[N]
	items     []*topNAggregatorItem[N]
	sort      modelv1.Sort
	aggrFunc  modelv1.AggregationFunction
	topN      int32
}

func (taggr *topNPostProcessor[N]) Put(entityValues pbv1.EntityValues, val N, timestampMillis uint64, version int64) {
	timeline, ok := taggr.timelines[timestampMillis]
	key := entityValues.String()
	if !ok {
		timeline = &topNTimelineItem[N]{
			queue: flow.NewPriorityQueue(func(a, b interface{}) int {
				aVal := a.(*topNAggregatorItem[N]).val
				bVal := b.(*topNAggregatorItem[N]).val
				if taggr.sort == modelv1.Sort_SORT_DESC {
					if aVal < bVal {
						return -1
					} else if aVal > bVal {
						return 1
					}
					return 0
				}
				if aVal < bVal {
					return 1
				} else if aVal > bVal {
					return -1
				}
				return 0
			}, false),
			items: make(map[string]*topNAggregatorItem[N]),
		}

		newItem := &topNAggregatorItem[N]{
			val:     val,
			key:     key,
			values:  entityValues,
			version: version,
		}

		timeline.items[key] = newItem
		heap.Push(timeline.queue, newItem)
		taggr.timelines[timestampMillis] = timeline
		return
	}

	if item, exist := timeline.items[key]; exist {
		if version >= item.version {
			item.val = val
			item.version = version
			heap.Fix(timeline.queue, item.index)
		}

		return
	}

	newItem := &topNAggregatorItem[N]{
		val:     val,
		key:     key,
		values:  entityValues,
		version: version,
	}

	if taggr.topN <= 0 {
		heap.Push(timeline.queue, newItem)
		timeline.items[key] = newItem
		return
	}

	if timeline.queue.Len() < int(taggr.topN) {
		heap.Push(timeline.queue, newItem)
		timeline.items[key] = newItem
		return
	}

	if lowest := timeline.queue.Peek(); lowest != nil {
		lowestItem := lowest.(*topNAggregatorItem[N])

		var shouldReplace bool
		if taggr.sort == modelv1.Sort_SORT_DESC {
			shouldReplace = lowestItem.val < val
		} else {
			shouldReplace = lowestItem.val > val
		}

		if shouldReplace {
			delete(timeline.items, lowestItem.key)
			timeline.items[key] = newItem
			timeline.queue.ReplaceLowest(newItem)
		}
	}
}

func (taggr *topNPostProcessor[N]) Flush() ([]*topNAggregatorItem[N], error) {
	var result []*topNAggregatorItem[N]

	if taggr.aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		for _, timeline := range taggr.timelines {
			for timeline.queue.Len() > 0 {
				item := heap.Pop(timeline.queue).(*topNAggregatorItem[N])
				result = append(result, item)
			}
		}
	} else {
		for _, timeline := range taggr.timelines {
			for _, item := range timeline.items {
				if exist, found := taggr.cache[item.key]; found {
					exist.mapFunc.In(item.val)
					continue
				}

				aggregator, err := aggregation.NewMap[N](taggr.aggrFunc)
				if err != nil {
					return nil, err
				}
				item.mapFunc = aggregator
				item.mapFunc.In(item.val)
				taggr.cache[item.key] = item
			}
		}
		for _, item := range taggr.cache {
			if taggr.Len() < int(taggr.topN) {
				heap.Push(taggr, item)
			} else {
				taggr.tryEnqueue(item.key, item)
			}
		}
		result = make([]*topNAggregatorItem[N], 0, taggr.Len())
		for taggr.Len() > 0 {
			item := heap.Pop(taggr).(*topNAggregatorItem[N])
			result = append(result, item)
		}
	}
	taggr.Reset()

	return result, nil
}

func (taggr *topNPostProcessor[N]) Val(tagNames []string) ([]*measurev1.TopNList, error) {
	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		return taggr.valWithAggregation(tagNames)
	}

	return taggr.valWithoutAggregation(tagNames), nil
}

func (taggr *topNPostProcessor[N]) valWithAggregation(tagNames []string) ([]*measurev1.TopNList, error) {
	topNAggregatorItems, err := taggr.Flush()
	if err != nil {
		return nil, err
	}
	length := len(topNAggregatorItems)
	items := make([]*measurev1.TopNList_Item, length)

	for i, item := range topNAggregatorItems {
		targetIdx := length - 1 - i
		var fieldValue *modelv1.FieldValue
		var n N
		switch any(n).(type) {
		case float64:
			fieldValue = FloatToFieldValue(float64(item.mapFunc.Val()))
		default:
			fieldValue = IntToFieldValue(int64(item.mapFunc.Val()))
		}
		items[targetIdx] = &measurev1.TopNList_Item{
			Entity: item.GetTags(tagNames),
			Value:  fieldValue,
		}
	}
	return []*measurev1.TopNList{
		{
			Timestamp: timestamppb.Now(),
			Items:     items,
		},
	}, nil
}

func (taggr *topNPostProcessor[N]) valWithoutAggregation(tagNames []string) []*measurev1.TopNList {
	topNLists := make([]*measurev1.TopNList, 0, len(taggr.timelines))
	for ts, timeline := range taggr.timelines {
		items := make([]*measurev1.TopNList_Item, timeline.queue.Len())
		for idx, elem := range timeline.queue.Values() {
			elemItem := elem.(*topNAggregatorItem[N])
			var fieldValue *modelv1.FieldValue
			var n N
			switch any(n).(type) {
			case float64:
				fieldValue = FloatToFieldValue(float64(elemItem.val))
			default:
				fieldValue = IntToFieldValue(int64(elemItem.val))
			}
			items[idx] = &measurev1.TopNList_Item{
				Entity: elemItem.GetTags(tagNames),
				Value:  fieldValue,
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

func (taggr *topNPostProcessor[N]) Reset() {
	clear(taggr.timelines)

	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		clear(taggr.cache)

		taggr.items = taggr.items[:0]
	}
}
