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

// PostProcessor defines necessary methods for Top-N post processor with or without aggregation.
type PostProcessor[K TopSortKey] interface {
	Put(entityValues pbv1.EntityValues, val K, timestampMillis uint64, version int64)
	Flush() ([]*topNAggregatorItem[K], error)
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

func createTopNPostProcessor[K TopSortKey](topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor[K] {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		// if aggregation is not specified, we have to keep all timelines
		return &topNPostProcessor[K]{
			topN:      topN,
			sort:      sort,
			timelines: make(map[uint64]*topNTimelineItem[K]),
		}
	}
	aggregator := &topNPostProcessor[K]{
		topN:      topN,
		sort:      sort,
		aggrFunc:  aggrFunc,
		cache:     make(map[string]*topNAggregatorItem[K]),
		timelines: make(map[uint64]*topNTimelineItem[K]),
		items:     make([]*topNAggregatorItem[K], 0, topN),
	}
	heap.Init(aggregator)
	return aggregator
}

func (taggr *topNPostProcessor[K]) Len() int {
	return len(taggr.items)
}

// Less reports whether min/max heap has to be built.
// For DESC, a min heap has to be built,
// while for ASC, a max heap has to be built.
func (taggr *topNPostProcessor[K]) Less(i, j int) bool {
	if taggr.sort == modelv1.Sort_SORT_DESC {
		return taggr.items[i].mapFunc.Val() < taggr.items[j].mapFunc.Val()
	}
	return taggr.items[i].mapFunc.Val() > taggr.items[j].mapFunc.Val()
}

func (taggr *topNPostProcessor[K]) Swap(i, j int) {
	taggr.items[i], taggr.items[j] = taggr.items[j], taggr.items[i]
	taggr.items[i].index = i
	taggr.items[j].index = j
}

func (taggr *topNPostProcessor[K]) Push(x any) {
	n := len(taggr.items)
	item := x.(*topNAggregatorItem[K])
	item.index = n
	taggr.items = append(taggr.items, item)
}

func (taggr *topNPostProcessor[K]) Pop() any {
	old := taggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	taggr.items = old[0 : n-1]
	return item
}

func (taggr *topNPostProcessor[K]) tryEnqueue(key string, item *topNAggregatorItem[K]) {
	if len(taggr.items) == 0 {
		return
	}
	if lowest := taggr.items[0]; lowest != nil {
		shouldReplace := (taggr.sort == modelv1.Sort_SORT_DESC && lowest.mapFunc.Val() < item.mapFunc.Val()) ||
			(taggr.sort != modelv1.Sort_SORT_DESC && lowest.mapFunc.Val() > item.mapFunc.Val())

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

type topNAggregatorItem[K TopSortKey] struct {
	mapFunc aggregation.Map[K]
	val     K
	key     string
	values  pbv1.EntityValues
	version int64
	index   int
}

func (n *topNAggregatorItem[K]) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}

func (n *topNAggregatorItem[K]) GetIndex() int {
	return n.index
}

func (n *topNAggregatorItem[K]) SetIndex(i int) {
	n.index = i
}

type topNTimelineItem[K TopSortKey] struct {
	queue *flow.DedupPriorityQueue
	items map[string]*topNAggregatorItem[K]
}

type topNPostProcessor[K TopSortKey] struct {
	cache     map[string]*topNAggregatorItem[K]
	timelines map[uint64]*topNTimelineItem[K]
	items     []*topNAggregatorItem[K]
	sort      modelv1.Sort
	aggrFunc  modelv1.AggregationFunction
	topN      int32
}

func (taggr *topNPostProcessor[K]) Put(entityValues pbv1.EntityValues, val K, timestampMillis uint64, version int64) {
	timeline, ok := taggr.timelines[timestampMillis]
	key := entityValues.String()
	if !ok {
		timeline = &topNTimelineItem[K]{
			queue: flow.NewPriorityQueue(func(a, b interface{}) int {
				aVal := a.(*topNAggregatorItem[K]).val
				bVal := b.(*topNAggregatorItem[K]).val
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
			items: make(map[string]*topNAggregatorItem[K]),
		}

		newItem := &topNAggregatorItem[K]{
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

	newItem := &topNAggregatorItem[K]{
		val:     val,
		key:     key,
		values:  entityValues,
		version: version,
	}

	if timeline.queue.Len() < int(taggr.topN) {
		heap.Push(timeline.queue, newItem)
		timeline.items[key] = newItem
		return
	}

	if lowest := timeline.queue.Peek(); lowest != nil {
		lowestItem := lowest.(*topNAggregatorItem[K])

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

func (taggr *topNPostProcessor[K]) Flush() ([]*topNAggregatorItem[K], error) {
	var result []*topNAggregatorItem[K]

	if taggr.aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		for _, timeline := range taggr.timelines {
			for timeline.queue.Len() > 0 {
				item := heap.Pop(timeline.queue).(*topNAggregatorItem[K])
				result = append(result, item)
			}
		}
	} else {
		for _, timeline := range taggr.timelines {
			for _, item := range timeline.items {
				if exist, found := taggr.cache[item.key]; found {
					exist.mapFunc.In(item.val)
					heap.Fix(taggr, exist.index)
					continue
				}

				mapFunc, err := aggregation.NewMap[K](taggr.aggrFunc)
				if err != nil {
					return nil, err
				}

				item.mapFunc = mapFunc
				item.mapFunc.In(item.val)

				if taggr.Len() < int(taggr.topN) {
					taggr.cache[item.key] = item
					heap.Push(taggr, item)
				} else {
					taggr.tryEnqueue(item.key, item)
				}
			}
		}
		result = make([]*topNAggregatorItem[K], 0, taggr.Len())
		for taggr.Len() > 0 {
			item := heap.Pop(taggr).(*topNAggregatorItem[K])
			result = append(result, item)
		}
	}
	taggr.Reset()

	return result, nil
}

func (taggr *topNPostProcessor[K]) Val(tagNames []string) ([]*measurev1.TopNList, error) {
	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		return taggr.valWithAggregation(tagNames)
	}

	return taggr.valWithoutAggregation(tagNames), nil
}

func (taggr *topNPostProcessor[K]) valWithAggregation(tagNames []string) ([]*measurev1.TopNList, error) {
	topNAggregatorItems, err := taggr.Flush()
	if err != nil {
		return nil, err
	}
	length := len(topNAggregatorItems)
	items := make([]*measurev1.TopNList_Item, length)

	for i, item := range topNAggregatorItems {
		targetIdx := length - 1 - i
		fieldValue, convErr := aggregation.ToFieldValue(item.mapFunc.Val())
		if convErr != nil {
			return nil, convErr
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

func (taggr *topNPostProcessor[K]) valWithoutAggregation(tagNames []string) []*measurev1.TopNList {
	topNLists := make([]*measurev1.TopNList, 0, len(taggr.timelines))

	for ts, timeline := range taggr.timelines {
		items := make([]*measurev1.TopNList_Item, timeline.queue.Len())
		for idx, elem := range timeline.queue.Values() {
			elemItem := elem.(*topNAggregatorItem[K])
			fieldValue, _ := aggregation.ToFieldValue(elemItem.val)
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

func (taggr *topNPostProcessor[K]) Reset() {
	clear(taggr.timelines)

	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		clear(taggr.cache)

		taggr.items = taggr.items[:0]
	}
}
