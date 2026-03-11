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

// SortableValue represents a metric value that can be compared and aggregated for TopN operations.
type SortableValue interface {
	Less(other interface{}) bool
	Greater(other interface{}) bool
}

// IntValue is a wrapper for int64 that implements SortableValue.
type IntValue int64

// Less returns true if v is less than other.
func (v IntValue) Less(other interface{}) bool {
	return v < other.(IntValue)
}

// Greater returns true if v is greater than other.
func (v IntValue) Greater(other interface{}) bool {
	return v > other.(IntValue)
}

// FloatValue is a wrapper for float64 that implements SortableValue.
type FloatValue float64

// Less returns true if v is less than other.
func (v FloatValue) Less(other interface{}) bool {
	return v < other.(FloatValue)
}

// Greater returns true if v is greater than other.
func (v FloatValue) Greater(other interface{}) bool {
	return v > other.(FloatValue)
}

// FieldValueToSortableValue converts a FieldValue to a SortableValue.
func FieldValueToSortableValue(fv *modelv1.FieldValue) SortableValue {
	if fv.GetFloat() != nil {
		return FloatValue(fv.GetFloat().GetValue())
	}
	return IntValue(fv.GetInt().GetValue())
}

// SortableValueToFieldValue converts a SortableValue to a FieldValue.
func SortableValueToFieldValue(m SortableValue) *modelv1.FieldValue {
	if v, ok := m.(FloatValue); ok {
		return &modelv1.FieldValue{
			Value: &modelv1.FieldValue_Float{
				Float: &modelv1.Float{Value: float64(v)},
			},
		}
	}
	return &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Int{
			Int: &modelv1.Int{Value: int64(m.(IntValue))},
		},
	}
}

// Aggregator defines the interface for aggregating SortableValue values.
type Aggregator interface {
	Aggregate(other SortableValue)
	Val() SortableValue
}

// NewAggregator creates an aggregator based on the aggregation function and initial value.
func NewAggregator(aggrFunc modelv1.AggregationFunction, val SortableValue) (Aggregator, error) {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT {
		mapFunc, err := aggregation.NewMap[int64](aggrFunc)
		if err != nil {
			return nil, err
		}
		return &CountAggregator{mapFunc}, nil
	}
	if _, ok := val.(FloatValue); ok {
		mapFunc, err := aggregation.NewMap[float64](aggrFunc)
		if err != nil {
			return nil, err
		}
		return &FloatAggregator{mapFunc}, nil
	}
	mapFunc, err := aggregation.NewMap[int64](aggrFunc)
	if err != nil {
		return nil, err
	}
	return &IntAggregator{mapFunc}, nil
}

// CountAggregator aggregates count for any SortableValue.
type CountAggregator struct {
	mapFunc aggregation.Map[int64]
}

// Aggregate increments the count by 1.
func (a *CountAggregator) Aggregate(_ SortableValue) {
	a.mapFunc.In(1)
}

// Val returns the aggregated value.
func (a *CountAggregator) Val() SortableValue {
	return IntValue(a.mapFunc.Val())
}

// IntAggregator aggregates IntValue values.
type IntAggregator struct {
	mapFunc aggregation.Map[int64]
}

// Aggregate aggregates the given IntValue into the internal map.
func (a *IntAggregator) Aggregate(other SortableValue) {
	n, ok := other.(IntValue)
	if !ok {
		return
	}
	a.mapFunc.In(int64(n))
}

// Val returns the aggregated IntValue.
func (a *IntAggregator) Val() SortableValue {
	return IntValue(a.mapFunc.Val())
}

// FloatAggregator aggregates FloatValue values.
type FloatAggregator struct {
	mapFunc aggregation.Map[float64]
}

// Aggregate aggregates the given FloatValue into the internal map.
func (a *FloatAggregator) Aggregate(other SortableValue) {
	n, ok := other.(FloatValue)
	if !ok {
		return
	}
	a.mapFunc.In(float64(n))
}

// Val returns the aggregated FloatValue.
func (a *FloatAggregator) Val() SortableValue {
	return FloatValue(a.mapFunc.Val())
}

// PostProcessor defines necessary methods for Top-N post processor with or without aggregation.
type PostProcessor interface {
	Put(entityValues pbv1.EntityValues, val SortableValue, timestampMillis uint64, version int64)
	Flush() ([]*topNAggregatorItem, error)
	Val([]string) ([]*measurev1.TopNList, error)
	Reset()
}

// CreateTopNPostProcessor creates a Top-N post processor with or without aggregation.
func CreateTopNPostProcessor(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		// if aggregation is not specified, we have to keep all timelines
		return &topNPostProcessor{
			topN:      topN,
			sort:      sort,
			timelines: make(map[uint64]*topNTimelineItem),
		}
	}
	aggregator := &topNPostProcessor{
		topN:      topN,
		sort:      sort,
		aggrFunc:  aggrFunc,
		cache:     make(map[string]*topNAggregatorItem),
		timelines: make(map[uint64]*topNTimelineItem),
		items:     make([]*topNAggregatorItem, 0, topN),
	}
	heap.Init(aggregator)
	return aggregator
}

func (taggr *topNPostProcessor) Len() int {
	return len(taggr.items)
}

// Less reports whether min/max heap has to be built.
// For DESC, a min heap has to be built,
// while for ASC, a max heap has to be built.
func (taggr *topNPostProcessor) Less(i, j int) bool {
	if taggr.sort == modelv1.Sort_SORT_DESC {
		return taggr.items[i].aggregator.Val().Less(taggr.items[j].aggregator.Val())
	}
	return taggr.items[i].aggregator.Val().Greater(taggr.items[j].aggregator.Val())
}

func (taggr *topNPostProcessor) Swap(i, j int) {
	taggr.items[i], taggr.items[j] = taggr.items[j], taggr.items[i]
	taggr.items[i].index = i
	taggr.items[j].index = j
}

func (taggr *topNPostProcessor) Push(x any) {
	n := len(taggr.items)
	item := x.(*topNAggregatorItem)
	item.index = n
	taggr.items = append(taggr.items, item)
}

func (taggr *topNPostProcessor) Pop() any {
	old := taggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	taggr.items = old[0 : n-1]
	return item
}

func (taggr *topNPostProcessor) tryEnqueue(key string, item *topNAggregatorItem) {
	if len(taggr.items) == 0 {
		return
	}
	if lowest := taggr.items[0]; lowest != nil {
		shouldReplace := (taggr.sort == modelv1.Sort_SORT_DESC && lowest.aggregator.Val().Less(item.aggregator.Val())) ||
			(taggr.sort != modelv1.Sort_SORT_DESC && lowest.aggregator.Val().Greater(item.aggregator.Val()))

		if shouldReplace {
			delete(taggr.cache, lowest.key)
			taggr.cache[key] = item
			taggr.items[0] = item
			item.index = 0
			heap.Fix(taggr, 0)
		}
	}
}

var _ flow.Element = (*topNAggregatorItem)(nil)

type topNAggregatorItem struct {
	aggregator Aggregator
	val        SortableValue
	key        string
	values     pbv1.EntityValues
	version    int64
	index      int
}

func (n *topNAggregatorItem) GetTags(tagNames []string) []*modelv1.Tag {
	tags := make([]*modelv1.Tag, len(n.values))
	for i := 0; i < len(tags); i++ {
		tags[i] = &modelv1.Tag{
			Key:   tagNames[i],
			Value: n.values[i],
		}
	}
	return tags
}

func (n *topNAggregatorItem) GetIndex() int {
	return n.index
}

func (n *topNAggregatorItem) SetIndex(i int) {
	n.index = i
}

type topNTimelineItem struct {
	queue *flow.DedupPriorityQueue
	items map[string]*topNAggregatorItem
}

type topNPostProcessor struct {
	cache     map[string]*topNAggregatorItem
	timelines map[uint64]*topNTimelineItem
	items     []*topNAggregatorItem
	sort      modelv1.Sort
	aggrFunc  modelv1.AggregationFunction
	topN      int32
}

func (taggr *topNPostProcessor) Put(entityValues pbv1.EntityValues, val SortableValue, timestampMillis uint64, version int64) {
	timeline, ok := taggr.timelines[timestampMillis]
	key := entityValues.String()
	if !ok {
		timeline = &topNTimelineItem{
			queue: flow.NewPriorityQueue(func(a, b interface{}) int {
				aVal := a.(*topNAggregatorItem).val
				bVal := b.(*topNAggregatorItem).val
				if taggr.sort == modelv1.Sort_SORT_DESC {
					if aVal.Less(bVal) {
						return -1
					} else if aVal.Greater(bVal) {
						return 1
					}
					return 0
				}
				if aVal.Less(bVal) {
					return 1
				} else if aVal.Greater(bVal) {
					return -1
				}
				return 0
			}, false),
			items: make(map[string]*topNAggregatorItem),
		}

		newItem := &topNAggregatorItem{
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

	newItem := &topNAggregatorItem{
		val:     val,
		key:     key,
		values:  entityValues,
		version: version,
	}

	// If topN <= 0, accept all items without truncation (unbounded mode for distributed aggregation)
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
		lowestItem := lowest.(*topNAggregatorItem)

		shouldReplace := (taggr.sort == modelv1.Sort_SORT_DESC && lowestItem.val.Less(val)) ||
			(taggr.sort != modelv1.Sort_SORT_DESC && lowestItem.val.Greater(val))

		if shouldReplace {
			delete(timeline.items, lowestItem.key)
			timeline.items[key] = newItem
			timeline.queue.ReplaceLowest(newItem)
		}
	}
}

func (taggr *topNPostProcessor) Flush() ([]*topNAggregatorItem, error) {
	var result []*topNAggregatorItem

	if taggr.aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		for _, timeline := range taggr.timelines {
			for timeline.queue.Len() > 0 {
				item := heap.Pop(timeline.queue).(*topNAggregatorItem)
				result = append(result, item)
			}
		}
	} else {
		for _, timeline := range taggr.timelines {
			for _, item := range timeline.items {
				if exist, found := taggr.cache[item.key]; found {
					exist.aggregator.Aggregate(item.val)
					continue
				}

				aggregator, err := NewAggregator(taggr.aggrFunc, item.val)
				if err != nil {
					return nil, err
				}
				item.aggregator = aggregator
				item.aggregator.Aggregate(item.val)
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
		result = make([]*topNAggregatorItem, 0, taggr.Len())
		for taggr.Len() > 0 {
			item := heap.Pop(taggr).(*topNAggregatorItem)
			result = append(result, item)
		}
	}
	taggr.Reset()

	return result, nil
}

func (taggr *topNPostProcessor) Val(tagNames []string) ([]*measurev1.TopNList, error) {
	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		return taggr.valWithAggregation(tagNames)
	}

	return taggr.valWithoutAggregation(tagNames), nil
}

func (taggr *topNPostProcessor) valWithAggregation(tagNames []string) ([]*measurev1.TopNList, error) {
	topNAggregatorItems, err := taggr.Flush()
	if err != nil {
		return nil, err
	}
	length := len(topNAggregatorItems)
	items := make([]*measurev1.TopNList_Item, length)

	for i, item := range topNAggregatorItems {
		targetIdx := length - 1 - i

		items[targetIdx] = &measurev1.TopNList_Item{
			Entity: item.GetTags(tagNames),
			Value:  SortableValueToFieldValue(item.aggregator.Val()),
		}
	}
	return []*measurev1.TopNList{
		{
			Timestamp: timestamppb.Now(),
			Items:     items,
		},
	}, nil
}

func (taggr *topNPostProcessor) valWithoutAggregation(tagNames []string) []*measurev1.TopNList {
	topNLists := make([]*measurev1.TopNList, 0, len(taggr.timelines))
	for ts, timeline := range taggr.timelines {
		items := make([]*measurev1.TopNList_Item, timeline.queue.Len())
		for idx, elem := range timeline.queue.Values() {
			items[idx] = &measurev1.TopNList_Item{
				Entity: elem.(*topNAggregatorItem).GetTags(tagNames),
				Value:  SortableValueToFieldValue(elem.(*topNAggregatorItem).val),
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

func (taggr *topNPostProcessor) Reset() {
	clear(taggr.timelines)

	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		clear(taggr.cache)

		taggr.items = taggr.items[:0]
	}
}
