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

// PostProcessor defines necessary methods for Top-N post processor with or without aggregation.
type PostProcessor interface {
	Put(entityValues pbv1.EntityValues, val int64, timestampMillis uint64, version int64)
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
		return taggr.items[i].mapFunc.Val() < taggr.items[j].mapFunc.Val()
	}
	return taggr.items[i].mapFunc.Val() > taggr.items[j].mapFunc.Val()
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

var _ flow.Element = (*topNAggregatorItem)(nil)

type topNAggregatorItem struct {
	mapFunc aggregation.Map[int64]
	key     string
	values  pbv1.EntityValues
	val     int64
	version int64
	index   int
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

func (taggr *topNPostProcessor) Put(entityValues pbv1.EntityValues, val int64, timestampMillis uint64, version int64) {
	timeline, ok := taggr.timelines[timestampMillis]
	key := entityValues.String()
	if !ok {
		timeline = &topNTimelineItem{
			queue: flow.NewPriorityQueue(func(a, b interface{}) int {
				if taggr.sort == modelv1.Sort_SORT_DESC {
					if a.(*topNAggregatorItem).val < b.(*topNAggregatorItem).val {
						return -1
					} else if a.(*topNAggregatorItem).val == b.(*topNAggregatorItem).val {
						return 0
					}
					return 1
				}
				if a.(*topNAggregatorItem).val < b.(*topNAggregatorItem).val {
					return 1
				} else if a.(*topNAggregatorItem).val == b.(*topNAggregatorItem).val {
					return 0
				}
				return -1
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

	if timeline.queue.Len() < int(taggr.topN) {
		heap.Push(timeline.queue, newItem)
		timeline.items[key] = newItem
		return
	}

	if lowest := timeline.queue.Peek(); lowest != nil {
		lowestItem := lowest.(*topNAggregatorItem)

		shouldReplace := (taggr.sort == modelv1.Sort_SORT_DESC && lowestItem.val < val) ||
			(taggr.sort != modelv1.Sort_SORT_DESC && lowestItem.val > val)

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
					exist.mapFunc.In(item.val)
					heap.Fix(taggr, exist.index)
					continue
				}

				mapFunc, err := aggregation.NewMap[int64](taggr.aggrFunc)
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
			Value: &modelv1.FieldValue{
				Value: &modelv1.FieldValue_Int{
					Int: &modelv1.Int{Value: item.mapFunc.Val()},
				},
			},
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
				Value: &modelv1.FieldValue{
					Value: &modelv1.FieldValue_Int{
						Int: &modelv1.Int{Value: elem.(*topNAggregatorItem).val},
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

func (taggr *topNPostProcessor) Reset() {
	clear(taggr.timelines)

	if taggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		clear(taggr.cache)

		taggr.items = taggr.items[:0]
	}
}
