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
	Put(entityValues pbv1.EntityValues, val int64, timestampMillis uint64, version int64) error
	Flush() ([]*topNAggregatorItem, error)
	Val([]string) []*measurev1.TopNList
}

// CreateTopNPostAggregator creates a Top-N post processor with or without aggregation.
func CreateTopNPostAggregator(topN int32, aggrFunc modelv1.AggregationFunction, sort modelv1.Sort) PostProcessor {
	if aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		// if aggregation is not specified, we have to keep all timelines
		return &postNonAggregationProcessor{
			topN:      topN,
			sort:      sort,
			timelines: make(map[uint64]*topNTimelineItem),
		}
	}
	aggregator := &postNonAggregationProcessor{
		topN:     topN,
		sort:     sort,
		aggrFunc: aggrFunc,
		cache:    make(map[string]*topNAggregatorItem),
		items:    make([]*topNAggregatorItem, 0, topN),
	}
	heap.Init(aggregator)
	return aggregator
}

func (aggr *postNonAggregationProcessor) Len() int {
	return len(aggr.items)
}

// Less reports whether min/max heap has to be built.
// For DESC, a min heap has to be built,
// while for ASC, a max heap has to be built.
func (aggr *postNonAggregationProcessor) Less(i, j int) bool {
	if aggr.sort == modelv1.Sort_SORT_DESC {
		return aggr.items[i].int64Func.Val() < aggr.items[j].int64Func.Val()
	}
	return aggr.items[i].int64Func.Val() > aggr.items[j].int64Func.Val()
}

func (aggr *postNonAggregationProcessor) Swap(i, j int) {
	aggr.items[i], aggr.items[j] = aggr.items[j], aggr.items[i]
	aggr.items[i].index = i
	aggr.items[j].index = j
}

func (aggr *postNonAggregationProcessor) Push(x any) {
	n := len(aggr.items)
	item := x.(*topNAggregatorItem)
	item.index = n
	aggr.items = append(aggr.items, item)
}

func (aggr *postNonAggregationProcessor) Pop() any {
	old := aggr.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	aggr.items = old[0 : n-1]
	return item
}

func (aggr *postNonAggregationProcessor) tryEnqueue(key string, item *topNAggregatorItem) {

	if lowest := aggr.items[0]; lowest != nil {
		if aggr.sort == modelv1.Sort_SORT_DESC && lowest.int64Func.Val() < item.int64Func.Val() {
			delete(aggr.cache, lowest.key)
			aggr.cache[key] = item
			aggr.items[0] = item
			heap.Fix(aggr, 0)
		} else if aggr.sort != modelv1.Sort_SORT_DESC && lowest.int64Func.Val() > item.int64Func.Val() {
			delete(aggr.cache, lowest.key)
			aggr.cache[key] = item
			aggr.items[0] = item
			heap.Fix(aggr, 0)
		}
	}
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

var _ flow.Element = (*topNAggregatorItem)(nil)

type topNAggregatorItem struct {
	key       string
	values    pbv1.EntityValues
	int64Func aggregation.Func[int64]
	val       int64
	version   int64
	index     int
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

type postNonAggregationProcessor struct {
	cache     map[string]*topNAggregatorItem
	timelines map[uint64]*topNTimelineItem
	topN      int32
	sort      modelv1.Sort
	aggrFunc  modelv1.AggregationFunction
	items     []*topNAggregatorItem
}

func (naggr *postNonAggregationProcessor) Put(entityValues pbv1.EntityValues, val int64, timestampMillis uint64, version int64) error {
	timeline, ok := naggr.timelines[timestampMillis]
	key := entityValues.String()
	if !ok {
		timeline := &topNTimelineItem{
			queue: flow.NewPriorityQueue(func(a, b interface{}) int {
				if naggr.sort == modelv1.Sort_SORT_DESC {
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
		naggr.timelines[timestampMillis] = timeline
		return nil
	}

	if item, exist := timeline.items[key]; exist {
		if version > item.version {
			item.val = val
			item.version = version

			heap.Fix(timeline.queue, item.index)
		}

		return nil
	}

	newItem := &topNAggregatorItem{
		val:     val,
		key:     key,
		values:  entityValues,
		version: version,
	}

	if timeline.queue.Len() < int(naggr.topN) {
		heap.Push(timeline.queue, newItem)
		timeline.items[key] = newItem
		return nil
	}

	if lowest := timeline.queue.Peek(); lowest != nil {
		lowestItem := lowest.(*topNAggregatorItem)

		shouldReplace :=
			(naggr.sort == modelv1.Sort_SORT_DESC && lowestItem.val < val) ||
				(naggr.sort != modelv1.Sort_SORT_DESC && lowestItem.val > val)

		if shouldReplace {
			delete(timeline.items, lowestItem.key)
			timeline.items[key] = newItem
			timeline.queue.ReplaceLowest(newItem)
		}
	}

	return nil
}

func (naggr *postNonAggregationProcessor) Flush() ([]*topNAggregatorItem, error) {

	var result []*topNAggregatorItem

	for _, timeline := range naggr.timelines {
		for _, nonAggItem := range timeline.items {
			result = append(result, nonAggItem)
		}
	}
	clear(naggr.timelines)

	if naggr.aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		return result, nil
	}

	for _, item := range result {
		if exist, found := naggr.cache[item.key]; found {
			exist.int64Func.In(item.val)
			continue
		}

		aggrFunc, err := aggregation.NewFunc[int64](naggr.aggrFunc)
		if err != nil {
			return nil, err
		}

		item.int64Func = aggrFunc
		item.int64Func.In(item.val)

		if naggr.Len() < int(naggr.topN) {
			naggr.cache[item.key] = item
			heap.Push(naggr, item)
		} else {
			naggr.tryEnqueue(item.key, item)
		}

	}

	aggregationItems := make([]*topNAggregatorItem, 0, naggr.Len())
	for naggr.Len() > 0 {
		item := heap.Pop(naggr).(*topNAggregatorItem)
		aggregationItems = append(aggregationItems, item)
	}
	return aggregationItems, nil
}

func (naggr *postNonAggregationProcessor) Val(tagNames []string) []*measurev1.TopNList {

	if naggr.aggrFunc != modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED {
		topNAggregatorItems, err := naggr.Flush()
		if err != nil {
			return []*measurev1.TopNList{}
		}
		items := make([]*measurev1.TopNList_Item, len(topNAggregatorItems))
		for i, item := range topNAggregatorItems {
			items[i] = &measurev1.TopNList_Item{
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
				Timestamp: timestamppb.Now(),
				Items:     items,
			},
		}
	}

	topNLists := make([]*measurev1.TopNList, 0, len(naggr.timelines))
	for ts, timeline := range naggr.timelines {
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
