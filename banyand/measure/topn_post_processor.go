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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// PostProcessor keeps the top N items of every timeline (timestamp) of the TopN values put into it.
type PostProcessor[K streaming.TopSortKey] interface {
	Put(entityValues pbv1.EntityValues, val K, timestampMillis uint64, version int64)
	Flush() ([]*topNAggregatorItem[K], error)
	Reset()
}

// CreateTopNPostProcessorInt creates a Top-N post processor for int64 values.
func CreateTopNPostProcessorInt(topN int32, sort modelv1.Sort) PostProcessor[int64] {
	return createTopNPostProcessor[int64](topN, sort)
}

// CreateTopNPostProcessorFloat creates a Top-N post processor for float64 values.
func CreateTopNPostProcessorFloat(topN int32, sort modelv1.Sort) PostProcessor[float64] {
	return createTopNPostProcessor[float64](topN, sort)
}

func createTopNPostProcessor[K streaming.TopSortKey](topN int32, sort modelv1.Sort) PostProcessor[K] {
	return &topNPostProcessor[K]{
		topN:      topN,
		sort:      sort,
		timelines: make(map[uint64]*topNTimelineItem[K]),
	}
}

var (
	_ flow.Element = (*topNAggregatorItem[int64])(nil)
	_ flow.Element = (*topNAggregatorItem[float64])(nil)
)

type topNAggregatorItem[K streaming.TopSortKey] struct {
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

type topNTimelineItem[K streaming.TopSortKey] struct {
	queue *flow.DedupPriorityQueue
	items map[string]*topNAggregatorItem[K]
}

type topNPostProcessor[K streaming.TopSortKey] struct {
	timelines map[uint64]*topNTimelineItem[K]
	sort      modelv1.Sort
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
	for _, timeline := range taggr.timelines {
		for timeline.queue.Len() > 0 {
			item := heap.Pop(timeline.queue).(*topNAggregatorItem[K])
			result = append(result, item)
		}
	}
	taggr.Reset()
	return result, nil
}

func (taggr *topNPostProcessor[K]) Reset() {
	clear(taggr.timelines)
}
