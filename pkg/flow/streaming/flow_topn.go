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

package streaming

import (
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
)

var _ api.AggregateFunction = (*topNAggregator)(nil)

type windowedFlow struct {
	f  *streamingFlow
	wa api.WindowAssigner
}

func (s *windowedFlow) TopN(topNum int, opts ...any) api.Flow {
	topNAggrFunc := &topNAggregator{
		topNum:    topNum,
		cacheSize: 1000, // default cache size is 1000
		treeMap:   treemap.NewWith(utils.Int64Comparator),
	}
	// apply user customized options
	for _, opt := range opts {
		if applier, ok := opt.(TopNOption); ok {
			applier(topNAggrFunc)
		}
	}
	if topNAggrFunc.sortKeyExtractor == nil {
		s.f.drainErr(errors.New("sortKeyExtractor must be specified"))
	}
	s.wa.(*SlidingTimeWindows).aggrFunc = topNAggrFunc
	return s.f
}

type topNAggregator struct {
	topNum int
	// cacheSize is the maximum number of entries which can be held in the buffer, i.e. treeMap
	cacheSize int
	// currentTopNum indicates how many records are tracked.
	// This should not exceed cacheSize
	currentTopNum int
	treeMap       *treemap.Map
	// sortKeyExtractor is an extractor to fetch sort key from the record
	// TODO: currently we only support sorting numeric field, i.e. int64
	sortKeyExtractor func(interface{}) int64
}

type TopNOption func(aggregator *topNAggregator)

func WithCacheSize(cacheSize int) TopNOption {
	return func(aggregator *topNAggregator) {
		aggregator.cacheSize = cacheSize
	}
}

func WithSortKeyExtractor(sortKeyExtractor func(interface{}) int64) TopNOption {
	return func(aggregator *topNAggregator) {
		aggregator.sortKeyExtractor = sortKeyExtractor
	}
}

func (t *topNAggregator) Add(input []interface{}) {
	for _, item := range input {
		sortKey := t.sortKeyExtractor(item)
		// check
		if t.checkSortKeyInBufferRange(sortKey) {
			t.put(sortKey, item)
			// do cleanup: maintain the treeMap size
			if t.currentTopNum > t.cacheSize {
				lastKey, lastValues := t.treeMap.Max()
				size := len(lastValues.([]interface{}))
				// remove last one
				if size <= 1 {
					t.currentTopNum -= size
					t.treeMap.Remove(lastKey)
				} else {
					t.currentTopNum--
					t.treeMap.Put(lastKey, lastValues.([]interface{})[0:size-1])
				}
			}
		}
	}
}

func (t *topNAggregator) put(sortKey int64, data interface{}) {
	t.currentTopNum++
	if existingList, ok := t.treeMap.Get(sortKey); ok {
		existingList = append(existingList.([]interface{}), data)
		t.treeMap.Put(sortKey, existingList)
	} else {
		t.treeMap.Put(sortKey, []interface{}{data})
	}
}

func (t *topNAggregator) checkSortKeyInBufferRange(sortKey int64) bool {
	// TODO: sort direction?
	worstKey, _ := t.treeMap.Max()
	if worstKey == nil {
		// return true if the buffer is empty.
		return true
	}
	// TODO: sort direction?
	if sortKey < worstKey.(int64) {
		return true
	}
	return t.currentTopNum < t.cacheSize
}

type Tuple2 struct {
	First  interface{}
	Second interface{}
}

func (t *Tuple2) Equal(other *Tuple2) bool {
	return cmp.Equal(t.First, other.First) && cmp.Equal(t.Second, other.Second)
}

func (t *topNAggregator) GetResult() interface{} {
	iter := t.treeMap.Iterator()
	items := make([]*Tuple2, 0, t.topNum)
	for iter.Next() && len(items) < t.topNum {
		list := iter.Value().([]interface{})
		if len(items)+len(list) > t.topNum {
			list = list[0 : t.topNum-len(items)]
		}
		for _, itemInList := range list {
			items = append(items, &Tuple2{iter.Key(), itemInList})
		}
	}
	return items
}
