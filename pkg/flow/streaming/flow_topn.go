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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
)

var _ api.AggregateFunction = (*topNAggregator)(nil)

type windowedFlow struct {
	f  *streamingFlow
	wa api.WindowAssigner
}

func (s *windowedFlow) TopN(topNum int, opts ...any) api.Flow {
	topNAggrFunc := &topNAggregator{
		cacheSize: topNum,
		sort:      modelv1.Sort_SORT_UNSPECIFIED,
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
	if topNAggrFunc.sort == modelv1.Sort_SORT_DESC {
		topNAggrFunc.comparator = func(a, b interface{}) int {
			return utils.Int64Comparator(b, a)
		}
	} else {
		topNAggrFunc.comparator = utils.Int64Comparator
	}
	topNAggrFunc.treeMap = treemap.NewWith(topNAggrFunc.comparator)
	s.wa.(*SlidingTimeWindows).aggrFunc = topNAggrFunc
	return s.f
}

type topNAggregator struct {
	// cacheSize is the maximum number of entries which can be held in the buffer, i.e. treeMap
	cacheSize int
	// currentTopNum indicates how many records are tracked.
	// This should not exceed cacheSize
	currentTopNum int
	treeMap       *treemap.Map
	// sortKeyExtractor is an extractor to fetch sort key from the record
	// TODO: currently we only support sorting numeric field, i.e. int64
	sortKeyExtractor func(interface{}) int64
	// sort indicates the order of results
	sort       modelv1.Sort
	comparator utils.Comparator
}

type TopNOption func(aggregator *topNAggregator)

func WithSortKeyExtractor(sortKeyExtractor func(interface{}) int64) TopNOption {
	return func(aggregator *topNAggregator) {
		aggregator.sortKeyExtractor = sortKeyExtractor
	}
}

func OrderBy(sort modelv1.Sort) TopNOption {
	return func(aggregator *topNAggregator) {
		aggregator.sort = sort
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
	// get the "maximum" item
	// - if ASC, the maximum item
	// - else DESC, the minimum item
	worstKey, _ := t.treeMap.Max()
	if worstKey == nil {
		// return true if the buffer is empty.
		return true
	}
	if t.comparator(sortKey, worstKey.(int64)) < 0 {
		return true
	}
	return t.currentTopNum < t.cacheSize
}

type Tuple2 struct {
	First  interface{} `json:"first"`
	Second interface{} `json:"second"`
}

func (t *Tuple2) Equal(other *Tuple2) bool {
	return cmp.Equal(t.First, other.First) && cmp.Equal(t.Second, other.Second)
}

func (t *topNAggregator) GetResult() interface{} {
	iter := t.treeMap.Iterator()
	items := make([]*Tuple2, 0, t.treeMap.Size())
	for iter.Next() {
		list := iter.Value().([]interface{})
		for _, item := range list {
			items = append(items, &Tuple2{iter.Key(), item})
		}
	}
	return items
}
