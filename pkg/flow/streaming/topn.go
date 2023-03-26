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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow"
)

// TopNSort defines the order of sorting.
type TopNSort uint8

// The available order of sorting.
const (
	DESC TopNSort = iota
	ASC
)

type windowedFlow struct {
	f  *streamingFlow
	wa flow.WindowAssigner
}

func (s *windowedFlow) TopN(topNum int, opts ...any) flow.Flow {
	s.wa.(*tumblingTimeWindows).aggregationFactory = func() flow.AggregationOp {
		topNAggrFunc := &topNAggregatorGroup{
			cacheSize: topNum,
			sort:      DESC,
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
		if topNAggrFunc.sort == ASC {
			topNAggrFunc.comparator = utils.Int64Comparator
		} else { // DESC
			topNAggrFunc.comparator = func(a, b interface{}) int {
				return utils.Int64Comparator(b, a)
			}
		}
		topNAggrFunc.aggregatorGroup = make(map[string]*topNAggregator)
		return topNAggrFunc
	}
	return s.f
}

type topNAggregatorGroup struct {
	aggregatorGroup   map[string]*topNAggregator
	sortKeyExtractor  func(flow.StreamRecord) int64
	groupKeyExtractor func(flow.StreamRecord) string
	comparator        utils.Comparator
	cacheSize         int
	sort              TopNSort
}

type topNAggregator struct {
	*topNAggregatorGroup
	treeMap       *treemap.Map
	currentTopNum int
	dirty         bool
}

// TopNOption is the option to set up a top-n aggregator group.
type TopNOption func(aggregator *topNAggregatorGroup)

// WithSortKeyExtractor sets a closure to extract the sorting key.
func WithSortKeyExtractor(sortKeyExtractor func(flow.StreamRecord) int64) TopNOption {
	return func(aggregator *topNAggregatorGroup) {
		aggregator.sortKeyExtractor = sortKeyExtractor
	}
}

// WithGroupKeyExtractor extract group key from the StreamRecord.
func WithGroupKeyExtractor(groupKeyExtractor func(flow.StreamRecord) string) TopNOption {
	return func(aggregator *topNAggregatorGroup) {
		aggregator.groupKeyExtractor = groupKeyExtractor
	}
}

// OrderBy sets the sorting order.
func OrderBy(sort TopNSort) TopNOption {
	return func(aggregator *topNAggregatorGroup) {
		aggregator.sort = sort
	}
}

func (t *topNAggregatorGroup) Add(input []flow.StreamRecord) {
	for _, item := range input {
		sortKey := t.sortKeyExtractor(item)
		groupKey := t.groupKeyExtractor(item)
		aggregator := t.getOrCreateGroup(groupKey)
		if aggregator.checkSortKeyInBufferRange(sortKey) {
			aggregator.put(sortKey, item)
			aggregator.doCleanUp()
		}
	}
}

func (t *topNAggregatorGroup) Snapshot() interface{} {
	groupRanks := make(map[string][]*Tuple2)
	for group, aggregator := range t.aggregatorGroup {
		if !aggregator.dirty {
			continue
		}
		aggregator.dirty = false
		iter := aggregator.treeMap.Iterator()
		items := make([]*Tuple2, 0, aggregator.currentTopNum)
		for iter.Next() {
			list := iter.Value().([]interface{})
			for _, item := range list {
				items = append(items, &Tuple2{iter.Key(), item})
			}
		}
		groupRanks[group] = items
	}
	return groupRanks
}

func (t *topNAggregatorGroup) Dirty() bool {
	for _, aggregator := range t.aggregatorGroup {
		if aggregator.dirty {
			return true
		}
	}
	return false
}

func (t *topNAggregatorGroup) getOrCreateGroup(group string) *topNAggregator {
	aggregator, groupExist := t.aggregatorGroup[group]
	if groupExist {
		return aggregator
	}
	t.aggregatorGroup[group] = &topNAggregator{
		topNAggregatorGroup: t,
		treeMap:             treemap.NewWith(t.comparator),
	}
	return t.aggregatorGroup[group]
}

func (t *topNAggregator) doCleanUp() {
	// do cleanup: maintain the treeMap windowSize
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

func (t *topNAggregator) put(sortKey int64, data flow.StreamRecord) {
	t.currentTopNum++
	t.dirty = true
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

// Tuple2 is a tuple with 2 fields. Each field may be a separate type.
type Tuple2 struct {
	V1 interface{} `json:"v1"`
	V2 interface{} `json:"v2"`
}
