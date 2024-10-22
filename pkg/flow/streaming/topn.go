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
	"strconv"
	"strings"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
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
	l  *logger.Logger
}

func (s *windowedFlow) TopN(topNum int, opts ...any) flow.Flow {
	s.wa.(*tumblingTimeWindows).aggregationFactory = func() flow.AggregationOp {
		topNAggrFunc := &topNAggregatorGroup{
			cacheSize: topNum,
			sort:      DESC,
			l:         s.l,
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
	keyExtractor      func(flow.StreamRecord) uint64
	sortKeyExtractor  func(flow.StreamRecord) int64
	groupKeyExtractor func(flow.StreamRecord) string
	comparator        utils.Comparator
	l                 *logger.Logger
	cacheSize         int
	sort              TopNSort
}

type topNAggregator struct {
	*topNAggregatorGroup
	treeMap *treemap.Map
	dict    map[uint64]int64
	dirty   bool
}

// TopNOption is the option to set up a top-n aggregator group.
type TopNOption func(aggregator *topNAggregatorGroup)

// WithSortKeyExtractor sets a closure to extract the sorting key.
func WithSortKeyExtractor(sortKeyExtractor func(flow.StreamRecord) int64) TopNOption {
	return func(aggregator *topNAggregatorGroup) {
		aggregator.sortKeyExtractor = sortKeyExtractor
	}
}

// WithKeyExtractor sets a closure to extract the key.
func WithKeyExtractor(keyExtractor func(flow.StreamRecord) uint64) TopNOption {
	return func(aggregator *topNAggregatorGroup) {
		aggregator.keyExtractor = keyExtractor
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
		key := t.keyExtractor(item)
		sortKey := t.sortKeyExtractor(item)
		groupKey := t.groupKeyExtractor(item)
		aggregator := t.getOrCreateGroup(groupKey)
		aggregator.removeExistedItem(key)
		if aggregator.checkSortKeyInBufferRange(sortKey) {
			if e := t.l.Debug(); e.Enabled() {
				e.Str("group", groupKey).Uint64("key", key).Time("elem_ts", time.Unix(0, item.TimestampMillis()*int64(time.Millisecond))).Msg("put into topN buffer")
			}
			aggregator.put(key, sortKey, item)
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
		items := make([]*Tuple2, 0, aggregator.size())
		for iter.Next() {
			list := iter.Value().([]interface{})
			for _, item := range list {
				items = append(items, &Tuple2{iter.Key(), item})
			}
		}
		groupRanks[group] = items
	}
	if len(groupRanks) > 0 {
		if e := t.l.Debug(); e.Enabled() {
			sb := strings.Builder{}
			for g, item := range groupRanks {
				sb.WriteString("{")
				sb.WriteString(g)
				sb.WriteString(":")
				sb.WriteString(strconv.Itoa(len(item)))
				sb.WriteString("}")
			}
			t.l.Debug().Interface("snapshot", sb.String()).Msg("taken a topN snapshot")
		}
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
		dict:                make(map[uint64]int64),
	}
	return t.aggregatorGroup[group]
}

func (t *topNAggregator) doCleanUp() {
	// do cleanup: maintain the treeMap windowSize
	if t.size() > t.cacheSize {
		lastKey, lastValues := t.treeMap.Max()
		l := lastValues.([]interface{})
		delete(t.dict, t.keyExtractor(l[len(l)-1].(flow.StreamRecord)))
		// remove last one
		if len(l) <= 1 {
			t.treeMap.Remove(lastKey)
		} else {
			t.treeMap.Put(lastKey, l[:len(l)-1])
		}
	}
}

func (t *topNAggregator) put(key uint64, sortKey int64, data flow.StreamRecord) {
	t.dirty = true
	if existingList, ok := t.treeMap.Get(sortKey); ok {
		existingList = append(existingList.([]interface{}), data)
		t.treeMap.Put(sortKey, existingList)
	} else {
		t.treeMap.Put(sortKey, []interface{}{data})
	}
	t.dict[key] = sortKey
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
	return t.size() < t.cacheSize
}

func (t *topNAggregator) removeExistedItem(key uint64) {
	existed, ok := t.dict[key]
	if !ok {
		return
	}
	delete(t.dict, key)
	list, ok := t.treeMap.Get(existed)
	if !ok {
		return
	}
	l := list.([]interface{})
	for i := 0; i < len(l); i++ {
		if t.keyExtractor(l[i].(flow.StreamRecord)) == key {
			l = append(l[:i], l[i+1:]...)
		}
	}
	if len(l) == 0 {
		t.treeMap.Remove(existed)
		return
	}
	t.treeMap.Put(existed, l)
}

func (t *topNAggregator) size() int {
	return len(t.dict)
}

func (t *topNAggregatorGroup) leakCheck() {
	for g, agg := range t.aggregatorGroup {
		if agg.size() > t.cacheSize {
			panic(g + "leak detected: topN buffer size exceed the cache size")
		}
		iter := agg.treeMap.Iterator()
		count := 0
		for iter.Next() {
			count += len(iter.Value().([]interface{}))
		}
		if count != agg.size() {
			panic(g + "leak detected: treeMap size not match dictionary size")
		}
	}
}

// Tuple2 is a tuple with 2 fields. Each field may be a separate type.
type Tuple2 struct {
	V1 interface{} `json:"v1"`
	V2 interface{} `json:"v2"`
}
