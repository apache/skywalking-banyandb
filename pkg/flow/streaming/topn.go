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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TopSortKey defines the constraint for sort keys in TopN operations.
type TopSortKey interface {
	int64 | float64
}

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

// TopN creates a TopN flow from a windowed flow.
func TopN[K TopSortKey](wf flow.WindowedFlow, topNum int, sortKeyExtractor func(flow.StreamRecord) K, opts ...any) flow.Flow {
	s := wf.(*windowedFlow)
	config := s.buildTopNConfig(topNum, opts)
	s.wa.(*tumblingTimeWindows).aggregationFactory = func() flow.AggregationOp {
		return newTopNAggregatorGroup[K](config, sortKeyExtractor)
	}
	return s.f
}

func (s *windowedFlow) buildTopNConfig(topNum int, opts []any) *topNConfig {
	config := &topNConfig{
		cacheSize: topNum,
		sort:      DESC,
		l:         s.l,
	}
	for _, opt := range opts {
		if applier, ok := opt.(TopNOption); ok {
			applier(config)
		}
	}
	return config
}

type topNAggregatorGroup[K TopSortKey] struct {
	aggregatorGroup  map[string]*topNAggregator[K]
	sortKeyExtractor func(flow.StreamRecord) K
	config           topNConfig
}

// topNAggregator implements TopN aggregation using a generic heap.
type topNAggregator[K TopSortKey] struct {
	group *topNAggregatorGroup[K]
	heap  *topNHeap[K]
	dict  map[uint64]K
	dirty bool
}

// topNConfig holds configuration for TopN aggregation.
type topNConfig struct {
	groupKeyExtractor func(flow.StreamRecord) string
	keyExtractor      func(flow.StreamRecord) uint64
	l                 *logger.Logger
	sort              TopNSort
	cacheSize         int
}

// TopNOption is the option to set up a TopN config.
type TopNOption func(*topNConfig)

// WithKeyExtractor sets a closure to extract the key.
func WithKeyExtractor(keyExtractor func(flow.StreamRecord) uint64) TopNOption {
	return func(config *topNConfig) {
		config.keyExtractor = keyExtractor
	}
}

// WithGroupKeyExtractor extract group key from the StreamRecord.
func WithGroupKeyExtractor(groupKeyExtractor func(flow.StreamRecord) string) TopNOption {
	return func(config *topNConfig) {
		config.groupKeyExtractor = groupKeyExtractor
	}
}

// OrderBy sets the sorting order.
func OrderBy(sort TopNSort) TopNOption {
	return func(config *topNConfig) {
		config.sort = sort
	}
}

// newTopNAggregatorGroup creates a new TopN aggregator group.
func newTopNAggregatorGroup[K TopSortKey](config *topNConfig, sortKeyExtractor func(flow.StreamRecord) K) *topNAggregatorGroup[K] {
	return &topNAggregatorGroup[K]{
		config:           *config,
		aggregatorGroup:  make(map[string]*topNAggregator[K]),
		sortKeyExtractor: sortKeyExtractor,
	}
}

func (t *topNAggregatorGroup[K]) Add(input []flow.StreamRecord) {
	for _, item := range input {
		key := t.config.keyExtractor(item)
		sortKey := t.sortKeyExtractor(item)
		groupKey := t.config.groupKeyExtractor(item)
		aggregator := t.getOrCreateGroup(groupKey)
		aggregator.removeExistedItem(key)
		if aggregator.checkSortKeyInBufferRange(sortKey) {
			if t.config.l != nil {
				if e := t.config.l.Debug(); e.Enabled() {
					e.Str("group", groupKey).Uint64("key", key).Time("elem_ts", time.Unix(0, item.TimestampMillis()*int64(time.Millisecond))).Msg("put into topN buffer")
				}
			}
			aggregator.put(key, sortKey, item)
			aggregator.doCleanUp()
		}
	}
}

func (t *topNAggregatorGroup[K]) Snapshot() interface{} {
	groupRanks := make(map[string][]*Tuple2[K])
	for group, aggregator := range t.aggregatorGroup {
		if !aggregator.dirty {
			continue
		}
		aggregator.dirty = false
		entries := aggregator.heap.iterAll()
		items := make([]*Tuple2[K], 0, aggregator.heap.totalRecords())
		for _, entry := range entries {
			for _, record := range entry.records {
				items = append(items, &Tuple2[K]{V1: entry.sortKey, V2: record})
			}
		}
		// Sort items based on sort order
		if t.config.sort == ASC {
			sort.Slice(items, func(i, j int) bool {
				return items[i].V1 < items[j].V1
			})
		} else {
			sort.Slice(items, func(i, j int) bool {
				return items[i].V1 > items[j].V1
			})
		}
		groupRanks[group] = items
	}
	if len(groupRanks) > 0 {
		if t.config.l != nil {
			if e := t.config.l.Debug(); e.Enabled() {
				sb := strings.Builder{}
				for g, item := range groupRanks {
					sb.WriteString("{")
					sb.WriteString(g)
					sb.WriteString(":")
					sb.WriteString(strconv.Itoa(len(item)))
					sb.WriteString("}")
				}
				t.config.l.Debug().Interface("snapshot", sb.String()).Msg("taken a topN snapshot")
			}
		}
	}
	return groupRanks
}

func (t *topNAggregatorGroup[K]) Dirty() bool {
	for _, aggregator := range t.aggregatorGroup {
		if aggregator.dirty {
			return true
		}
	}
	return false
}

func (t *topNAggregatorGroup[K]) getOrCreateGroup(group string) *topNAggregator[K] {
	aggregator, groupExist := t.aggregatorGroup[group]
	if groupExist {
		return aggregator
	}
	var lessFn func(a, b K) bool
	if t.config.sort == ASC {
		lessFn = func(a, b K) bool { return a > b }
	} else {
		lessFn = func(a, b K) bool { return a < b }
	}
	t.aggregatorGroup[group] = &topNAggregator[K]{
		group: t,
		heap:  newTopNHeap(lessFn),
		dict:  make(map[uint64]K),
	}
	return t.aggregatorGroup[group]
}

func (t *topNAggregator[K]) doCleanUp() {
	if t.size() <= t.group.config.cacheSize {
		return
	}
	worst := t.heap.peek()
	if worst == nil {
		return
	}
	l := worst.records
	delete(t.dict, t.group.config.keyExtractor(l[len(l)-1]))
	if len(l) <= 1 {
		t.heap.popWorst()
	} else {
		worst.records = l[:len(l)-1]
	}
}

func (t *topNAggregator[K]) put(key uint64, sortKey K, data flow.StreamRecord) {
	t.dirty = true
	t.heap.put(sortKey, data)
	t.dict[key] = sortKey
}

func (t *topNAggregator[K]) checkSortKeyInBufferRange(sortKey K) bool {
	// get the "maximum" item
	// - if ASC, the maximum item
	// - else DESC, the minimum item
	worst := t.heap.peek()
	if worst == nil {
		return true
	}
	if t.group.config.sort == ASC {
		if sortKey < worst.sortKey {
			return true
		}
	} else {
		if sortKey > worst.sortKey {
			return true
		}
	}
	return t.size() < t.group.config.cacheSize
}

func (t *topNAggregator[K]) removeExistedItem(key uint64) {
	existedSortKey, ok := t.dict[key]
	if !ok {
		return
	}
	delete(t.dict, key)
	entry := t.heap.get(existedSortKey)
	if entry == nil {
		return
	}
	l := entry.records
	for idx := range l {
		if t.group.config.keyExtractor(l[idx]) == key {
			l = append(l[:idx], l[idx+1:]...)
			break
		}
	}
	if len(l) == 0 {
		t.heap.remove(existedSortKey)
	} else {
		t.heap.update(existedSortKey, l)
	}
}

func (t *topNAggregator[K]) size() int {
	return len(t.dict)
}

func (t *topNAggregatorGroup[K]) leakCheck() {
	for g, agg := range t.aggregatorGroup {
		if agg.size() > t.config.cacheSize {
			panic(g + "leak detected: topN buffer size exceed the cache size")
		}
		if agg.heap.totalRecords() != agg.size() {
			panic(g + " leak detected: heap size not match dictionary size")
		}
	}
}

// Tuple2 is a tuple with 2 fields. V1 is the sort key (K), V2 is the record.
type Tuple2[K TopSortKey] struct {
	V1 K                 `json:"v1"`
	V2 flow.StreamRecord `json:"v2"`
}
