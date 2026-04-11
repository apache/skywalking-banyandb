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

package dquery

import (
	"container/heap"
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
)

type entityAccumulator struct {
	reduceFunc   aggregation.Reduce[int64]
	entityValues pbv1.EntityValues
}

type heapEntry struct {
	acc *entityAccumulator
	key string
}

var _ heap.Interface = (*topNEntityHeap)(nil)

type topNEntityHeap struct {
	keyToIdx map[string]int
	items    []*heapEntry
	sortDir  modelv1.Sort
}

func (h *topNEntityHeap) Len() int { return len(h.items) }

func (h *topNEntityHeap) Less(i, j int) bool {
	vi := h.items[i].acc.reduceFunc.Val()
	vj := h.items[j].acc.reduceFunc.Val()
	if h.sortDir == modelv1.Sort_SORT_DESC {
		return vi < vj
	}
	return vi > vj
}

func (h *topNEntityHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.keyToIdx[h.items[i].key] = i
	h.keyToIdx[h.items[j].key] = j
}

func (h *topNEntityHeap) Push(x any) {
	entry := x.(*heapEntry)
	h.keyToIdx[entry.key] = len(h.items)
	h.items = append(h.items, entry)
}

func (h *topNEntityHeap) Pop() any {
	old := h.items
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	h.items = old[:n-1]
	delete(h.keyToIdx, entry.key)
	return entry
}

func (h *topNEntityHeap) tryInsert(key string, acc *entityAccumulator, topN int32) {
	if h.Len() < int(topN) {
		heap.Push(h, &heapEntry{key: key, acc: acc})
		return
	}
	rootVal := h.items[0].acc.reduceFunc.Val()
	newVal := acc.reduceFunc.Val()
	sortDir := h.sortDir
	shouldReplace := (sortDir == modelv1.Sort_SORT_DESC && newVal > rootVal) ||
		(sortDir == modelv1.Sort_SORT_ASC && newVal < rootVal)
	if shouldReplace {
		heap.Pop(h)
		heap.Push(h, &heapEntry{key: key, acc: acc})
	}
}

func (h *topNEntityHeap) drainSorted() []*heapEntry {
	n := h.Len()
	entries := make([]*heapEntry, n)
	for idx := n - 1; idx >= 0; idx-- {
		entries[idx] = heap.Pop(h).(*heapEntry)
	}
	return entries
}

type reduceTopNAggregator struct {
	entities   map[string]*entityAccumulator
	entityHeap *topNEntityHeap
	seen       map[shardEntityKey]struct{}
	topN       int32
	sortDir    modelv1.Sort
	aggrFunc   modelv1.AggregationFunction
	minMax     bool
}

type shardEntityKey struct {
	entityValues string
	shardID      uint32
}

func newReduceTopNAggregator(topN int32, aggrFunc modelv1.AggregationFunction, sortDir modelv1.Sort) *reduceTopNAggregator {
	minMax := aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX ||
		aggrFunc == modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN
	d := &reduceTopNAggregator{
		aggrFunc: aggrFunc,
		sortDir:  sortDir,
		topN:     topN,
		entities: make(map[string]*entityAccumulator),
		minMax:   minMax,
		seen:     make(map[shardEntityKey]struct{}),
		entityHeap: &topNEntityHeap{
			items:    make([]*heapEntry, 0, topN),
			keyToIdx: make(map[string]int),
			sortDir:  sortDir,
		},
	}
	return d
}

func (d *reduceTopNAggregator) putPartial(shardID uint32, entityValues pbv1.EntityValues, partial aggregation.Partial[int64]) error {
	entityKey := entityValues.String()
	dedupeKey := shardEntityKey{shardID: shardID, entityValues: entityKey}
	if _, seen := d.seen[dedupeKey]; seen {
		return nil
	}
	// Deduplicate across replicas: since all replicas are queried concurrently,
	// the same shard-entity pair may appear multiple times and must be deduplicated
	// to avoid double-counting.
	d.seen[dedupeKey] = struct{}{}
	acc, exists := d.entities[entityKey]
	if !exists {
		reduceFunc, reduceErr := aggregation.NewReduce[int64](d.aggrFunc)
		if reduceErr != nil {
			return fmt.Errorf("failed to create reduce function: %w", reduceErr)
		}
		reduceFunc.Combine(partial)
		acc = &entityAccumulator{
			entityValues: entityValues,
			reduceFunc:   reduceFunc,
		}
		d.entities[entityKey] = acc
		if d.minMax {
			d.entityHeap.tryInsert(entityKey, acc, d.topN)
		}
		return nil
	}
	acc.reduceFunc.Combine(partial)
	if d.minMax {
		if idx, inHeap := d.entityHeap.keyToIdx[entityKey]; inHeap {
			heap.Fix(d.entityHeap, idx)
		} else {
			d.entityHeap.tryInsert(entityKey, acc, d.topN)
		}
	}
	return nil
}

func (d *reduceTopNAggregator) buildResult(tagNames []string) []*measurev1.TopNList {
	if len(d.entities) == 0 {
		return nil
	}
	if !d.minMax {
		for key, acc := range d.entities {
			d.entityHeap.tryInsert(key, acc, d.topN)
		}
	}
	entries := d.entityHeap.drainSorted()
	items := make([]*measurev1.TopNList_Item, len(entries))
	for idx, entry := range entries {
		items[idx] = d.buildItem(entry.acc, tagNames)
	}
	return []*measurev1.TopNList{{Items: items}}
}

func (d *reduceTopNAggregator) buildItem(acc *entityAccumulator, tagNames []string) *measurev1.TopNList_Item {
	val := acc.reduceFunc.Val()
	fieldValue := &modelv1.FieldValue{
		Value: &modelv1.FieldValue_Int{
			Int: &modelv1.Int{Value: val},
		},
	}
	tags := make([]*modelv1.Tag, len(tagNames))
	for idx, tagName := range tagNames {
		tags[idx] = &modelv1.Tag{
			Key:   tagName,
			Value: acc.entityValues[idx],
		}
	}
	return &measurev1.TopNList_Item{
		Entity: tags,
		Value:  fieldValue,
	}
}
