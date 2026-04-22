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
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// dedupHeapItem tracks an entity's best value and position in the deduplication heap.
type dedupHeapItem struct {
	idp   *measurev1.InternalDataPoint
	key   string
	val   int64
	index int
}

// entityDedupTopN is a heap-based structure that maintains top N distinct entities.
// For DESC sorting, it uses a min-heap (root = smallest of top N).
// For ASC sorting, it uses a max-heap (root = largest of top N).
// The entityMap only tracks entities currently in the heap, so memory is bounded to O(topN).
type entityDedupTopN struct {
	entityMap map[string]*dedupHeapItem
	items     []*dedupHeapItem
	topN      int
	sortDesc  bool
}

// newEntityDedupTopN creates a new entity deduplication heap.
func newEntityDedupTopN(topN int, sortDesc bool) *entityDedupTopN {
	return &entityDedupTopN{
		entityMap: make(map[string]*dedupHeapItem),
		items:     make([]*dedupHeapItem, 0, topN),
		topN:      topN,
		sortDesc:  sortDesc,
	}
}

func (h *entityDedupTopN) Len() int { return len(h.items) }

func (h *entityDedupTopN) Less(i, j int) bool {
	if h.sortDesc {
		return h.items[i].val < h.items[j].val
	}
	return h.items[i].val > h.items[j].val
}

func (h *entityDedupTopN) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *entityDedupTopN) Push(x any) {
	item := x.(*dedupHeapItem)
	item.index = len(h.items)
	h.entityMap[item.key] = item
	h.items = append(h.items, item)
}

func (h *entityDedupTopN) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	delete(h.entityMap, item.key)
	h.items = old[0 : n-1]
	return item
}

func (h *entityDedupTopN) isBetter(newVal, existingVal int64) bool {
	if h.sortDesc {
		return newVal > existingVal
	}
	return newVal < existingVal
}

// Put inserts or updates an entity in the deduplication heap.
func (h *entityDedupTopN) Put(key string, val int64, idp *measurev1.InternalDataPoint) {
	if item, exists := h.entityMap[key]; exists {
		if !h.isBetter(val, item.val) {
			return
		}
		item.val = val
		item.idp = idp
		heap.Fix(h, item.index)
		return
	}
	h.tryAddToHeap(key, val, idp)
}

func (h *entityDedupTopN) tryAddToHeap(key string, val int64, idp *measurev1.InternalDataPoint) {
	if h.topN <= 0 {
		return
	}
	if len(h.items) < h.topN {
		heap.Push(h, &dedupHeapItem{key: key, val: val, idp: idp})
		return
	}
	root := h.items[0]
	if h.isBetter(val, root.val) {
		heap.Pop(h)
		heap.Push(h, &dedupHeapItem{key: key, val: val, idp: idp})
	}
}

// Elements returns the top N distinct entities sorted by best value.
func (h *entityDedupTopN) Elements() []*measurev1.InternalDataPoint {
	result := make([]*dedupHeapItem, len(h.items))
	copy(result, h.items)
	sort.Slice(result, func(i, j int) bool {
		if h.sortDesc {
			return result[i].val > result[j].val
		}
		return result[i].val < result[j].val
	})
	idps := make([]*measurev1.InternalDataPoint, len(result))
	for i, item := range result {
		idps[i] = item.idp
	}
	return idps
}

var _ logical.UnresolvedPlan = (*unresolvedTopNDistinct)(nil)

// unresolvedTopNDistinct is an unresolved plan for top N distinct entity selection.
type unresolvedTopNDistinct struct {
	unresolvedInput logical.UnresolvedPlan
	fieldName       string
	topN            int32
	sort            modelv1.Sort
}

func topNDistinct(input logical.UnresolvedPlan, topN int32, sort modelv1.Sort, fieldName string) logical.UnresolvedPlan {
	return &unresolvedTopNDistinct{
		unresolvedInput: input,
		topN:            topN,
		sort:            sort,
		fieldName:       fieldName,
	}
}

func (u *unresolvedTopNDistinct) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
	prevPlan, analyzeErr := u.unresolvedInput.Analyze(measureSchema)
	if analyzeErr != nil {
		return nil, analyzeErr
	}
	fieldRefs, fieldErr := prevPlan.Schema().CreateFieldRef(logical.NewField(u.fieldName))
	if fieldErr != nil {
		return nil, fieldErr
	}
	if len(fieldRefs) == 0 {
		return nil, errors.New("no field ref found for topN distinct")
	}
	sortDesc := u.sort == modelv1.Sort_SORT_DESC
	return &topNDistinctOp{
		Parent: &logical.Parent{
			UnresolvedInput: u.unresolvedInput,
			Input:           prevPlan,
		},
		topN:     int(u.topN),
		sortDesc: sortDesc,
		fieldIdx: fieldRefs[0].Spec.FieldIdx,
	}, nil
}

var _ logical.Plan = (*topNDistinctOp)(nil)

// topNDistinctOp selects top N distinct entities by best value.
type topNDistinctOp struct {
	*logical.Parent
	topN     int
	sortDesc bool
	fieldIdx int
}

func (t *topNDistinctOp) String() string {
	dir := "DESC"
	if !t.sortDesc {
		dir = "ASC"
	}
	return fmt.Sprintf("TopNDistinct(topN=%d,sort=%s)", t.topN, dir)
}

func (t *topNDistinctOp) Children() []logical.Plan {
	return []logical.Plan{t.Input}
}

func (t *topNDistinctOp) Schema() logical.Schema {
	return t.Input.Schema()
}

func (t *topNDistinctOp) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	iter, execErr := t.Input.(executor.MeasureExecutable).Execute(ctx)
	if execErr != nil {
		return nil, execErr
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	edHeap := newEntityDedupTopN(t.topN, t.sortDesc)
	for iter.Next() {
		dataPoints := iter.Current()
		for _, idp := range dataPoints {
			dp := idp.GetDataPoint()
			if len(dp.GetTagFamilies()) == 0 || len(dp.GetTagFamilies()[0].GetTags()) == 0 {
				continue
			}
			entityValues := make(pbv1.EntityValues, 0, len(dp.GetTagFamilies()[0].GetTags()))
			for _, tag := range dp.GetTagFamilies()[0].GetTags() {
				entityValues = append(entityValues, tag.Value)
			}
			entityKey := entityValues.String()
			fieldVal := dp.GetFields()[t.fieldIdx].GetValue().GetInt().GetValue()
			edHeap.Put(entityKey, fieldVal, idp)
		}
	}
	return &topNDistinctIterator{
		elements: edHeap.Elements(),
		index:    -1,
	}, nil
}

// topNDistinctIterator iterates over the top N distinct entity results.
type topNDistinctIterator struct {
	elements []*measurev1.InternalDataPoint
	index    int
}

func (t *topNDistinctIterator) Next() bool {
	t.index++
	return t.index < len(t.elements)
}

func (t *topNDistinctIterator) Current() []*measurev1.InternalDataPoint {
	return []*measurev1.InternalDataPoint{t.elements[t.index]}
}

func (t *topNDistinctIterator) Close() error {
	return nil
}
