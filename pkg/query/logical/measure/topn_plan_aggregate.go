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
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/query/aggregation"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// topNAggregateStateBudget bounds, in estimated bytes, the per-item state one TopN aggregation retains.
// It is a variable only so that tests can lower it.
var topNAggregateStateBudget int64 = 512 << 20

// aggregatedItemOverhead estimates the bytes an item retains beyond its key: the map entry, the
// aggregation function and the retained tag values.
const aggregatedItemOverhead = 256

var _ logical.UnresolvedPlan = (*unresolvedTopNAggregate[int64])(nil)

// unresolvedTopNAggregate is an unresolved plan that aggregates every window value of an item.
type unresolvedTopNAggregate[K streaming.TopSortKey] struct {
	unresolvedInput logical.UnresolvedPlan
	fieldName       string
	topN            int32
	partialLimit    int32
	sort            modelv1.Sort
	aggFunc         modelv1.AggregationFunction
}

// topNAggregate aggregates the values of every item over all the windows in the time range.
// With partialLimit 0 it answers the final aggregates of the top N items (a standalone server).
// Otherwise it answers, for each shard, the partial aggregates of the best partialLimit items of that shard,
// which a liaison merges (a data node).
func topNAggregate(input logical.UnresolvedPlan, topN, partialLimit int32, sortDirection modelv1.Sort, aggFunc modelv1.AggregationFunction,
	fieldName string, fieldType databasev1.FieldType,
) logical.UnresolvedPlan {
	if fieldType == databasev1.FieldType_FIELD_TYPE_FLOAT {
		return &unresolvedTopNAggregate[float64]{
			unresolvedInput: input,
			topN:            topN,
			partialLimit:    partialLimit,
			sort:            sortDirection,
			aggFunc:         aggFunc,
			fieldName:       fieldName,
		}
	}
	return &unresolvedTopNAggregate[int64]{
		unresolvedInput: input,
		topN:            topN,
		partialLimit:    partialLimit,
		sort:            sortDirection,
		aggFunc:         aggFunc,
		fieldName:       fieldName,
	}
}

func (u *unresolvedTopNAggregate[K]) Analyze(measureSchema logical.Schema) (logical.Plan, error) {
	prevPlan, analyzeErr := u.unresolvedInput.Analyze(measureSchema)
	if analyzeErr != nil {
		return nil, analyzeErr
	}
	fieldRefs, fieldErr := prevPlan.Schema().CreateFieldRef(logical.NewField(u.fieldName))
	if fieldErr != nil {
		return nil, fieldErr
	}
	if len(fieldRefs) == 0 {
		return nil, errors.New("no field ref found for topN aggregate")
	}
	return &topNAggregateOp[K]{
		Parent: &logical.Parent{
			UnresolvedInput: u.unresolvedInput,
			Input:           prevPlan,
		},
		topN:         int(u.topN),
		partialLimit: int(u.partialLimit),
		sortDesc:     u.sort == modelv1.Sort_SORT_DESC,
		fieldIdx:     fieldRefs[0].Spec.FieldIdx,
		aggFunc:      u.aggFunc,
	}, nil
}

var _ logical.Plan = (*topNAggregateOp[int64])(nil)

// topNAggregateOp aggregates all values of every item, then keeps the best items by the aggregated value:
// the top N of all items for a final answer, or the best partialLimit items of every shard for a partial one.
type topNAggregateOp[K streaming.TopSortKey] struct {
	*logical.Parent
	topN         int
	partialLimit int
	fieldIdx     int
	aggFunc      modelv1.AggregationFunction
	sortDesc     bool
}

func (t *topNAggregateOp[K]) String() string {
	dir := "DESC"
	if !t.sortDesc {
		dir = "ASC"
	}
	if t.partial() {
		return fmt.Sprintf("TopNAggregate(partialLimit=%d,sort=%s,agg=%s)", t.partialLimit, dir, t.aggFunc)
	}
	return fmt.Sprintf("TopNAggregate(topN=%d,sort=%s,agg=%s)", t.topN, dir, t.aggFunc)
}

func (t *topNAggregateOp[K]) Children() []logical.Plan {
	return []logical.Plan{t.Input}
}

func (t *topNAggregateOp[K]) Schema() logical.Schema {
	return t.Input.Schema()
}

func (t *topNAggregateOp[K]) partial() bool {
	return t.partialLimit > 0
}

type aggregatedItem[K streaming.TopSortKey] struct {
	mapFunc     aggregation.Map[K]
	key         string
	fieldName   string
	tagFamilies []*modelv1.TagFamily
	shardID     uint32
}

// itemKey identifies an aggregated item: a partial answer keeps the items of every shard apart.
type itemKey struct {
	entity  string
	shardID uint32
}

// itemHeap keeps the best items: its root is the worst of them.
type itemHeap[K streaming.TopSortKey] struct {
	items    []*aggregatedItem[K]
	sortDesc bool
}

func (h *itemHeap[K]) Len() int { return len(h.items) }

// better reports whether a ranks before b. Equal aggregates are ranked by the item key, so that the items
// kept at the cutoff do not depend on the scan order.
func (h *itemHeap[K]) better(a, b *aggregatedItem[K]) bool {
	left, right := a.mapFunc.Val(), b.mapFunc.Val()
	if left != right {
		if h.sortDesc {
			return left > right
		}
		return left < right
	}
	return a.key < b.key
}

func (h *itemHeap[K]) Less(i, j int) bool { return h.better(h.items[j], h.items[i]) }

func (h *itemHeap[K]) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *itemHeap[K]) Push(x any) { h.items = append(h.items, x.(*aggregatedItem[K])) }

func (h *itemHeap[K]) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	return item
}

// offer keeps item if it is among the best limit items seen so far.
func (h *itemHeap[K]) offer(item *aggregatedItem[K], limit int) {
	switch {
	case h.Len() < limit:
		heap.Push(h, item)
	case h.Len() > 0 && h.better(item, h.items[0]):
		h.items[0] = item
		heap.Fix(h, 0)
	}
}

// ranked returns the kept items, best first.
func (h *itemHeap[K]) ranked() []*aggregatedItem[K] {
	ranked := h.items
	sort.Slice(ranked, func(i, j int) bool { return h.better(ranked[i], ranked[j]) })
	return ranked
}

func (t *topNAggregateOp[K]) Execute(ctx context.Context) (mit executor.MIterator, err error) {
	iter, execErr := t.Input.(executor.MeasureExecutable).Execute(ctx)
	if execErr != nil {
		return nil, execErr
	}
	defer func() {
		err = multierr.Append(err, iter.Close())
	}()
	extractValue := topNFieldValue[K](t.fieldIdx)
	items := make(map[itemKey]*aggregatedItem[K])
	var retained int64
	for iter.Next() {
		for _, idp := range iter.Current() {
			dp := idp.GetDataPoint()
			entityKey, hasEntity := topNEntityKey(dp)
			if !hasEntity {
				continue
			}
			key := itemKey{entity: entityKey}
			if t.partial() {
				key.shardID = idp.GetShardId()
			}
			item, ok := items[key]
			if !ok {
				retained += int64(2*len(key.entity)+aggregatedItemOverhead) + tagFamiliesSize(dp.GetTagFamilies()) + int64(len(dp.GetFields()[t.fieldIdx].GetName()))
				if retained > topNAggregateStateBudget {
					return nil, errors.Errorf("topN aggregation retains more than %d bytes of per-item state after %d items; narrow the time range or the conditions",
						topNAggregateStateBudget, len(items))
				}
				mapFunc, mapErr := aggregation.NewMap[K](t.aggFunc)
				if mapErr != nil {
					return nil, mapErr
				}
				item = &aggregatedItem[K]{
					key:         strings.Clone(key.entity),
					tagFamilies: detachTagFamilies(dp.GetTagFamilies()),
					fieldName:   strings.Clone(dp.GetFields()[t.fieldIdx].GetName()),
					mapFunc:     mapFunc,
					shardID:     key.shardID,
				}
				items[key] = item
			}
			item.mapFunc.In(extractValue(dp))
		}
	}
	if t.partial() {
		return t.partialAnswer(items)
	}
	return t.finalAnswer(items)
}

// finalAnswer returns the final aggregates of the top N items.
func (t *topNAggregateOp[K]) finalAnswer(items map[itemKey]*aggregatedItem[K]) (executor.MIterator, error) {
	best := &itemHeap[K]{sortDesc: t.sortDesc}
	for _, item := range items {
		best.offer(item, t.topN)
	}
	ranked := best.ranked()
	elements := make([]*measurev1.InternalDataPoint, 0, len(ranked))
	for _, item := range ranked {
		fieldValue, convErr := aggregation.ToFieldValue(item.mapFunc.Val())
		if convErr != nil {
			return nil, convErr
		}
		elements = append(elements, &measurev1.InternalDataPoint{DataPoint: &measurev1.DataPoint{
			TagFamilies: item.tagFamilies,
			Fields:      []*measurev1.DataPoint_Field{{Name: item.fieldName, Value: fieldValue}},
		}})
	}
	return &topNResultIterator{elements: elements, index: -1}, nil
}

// partialAnswer returns, shard by shard, the partial aggregates of the best partialLimit items of each shard.
// Every element carries its shard, and its fields hold the partial as aggregation.PartialToFieldValues encodes
// it: the sum and then the count for MEAN, a single value otherwise.
func (t *topNAggregateOp[K]) partialAnswer(items map[itemKey]*aggregatedItem[K]) (executor.MIterator, error) {
	shards := make(map[uint32]*itemHeap[K])
	for _, item := range items {
		best, ok := shards[item.shardID]
		if !ok {
			best = &itemHeap[K]{sortDesc: t.sortDesc}
			shards[item.shardID] = best
		}
		best.offer(item, t.partialLimit)
	}
	shardIDs := make([]uint32, 0, len(shards))
	for shardID := range shards {
		shardIDs = append(shardIDs, shardID)
	}
	sort.Slice(shardIDs, func(i, j int) bool { return shardIDs[i] < shardIDs[j] })
	var elements []*measurev1.InternalDataPoint
	for _, shardID := range shardIDs {
		for _, item := range shards[shardID].ranked() {
			values, convErr := aggregation.PartialToFieldValues(t.aggFunc, item.mapFunc.Partial())
			if convErr != nil {
				return nil, convErr
			}
			fields := make([]*measurev1.DataPoint_Field, 0, len(values))
			for _, v := range values {
				fields = append(fields, &measurev1.DataPoint_Field{Name: item.fieldName, Value: v})
			}
			elements = append(elements, &measurev1.InternalDataPoint{
				DataPoint: &measurev1.DataPoint{TagFamilies: item.tagFamilies, Fields: fields},
				ShardId:   shardID,
			})
		}
	}
	return &topNResultIterator{elements: elements, index: -1}, nil
}

// ToInternalTopNResponse converts the elements of a partial answer into the response of a data node, one entry
// per shard, in the order the shards first appear. The items of a shard keep their order, best first. The liaison
// counts only the first entry of a shard, so the elements of a shard are grouped whatever their order.
func ToInternalTopNResponse(idps []*measurev1.InternalDataPoint) *measurev1.InternalTopNResponse {
	resp := &measurev1.InternalTopNResponse{}
	shards := make(map[uint32]*measurev1.InternalTopNShard)
	for _, idp := range idps {
		current, ok := shards[idp.GetShardId()]
		if !ok {
			current = &measurev1.InternalTopNShard{ShardId: idp.GetShardId()}
			shards[idp.GetShardId()] = current
			resp.Shards = append(resp.Shards, current)
		}
		dp := idp.GetDataPoint()
		values := make([]*modelv1.FieldValue, 0, len(dp.GetFields()))
		for _, f := range dp.GetFields() {
			values = append(values, f.GetValue())
		}
		current.Partials = append(current.Partials, &measurev1.InternalTopNPartial{
			Entity:  dp.GetTagFamilies()[0].GetTags(),
			Partial: values,
		})
	}
	return resp
}

// tagFamiliesSize estimates the bytes detachTagFamilies retains.
func tagFamiliesSize(families []*modelv1.TagFamily) int64 {
	var size int64
	for _, family := range families {
		size += int64(len(family.GetName())) + 32
		for _, tag := range family.GetTags() {
			size += int64(len(tag.GetKey())) + int64(proto.Size(tag.GetValue())) + 64
		}
	}
	return size
}

// detachTagFamilies copies the tag families so that they do not pin the encoded window buffer that the scan
// decoded them from: the decoded strings alias that buffer.
func detachTagFamilies(families []*modelv1.TagFamily) []*modelv1.TagFamily {
	detached := make([]*modelv1.TagFamily, 0, len(families))
	for _, family := range families {
		tags := make([]*modelv1.Tag, 0, len(family.GetTags()))
		for _, tag := range family.GetTags() {
			tags = append(tags, &modelv1.Tag{Key: strings.Clone(tag.GetKey()), Value: proto.Clone(tag.GetValue()).(*modelv1.TagValue)})
		}
		detached = append(detached, &modelv1.TagFamily{Name: strings.Clone(family.GetName()), Tags: tags})
	}
	return detached
}
