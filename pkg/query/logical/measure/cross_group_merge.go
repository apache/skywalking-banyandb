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
	"fmt"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// CrossGroupMergeOrder is the resolved ordering for a multi-group
// (multi-measure) query: which field each per-group stream is sorted on
// and whether the k-way merge is descending. It is derived from the
// request's OrderBy against the projected merged multi-group schema,
// reproducing exactly the row path's unresolvedMerger.Analyze ordering.
type CrossGroupMergeOrder struct {
	sortTagSpec logical.TagSpec
	sortByTime  bool
	desc        bool
}

// resolveCrossGroupMergeOrder is the order-resolution body shared
// verbatim by the row unresolvedMerger.Analyze and the exported
// ResolveCrossGroupMergeOrder. s MUST be the projected merged schema
// (post-ProjTags when a tag projection is set) so an index-rule
// OrderBy's TagFamilyIdx/TagIdx match the projected InternalDataPoint
// layout newComparableElement indexes into.
func resolveCrossGroupMergeOrder(orderBy *modelv1.QueryOrder, s logical.Schema) (CrossGroupMergeOrder, error) {
	var order CrossGroupMergeOrder
	if orderBy == nil {
		order.sortByTime = true
		return order, nil
	}
	if orderBy.IndexRuleName == "" {
		order.sortByTime = true
		if orderBy.Sort == modelv1.Sort_SORT_DESC {
			order.desc = true
		}
		return order, nil
	}

	ok, indexRule := s.IndexRuleDefined(orderBy.IndexRuleName)
	if !ok {
		return order, fmt.Errorf("index rule %s not found", orderBy.IndexRuleName)
	}
	if len(indexRule.Tags) != 1 {
		return order, fmt.Errorf("index rule %s should have one tag", orderBy.IndexRuleName)
	}
	sortTagSpec := s.FindTagSpecByName(indexRule.Tags[0])
	if sortTagSpec == nil {
		return order, fmt.Errorf("tag %s not found", indexRule.Tags[0])
	}
	order.sortTagSpec = *sortTagSpec
	if orderBy.Sort == modelv1.Sort_SORT_DESC {
		order.desc = true
	}
	return order, nil
}

// ResolveCrossGroupMergeOrder reproduces the projected-merged-schema
// resolution that Analyze + unresolvedMerger.Analyze perform for a
// multi-group request, so a caller holding per-group executor.MIterator
// streams (banyand/query/processor.go, G9f.1) merges them with the row
// path's exact cross-group ordering. ss are the per-group logical
// schemas in request order; they are merged via mergeSchema and, when
// criteria carries a tag projection, projected via ProjTags exactly as
// the row analyzer does before the OrderBy is resolved.
func ResolveCrossGroupMergeOrder(criteria *measurev1.QueryRequest, ss []logical.Schema) (CrossGroupMergeOrder, error) {
	var order CrossGroupMergeOrder
	s, mergeErr := mergeSchema(ss)
	if mergeErr != nil {
		return order, mergeErr
	}
	if s == nil {
		if criteria.GetOrderBy() == nil || criteria.GetOrderBy().GetIndexRuleName() == "" {
			return resolveCrossGroupMergeOrder(criteria.GetOrderBy(), nil)
		}
		return order, fmt.Errorf("cannot resolve order_by %s: no schema", criteria.GetOrderBy().GetIndexRuleName())
	}
	tagProjection := logical.ToTags(criteria.GetTagProjection())
	if len(tagProjection) > 0 {
		projectionTagRefs, refErr := s.CreateTagRef(tagProjection...)
		if refErr != nil {
			return order, refErr
		}
		s = s.ProjTags(projectionTagRefs...)
	}
	return resolveCrossGroupMergeOrder(criteria.GetOrderBy(), s)
}

// MergeGroupMIterators wraps the per-group executor.MIterator streams in
// the identical sortableDataPoints -> sort.NewItemIter -> sortedMIterator
// stack mergePlan.Execute builds for the row path. Because the merge and
// equal-SortedField version dedup machinery is shared verbatim, the
// cross-group order and dedup are reproduced by construction, not by a
// parallel reimplementation. The input iters are consumed lazily and are
// closed by the returned iterator's Close.
func MergeGroupMIterators(iters []executor.MIterator, order CrossGroupMergeOrder) executor.MIterator {
	sortableIters := make([]sort.Iterator[*comparableDataPoint], 0, len(iters))
	for _, it := range iters {
		sortableIters = append(sortableIters, &sortableDataPoints{
			iter:        it,
			sortTagSpec: order.sortTagSpec,
			sortByTime:  order.sortByTime,
		})
	}
	merged := &sortedMIterator{
		Iterator: sort.NewItemIter(sortableIters, order.desc),
	}
	merged.init()
	return merged
}
