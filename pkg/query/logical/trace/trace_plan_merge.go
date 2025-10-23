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

package trace

import (
	"context"
	"fmt"

	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

var _ logical.UnresolvedPlan = (*unresolvedTraceMerger)(nil)

type unresolvedTraceMerger struct {
	criteria          *tracev1.QueryRequest
	metadata          []*commonv1.Metadata
	ecc               []executor.TraceExecutionContext
	tagProjection     [][]*logical.Tag
	traceIDTagNames   []string
	spanIDTagNames    []string
	timestampTagNames []string
}

// Analyze implements logical.UnresolvedPlan.
func (u *unresolvedTraceMerger) Analyze(s logical.Schema) (logical.Plan, error) {
	ss := s.Children()
	if len(ss) != len(u.metadata) {
		return nil, fmt.Errorf("number of schemas %d not equal to metadata count %d", len(ss), len(u.metadata))
	}

	if len(u.tagProjection) > 0 {
		projectionTagRefs, err := s.CreateTagRef(u.tagProjection...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projectionTagRefs...)
	}
	mp := &traceMergePlan{
		s: s,
	}

	for i := range u.metadata {
		var orderByTag string
		if u.criteria.OrderBy != nil && u.criteria.OrderBy.IndexRuleName != "" {
			ok, indexRule := ss[i].IndexRuleDefined(u.criteria.OrderBy.IndexRuleName)
			if !ok {
				return nil, fmt.Errorf("index rule %s not found in schema %d", u.criteria.OrderBy.IndexRuleName, i)
			}
			tags := indexRule.GetTags()
			if len(tags) > 0 {
				orderByTag = tags[len(tags)-1]
			}
		}
		subPlan := parseTraceTags(u.criteria, u.metadata[i], u.ecc[i], u.tagProjection, u.traceIDTagNames[i], u.spanIDTagNames[i], u.timestampTagNames[i], orderByTag, i)
		sp, err := subPlan.Analyze(ss[i])
		if err != nil {
			return nil, err
		}
		mp.subPlans = append(mp.subPlans, sp)
	}

	if u.criteria.OrderBy == nil {
		mp.sortByTraceID = true
		return mp, nil
	}
	if u.criteria.OrderBy.IndexRuleName == "" {
		mp.sortByTraceID = true
		if u.criteria.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			mp.desc = true
		}
		return mp, nil
	}
	mp.sortByTraceID = false
	if u.criteria.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		mp.desc = true
	}
	return mp, nil
}

var (
	_ logical.Plan             = (*traceMergePlan)(nil)
	_ executor.TraceExecutable = (*traceMergePlan)(nil)
)

type traceMergePlan struct {
	s             logical.Schema
	subPlans      []logical.Plan
	sortByTraceID bool
	desc          bool
}

func (t *traceMergePlan) Close() {
	for _, sp := range t.subPlans {
		sp.(executor.TraceExecutable).Close()
	}
}

func (t *traceMergePlan) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	var allErr error
	var iters []sort.Iterator[*comparableTraceResult]

	for _, sp := range t.subPlans {
		resultIter, err := sp.(executor.TraceExecutable).Execute(ctx)
		if err != nil {
			allErr = multierr.Append(allErr, err)
			continue
		}
		iter := newSortableTraceResults(resultIter, t.sortByTraceID)
		iters = append(iters, iter)
	}
	if allErr != nil {
		return iter.Empty[model.TraceResult](), allErr
	}

	sortedIter := sort.NewItemIter(iters, t.desc)
	return &mergedTraceResultIterator{
		Iterator: sortedIter,
	}, nil
}

func (t *traceMergePlan) Children() []logical.Plan {
	return t.subPlans
}

func (t *traceMergePlan) Schema() logical.Schema {
	return t.s
}

func (t *traceMergePlan) String() string {
	return fmt.Sprintf("TraceMergePlan: subPlans=%d, sortByTraceID=%t, desc=%t",
		len(t.subPlans), t.sortByTraceID, t.desc)
}

type comparableTraceResult struct {
	sortedField []byte
	result      model.TraceResult
	groupIndex  int
}

func newComparableTraceResult(result model.TraceResult, sortByTraceID bool, groupIndex int) *comparableTraceResult {
	ct := &comparableTraceResult{
		result:     result,
		groupIndex: groupIndex,
	}
	if sortByTraceID {
		ct.sortedField = []byte(result.TID)
	} else {
		ct.sortedField = convert.Int64ToBytes(result.Key)
	}
	return ct
}

func (c *comparableTraceResult) SortedField() []byte {
	return c.sortedField
}

type sortableTraceResults struct {
	iter          iter.Iterator[model.TraceResult]
	current       *comparableTraceResult
	sortByTraceID bool
}

func newSortableTraceResults(iter iter.Iterator[model.TraceResult], sortByTraceID bool) *sortableTraceResults {
	return &sortableTraceResults{
		iter:          iter,
		sortByTraceID: sortByTraceID,
	}
}

func (s *sortableTraceResults) Next() bool {
	result, hasNext := s.iter.Next()
	if !hasNext {
		return false
	}
	if result.Error != nil {
		return false
	}

	s.current = newComparableTraceResult(result, s.sortByTraceID, result.GroupIndex)
	return true
}

func (s *sortableTraceResults) Val() *comparableTraceResult {
	return s.current
}

func (s *sortableTraceResults) Close() error {
	return nil
}

type mergedTraceResultIterator struct {
	Iterator sort.Iterator[*comparableTraceResult]
}

func (s *mergedTraceResultIterator) Next() (model.TraceResult, bool) {
	if !s.Iterator.Next() {
		return model.TraceResult{}, false
	}

	ct := s.Iterator.Val()
	if ct == nil {
		return model.TraceResult{}, false
	}
	return ct.result, true
}
