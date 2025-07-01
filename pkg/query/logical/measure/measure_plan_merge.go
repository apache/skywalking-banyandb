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
	"context"
	"fmt"

	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

var _ logical.UnresolvedPlan = (*unresolvedMerger)(nil)

type unresolvedMerger struct {
	criteria      *measurev1.QueryRequest
	metadata      []*commonv1.Metadata
	ecc           []executor.MeasureExecutionContext
	tagProjection [][]*logical.Tag
	groupByEntity bool
}

func (u *unresolvedMerger) Analyze(s logical.Schema) (logical.Plan, error) {
	ss := s.Children()
	if len(ss) != len(u.metadata) {
		return nil, fmt.Errorf("number of schemas %d not equal to metadata count %d", len(ss), len(u.metadata))
	}

	projTags := make([]model.TagProjection, len(u.tagProjection))
	if len(u.tagProjection) > 0 {
		for i := range u.tagProjection {
			for _, tag := range u.tagProjection[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
			}
		}
		projectionTagRefs, err := s.CreateTagRef(u.tagProjection...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projectionTagRefs...)
	}

	mp := &mergePlan{
		s: s,
	}

	for i := range u.metadata {
		subPlan := parseFields(u.criteria, u.metadata[i], u.ecc[i], u.groupByEntity, u.tagProjection)
		sp, err := subPlan.Analyze(ss[i])
		if err != nil {
			return nil, err
		}
		mp.subPlans = append(mp.subPlans, sp)
	}

	if u.criteria.OrderBy == nil {
		mp.sortByTime = true
		return mp, nil
	}

	if u.criteria.OrderBy.IndexRuleName == "" {
		mp.sortByTime = true
		if u.criteria.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			mp.desc = true
		}
		return mp, nil
	}

	ok, indexRule := s.IndexRuleDefined(u.criteria.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", u.criteria.OrderBy.IndexRuleName)
	}

	if len(indexRule.Tags) != 1 {
		return nil, fmt.Errorf("index rule %s should have one tag", u.criteria.OrderBy.IndexRuleName)
	}

	sortTagSpec := s.FindTagSpecByName(indexRule.Tags[0])
	if sortTagSpec == nil {
		return nil, fmt.Errorf("tag %s not found", indexRule.Tags[0])
	}

	mp.sortTagSpec = *sortTagSpec
	if u.criteria.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		mp.desc = true
	}

	return mp, nil
}

var (
	_ logical.Plan               = (*mergePlan)(nil)
	_ executor.MeasureExecutable = (*mergePlan)(nil)
)

type mergePlan struct {
	s           logical.Schema
	subPlans    []logical.Plan
	sortTagSpec logical.TagSpec
	sortByTime  bool
	desc        bool
}

func (m *mergePlan) Execute(ctx context.Context) (executor.MIterator, error) {
	var allErr error
	var iters []sort.Iterator[*comparableDataPoint]

	for _, sp := range m.subPlans {
		iter, err := sp.(executor.MeasureExecutable).Execute(ctx)
		if err != nil {
			allErr = multierr.Append(allErr, err)
			continue
		}

		see := &sortableDataPoints{
			iter:        iter,
			sortTagSpec: m.sortTagSpec,
			sortByTime:  m.sortByTime,
		}
		iters = append(iters, see)
	}
	if allErr != nil {
		return nil, allErr
	}

	iter := &sortedMIterator{
		Iterator: sort.NewItemIter(iters, m.desc),
	}
	iter.init()
	return iter, nil
}

func (m *mergePlan) Children() []logical.Plan {
	return m.subPlans
}

func (m *mergePlan) Schema() logical.Schema {
	return m.s
}

func (m *mergePlan) String() string {
	return fmt.Sprintf("MergePlan: subPlans=%d, sortByTime=%t, desc=%t, sortTag=%s",
		len(m.subPlans), m.sortByTime, m.desc, m.sortTagSpec.Spec.GetName())
}

type sortableDataPoints struct {
	iter        executor.MIterator
	current     *comparableDataPoint
	sortTagSpec logical.TagSpec
	sortByTime  bool
}

func (s *sortableDataPoints) Next() bool {
	if !s.iter.Next() {
		return false
	}

	dp := s.iter.Current()[0]
	var err error
	s.current, err = newComparableElement(dp, s.sortByTime, s.sortTagSpec)
	return err == nil
}

func (s *sortableDataPoints) Val() *comparableDataPoint {
	return s.current
}

func (s *sortableDataPoints) Close() error {
	return s.iter.Close()
}
