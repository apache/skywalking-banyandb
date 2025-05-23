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

package stream

import (
	"context"
	"fmt"

	"go.uber.org/multierr"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

var _ logical.UnresolvedPlan = (*unresolvedMerger)(nil)

type unresolvedMerger struct {
	criteria      *streamv1.QueryRequest
	metadata      []*commonv1.Metadata
	ecc           []executor.StreamExecutionContext
	tagProjection [][]*logical.Tag
}

// Analyze implements logical.UnresolvedPlan.
func (u *unresolvedMerger) Analyze(s logical.Schema) (logical.Plan, error) {
	ss := s.Children()
	if len(ss) != len(u.metadata) {
		return nil, fmt.Errorf("number of schemas %d not equal to number of metadata %d", len(ss), len(u.metadata))
	}
	projTags := make([]model.TagProjection, len(u.tagProjection))
	if len(u.tagProjection) > 0 {
		for i := range u.tagProjection {
			for _, tag := range u.tagProjection[i] {
				projTags[i].Family = tag.GetFamilyName()
				projTags[i].Names = append(projTags[i].Names, tag.GetTagName())
			}
		}
		projectionTagRefs, errProject := s.CreateTagRef(u.tagProjection...)
		if errProject != nil {
			return nil, errProject
		}
		s = s.ProjTags(projectionTagRefs...)
	}

	mp := &mergePlan{
		s: s,
	}
	for i := range u.metadata {
		subPlan := parseTags(u.criteria, u.metadata[i], u.ecc[i], u.tagProjection)
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
		return nil, fmt.Errorf("index rule %s should have only one tag", u.criteria.OrderBy.IndexRuleName)
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
	_ logical.Plan              = (*mergePlan)(nil)
	_ executor.StreamExecutable = (*mergePlan)(nil)
)

type mergePlan struct {
	s           logical.Schema
	subPlans    []logical.Plan
	sortTagSpec logical.TagSpec
	sortByTime  bool
	desc        bool
}

// Close implements executor.StreamExecutable.
func (m *mergePlan) Close() {
	for _, p := range m.subPlans {
		p.(executor.StreamExecutable).Close()
	}
}

// Execute implements executor.StreamExecutable.
func (m *mergePlan) Execute(ctx context.Context) ([]*streamv1.Element, error) {
	var allErr error
	var see []sort.Iterator[*comparableElement]

	for _, sp := range m.subPlans {
		elements, err := sp.(executor.StreamExecutable).Execute(ctx)
		if err != nil {
			allErr = multierr.Append(allErr, err)
			continue
		}

		iter := &sortableElements{
			elements:     elements,
			isSortByTime: m.sortByTime,
			sortTagSpec:  m.sortTagSpec,
		}
		see = append(see, iter)
	}

	iter := sort.NewItemIter(see, m.desc)
	var result []*streamv1.Element
	for iter.Next() {
		result = append(result, iter.Val().Element)
	}

	return result, allErr
}

// Children implements logical.Plan.
func (m *mergePlan) Children() []logical.Plan {
	return m.subPlans
}

// Schema implements logical.Plan.
func (m *mergePlan) Schema() logical.Schema {
	return m.s
}

// String implements logical.Plan.
func (m *mergePlan) String() string {
	return fmt.Sprintf("MergePlan: subPlans=%d, sortByTime=%t, desc=%t, sortTag=%s",
		len(m.subPlans), m.sortByTime, m.desc, m.sortTagSpec.Spec.GetName())
}
