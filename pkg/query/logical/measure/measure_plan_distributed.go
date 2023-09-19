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
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var _ logical.UnresolvedPlan = (*unresolvedDistributed)(nil)

type unresolvedDistributed struct {
	originalQuery *measurev1.QueryRequest
}

func newUnresolvedDistributed(query *measurev1.QueryRequest) logical.UnresolvedPlan {
	return &unresolvedDistributed{
		originalQuery: query,
	}
}

func (ud *unresolvedDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
	if ud.originalQuery.TagProjection == nil {
		return nil, fmt.Errorf("tag projection is required")
	}
	projectionTags := logical.ToTags(ud.originalQuery.GetTagProjection())
	if len(projectionTags) > 0 {
		var err error
		projTagsRefs, err := s.CreateTagRef(projectionTags...)
		if err != nil {
			return nil, err
		}
		s = s.ProjTags(projTagsRefs...)
	}
	projectionFields := make([]*logical.Field, len(ud.originalQuery.GetFieldProjection().GetNames()))
	for i, fieldNameProj := range ud.originalQuery.GetFieldProjection().GetNames() {
		projectionFields[i] = logical.NewField(fieldNameProj)
	}
	if len(projectionFields) > 0 {
		var err error
		projFieldRefs, err := s.CreateFieldRef(projectionFields...)
		if err != nil {
			return nil, err
		}
		s = s.ProjFields(projFieldRefs...)
	}
	limit := ud.originalQuery.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	temp := &measurev1.QueryRequest{
		TagProjection:   ud.originalQuery.TagProjection,
		FieldProjection: ud.originalQuery.FieldProjection,
		Metadata:        ud.originalQuery.Metadata,
		Criteria:        ud.originalQuery.Criteria,
		Limit:           limit,
		OrderBy:         ud.originalQuery.OrderBy,
	}
	if ud.originalQuery.OrderBy == nil {
		return &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}, nil
	}
	if ud.originalQuery.OrderBy.IndexRuleName == "" {
		result := &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    true,
		}
		if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
	}
	ok, indexRule := s.IndexRuleDefined(ud.originalQuery.OrderBy.IndexRuleName)
	if !ok {
		return nil, fmt.Errorf("index rule %s not found", ud.originalQuery.OrderBy.IndexRuleName)
	}
	if len(indexRule.Tags) != 1 {
		return nil, fmt.Errorf("index rule %s should have only one tag", ud.originalQuery.OrderBy.IndexRuleName)
	}
	sortTagSpec := s.FindTagSpecByName(indexRule.Tags[0])
	if sortTagSpec == nil {
		return nil, fmt.Errorf("tag %s not found", indexRule.Tags[0])
	}
	result := &distributedPlan{
		queryTemplate: temp,
		s:             s,
		sortByTime:    false,
		sortTagSpec:   *sortTagSpec,
	}
	if ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
		result.desc = true
	}
	return result, nil
}

type distributedPlan struct {
	s                 logical.Schema
	queryTemplate     *measurev1.QueryRequest
	sortTagSpec       logical.TagSpec
	sortByTime        bool
	desc              bool
	maxDataPointsSize uint32
}

func (t *distributedPlan) Execute(ctx context.Context) (executor.MIterator, error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	query := proto.Clone(t.queryTemplate).(*measurev1.QueryRequest)
	query.TimeRange = dctx.TimeRange()
	if t.maxDataPointsSize > 0 {
		query.Limit = t.maxDataPointsSize
	}
	ff, err := dctx.Broadcast(data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(dctx.TimeRange().Begin.Nanos), query))
	if err != nil {
		return nil, err
	}
	var allErr error
	var see []sort.Iterator[*comparableDataPoint]
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			allErr = multierr.Append(allErr, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			see = append(see,
				newSortableElements(d.(*measurev1.QueryResponse).DataPoints,
					t.sortByTime, t.sortTagSpec))
		}
	}
	return &sortedMIterator{
		Iterator: sort.NewItemIter[*comparableDataPoint](see, t.desc),
	}, allErr
}

func (t *distributedPlan) String() string {
	return fmt.Sprintf("distributed:%s", t.queryTemplate.String())
}

func (t *distributedPlan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (t *distributedPlan) Schema() logical.Schema {
	return t.s
}

func (t *distributedPlan) Limit(max int) {
	t.maxDataPointsSize = uint32(max)
}

var _ sort.Comparable = (*comparableDataPoint)(nil)

type comparableDataPoint struct {
	*measurev1.DataPoint
	sortField []byte
}

func newComparableElement(e *measurev1.DataPoint, sortByTime bool, sortTagSpec logical.TagSpec) (*comparableDataPoint, error) {
	var sortField []byte
	if sortByTime {
		sortField = convert.Uint64ToBytes(uint64(e.Timestamp.AsTime().UnixNano()))
	} else {
		var err error
		sortField, err = pbv1.MarshalTagValue(e.TagFamilies[sortTagSpec.TagFamilyIdx].Tags[sortTagSpec.TagIdx].Value)
		if err != nil {
			return nil, err
		}
	}

	return &comparableDataPoint{
		DataPoint: e,
		sortField: sortField,
	}, nil
}

func (e *comparableDataPoint) SortedField() []byte {
	return e.sortField
}

var _ sort.Iterator[*comparableDataPoint] = (*sortableElements)(nil)

type sortableElements struct {
	cur          *comparableDataPoint
	dataPoints   []*measurev1.DataPoint
	sortTagSpec  logical.TagSpec
	index        int
	isSortByTime bool
}

func newSortableElements(dataPoints []*measurev1.DataPoint, isSortByTime bool, sortTagSpec logical.TagSpec) *sortableElements {
	return &sortableElements{
		dataPoints:   dataPoints,
		isSortByTime: isSortByTime,
		sortTagSpec:  sortTagSpec,
	}
}

func (*sortableElements) Close() error {
	return nil
}

func (s *sortableElements) Next() bool {
	return s.iter(func(e *measurev1.DataPoint) (*comparableDataPoint, error) {
		return newComparableElement(e, s.isSortByTime, s.sortTagSpec)
	})
}

func (s *sortableElements) Val() *comparableDataPoint {
	return s.cur
}

func (s *sortableElements) iter(fn func(*measurev1.DataPoint) (*comparableDataPoint, error)) bool {
	if s.index >= len(s.dataPoints) {
		return false
	}
	cur, err := fn(s.dataPoints[s.index])
	s.index++
	if err != nil {
		return s.iter(fn)
	}
	s.cur = cur
	return s.index <= len(s.dataPoints)
}

var _ executor.MIterator = (*sortedMIterator)(nil)

type sortedMIterator struct {
	sort.Iterator[*comparableDataPoint]
}

func (s *sortedMIterator) Current() []*measurev1.DataPoint {
	return []*measurev1.DataPoint{s.Val().DataPoint}
}
