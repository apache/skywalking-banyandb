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
	"bytes"
	"container/list"
	"context"
	"fmt"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

const defaultQueryTimeout = 30 * time.Second

var _ logical.UnresolvedPlan = (*unresolvedDistributed)(nil)

type unresolvedDistributed struct {
	originalQuery *measurev1.QueryRequest
	groupByEntity bool
}

func newUnresolvedDistributed(query *measurev1.QueryRequest) logical.UnresolvedPlan {
	return &unresolvedDistributed{
		originalQuery: query,
	}
}

func (ud *unresolvedDistributed) Analyze(s logical.Schema) (logical.Plan, error) {
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
		Name:            ud.originalQuery.Name,
		Groups:          ud.originalQuery.Groups,
		Criteria:        ud.originalQuery.Criteria,
		Limit:           limit + ud.originalQuery.Offset,
		OrderBy:         ud.originalQuery.OrderBy,
	}
	if ud.groupByEntity {
		e := s.EntityList()[0]
		sortTagSpec := s.FindTagSpecByName(e)
		if sortTagSpec == nil {
			return nil, fmt.Errorf("entity tag %s not found", e)
		}
		result := &distributedPlan{
			queryTemplate: temp,
			s:             s,
			sortByTime:    false,
			sortTagSpec:   *sortTagSpec,
		}
		if ud.originalQuery.OrderBy != nil && ud.originalQuery.OrderBy.Sort == modelv1.Sort_SORT_DESC {
			result.desc = true
		}
		return result, nil
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

func (t *distributedPlan) Execute(ctx context.Context) (mi executor.MIterator, err error) {
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(t.queryTemplate).(*measurev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()
	if t.maxDataPointsSize > 0 {
		queryRequest.Limit = t.maxDataPointsSize
	}
	tracer := query.GetTracer(ctx)
	var span *query.Span
	if tracer != nil {
		span, _ = tracer.StartSpan(ctx, "distributed-client")
		queryRequest.Trace = true
		span.Tag("request", convert.BytesToString(logger.Proto(queryRequest)))
		defer func() {
			if err != nil {
				span.Error(err)
			} else {
				span.Stop()
			}
		}()
	}
	ff, err := dctx.Broadcast(defaultQueryTimeout, data.TopicMeasureQuery, bus.NewMessage(bus.MessageID(dctx.TimeRange().Begin.Nanos), queryRequest))
	if err != nil {
		return nil, err
	}
	var see []sort.Iterator[*comparableDataPoint]
	for _, f := range ff {
		if m, getErr := f.Get(); getErr != nil {
			err = multierr.Append(err, getErr)
		} else {
			d := m.Data()
			if d == nil {
				continue
			}
			resp := d.(*measurev1.QueryResponse)
			if span != nil {
				span.AddSubTrace(resp.Trace)
			}
			see = append(see,
				newSortableElements(resp.DataPoints,
					t.sortByTime, t.sortTagSpec))
		}
	}
	smi := &sortedMIterator{
		Iterator: sort.NewItemIter(see, t.desc),
	}
	smi.init()
	return smi, err
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
	data        *list.List
	uniqueData  map[uint64]*measurev1.DataPoint
	cur         *measurev1.DataPoint
	initialized bool
	closed      bool
}

func (s *sortedMIterator) init() {
	if s.initialized {
		return
	}
	s.initialized = true
	if !s.Iterator.Next() {
		s.closed = true
		return
	}
	s.data = list.New()
	s.uniqueData = make(map[uint64]*measurev1.DataPoint)
	s.loadDps()
}

func (s *sortedMIterator) Next() bool {
	if s.data == nil {
		return false
	}
	if s.data.Len() == 0 {
		s.loadDps()
		if s.data.Len() == 0 {
			return false
		}
	}
	dp := s.data.Front()
	s.data.Remove(dp)
	s.cur = dp.Value.(*measurev1.DataPoint)
	return true
}

func (s *sortedMIterator) loadDps() {
	if s.closed {
		return
	}
	for k := range s.uniqueData {
		delete(s.uniqueData, k)
	}
	first := s.Iterator.Val()
	s.uniqueData[hashDataPoint(first.DataPoint)] = first.DataPoint
	for {
		if !s.Iterator.Next() {
			s.closed = true
			break
		}
		v := s.Iterator.Val()
		if bytes.Equal(first.SortedField(), v.SortedField()) {
			key := hashDataPoint(v.DataPoint)
			if existed, ok := s.uniqueData[key]; ok {
				if v.DataPoint.Version > existed.Version {
					s.uniqueData[key] = v.DataPoint
				}
			} else {
				s.uniqueData[key] = v.DataPoint
			}
		} else {
			break
		}
	}
	for _, v := range s.uniqueData {
		s.data.PushBack(v)
	}
}

func (s *sortedMIterator) Current() []*measurev1.DataPoint {
	return []*measurev1.DataPoint{s.cur}
}

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// hashDataPoint calculates the hash value of a data point with fnv64a.
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
func hashDataPoint(dp *measurev1.DataPoint) uint64 {
	h := uint64(offset64)
	h = (h ^ dp.Sid) * prime64
	h = (h ^ uint64(dp.Timestamp.Seconds)) * prime64
	h = (h ^ uint64(dp.Timestamp.Nanos)) * prime64
	return h
}
