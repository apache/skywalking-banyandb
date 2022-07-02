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

package tsdb

import (
	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var (
	_ LogicalIndexedCondition = (*indexedCondition)(nil)
	_ LogicalIndexedCondition = (*andIndexedConditions)(nil)
	_ LogicalIndexedCondition = (*orIndexedConditions)(nil)
)

type LogicalIndexedCondition interface {
	build(seriesSpan SeriesSpan, b BlockDelegate) (ItemFilter, error)
}

type indexedCondition struct {
	indexRuleType databasev1.IndexRule_Type
	indexRuleID   uint32
	condition     Condition
}

func NewConditionWithIndexRule(condition Condition, indexRule *databasev1.IndexRule) LogicalIndexedCondition {
	return &indexedCondition{
		condition:     condition,
		indexRuleID:   indexRule.GetMetadata().GetId(),
		indexRuleType: indexRule.GetType(),
	}
}

func (i *indexedCondition) buildConditions(seriesID common.SeriesID) index.Condition {
	cond := make(index.Condition)
	term := index.FieldKey{
		IndexRuleID: i.indexRuleID,
		SeriesID:    seriesID,
	}
	for _, cv := range i.condition {
		cond[term] = cv
		break
	}
	return cond
}

func (i *indexedCondition) build(seriesSpan SeriesSpan, b BlockDelegate) (ItemFilter, error) {
	seriesID := seriesSpan.SeriesID()
	switch i.indexRuleType {
	case databasev1.IndexRule_TYPE_INVERTED:
		list, valid, innerErr := buildSingleFilterSet(b.invertedIndexReader(), i.buildConditions(seriesID), seriesID, i.indexRuleID)
		if innerErr != nil {
			return nil, innerErr
		}
		return &roaringBitmapFilter{
			list:  list,
			valid: valid,
		}, nil
	case databasev1.IndexRule_TYPE_TREE:
		list, valid, innerErr := buildSingleFilterSet(b.lsmIndexReader(), i.buildConditions(seriesID), seriesID, i.indexRuleID)
		if innerErr != nil {
			return nil, innerErr
		}
		return &roaringBitmapFilter{
			list:  list,
			valid: valid,
		}, nil
	default:
		return nil, ErrUnsupportedIndexRule
	}
}

type andIndexedConditions []LogicalIndexedCondition

func AndConditions(conditions ...LogicalIndexedCondition) LogicalIndexedCondition {
	return andIndexedConditions(conditions)
}

func (a andIndexedConditions) build(seriesSpan SeriesSpan, b BlockDelegate) (ItemFilter, error) {
	var singleFilter ItemFilter
	for _, child := range a {
		filter, err := child.build(seriesSpan, b)
		if err != nil {
			return nil, err
		}
		if singleFilter == nil {
			singleFilter = filter
		} else {
			var innerErr error
			singleFilter, innerErr = singleFilter.And(filter)
			if innerErr != nil {
				return nil, innerErr
			}
		}
	}
	if singleFilter == nil {
		return TrueFilter(), nil
	}
	return singleFilter, nil
}

type orIndexedConditions []LogicalIndexedCondition

func OrConditions(conditions ...LogicalIndexedCondition) LogicalIndexedCondition {
	return orIndexedConditions(conditions)
}

func (o orIndexedConditions) build(seriesSpan SeriesSpan, b BlockDelegate) (ItemFilter, error) {
	var singleFilter ItemFilter
	for _, child := range o {
		filter, err := child.build(seriesSpan, b)
		if err != nil {
			return nil, err
		}
		if singleFilter == nil {
			singleFilter = filter
		} else {
			var innerErr error
			singleFilter, innerErr = singleFilter.Or(filter)
			if innerErr != nil {
				return nil, innerErr
			}
		}
	}
	if singleFilter == nil {
		return TrueFilter(), nil
	}
	return singleFilter, nil
}

func TrueFilter() ItemFilter {
	return FilterFn(func(item Item) bool {
		return true
	})
}

func FalseFilter() ItemFilter {
	return FilterFn(func(item Item) bool {
		return false
	})
}

func EmptyFilter() ItemFilter {
	return FilterFn(nil)
}

type ItemFilter interface {
	Predicate(Item) bool
	And(filters ...ItemFilter) (ItemFilter, error)
	Or(filters ...ItemFilter) (ItemFilter, error)
}

type roaringBitmapFilter struct {
	list  posting.List
	valid bool
}

func (r *roaringBitmapFilter) Predicate(i Item) bool {
	return r.list.Contains(i.ID())
}

func (r *roaringBitmapFilter) And(filters ...ItemFilter) (ItemFilter, error) {
	allList := r.list
	for _, filter := range filters {
		if rbf, ok := filter.(*roaringBitmapFilter); ok {
			innerErr := allList.Intersect(rbf.list)
			if innerErr != nil {
				return nil, innerErr
			}
			// if one of the list is empty, it would always return false
			if allList.IsEmpty() {
				return FilterFn(func(_ Item) bool {
					return false
				}), nil
			}
		}
	}
	return &roaringBitmapFilter{
		list: allList,
	}, nil
}

func (r *roaringBitmapFilter) Or(filters ...ItemFilter) (ItemFilter, error) {
	allList := r.list
	for _, filter := range filters {
		if rbf, ok := filter.(*roaringBitmapFilter); ok {
			innerErr := allList.Union(rbf.list)
			if innerErr != nil {
				return nil, innerErr
			}
		}
	}
	return &roaringBitmapFilter{
		list: allList,
	}, nil
}

func (f FilterFn) And(filters ...ItemFilter) (ItemFilter, error) {
	if f == nil {
		return and(filters), nil
	}
	return and(append([]ItemFilter{f}, filters...)), nil
}

func (f FilterFn) Or(filters ...ItemFilter) (ItemFilter, error) {
	if f == nil {
		return or(filters), nil
	}
	return or(append([]ItemFilter{f}, filters...)), nil
}

func (f FilterFn) Predicate(item Item) bool {
	if f == nil {
		panic("Call Predicate on a nil object will cause undefined behavior")
	}
	return f(item)
}

type and []ItemFilter

func (allFilters and) Predicate(item Item) bool {
	for _, filter := range allFilters {
		if !filter.Predicate(item) {
			return false
		}
	}
	return true
}

func (allFilters and) And(filters ...ItemFilter) (ItemFilter, error) {
	return append(allFilters, filters...), nil
}

func (allFilters and) Or(filters ...ItemFilter) (ItemFilter, error) {
	if len(allFilters) == 0 {
		return or(filters), nil
	}
	return or(append([]ItemFilter{allFilters}, filters...)), nil
}

type or []ItemFilter

func (allFilters or) Predicate(item Item) bool {
	for _, filter := range allFilters {
		if filter.Predicate(item) {
			return true
		}
	}
	return false
}

func (allFilters or) And(filters ...ItemFilter) (ItemFilter, error) {
	if len(filters) == 0 {
		return and(filters), nil
	}
	return and(append([]ItemFilter{allFilters}, filters...)), nil
}

func (allFilters or) Or(filters ...ItemFilter) (ItemFilter, error) {
	return append(allFilters, filters...), nil
}
