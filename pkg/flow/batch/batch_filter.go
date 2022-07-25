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

package batch

import (
	"context"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	batchApi "github.com/apache/skywalking-banyandb/pkg/flow/batch/api"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

var (
	// func (tsdb.Series) tsdb.SeriesSpan
	_ batchApi.Operator[tsdb.Series, tsdb.SeriesSpan] = (*timeRangeOperator)(nil)
	// func (tsdb.SeriesSpan) tsdb.Item
	_ batchApi.Operator[tsdb.SeriesSpan, tsdb.Item] = (*conditionalFilterOperator)(nil)
	_ api.UnaryOperation[bool]                      = (*conditionalFilterOperator)(nil)
)

type timeRangeOperator struct {
	f *batchFlow
}

func (tro *timeRangeOperator) Analyze(schema logical.Schema) (logical.Schema, error) {
	var err error
	tro.f.filter, err = tro.f.filter.And(tsdb.FilterFn(func(item tsdb.Item) bool {
		valid := tro.f.src.TimeRange().Contains(item.Time())
		return valid
	}))
	if err != nil {
		return schema, err
	}
	return schema, nil
}

func (tro *timeRangeOperator) Transform(input iter.Iterator[tsdb.Series]) iter.Iterator[tsdb.SeriesSpan] {
	return iter.Map(input, func(series tsdb.Series) tsdb.SeriesSpan {
		seriesSpan, err := series.Span(tro.f.src.TimeRange())
		if err != nil {
			// TODO: error?
			return nil
		}
		return seriesSpan
	})
}

type conditionalFilterOperator struct {
	f *batchFlow
	// unresolvedCriteria is the unresolved criteria given by users
	unresolvedCriteria []*modelv1.Criteria
	// resolvedCriteria is the resolved condition attached with index rules
	resolvedCriteria tsdb.LogicalIndexedCondition
}

func NewConditionalFilter(criteria ...*modelv1.Criteria) api.UnaryOperation[bool] {
	return &conditionalFilterOperator{
		unresolvedCriteria: criteria,
	}
}

func (cfo *conditionalFilterOperator) Apply(ctx context.Context, data interface{}) bool {
	// TODO implement me
	panic("implement me")
}

func (cfo *conditionalFilterOperator) Analyze(schema logical.Schema) (logical.Schema, error) {
	entityList := schema.EntityList()
	cfo.f.entity = make([]tsdb.Entry, len(entityList))
	for idx, e := range entityList {
		cfo.f.entityMap[e] = idx
		// fill AnyEntry by default
		cfo.f.entity[idx] = tsdb.AnyEntry
	}

	var tagExprs []logical.Expr
	for _, criteriaFamily := range cfo.unresolvedCriteria {
		for _, pairQuery := range criteriaFamily.GetConditions() {
			op := pairQuery.GetOp()
			typedTagValue := pairQuery.GetValue()
			var e logical.Expr
			switch v := typedTagValue.GetValue().(type) {
			case *modelv1.TagValue_Str:
				if entityIdx, ok := cfo.f.entityMap[pairQuery.GetName()]; ok {
					cfo.f.entity[entityIdx] = []byte(v.Str.GetValue())
				} else {
					e = logical.Str(v.Str.GetValue())
				}
			case *modelv1.TagValue_Id:
				if entityIdx, ok := cfo.f.entityMap[pairQuery.GetName()]; ok {
					cfo.f.entity[entityIdx] = []byte(v.Id.GetValue())
				} else {
					e = logical.ID(v.Id.GetValue())
				}
			case *modelv1.TagValue_StrArray:
				e = logical.Strs(v.StrArray.GetValue()...)
			case *modelv1.TagValue_Int:
				if entityIdx, ok := cfo.f.entityMap[pairQuery.GetName()]; ok {
					cfo.f.entity[entityIdx] = convert.Int64ToBytes(v.Int.GetValue())
				} else {
					e = logical.Int(v.Int.GetValue())
				}
			case *modelv1.TagValue_IntArray:
				e = logical.Ints(v.IntArray.GetValue()...)
			default:
				return nil, logical.ErrInvalidConditionType
			}
			// we collect Condition only if it is not a part of entity
			if e != nil {
				tagExprs = append(tagExprs, logical.OpFactory(op)(logical.NewTagRef(criteriaFamily.GetTagFamilyName(), pairQuery.GetName()), e))
			}
		}
	}

	conditionMap, err := logical.ParseConditionMap(tagExprs, schema)
	if err != nil {
		return nil, err
	}

	conds := make([]tsdb.LogicalIndexedCondition, 0, len(conditionMap))
	for idxRule, exprsPerIndex := range conditionMap {
		conds = append(conds, tsdb.NewConditionWithIndexRule(logical.ExprToCondition(exprsPerIndex), idxRule))
	}

	// TODO: we have to support AND and/or OR
	cfo.resolvedCriteria = tsdb.AndConditions(conds...)

	return schema, nil
}

func (cfo *conditionalFilterOperator) Transform(input iter.Iterator[tsdb.SeriesSpan]) iter.Iterator[tsdb.Item] {
	return iter.Flatten(iter.Flatten(iter.Map(input, func(seriesSpan tsdb.SeriesSpan) iter.Iterator[iter.Iterator[tsdb.Item]] {
		var itemIterators []iter.Iterator[tsdb.Item]
		for _, b := range seriesSpan.Blocks() {
			var err error
			// blockFilter represents all conditions including AND, OR logical operations
			conditionalFilter, err := seriesSpan.BuildConditionalFilter(b, cfo.resolvedCriteria)
			if err != nil {
				continue
			}
			blockFilter, err := cfo.f.filter.And(conditionalFilter)
			if err != nil {
				continue
			}
			itemIter, err := seriesSpan.BuildItemIterators(b, blockFilter)
			if err != nil {
				continue
			}
			itemIterators = append(itemIterators, itemIter)
		}
		return iter.FromSlice(itemIterators)
	})))
}
