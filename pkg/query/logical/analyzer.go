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

package logical

import (
	"context"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	seriesSchema "github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
)

var (
	ErrFieldNotDefined            = errors.New("field is not defined")
	ErrInvalidConditionType       = errors.New("invalid pair type")
	ErrIncompatibleQueryCondition = errors.New("incompatible query condition type")
	ErrInvalidSchema              = errors.New("invalid schema")
	ErrIndexNotDefined            = errors.New("index is not define for the field")
	ErrTraceIDWrongType           = errors.New("trace id type should be string")
	ErrStateWrongType             = errors.New("state type should be int")
)

var (
	DefaultLimit uint32 = 20
)

type Analyzer struct {
	traceSeries seriesSchema.TraceSeries
	indexRule   seriesSchema.IndexRule
}

func DefaultAnalyzer() *Analyzer {
	return &Analyzer{
		sw.NewTraceSeries(),
		sw.NewIndexRule(),
	}
}

func (a *Analyzer) BuildTraceSchema(ctx context.Context, metadata common.Metadata) (Schema, error) {
	traceSeries, err := a.traceSeries.Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	indexRule, err := a.indexRule.Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	s := &schema{
		traceSeries: traceSeries.Spec,
		indexRule:   indexRule,
		fieldMap:    make(map[string]*fieldSpec),
	}

	// generate the schema of the fields for the traceSeries
	for i, f := range traceSeries.Spec.GetFields() {
		s.RegisterField(f.GetName(), i, f)
	}

	return s, nil
}

func (a *Analyzer) Analyze(_ context.Context, criteria *tracev1.QueryRequest, traceMetadata *common.Metadata, s Schema) (Plan, error) {
	// parse fields
	plan, err := parseFields(criteria, traceMetadata, s)
	if err != nil {
		return nil, err
	}

	// parse orderBy
	queryOrder := criteria.GetOrderBy()
	if queryOrder != nil {
		plan = OrderBy(plan, queryOrder.GetKeyName(), queryOrder.GetSort())
	}

	// parse offset
	plan = Offset(plan, criteria.GetOffset())

	// parse limit
	limitParameter := criteria.GetLimit()
	if limitParameter == 0 {
		limitParameter = DefaultLimit
	}
	plan = Limit(plan, limitParameter)

	return plan.Analyze(s)
}

func parseFields(criteria *tracev1.QueryRequest, traceMetadata *common.Metadata, s Schema) (UnresolvedPlan, error) {
	timeRange := criteria.GetTimeRange()

	projStr := criteria.GetProjection().GetKeyNames()

	if criteria.GetFields() == nil || len(criteria.GetFields()) < 1 {
		return TableScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata,
			series.TraceStateDefault, projStr...), nil
	}

	var plan UnresolvedPlan
	// mark if there is indexScan request
	useIndexScan := false
	var fieldExprs []Expr
	traceState := series.TraceStateDefault

fieldsLoop:
	for _, pairQuery := range criteria.GetFields() {
		op := pairQuery.GetOp()
		typedPair := pairQuery.GetCondition()
		switch typedPair.GetKey() {
		case s.TraceIDFieldName():
			traceIDPair := typedPair.GetStrPair()
			if traceIDPair == nil {
				return nil, ErrTraceIDWrongType
			}
			plan = TraceIDFetch(typedPair.GetStrPair().GetValue(), traceMetadata, projStr...)
			break fieldsLoop
		case s.TraceStateFieldName():
			statePair := typedPair.GetIntPair()
			if statePair == nil {
				return nil, ErrStateWrongType
			}
			traceState = series.TraceState(statePair.Value)
			continue
		}
		useIndexScan = true
		var e Expr
		switch v := typedPair.GetTyped().(type) {
		case *modelv1.TypedPair_StrPair:
			e = &strLiteral{
				string: v.StrPair.GetValue(),
			}
		case *modelv1.TypedPair_StrArrayPair:
			e = &strArrLiteral{
				arr: v.StrArrayPair.GetValue(),
			}
		case *modelv1.TypedPair_IntPair:
			e = &int64Literal{
				int64: v.IntPair.GetValue(),
			}
		case *modelv1.TypedPair_IntArrayPair:
			e = &int64ArrLiteral{
				arr: v.IntArrayPair.GetValue(),
			}
		default:
			return nil, ErrInvalidConditionType
		}
		fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(typedPair.GetKey()), e))
	}

	// if plan is already assigned, skip
	if plan != nil {
		return plan, nil
	}
	// first check if we can use index-scan
	if useIndexScan {
		return IndexScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata,
			fieldExprs, traceState, projStr...), nil
	}
	return TableScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata,
		traceState, projStr...), nil
}
