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
	// parse tableScan
	timeRange := criteria.GetTimeRange()

	// parse projection
	projStr := criteria.GetProjection().GetKeyNames()

	var plan UnresolvedPlan

	// parse selection
	if criteria.GetFields() != nil && len(criteria.GetFields()) > 0 {
		// mark if there is indexScan request
		useIndexScan := false
		var fieldExprs []Expr
		traceState := series.TraceStateDefault

		for _, pairQuery := range criteria.GetFields() {
			op := pairQuery.GetOp()
			typedPair := pairQuery.GetCondition()
			switch v := typedPair.GetTyped().(type) {
			case *modelv1.TypedPair_StrPair:
				// check special field `trace_id`
				if v.StrPair.GetKey() == s.TraceIDFieldName() {
					plan = TraceIDFetch(v.StrPair.GetValues()[0], traceMetadata, projStr...)
					break
				}
				useIndexScan = true
				lit := parseStrLiteral(v.StrPair)
				fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(v.StrPair.GetKey()), lit))
			case *modelv1.TypedPair_IntPair:
				// check special field `state`
				if v.IntPair.GetKey() == s.TraceStateFieldName() {
					traceState = series.TraceState(v.IntPair.GetValues()[0])
					continue
				}
				lit := parseIntLiteral(v.IntPair)
				fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(v.IntPair.GetKey()), lit))
				useIndexScan = true
			default:
				return nil, ErrInvalidConditionType
			}
		}

		// if plan is already assigned, skip
		if plan == nil {
			// first check if we can use index-scan
			if useIndexScan {
				plan = IndexScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata, fieldExprs, traceState, projStr...)
			} else {
				plan = TableScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata, traceState, projStr...)
			}
		}
	} else {
		plan = TableScan(timeRange.GetBegin().AsTime().UnixNano(), timeRange.GetEnd().AsTime().UnixNano(), traceMetadata, series.TraceStateDefault, projStr...)
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

func parseStrLiteral(pair *modelv1.StrPair) Expr {
	if len(pair.GetValues()) == 1 {
		return &strLiteral{
			pair.GetValues()[0],
		}
	}
	return &strArrLiteral{arr: pair.GetValues()}
}

func parseIntLiteral(pair *modelv1.IntPair) Expr {
	if len(pair.GetValues()) == 1 {
		return &int64Literal{
			pair.GetValues()[0],
		}
	}
	return &int64ArrLiteral{arr: pair.GetValues()}
}
