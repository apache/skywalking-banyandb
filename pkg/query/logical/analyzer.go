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

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	seriesSchema "github.com/apache/skywalking-banyandb/banyand/series/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema/sw"
)

var (
	FieldNotDefinedErr            = errors.New("field is not defined")
	PairQueryInspectionErr        = errors.New("pairQuery cannot be inspected")
	InvalidConditionTypeErr       = errors.New("invalid pair type")
	TypePairInspectionErr         = errors.New("pair cannot be inspected")
	IncompatibleQueryConditionErr = errors.New("incompatible query condition type")
	InvalidSchemaErr              = errors.New("invalid schema")
	IndexNotDefinedErr            = errors.New("index is not define for the field")
)

var (
	DefaultLimit uint32 = 20
)

type analyzer struct {
	traceSeries seriesSchema.TraceSeries
	indexRule   seriesSchema.IndexRule
}

func DefaultAnalyzer() *analyzer {
	return &analyzer{
		sw.NewTraceSeries(),
		sw.NewIndexRule(),
	}
}

func (a *analyzer) BuildTraceSchema(ctx context.Context, metadata common.Metadata) (Schema, error) {
	traceSeries, err := a.traceSeries.Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	indexRule, err := a.indexRule.Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	s := &schema{
		indexRule: indexRule,
		fieldMap:  make(map[string]*fieldSpec),
	}

	// generate the schema of the fields for the traceSeries
	for i := 0; i < traceSeries.Spec.FieldsLength(); i++ {
		var fs apiv1.FieldSpec
		if ok := traceSeries.Spec.Fields(&fs, i); !ok {
			return nil, err
		}
		s.RegisterField(string(fs.Name()), i, &fs)
	}

	return s, nil
}

func (a *analyzer) Analyze(ctx context.Context, criteria *apiv1.EntityCriteria, traceMetadata *common.Metadata, s Schema) (Plan, error) {
	// parse tableScan
	timeRange := criteria.TimestampNanoseconds(nil)

	var projStr []string
	// parse projection
	proj := criteria.Projection(nil)
	if proj != nil {
		for i := 0; i < proj.KeyNamesLength(); i++ {
			projStr = append(projStr, string(proj.KeyNames(i)))
		}
	}

	var plan UnresolvedPlan

	// parse selection
	if criteria.FieldsLength() > 0 {
		// mark if there is indexScan request
		useIndexScan := false
		var fieldExprs []Expr
		traceState := series.TraceStateDefault
		for i := 0; i < criteria.FieldsLength(); i++ {
			var pairQuery apiv1.PairQuery
			if ok := criteria.Fields(&pairQuery, i); !ok {
				return nil, PairQueryInspectionErr
			}
			op := pairQuery.Op()
			pair := pairQuery.Condition(nil)
			unionPairTable := new(flatbuffers.Table)
			if ok := pair.Pair(unionPairTable); !ok {
				return nil, TypePairInspectionErr
			}
			if pair.PairType() == apiv1.TypedPairStrPair {
				unionStrPair := new(apiv1.StrPair)
				unionStrPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
				// check special field `trace_id`
				if string(unionStrPair.Key()) == "trace_id" {
					return TraceIDFetch(string(unionStrPair.Values(0)), traceMetadata, s), nil
				}
				useIndexScan = true
				lit := parseStrLiteral(unionStrPair)
				fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(string(unionStrPair.Key())), lit))
			} else if pair.PairType() == apiv1.TypedPairIntPair {
				unionIntPair := new(apiv1.IntPair)
				unionIntPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
				// check special field `state`
				if string(unionIntPair.Key()) == "state" {
					traceState = series.TraceState(unionIntPair.Values(0))
					continue
				}
				lit := parseIntLiteral(unionIntPair)
				fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(string(unionIntPair.Key())), lit))
				useIndexScan = true
			} else {
				return nil, InvalidConditionTypeErr
			}
		}

		// first check if we can use index-scan
		if useIndexScan {
			plan = IndexScan(timeRange.Begin(), timeRange.End(), traceMetadata, fieldExprs, traceState, projStr...)
		} else {
			plan = TableScan(timeRange.Begin(), timeRange.End(), traceMetadata, traceState, projStr...)
		}
	} else {
		plan = TableScan(timeRange.Begin(), timeRange.End(), traceMetadata, series.TraceStateDefault, projStr...)
	}

	// parse orderBy
	queryOrder := criteria.OrderBy(nil)
	if queryOrder != nil {
		plan = OrderBy(plan, string(queryOrder.KeyName()), queryOrder.Sort())
	}

	// parse offset
	plan = Offset(plan, criteria.Offset())

	// parse limit
	limitParameter := criteria.Limit()
	if limitParameter == 0 {
		limitParameter = DefaultLimit
	}
	plan = Limit(plan, limitParameter)

	return plan.Analyze(s)
}

func parseStrLiteral(pair *apiv1.StrPair) Expr {
	if pair.ValuesLength() == 1 {
		return &strLiteral{
			string(pair.Values(0)),
		}
	}
	var arr []string
	for i := 0; i < pair.ValuesLength(); i++ {
		arr = append(arr, string(pair.Values(i)))
	}
	return &strArrLiteral{arr: arr}
}

func parseIntLiteral(pair *apiv1.IntPair) Expr {
	if pair.ValuesLength() == 1 {
		return &int64Literal{
			pair.Values(0),
		}
	}
	var arr []int64
	for i := 0; i < pair.ValuesLength(); i++ {
		arr = append(arr, pair.Values(i))
	}
	return &int64ArrLiteral{arr: arr}
}
