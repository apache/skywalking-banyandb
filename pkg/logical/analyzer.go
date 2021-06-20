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
)

var (
	DefaultLimit uint32 = 20
)

type analyzer struct {
	traceSeries seriesSchema.TraceSeries
}

func DefaultAnalyzer() *analyzer {
	return &analyzer{
		sw.NewTraceSeries(),
	}
}

func (a *analyzer) BuildTraceSchema(ctx context.Context, metadata common.Metadata) (Schema, error) {
	traceSeries, err := a.traceSeries.Get(ctx, metadata)

	if err != nil {
		return nil, err
	}

	fs := &schema{
		fieldMap: make(map[string]*fieldSpec),
	}

	// generate the schema of the fields for the traceSeries
	for i := 0; i < traceSeries.Spec.FieldsLength(); i++ {
		var fieldSpec apiv1.FieldSpec
		if ok := traceSeries.Spec.Fields(&fieldSpec, i); ok {
			fs.RegisterField(string(fieldSpec.Name()), i, &fieldSpec)
		} else {
			return nil, err
		}
	}

	return fs, nil
}

func (a *analyzer) Analyze(ctx context.Context, criteria *apiv1.EntityCriteria, traceMetadata *common.Metadata, s Schema) (Plan, error) {
	// parse scan
	timeRange := criteria.TimestampNanoseconds(nil)
	plan := Scan(timeRange.Begin(), timeRange.End(), traceMetadata)

	// parse selection
	if criteria.FieldsLength() > 0 {
		var fieldExprs []Expr
		for i := 0; i < criteria.FieldsLength(); i++ {
			var pairQuery apiv1.PairQuery
			if criteria.Fields(&pairQuery, i) {
				op := pairQuery.Op()
				pair := pairQuery.Condition(nil)
				unionPairTable := new(flatbuffers.Table)
				if pair.Pair(unionPairTable) {
					if pair.PairType() == apiv1.TypedPairStrPair {
						unionStrPair := new(apiv1.StrPair)
						unionStrPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
						lit := parseStrLiteral(unionStrPair)
						fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(string(unionStrPair.Key())), lit))
					} else if pair.PairType() == apiv1.TypedPairIntPair {
						unionIntPair := new(apiv1.IntPair)
						unionIntPair.Init(unionPairTable.Bytes, unionPairTable.Pos)
						lit := parseIntLiteral(unionIntPair)
						fieldExprs = append(fieldExprs, binaryOpFactory[op](NewFieldRef(string(unionIntPair.Key())), lit))
					} else {
						return nil, InvalidConditionTypeErr
					}
				} else {
					return nil, TypePairInspectionErr
				}
			} else {
				return nil, PairQueryInspectionErr
			}
		}
		plan = Selection(plan, fieldExprs...)
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

	// parse projection
	proj := criteria.Projection(nil)
	if proj != nil {
		var projStr []string
		for i := 0; i < proj.KeyNamesLength(); i++ {
			projStr = append(projStr, string(proj.KeyNames(i)))
		}

		plan = Projection(plan, projStr)
	}

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
