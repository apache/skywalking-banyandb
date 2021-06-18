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
	"errors"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	apischema "github.com/apache/skywalking-banyandb/api/schema"
	"github.com/apache/skywalking-banyandb/banyand/series/schema"
)

var (
	FieldNotDefinedErr = errors.New("field is not defined")
)

type analyzer struct {
	traceSeries schema.TraceSeries
}

func (a *analyzer) Analyze(ctx context.Context, criteria *apiv1.EntityCriteria) (Plan, error) {
	traceMetadata := common.Metadata{
		KindVersion: apischema.SeriesKindVersion,
		Spec:        *criteria.Metadata(nil),
	}

	var traceSeries apischema.TraceSeries
	var err error
	if traceSeries, err = a.traceSeries.Get(ctx, traceMetadata); err != nil {
		return nil, err
	}

	fs := &fieldSchema{
		fieldMap: make(map[string]int),
		fields:   make([]*apiv1.FieldSpec, 0),
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

	// parse scan
	timeRange := criteria.TimestampNanoseconds(nil)
	scanPlan := NewScan(timeRange.Begin(), timeRange.End(), &traceMetadata)

	// TODO: parse selection

	// parse orderBy
	queryOrder := criteria.OrderBy(nil)
	orderExpr, err := fs.CreateRef(string(queryOrder.KeyName()))
	if err != nil {
		return nil, err
	}
	orderByPlan := NewOrderBy(scanPlan, orderExpr, queryOrder.Sort())

	// parse offset
	offsetPlan := NewOffset(orderByPlan, criteria.Offset())

	// parse limit
	limitPlan := NewLimit(offsetPlan, criteria.Limit())

	// parse projection
	proj := criteria.Projection(nil)
	var projExpr []Expr
	for i := 0; i < proj.KeyNamesLength(); i++ {
		ref, err := fs.CreateRef(string(proj.KeyNames(i)))
		if err != nil {
			return nil, err
		}
		projExpr = append(projExpr, ref)
	}

	projectionPlan := NewProjection(limitPlan, projExpr)

	return projectionPlan, nil
}
