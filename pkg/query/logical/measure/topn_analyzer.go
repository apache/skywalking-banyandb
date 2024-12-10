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

// Package measure implements execution operations for querying measure data.
package measure

import (
	"errors"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	pkgschema "github.com/apache/skywalking-banyandb/pkg/schema"
)

// TopNAnalyze converts logical expressions to executable operation tree represented by Plan.
func TopNAnalyze(criteria *measurev1.TopNRequest, sourceMeasureSchema *databasev1.Measure, topNAggSchema *databasev1.TopNAggregation,
) (logical.Plan, error) {
	// parse fields
	timeRange := criteria.GetTimeRange()
	var plan logical.UnresolvedPlan
	plan = &unresolvedLocalScan{
		name:        criteria.Name,
		startTime:   timeRange.GetBegin().AsTime(),
		endTime:     timeRange.GetEnd().AsTime(),
		conditions:  criteria.Conditions,
		sort:        criteria.FieldValueSort,
		groupByTags: topNAggSchema.GroupByTagNames,
	}
	s, err := buildVirtualSchema(sourceMeasureSchema, topNAggSchema.FieldName)
	if err != nil {
		return nil, err
	}

	if criteria.GetAgg() != 0 {
		groupByProjectionTags := sourceMeasureSchema.GetEntity().GetTagNames()
		groupByTags := [][]*logical.Tag{logical.NewTags(pkgschema.TopNTagFamily, groupByProjectionTags...)}
		plan = newUnresolvedGroupBy(plan, groupByTags, false)
		plan = newUnresolvedAggregation(plan,
			&logical.Field{Name: topNAggSchema.FieldName},
			criteria.GetAgg(),
			true)
	}

	plan = top(plan, &measurev1.QueryRequest_Top{
		Number:         criteria.GetTopN(),
		FieldName:      topNAggSchema.FieldName,
		FieldValueSort: criteria.GetFieldValueSort(),
	})

	p, err := plan.Analyze(s)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func buildVirtualSchema(sourceMeasureSchema *databasev1.Measure, fieldName string) (logical.Schema, error) {
	var tags []*databasev1.TagSpec
	for _, tag := range sourceMeasureSchema.GetEntity().TagNames {
		for i := range sourceMeasureSchema.GetTagFamilies() {
			for j := range sourceMeasureSchema.GetTagFamilies()[i].Tags {
				if sourceMeasureSchema.GetTagFamilies()[i].Tags[j].Name == tag {
					tags = append(tags, sourceMeasureSchema.GetTagFamilies()[i].Tags[j])
					if len(tags) == len(sourceMeasureSchema.GetEntity().TagNames) {
						break
					}
				}
			}
		}
	}
	if len(tags) != len(sourceMeasureSchema.GetEntity().TagNames) {
		return nil, errors.New("failed to build topn schema, source measure schema is invalid:" + sourceMeasureSchema.String())
	}
	var fields []*databasev1.FieldSpec
	for _, field := range sourceMeasureSchema.GetFields() {
		if field.GetName() == fieldName {
			fields = append(fields, field)
			break
		}
	}
	md := &databasev1.Measure{
		Metadata: sourceMeasureSchema.Metadata,
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: pkgschema.TopNTagFamily,
				Tags: tags,
			},
		},
		Fields: fields,
		Entity: &databasev1.Entity{
			TagNames: sourceMeasureSchema.Entity.TagNames,
		},
	}
	ms := &schema{
		common: &logical.CommonSchema{
			TagSpecMap: make(map[string]*logical.TagSpec),
			EntityList: md.GetEntity().GetTagNames(),
		},
		measure:  md,
		fieldMap: make(map[string]*logical.FieldSpec),
	}
	ms.common.RegisterTagFamilies(md.GetTagFamilies())
	for fieldIdx, spec := range md.GetFields() {
		ms.registerField(fieldIdx, spec)
	}
	return ms, nil
}
