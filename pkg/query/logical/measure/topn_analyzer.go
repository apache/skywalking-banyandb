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
	"strconv"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

// TopNAnalyze converts logical expressions to executable operation tree represented by Plan.
func TopNAnalyze(criteria *measurev1.TopNRequest, sourceMeasureSchemaList []*databasev1.Measure, topNAggSchemaList []*databasev1.TopNAggregation,
	ecc []executor.MeasureExecutionContext,
) (logical.Plan, error) {
	// Ensure input slices have identical lengths
	if len(sourceMeasureSchemaList) != len(topNAggSchemaList) || len(sourceMeasureSchemaList) != len(ecc) {
		return nil, errors.New("sourceMeasureSchemaList, topNAggSchemaList, and ecc must have identical lengths")
	}
	if len(sourceMeasureSchemaList) == 0 {
		return nil, errors.New("sourceMeasureSchemaList is empty")
	}

	timeRange := criteria.GetTimeRange()
	var plan logical.UnresolvedPlan
	var sourceMeasureSchema *databasev1.Measure
	var topNAggSchema *databasev1.TopNAggregation
	var ec executor.MeasureExecutionContext
	if len(sourceMeasureSchemaList) == 1 {
		sourceMeasureSchema = sourceMeasureSchemaList[0]
		topNAggSchema = topNAggSchemaList[0]
		ec = ecc[0]
		// parse fields
		plan = &unresolvedLocalScan{
			name:        criteria.Name,
			startTime:   timeRange.GetBegin().AsTime(),
			endTime:     timeRange.GetEnd().AsTime(),
			conditions:  criteria.Conditions,
			sort:        criteria.FieldValueSort,
			groupByTags: topNAggSchema.GetGroupByTagNames(),
			ec:          ec,
		}
	} else {
		subPlans := make([]*unresolvedLocalScan, 0, len(sourceMeasureSchemaList))
		for i := range sourceMeasureSchemaList {
			subPlans = append(subPlans, &unresolvedLocalScan{
				name:        criteria.Name,
				startTime:   timeRange.GetBegin().AsTime(),
				endTime:     timeRange.GetEnd().AsTime(),
				conditions:  criteria.Conditions,
				sort:        criteria.FieldValueSort,
				groupByTags: topNAggSchemaList[i].GetGroupByTagNames(),
				ec:          ecc[i],
			})
		}
		plan = &unresolvedTopNMerger{
			subPlans: subPlans,
		}
		baseEntity := sourceMeasureSchemaList[0].GetEntity()
		baseTagNames := baseEntity.GetTagNames()
		for i, m := range sourceMeasureSchemaList[1:] {
			entity := m.GetEntity()
			tagNames := entity.GetTagNames()
			if len(tagNames) != len(baseTagNames) {
				return nil, errors.New("all source measures must have the same entity: tag name count mismatch at index " + strconv.Itoa(i+1))
			}
			for j := range tagNames {
				if tagNames[j] != baseTagNames[j] {
					return nil, errors.New("all source measures must have the same entity: tag name mismatch at index " + strconv.Itoa(i+1))
				}
			}
		}

		topNAggSchema = topNAggSchemaList[0]
		baseFieldName := topNAggSchema.FieldName
		for i, agg := range topNAggSchemaList[1:] {
			if agg.GetFieldName() != baseFieldName {
				return nil, errors.New("all TopNAggregation must have the same FieldName: mismatch at index " + strconv.Itoa(i+1))
			}
		}

		sourceMeasureSchema = sourceMeasureSchemaList[0]
	}

	s, err := buildVirtualSchema(sourceMeasureSchema, topNAggSchema.FieldName)
	if err != nil {
		return nil, err
	}

	if criteria.GetAgg() != 0 {
		groupByProjectionTags := sourceMeasureSchema.GetEntity().GetTagNames()
		groupByTags := [][]*logical.Tag{logical.NewTags(measure.TopNTagFamily, groupByProjectionTags...)}
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
				Name: measure.TopNTagFamily,
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
