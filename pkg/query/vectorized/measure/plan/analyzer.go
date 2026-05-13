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

package plan

import (
	"fmt"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// defaultLimit mirrors the row-path measure_analyzer.go default. Matched so
// vec and row produce identical paging for requests that don't set Limit.
const defaultLimit uint32 = 100

// Analyze translates a measurev1.QueryRequest + its Measure schema into a
// VecPlan tree. It is the vec counterpart of pkg/query/logical/measure
// (deprecated) but produces vec plan nodes — there is no leaf
// substitution into a row plan.
//
// The returned tree is structural: it carries the static (proto-derived)
// query parameters in `Scan.Params` and the BatchSchema for downstream
// nodes to consult. Runtime fields that depend on the executor's
// MeasureExecutionContext — the resolved `index.Query` and the entity
// table — are NOT populated here; the executor (G8c) fills them in
// before invoking Build.
//
// Errors are returned for:
//   - nil schema
//   - tag/field projection naming columns not in the schema
//   - GroupBy referencing a tag absent from the schema
//   - Agg referencing a field absent from the schema
//   - GroupBy set without Agg (raw groupby not supported in v1)
//   - Agg set without GroupBy (scalar reduce not supported in v1)
func Analyze(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (VecPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("plan.Analyze: nil request")
	}
	if measureSchema == nil {
		return nil, fmt.Errorf("plan.Analyze: nil Measure schema")
	}

	tagProjection := buildTagProjection(req)
	fieldProjection := req.GetFieldProjection().GetNames()
	opts := model.MeasureQueryOptions{
		TagProjection:   tagProjection,
		FieldProjection: fieldProjection,
	}
	batchSchema, schemaErr := measure.BuildBatchSchema(measureSchema, opts)
	if schemaErr != nil {
		return nil, fmt.Errorf("plan.Analyze: %w", schemaErr)
	}

	var tr *timestamp.TimeRange
	if t := req.GetTimeRange(); t != nil {
		r := timestamp.NewInclusiveTimeRange(t.GetBegin().AsTime(), t.GetEnd().AsTime())
		tr = &r
	}

	var plan VecPlan = NewScan(batchSchema, ScanParams{
		Measure:         measureSchema,
		TimeRange:       tr,
		TagProjection:   tagProjection,
		FieldProjection: fieldProjection,
	})

	// GroupBy + Agg coalesced into a single GroupByAgg node. Validation
	// matches the G7d planner's contract.
	hasGroupBy := req.GetGroupBy() != nil
	hasAgg := req.GetAgg() != nil
	switch {
	case hasGroupBy && hasAgg:
		gb, aggSpec, validateErr := translateGroupByAgg(req, measureSchema)
		if validateErr != nil {
			return nil, validateErr
		}
		gba, gbaErr := NewGroupByAgg(plan, gb, aggSpec)
		if gbaErr != nil {
			return nil, gbaErr
		}
		plan = gba
	case hasGroupBy:
		return nil, fmt.Errorf("plan.Analyze: GroupBy without Agg is not supported in v1")
	case hasAgg:
		return nil, fmt.Errorf("plan.Analyze: Agg without GroupBy (scalar reduce) is not supported in v1")
	}

	if t := req.GetTop(); t != nil {
		asc := t.GetFieldValueSort() == 1 // SORT_ASC == 1 in modelv1.Sort
		plan = NewTop(plan, t.GetFieldName(), int(t.GetNumber()), asc)
	}

	limitN := req.GetLimit()
	if limitN == 0 {
		limitN = defaultLimit
	}
	plan = NewLimit(plan, req.GetOffset(), limitN)

	return plan, nil
}

// buildTagProjection converts the proto tag projection into the model-level
// slice the BatchSchema builder consumes.
func buildTagProjection(req *measurev1.QueryRequest) []model.TagProjection {
	tp := req.GetTagProjection()
	if tp == nil {
		return nil
	}
	families := tp.GetTagFamilies()
	out := make([]model.TagProjection, 0, len(families))
	for _, tf := range families {
		out = append(out, model.TagProjection{
			Family: tf.GetName(),
			Names:  append([]string(nil), tf.GetTags()...),
		})
	}
	return out
}

// translateGroupByAgg builds the model GroupBy/Agg structs from the proto,
// validating that:
//   - the GroupBy tag_projection names exactly one family with non-empty tags
//     (v1 single-family limitation)
//   - the named GroupBy tags exist in the Measure schema
//   - the Agg field exists in the Measure schema
func translateGroupByAgg(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (
	*model.MeasureGroupBy, *model.MeasureAgg, error,
) {
	gbProto := req.GetGroupBy()
	families := gbProto.GetTagProjection().GetTagFamilies()
	if len(families) == 0 {
		return nil, nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection must list at least one tag family")
	}
	if len(families) > 1 {
		return nil, nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection v1 supports a single tag family, got %d", len(families))
	}
	family := families[0]
	if len(family.GetTags()) == 0 {
		return nil, nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection family %q has no tags", family.GetName())
	}
	gb := &model.MeasureGroupBy{
		TagFamily: family.GetName(),
		TagNames:  append([]string(nil), family.GetTags()...),
	}
	if validateErr := validateGroupByTags(measureSchema, gb); validateErr != nil {
		return nil, nil, validateErr
	}

	aggProto := req.GetAgg()
	if validateErr := validateAggField(measureSchema, aggProto.GetFieldName()); validateErr != nil {
		return nil, nil, validateErr
	}
	agg := &model.MeasureAgg{
		FieldName: aggProto.GetFieldName(),
		Func:      aggProto.GetFunction(),
	}
	return gb, agg, nil
}

// validateGroupByTags ensures every name in gb.TagNames exists within the
// configured tag family of measureSchema.
func validateGroupByTags(measureSchema *databasev1.Measure, gb *model.MeasureGroupBy) error {
	for _, tf := range measureSchema.GetTagFamilies() {
		if tf.GetName() != gb.TagFamily {
			continue
		}
		known := make(map[string]struct{}, len(tf.GetTags()))
		for _, ts := range tf.GetTags() {
			known[ts.GetName()] = struct{}{}
		}
		for _, name := range gb.TagNames {
			if _, ok := known[name]; !ok {
				return fmt.Errorf("plan.Analyze: GroupBy tag %s.%s not present in measure schema", gb.TagFamily, name)
			}
		}
		return nil
	}
	return fmt.Errorf("plan.Analyze: GroupBy tag family %q not present in measure schema", gb.TagFamily)
}

// validateAggField ensures the agg field name is a field defined on the
// Measure schema. Type compatibility (int/float) is enforced later by the
// BatchAggregation operator.
func validateAggField(measureSchema *databasev1.Measure, fieldName string) error {
	for _, fs := range measureSchema.GetFields() {
		if fs.GetName() == fieldName {
			return nil
		}
	}
	return fmt.Errorf("plan.Analyze: Agg field %q not present in measure schema", fieldName)
}
