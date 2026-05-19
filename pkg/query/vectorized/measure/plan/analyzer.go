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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
//
// GroupBy and Agg may travel together (group + aggregate), or either
// alone: Agg without GroupBy is a scalar reduce (single output row);
// GroupBy without Agg is a raw GroupBy (first-seen row per group). Both
// mirror the row path (measure_plan_aggregation.go / measure_plan_groupby.go).
//
// mode selects the BatchAggregation strategy when an agg operator is built:
// AggModeAll for single-node final reduce, AggModeMap for the distributed
// Map phase (G9f.2) that emits typed-column partials. AggModeReduce is
// rejected (the reduce plan is built liaison-side in G9f.3).
func Analyze(req *measurev1.QueryRequest, measureSchema *databasev1.Measure, mode measure.AggMode) (VecPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("plan.Analyze: nil request")
	}
	if measureSchema == nil {
		return nil, fmt.Errorf("plan.Analyze: nil Measure schema")
	}

	tagProjection := buildTagProjection(req)
	fieldProjection := req.GetFieldProjection().GetNames()

	// GroupBy + Agg must be resolved BEFORE BuildBatchSchema so the Scan
	// node's BatchSchema declares native typed columns for the GroupBy
	// keys and Agg field. The operator (BatchAggregation.fold) hard-casts
	// those columns to TypedColumn[int64] / [float64]; passthrough
	// columns would panic. Storage's queryResult.batchSchema is rebuilt
	// from the same opts in banyand/measure/query.go, so both halves of
	// the bridge agree on column types.
	hasGroupBy := req.GetGroupBy() != nil
	hasAgg := req.GetAgg() != nil
	var gbModel *model.MeasureGroupBy
	var aggModel *model.MeasureAgg
	if hasGroupBy {
		var gbErr error
		gbModel, gbErr = translateGroupBy(req, measureSchema)
		if gbErr != nil {
			return nil, gbErr
		}
	}
	if hasAgg {
		var aggErr error
		aggModel, aggErr = translateAgg(req, measureSchema)
		if aggErr != nil {
			return nil, aggErr
		}
	}

	// Projection auto-coverage: BatchAggregation / BatchGroupBy locate
	// their key and value columns by name inside the BatchSchema, which
	// BuildBatchSchema derives from TagProjection + FieldProjection.
	// Extend the projection implicitly so the GroupBy keys and the Agg
	// field always materialize a column, instead of falling through to
	// the row path when the request omitted them from its projection.
	tagProjection = ensureGroupByProjected(tagProjection, gbModel)
	fieldProjection = ensureAggFieldProjected(fieldProjection, aggModel)

	opts := model.MeasureQueryOptions{
		TagProjection:   tagProjection,
		FieldProjection: fieldProjection,
		GroupBy:         gbModel,
		Agg:             aggModel,
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
		GroupBy:         gbModel,
		Agg:             aggModel,
	})

	if hasGroupBy || hasAgg {
		// One BatchAggregation / BatchGroupBy node covers all three
		// shapes (group+agg, scalar reduce, raw groupby); BuildOperators
		// routes on which of gbModel/aggModel is set.
		gba, gbaErr := NewGroupByAgg(plan, gbModel, aggModel, mode)
		if gbaErr != nil {
			return nil, gbaErr
		}
		plan = gba
	}

	if t := req.GetTop(); t != nil {
		// Match the row path (pkg/query/logical/measure.unresolvedTop):
		// FieldValueSort==SORT_ASC keeps the lowest N (BatchTop asc=true);
		// anything else (SORT_DESC / SORT_UNSPECIFIED) keeps the highest N.
		asc := t.GetFieldValueSort() == modelv1.Sort_SORT_ASC
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

// translateGroupBy builds the model GroupBy struct from the proto,
// validating that:
//   - the GroupBy tag_projection names exactly one family with non-empty tags
//     (v1 single-family limitation)
//   - the named GroupBy tags exist in the Measure schema
func translateGroupBy(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (*model.MeasureGroupBy, error) {
	families := req.GetGroupBy().GetTagProjection().GetTagFamilies()
	if len(families) == 0 {
		return nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection must list at least one tag family")
	}
	if len(families) > 1 {
		return nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection v1 supports a single tag family, got %d", len(families))
	}
	family := families[0]
	if len(family.GetTags()) == 0 {
		return nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection family %q has no tags", family.GetName())
	}
	gb := &model.MeasureGroupBy{
		TagFamily: family.GetName(),
		TagNames:  append([]string(nil), family.GetTags()...),
	}
	if validateErr := validateGroupByTags(measureSchema, gb); validateErr != nil {
		return nil, validateErr
	}
	return gb, nil
}

// translateAgg builds the model Agg struct from the proto, validating
// that the Agg field exists in the Measure schema.
func translateAgg(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (*model.MeasureAgg, error) {
	aggProto := req.GetAgg()
	if validateErr := validateAggField(measureSchema, aggProto.GetFieldName()); validateErr != nil {
		return nil, validateErr
	}
	return &model.MeasureAgg{
		FieldName: aggProto.GetFieldName(),
		Func:      aggProto.GetFunction(),
	}, nil
}

// ensureGroupByProjected returns a TagProjection slice guaranteed to
// include every GroupBy key tag. When the request already projects them
// the input slice is returned unchanged; otherwise the missing key tags
// are appended to (or create) the GroupBy family. Mirrors the row path,
// whose GroupBy resolves its key tag refs against the schema regardless
// of the request projection.
func ensureGroupByProjected(tp []model.TagProjection, gb *model.MeasureGroupBy) []model.TagProjection {
	if gb == nil || gb.TagFamily == "" || len(gb.TagNames) == 0 {
		return tp
	}
	out := append([]model.TagProjection(nil), tp...)
	familyIdx := -1
	for i := range out {
		if out[i].Family == gb.TagFamily {
			familyIdx = i
			break
		}
	}
	if familyIdx < 0 {
		out = append(out, model.TagProjection{
			Family: gb.TagFamily,
			Names:  append([]string(nil), gb.TagNames...),
		})
		return out
	}
	present := make(map[string]struct{}, len(out[familyIdx].Names))
	for _, n := range out[familyIdx].Names {
		present[n] = struct{}{}
	}
	names := append([]string(nil), out[familyIdx].Names...)
	for _, n := range gb.TagNames {
		if _, ok := present[n]; !ok {
			names = append(names, n)
		}
	}
	out[familyIdx].Names = names
	return out
}

// ensureAggFieldProjected returns a FieldProjection slice guaranteed to
// include the Agg field. When already present the input is returned
// unchanged; otherwise the field is appended. Mirrors the row path,
// whose aggregation resolves its field ref against the schema regardless
// of the request projection.
func ensureAggFieldProjected(fp []string, agg *model.MeasureAgg) []string {
	if agg == nil || agg.FieldName == "" {
		return fp
	}
	for _, n := range fp {
		if n == agg.FieldName {
			return fp
		}
	}
	out := append([]string(nil), fp...)
	return append(out, agg.FieldName)
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
