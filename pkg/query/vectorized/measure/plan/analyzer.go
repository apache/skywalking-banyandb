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
	"slices"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
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
//   - Agg referencing a field or tag absent from the schema, naming both or
//     neither of field_name/tag_name, or naming a tag whose type or family
//     qualification the target function does not support (design §6)
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
	if pushdownErr := validateCountDistinctPushdown(req, measureSchema); pushdownErr != nil {
		return nil, pushdownErr
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
	tagProjection = ensureAggTagProjected(tagProjection, aggModel)
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

// translateGroupBy builds the model GroupBy struct from the proto. A
// GroupBy must set tag_projection, time_bucket, or both (design §7.2 —
// bucketing alone, with no tag key, is a legal grouping). When
// tag_projection is present it must name exactly one family with non-empty
// tags (v1 single-family limitation), and every named tag must exist in the
// Measure schema. When time_bucket is present, its width is resolved per
// §5.3 and the request is checked against the bucketed streaming
// precondition (time-ascending input).
func translateGroupBy(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (*model.MeasureGroupBy, error) {
	groupByProto := req.GetGroupBy()
	families := groupByProto.GetTagProjection().GetTagFamilies()
	tbProto := groupByProto.GetTimeBucket()
	if len(families) == 0 && tbProto == nil {
		return nil, fmt.Errorf("plan.Analyze: GroupBy must set tag_projection, time_bucket, or both")
	}

	gb := &model.MeasureGroupBy{}
	if len(families) > 0 {
		if len(families) > 1 {
			return nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection v1 supports a single tag family, got %d", len(families))
		}
		family := families[0]
		if len(family.GetTags()) == 0 {
			return nil, fmt.Errorf("plan.Analyze: GroupBy.tag_projection family %q has no tags", family.GetName())
		}
		gb.TagFamily = family.GetName()
		gb.TagNames = append([]string(nil), family.GetTags()...)
		if validateErr := validateGroupByTags(measureSchema, gb); validateErr != nil {
			return nil, validateErr
		}
	}

	if tbProto != nil {
		// A time-bucketed GroupBy without an Agg has no execution support:
		// BatchTimeBucket's raw (no-AggSpec) shape reuses BatchAggregation's
		// output layout, which — unlike BatchGroupByFirst's full schema
		// passthrough — carries only tags and the bucket timestamp, silently
		// dropping every projected field (and, on the distributed path, the
		// series-id/version columns raw row merging requires). Reject rather
		// than silently lose data; a bucketed raw GroupBy is a possible
		// follow-up, not something this design ships.
		if req.GetAgg() == nil {
			return nil, fmt.Errorf("plan.Analyze: time_bucket requires Agg; a bucketed raw GroupBy (no aggregate) is not supported yet")
		}
		tb, tbErr := resolveTimeBucket(tbProto, measureSchema)
		if tbErr != nil {
			return nil, tbErr
		}
		if orderErr := validateBucketableOrdering(req); orderErr != nil {
			return nil, orderErr
		}
		gb.TimeBucket = tb
	}
	return gb, nil
}

// resolveTimeBucket implements the §5.3 width-resolution table: an explicit
// tb.Width wins; an empty Width falls back to the measure's own interval;
// both empty is rejected, and so is any width that fails to parse or is not
// strictly positive. There is deliberately no "must be a multiple of the
// interval" rule — Measure.interval is a declared write cadence, not an
// enforced storage invariant, so every positive width is equally meaningful.
func resolveTimeBucket(tb *measurev1.QueryRequest_GroupBy_TimeBucket, measureSchema *databasev1.Measure) (*model.MeasureTimeBucket, error) {
	raw := tb.GetWidth()
	if raw == "" {
		raw = measureSchema.GetInterval()
		if raw == "" {
			return nil, fmt.Errorf("plan.Analyze: time_bucket needs a width: measure %q declares no interval", measureSchema.GetMetadata().GetName())
		}
	}
	width, parseErr := timestamp.ParseDuration(raw)
	if parseErr != nil {
		return nil, fmt.Errorf("plan.Analyze: time_bucket width %q is not a valid duration: %w", raw, parseErr)
	}
	if width <= 0 {
		return nil, fmt.Errorf("plan.Analyze: time_bucket width %q must be positive, got %s", raw, width)
	}
	return &model.MeasureTimeBucket{
		Width:           raw,
		WidthNanos:      int64(width),
		UseIndexModeMap: measureSchema.GetIndexMode(),
	}, nil
}

// validateBucketableOrdering rejects a bucketed request whose order_by names
// a non-time index rule (design §7.2: both the streaming operator and the
// index-mode map fallback assume the request carries no ordering that would
// contradict time-ascending scan input). An aggregation request already
// carries no effective order_by today — Analyze never reads
// req.GetOrderBy(), and the distributed planner skips OrderBy resolution
// whenever Agg is set — so this is a belt-and-suspenders guard against a
// caller relying on a setting the engine would otherwise silently ignore,
// applied uniformly regardless of the streaming/map choice for a
// consistent contract.
func validateBucketableOrdering(req *measurev1.QueryRequest) error {
	orderBy := req.GetOrderBy()
	if ruleName := orderBy.GetIndexRuleName(); ruleName != "" {
		return fmt.Errorf("plan.Analyze: time_bucket requires time ordering; order_by.index_rule_name %q is not supported on a bucketed query", ruleName)
	}
	// An empty index_rule_name with Sort == SORT_DESC still resolves to a
	// time-ordered scan (index.OrderByTypeTime) per applyMeasureQueryOrdering
	// — just descending instead of ascending. BatchTimeBucket's streaming
	// path assumes ascending input specifically, not merely "time-ordered",
	// so this must be rejected too.
	if orderBy.GetSort() == modelv1.Sort_SORT_DESC {
		return fmt.Errorf("plan.Analyze: time_bucket requires ascending time order; order_by.sort SORT_DESC is not supported on a bucketed query")
	}
	return nil
}

// translateAgg builds the model Agg struct from the proto. Exactly one of
// field_name / tag_name must be set; a tag target additionally requires
// tag_family so the target is addressed explicitly and resolved against the
// right spec. That is belt-and-braces rather than disambiguation: a tag name
// is unique across a resource's families, which api/validate.tagFamily
// enforces. The target is then checked against the §6 semantics matrix.
func translateAgg(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) (*model.MeasureAgg, error) {
	aggProto := req.GetAgg()
	fieldName := aggProto.GetFieldName()
	tagName := aggProto.GetTagName()
	tagFamily := aggProto.GetTagFamily()

	switch {
	case fieldName != "" && tagName != "":
		return nil, fmt.Errorf("plan.Analyze: Agg must target exactly one of field_name or tag_name, got both (%q, %q)", fieldName, tagName)
	case fieldName == "" && tagName == "":
		return nil, fmt.Errorf("plan.Analyze: Agg must set exactly one of field_name or tag_name")
	case tagName != "" && tagFamily == "":
		return nil, fmt.Errorf("plan.Analyze: Agg.tag_name %q requires tag_family to be set", tagName)
	case fieldName != "":
		if validateErr := validateAggField(measureSchema, fieldName); validateErr != nil {
			return nil, validateErr
		}
		return &model.MeasureAgg{FieldName: fieldName, Func: aggProto.GetFunction()}, nil
	default:
		if validateErr := validateAggTag(measureSchema, tagFamily, tagName, aggProto.GetFunction()); validateErr != nil {
			return nil, validateErr
		}
		return &model.MeasureAgg{TagName: tagName, TagFamily: tagFamily, Func: aggProto.GetFunction()}, nil
	}
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

// ensureAggTagProjected mirrors ensureGroupByProjected for an Agg tag
// target: when the caller's tag_projection already names it, agg.HideTag
// stays false and the caller gets both a tag and a field of that name in
// the output (design §5.2 — separate namespaces, nothing collides).
// Otherwise the tag is appended to its family (creating the family if
// absent) and agg.HideTag is set so the injected copy is not also emitted
// as a first-seen tag column alongside the aggregation result.
func ensureAggTagProjected(tp []model.TagProjection, agg *model.MeasureAgg) []model.TagProjection {
	if agg == nil || agg.TagName == "" {
		return tp
	}
	for _, fam := range tp {
		if fam.Family != agg.TagFamily {
			continue
		}
		for _, n := range fam.Names {
			if n == agg.TagName {
				return tp
			}
		}
	}
	agg.HideTag = true
	out := append([]model.TagProjection(nil), tp...)
	for i := range out {
		if out[i].Family == agg.TagFamily {
			out[i].Names = append(append([]string(nil), out[i].Names...), agg.TagName)
			return out
		}
	}
	return append(out, model.TagProjection{Family: agg.TagFamily, Names: []string{agg.TagName}})
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

// validateAggTag resolves (tagFamily, tagName) against measureSchema and
// enforces the §6 semantics matrix: array and TIMESTAMP tags are rejected
// outright (mirroring keyComponentSupported, the operator's own key-encoding
// limit, so the two cannot drift apart); SUM/MIN/MAX/MEAN additionally
// require TAG_TYPE_INT, while COUNT and COUNT_DISTINCT accept any tag type
// that survives the array/timestamp check.
func validateAggTag(measureSchema *databasev1.Measure, tagFamily, tagName string, fn modelv1.AggregationFunction) error {
	spec := findTagSpec(measureSchema, tagFamily, tagName)
	if spec == nil {
		return fmt.Errorf("plan.Analyze: Agg tag %s.%s not present in measure schema", tagFamily, tagName)
	}
	colType, typeErr := tagTypeToColumnTypeMG(spec.GetType())
	if typeErr != nil {
		return fmt.Errorf("plan.Analyze: Agg tag %s.%s has type %s, which cannot be an aggregation target", tagFamily, tagName, spec.GetType())
	}
	if !measure.KeyComponentSupported(colType) {
		return fmt.Errorf("plan.Analyze: Agg tag %s.%s has type %s, which cannot be an aggregation target", tagFamily, tagName, spec.GetType())
	}
	switch fn {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT:
		return nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX,
		modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		if colType != vectorized.ColumnTypeInt64 {
			return fmt.Errorf("plan.Analyze: Agg tag %s.%s: %s is not supported over tag type %s", tagFamily, tagName, fn, spec.GetType())
		}
		return nil
	default:
		return fmt.Errorf("plan.Analyze: Agg.Function is UNSPECIFIED or unknown")
	}
}

// validateCountDistinctPushdown enforces design §7.4's decomposability
// condition for a COUNT_DISTINCT Agg: every one of the measure's routing
// tags (its sharding_key if configured, else its entity — the same
// fallback banyand/liaison/grpc/discovery.go's navigateByLocator already
// uses) must be covered by the GroupBy keys or be the aggregation target
// itself. If a routing tag is uncovered, a value sharing that routing tag
// value can land on more than one shard, so per-shard distinct counts are
// not disjoint and summing them would double-count.
//
// Evaluated identically for standalone and distributed requests — the
// design's own principle: a query either works everywhere or nowhere. A
// standalone deployment has no sharding and could serve any COUNT_DISTINCT
// correctly today, but must not silently diverge from what the same query
// does once the measure is sharded.
//
// Routing tag names are resolved against the schema by bare name — not
// family — because Entity/ShardingKey.TagNames carry no family qualifier
// at the schema level; pkg/partition/entity.go's NewEntityLocator /
// NewShardingKeyLocator already resolve them the same way via
// pbv1.FindTagByName. This function resolves each routing tag's actual
// family from the schema before comparing, so it does not introduce a new
// bare-name ambiguity on the GroupBy/target side, which IS family-qualified.
func validateCountDistinctPushdown(req *measurev1.QueryRequest, measureSchema *databasev1.Measure) error {
	agg := req.GetAgg()
	if agg == nil || agg.GetFunction() != modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT {
		return nil
	}
	routingTagNames := measureSchema.GetShardingKey().GetTagNames()
	if len(routingTagNames) == 0 {
		routingTagNames = measureSchema.GetEntity().GetTagNames()
	}
	families := measureSchema.GetTagFamilies()
	groupByFamily, groupByTagNames := distributedGroupByTagKey(req.GetGroupBy())
	targetFamily, targetName := agg.GetTagFamily(), agg.GetTagName()
	for _, routingName := range routingTagNames {
		fi, _, tagSpec := pbv1.FindTagByName(families, routingName)
		if tagSpec == nil {
			return fmt.Errorf("plan.Analyze: COUNT_DISTINCT cannot push down: routing tag %q is not present in the measure schema", routingName)
		}
		routingFamily := families[fi].GetName()
		covered := routingFamily == groupByFamily && slices.Contains(groupByTagNames, routingName)
		if !covered && routingFamily == targetFamily && routingName == targetName {
			covered = true
		}
		if !covered {
			return fmt.Errorf(
				"plan.Analyze: COUNT_DISTINCT cannot push down: routing tag %s.%s is neither a GroupBy key nor the aggregation target — "+
					"add it to GROUP BY, or query it directly instead of aggregating",
				routingFamily, routingName)
		}
	}
	return nil
}

// findTagSpec returns the TagSpec named (tagFamily, tagName) in
// measureSchema, or nil if no such family or tag exists.
func findTagSpec(measureSchema *databasev1.Measure, tagFamily, tagName string) *databasev1.TagSpec {
	for _, tf := range measureSchema.GetTagFamilies() {
		if tf.GetName() != tagFamily {
			continue
		}
		for _, ts := range tf.GetTags() {
			if ts.GetName() == tagName {
				return ts
			}
		}
		return nil
	}
	return nil
}
