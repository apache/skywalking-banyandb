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
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func startTraceSpan(ctx context.Context, msg string) (*query.Span, context.Context) {
	tracer := query.GetTracer(ctx)
	if tracer == nil {
		return nil, ctx
	}
	return tracer.StartSpan(ctx, "%s", msg)
}

func stopTraceSpan(span *query.Span) {
	if span != nil {
		span.Stop()
	}
}

func addTraceTag(span *query.Span, key, value string) {
	if span != nil {
		span.Tag(key, value)
	}
}

func addTraceTagf(span *query.Span, key, format string, args ...any) {
	if span != nil {
		span.Tagf(key, format, args...)
	}
}

func nodeSelectorCount(nodeSelectors map[string][]string) int {
	count := 0
	for _, selectors := range nodeSelectors {
		count += len(selectors)
	}
	return count
}

func batchRows(batches []*vectorized.RecordBatch) int {
	rows := 0
	for _, batch := range batches {
		if batch != nil {
			rows += batch.ActiveLen()
		}
	}
	return rows
}

// SupportsDistributedRows reports whether a non-aggregation request can use
// the native vectorized distributed row merge. Phase 2 lifts the
// OrderBy.IndexRuleName != "" gate; Phase 4 lifts the Top-without-Agg gate;
// Phase 5 lifts the GroupBy-without-Agg gate (raw GroupBy: first-seen row per
// group) and the multi-group + Top carve-out (per-group Limit is now
// calibrated rather than MaxUint32, so amplification is bounded).
//
// Rejected: Agg != nil — aggregation requests always go through executeAgg.
func SupportsDistributedRows(req *measurev1.QueryRequest) bool {
	if req == nil || req.GetAgg() != nil {
		return false
	}
	return true
}

// DistributedPlan is the vectorized liaison-side distributed measure plan.
// It consumes data-node raw frame bodies as []byte values from RawFrameCodec,
// decodes them into vectorized batches, and runs the liaison operators without
// routing through the row-compatible logical distributedPlan.
type DistributedPlan struct {
	patterns       []model.IndexedPattern
	queryTemplate  *measurev1.QueryRequest
	nodeTemplate   *measurev1.QueryRequest
	orderByTag     *resolvedOrderByTag
	hiddenOrderBy  logical.HiddenTagSet
	measureSchemas []*databasev1.Measure
	// hiddenTopField is the field name appended to the nodeTemplate's
	// FieldProjection for Top-without-Agg queries when the Top.FieldName
	// is not already in the user-visible FieldProjection. Data nodes
	// materialize the column so BatchTop can sort on it; the egress
	// hiddenFieldsMIterator strips it so the wire bytes match a query
	// without the extra projection.
	hiddenTopField string
	indexRules     [][]*databasev1.IndexRule
	cfg            vmeasure.VectorizedConfig
	// requiresSingleStage is true for a COUNT_DISTINCT Agg (design §7.4):
	// distinct counts are not additive across lifecycle stages (a value
	// can legitimately recur in both a hot and a warm stage), so a
	// request resolving to more than one stage must be rejected. The
	// analyzer has no visibility into resolved node selectors — stage
	// resolution happens later, in banyand/dquery — so this is a signal
	// for that caller to check post-resolution, not a rejection performed
	// here.
	requiresSingleStage bool
}

// RequiresSingleStage reports whether p's Agg is a COUNT_DISTINCT that
// must resolve to exactly one lifecycle stage (design §7.4). Callers that
// resolve node selectors from stages (banyand/dquery) must check this
// after resolution and before dispatch — AnalyzeDistributed cannot check
// it itself, since it never resolves stages.
func (p *DistributedPlan) RequiresSingleStage() bool { return p.requiresSingleStage }

// AnalyzeDistributed builds the vectorized distributed liaison plan.
// measureSchemas is the per-group slice of Measure schemas (one entry per
// req.Groups element). indexRules is the corresponding per-group slice of
// index rule sets (ec.GetIndexRules() for each group). Both slices must be
// in request-group order. Single-group callers may pass a length-1 slice for
// each (the existing behavior is preserved byte-for-byte).
//
// When indexRules is nil or empty and req.OrderBy.IndexRuleName is non-empty,
// the resolver surfaces an "index rule X not found" error byte-equivalent to
// the row path.
func AnalyzeDistributed(
	req *measurev1.QueryRequest,
	measureSchemas []*databasev1.Measure,
	indexRules [][]*databasev1.IndexRule,
	cfg vmeasure.VectorizedConfig, patterns ...model.IndexedPattern,
) (*DistributedPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("vec distributed analyze: nil request")
	}
	if len(patterns) > 0 {
		if len(measureSchemas) != len(req.Groups) || len(indexRules) != len(req.Groups) {
			return nil, fmt.Errorf("LIKE requires schemas and active index rules for every group")
		}
		for i, schema := range measureSchemas {
			if err := model.ValidateIndexedPatterns(schema, indexRules[i], patterns); err != nil {
				return nil, err
			}
		}
	}

	if len(measureSchemas) == 0 {
		return nil, fmt.Errorf("vec distributed analyze: no measure schemas supplied")
	}
	if cfgErr := cfg.Validate(); cfgErr != nil {
		return nil, fmt.Errorf("vec distributed analyze: %w", cfgErr)
	}
	// COUNT_DISTINCT does not support multi-group requests. Shard ids are
	// scoped per measure group, not globally unique — two independent
	// groups can each report "shard 0" for the same GroupBy key, and
	// markDedupSeen's (shardID, groupKey) dedup key cannot tell that
	// collision apart from a genuine replica duplicate. Depending on which
	// partial the collision drops, the result can undercount (two
	// groups' distinct target values collapse to one) or double-count
	// (the same target value happens to appear on both groups' colliding
	// shards). Rejecting outright is the honest signal until cross-group
	// distinctness has its own identity to dedup on — the same posture
	// design §7.4 takes for multi-stage requests, one layer up.
	if len(measureSchemas) > 1 && req.GetAgg().GetFunction() == modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT {
		return nil, fmt.Errorf("vec distributed analyze: COUNT_DISTINCT does not support multi-group requests — " +
			"shard ids are not unique across measure groups, so replica dedup cannot distinguish a genuine " +
			"duplicate from an unrelated partial; query each group separately")
	}
	// COUNT_DISTINCT's decomposability condition (design §7.4) must hold
	// for every group in a multi-group request — the same check Analyze
	// runs for standalone, since the rule is deliberately uniform across
	// deployment shapes.
	for _, ms := range measureSchemas {
		if pushdownErr := validateCountDistinctPushdown(req, ms); pushdownErr != nil {
			return nil, pushdownErr
		}
	}
	queryTemplate := proto.Clone(req).(*measurev1.QueryRequest)
	nodeTemplate := proto.Clone(req).(*measurev1.QueryRequest)
	limit := nodeTemplate.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	nodeTemplate.Limit = limit + nodeTemplate.GetOffset()
	nodeTemplate.Offset = 0
	// hadTop is captured before clearing Top so the data-node Limit can
	// be unbounded when the original query is Top-over-Agg. Without this,
	// each node aggregates its local rows and prunes to Limit before
	// returning, so the liaison's global Top sees only a per-node-truncated
	// subset of the groups and may miss the global winners entirely (e.g.
	// a node returns 2 groups out of 16, and svc15 - the actual top -
	// never crosses the wire).
	hadTop := nodeTemplate.GetTop() != nil
	origTop := req.GetTop() // capture before nodeTemplate.Top is cleared
	nodeTemplate.Top = nil
	// Phase 5: GroupBy without Agg (raw GroupBy) keeps GroupBy on the
	// nodeTemplate so each data node runs its per-node BatchGroupByFirst pass
	// and emits at most one row per group. This minimizes wire bytes: the
	// node sends one representative row per group instead of all matching rows.
	// GroupBy+Agg continues to keep both on the node for partial aggregation
	// (unchanged from prior phases). The old `nodeTemplate.GroupBy = nil` when
	// Agg == nil is intentionally dropped — raw GroupBy is now native.

	if hadTop {
		// Unbind the per-node Limit whenever the original query carries Top so
		// no data node can silently prune rows before the liaison's global
		// BatchTop or ApplyTopToReduce selects the true global top-N.
		//
		// For Top+Agg: MaxUint32 — per-node aggregation collapses rows to
		// per-group representatives; cardinality is unbounded at plan time
		// and the liaison's global Top must see all aggregated groups.
		//
		// For Top-without-Agg + GroupBy (Phase 5 / Phase 6 debt): the per-node
		// plan pushes Top back down to the data node so it runs BatchTop AFTER
		// BatchGroupByFirst. Without this push-down, data nodes return rows in
		// group-insertion order (not ranked by Top.FieldName); if perNodeLimit <
		// G_node the node silently drops groups that are the true global winners
		// but happen to sit beyond perNodeLimit in insertion order. With Top on the
		// node, each data node returns its local top-N groups (by Top.FieldName),
		// guaranteeing the liaison's global BatchTop sees the true per-node top-N
		// representatives.
		//
		// The per-node Top.Number is set to calibratedTopWithoutAggLimit so the
		// node's BatchTop retains the same number of rows as the Limit cap.
		//
		// For Top-without-Agg without GroupBy (Phase 4): MaxUint32 — the
		// per-node row count is unbounded and any finite cap risks dropping
		// global winners that haven't been deduplicated by GroupByFirst yet.
		switch {
		case nodeTemplate.GetAgg() != nil:
			nodeTemplate.Limit = math.MaxUint32
		case req.GetGroupBy() != nil:
			perNodeLimit := calibratedTopWithoutAggLimit(origTop, len(req.GetGroups()))
			nodeTemplate.Limit = perNodeLimit
			// Push the Top down to the data node: after BatchGroupByFirst emits
			// one row per group, BatchTop selects the local top-N by Top.FieldName.
			// This ensures the per-node response is already ranked, not
			// collection-order-truncated, so the liaison's global BatchTop sees
			// the true top-N representatives from every node.
			nodeTemplate.Top = proto.Clone(origTop).(*measurev1.QueryRequest_Top)
			nodeTemplate.Top.Number = int32(perNodeLimit)
		default:
			nodeTemplate.Limit = math.MaxUint32
		}
	}
	plan := &DistributedPlan{
		patterns:            append([]model.IndexedPattern(nil), patterns...),
		queryTemplate:       queryTemplate,
		nodeTemplate:        nodeTemplate,
		measureSchemas:      measureSchemas,
		indexRules:          indexRules,
		cfg:                 cfg,
		requiresSingleStage: req.GetAgg().GetFunction() == modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT,
	}
	// Resolve OrderBy by index rule for the non-agg row path. Agg requests
	// reduce on the liaison anyway and so do not need the cross-source
	// sort-column wiring; their OrderBy on the row stream is ignored.
	if req.GetAgg() == nil {
		orderBy := req.GetOrderBy()
		if orderBy != nil && orderBy.GetIndexRuleName() != "" {
			// For multi-group requests, search all groups' index rules and
			// accept the first match (mirrors the row path's mergeSchema-then-
			// resolve flow). Error text matches the row path byte-for-byte.
			resolved, resolveErr := resolveOrderByTagMultiGroup(measureSchemas, indexRules, orderBy.GetIndexRuleName())
			if resolveErr != nil {
				return nil, resolveErr
			}
			plan.orderByTag = &resolved
			// Hidden-projection augmentation: when the OrderBy tag is not
			// in the request's TagProjection, append it on the nodeTemplate
			// so data nodes materialize the column on the wire. The
			// liaison strips it at egress so the visible response matches
			// what a query without the OrderBy projection would emit.
			if !orderByProjectionVisible(req.GetTagProjection(), resolved) {
				nodeTemplate.TagProjection = appendOrderByToProjection(nodeTemplate.GetTagProjection(), resolved)
				plan.hiddenOrderBy = logical.NewHiddenTagSet()
				plan.hiddenOrderBy.Add(resolved.tag)
			}
		}
	}
	// Phase 4: Top-without-Agg field validation and hidden-projection augmentation.
	// Loud-failure rule: if Top.FieldName does not exist in any group's schema
	// the request is malformed and must not silently pass through.
	if top := req.GetTop(); top != nil && req.GetAgg() == nil {
		topFieldName := top.GetFieldName()
		if !topFieldExistsInSchemas(measureSchemas, topFieldName) {
			return nil, fmt.Errorf("vec distributed analyze: top field %s not found in schema", topFieldName)
		}
		// Hidden-projection augmentation: when the Top field is not in the
		// user-visible FieldProjection, append it to the nodeTemplate so data
		// nodes materialize the column for BatchTop sorting. The egress
		// hiddenFieldsMIterator strips it so the response matches a query
		// without the extra projection.
		if !topFieldProjectionVisible(req.GetFieldProjection(), topFieldName) {
			nodeTemplate.FieldProjection = appendTopFieldToProjection(nodeTemplate.GetFieldProjection(), topFieldName)
			plan.hiddenTopField = topFieldName
		}
	}
	return plan, nil
}

// resolveOrderByTagMultiGroup iterates per-group schemas and index-rule sets,
// accepting the first group in which indexRuleName resolves successfully.
// Error text on a total miss is byte-identical to the row path's resolver in
// pkg/query/logical/measure/cross_group_merge.go so distributed fixtures
// asserting WantErr land on identical messages across paths.
func resolveOrderByTagMultiGroup(measureSchemas []*databasev1.Measure, indexRules [][]*databasev1.IndexRule, indexRuleName string) (resolvedOrderByTag, error) {
	for groupIdx, ms := range measureSchemas {
		var rules []*databasev1.IndexRule
		if groupIdx < len(indexRules) {
			rules = indexRules[groupIdx]
		}
		resolved, resolveErr := resolveOrderByTag(ms, rules, indexRuleName)
		if resolveErr == nil {
			return resolved, nil
		}
	}
	return resolvedOrderByTag{}, fmt.Errorf("index rule %s not found", indexRuleName)
}

// orderByProjectionVisible reports whether the OrderBy tag is already
// projected on the user-facing TagProjection. When false, the analyzer
// augments the node template so data nodes materialize the column.
func orderByProjectionVisible(projection *modelv1.TagProjection, want resolvedOrderByTag) bool {
	if projection == nil {
		return false
	}
	for _, family := range projection.GetTagFamilies() {
		if family.GetName() == want.family && slices.Contains(family.GetTags(), want.tag) {
			return true
		}
	}
	return false
}

// appendOrderByToProjection clones the supplied projection and appends the
// OrderBy tag to its family (or appends a fresh family if needed). Mirrors
// augmentRequestWithHiddenTags's family-append shape: visible families keep
// their order and the OrderBy column lands at the end so the BatchSchema
// keeps the user-facing projection columns up front.
func appendOrderByToProjection(projection *modelv1.TagProjection, want resolvedOrderByTag) *modelv1.TagProjection {
	families := make([]*modelv1.TagProjection_TagFamily, 0)
	familyFound := false
	if projection != nil {
		for _, family := range projection.GetTagFamilies() {
			if family.GetName() == want.family {
				familyFound = true
				tags := append([]string(nil), family.GetTags()...)
				tags = append(tags, want.tag)
				families = append(families, &modelv1.TagProjection_TagFamily{Name: family.GetName(), Tags: tags})
				continue
			}
			tags := append([]string(nil), family.GetTags()...)
			families = append(families, &modelv1.TagProjection_TagFamily{Name: family.GetName(), Tags: tags})
		}
	}
	if !familyFound {
		families = append(families, &modelv1.TagProjection_TagFamily{Name: want.family, Tags: []string{want.tag}})
	}
	return &modelv1.TagProjection{TagFamilies: families}
}

// topFieldProjectionVisible reports whether fieldName is already present in the
// user-facing FieldProjection. When false, the analyzer augments the node
// template so data nodes materialize the column on the wire for BatchTop sorting.
func topFieldProjectionVisible(fp *measurev1.QueryRequest_FieldProjection, fieldName string) bool {
	if fp == nil {
		return false
	}
	return slices.Contains(fp.GetNames(), fieldName)
}

// appendTopFieldToProjection clones the supplied FieldProjection and appends
// fieldName to its names list. Mirrors appendOrderByToProjection's clone-before-
// mutate discipline: the user-facing FieldProjection is left untouched and only
// the nodeTemplate's copy is extended.
func appendTopFieldToProjection(fp *measurev1.QueryRequest_FieldProjection, fieldName string) *measurev1.QueryRequest_FieldProjection {
	var names []string
	if fp != nil {
		names = append([]string(nil), fp.GetNames()...)
	}
	names = append(names, fieldName)
	return &measurev1.QueryRequest_FieldProjection{Names: names}
}

// topFieldExistsInSchemas returns true iff fieldName is declared as a field in
// at least one of the supplied measure schemas. Used by AnalyzeDistributed to
// enforce the loud-failure rule for missing Top.FieldName.
func topFieldExistsInSchemas(measureSchemas []*databasev1.Measure, fieldName string) bool {
	for _, ms := range measureSchemas {
		for _, fs := range ms.GetFields() {
			if fs.GetName() == fieldName {
				return true
			}
		}
	}
	return false
}

// intersectTagProjection returns a TagProjection that contains only the tags that
// exist in the given group's schema. Tags in the request projection that are not
// defined in the group's schema are silently dropped so data nodes do not reject
// the request for unknown tag names.
func intersectTagProjection(proj *modelv1.TagProjection, ms *databasev1.Measure) *modelv1.TagProjection {
	if proj == nil || ms == nil {
		return proj
	}
	// Build lookup set from schema.
	schemaTagSet := make(map[string]struct{})
	for _, tf := range ms.GetTagFamilies() {
		for _, ts := range tf.GetTags() {
			schemaTagSet[tf.GetName()+"\x00"+ts.GetName()] = struct{}{}
		}
	}
	filteredFamilies := make([]*modelv1.TagProjection_TagFamily, 0, len(proj.GetTagFamilies()))
	for _, fam := range proj.GetTagFamilies() {
		filteredTags := make([]string, 0, len(fam.GetTags()))
		for _, tagName := range fam.GetTags() {
			if _, exists := schemaTagSet[fam.GetName()+"\x00"+tagName]; exists {
				filteredTags = append(filteredTags, tagName)
			}
		}
		if len(filteredTags) > 0 {
			filteredFamilies = append(filteredFamilies, &modelv1.TagProjection_TagFamily{
				Name: fam.GetName(),
				Tags: filteredTags,
			})
		}
	}
	if len(filteredFamilies) == 0 {
		return nil
	}
	return &modelv1.TagProjection{TagFamilies: filteredFamilies}
}

// intersectFieldProjection returns a FieldProjection that contains only the fields
// that exist in the given group's schema. Unknown fields are silently dropped.
func intersectFieldProjection(proj *measurev1.QueryRequest_FieldProjection, ms *databasev1.Measure) *measurev1.QueryRequest_FieldProjection {
	if proj == nil || ms == nil {
		return proj
	}
	schemaFieldSet := make(map[string]struct{}, len(ms.GetFields()))
	for _, fs := range ms.GetFields() {
		schemaFieldSet[fs.GetName()] = struct{}{}
	}
	filteredNames := make([]string, 0, len(proj.GetNames()))
	for _, name := range proj.GetNames() {
		if _, exists := schemaFieldSet[name]; exists {
			filteredNames = append(filteredNames, name)
		}
	}
	if len(filteredNames) == 0 {
		return nil
	}
	return &measurev1.QueryRequest_FieldProjection{Names: filteredNames}
}

// Execute broadcasts the internal query and executes the liaison-side vectorized plan.
// For single-group requests, one broadcast is issued for all groups (existing
// behavior). For multi-group requests, one broadcast is issued per group,
// each carrying a single-element Groups slice, so data nodes can answer with
// a schema that matches only their local group's columns.
func (p *DistributedPlan) Execute(ctx context.Context) (executor.MIterator, error) {
	result, err := p.ExecuteBatches(ctx)
	if err != nil {
		return nil, err
	}
	return result.NativeIterator(ctx)
}

// ExecuteBatches executes the native plan without constructing intermediate DataPoints. It scans the data nodes
// as streams of chunks and reads them only as far as the output is pulled: the rows merge decodes one chunk at a
// time, and a time-bucketed aggregation reduces and emits a bucket once every node has passed it. The returned
// result owns the node streams and every batch, and must be closed, including after cancellation: closing it
// cancels the scans on the data nodes.
func (p *DistributedPlan) ExecuteBatches(ctx context.Context) (result *BatchResult, err error) {
	queryRequest := proto.Clone(p.queryTemplate).(*measurev1.QueryRequest)
	if p.streamsBuckets(queryRequest) {
		// A bucket's state is returned when the bucket is emitted, so the lease follows what is held now.
		ctx = vectorized.WithLiveLeaseAccounting(ctx)
	}
	ctx = vectorized.WithQueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024)
	tracker := vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !data.MeasureWireModeRaw() {
		return nil, fmt.Errorf("vec distributed plan requires raw measure wire mode")
	}
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest.TimeRange = dctx.TimeRange()
	scanCtx, cancel := context.WithCancel(ctx)
	set := newScanSet(ctx, tracker, cancel, p.cfg.BatchSize)
	defer func() {
		if err != nil {
			set.close()
		}
	}()

	groups := queryRequest.GetGroups()
	if len(groups) <= 1 {
		nodeRequest := p.cloneNodeRequest(dctx)
		spanName := "broadcast-rows"
		if queryRequest.GetAgg() != nil {
			spanName = "broadcast-agg"
		}
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		if err = p.openScan(ctx, scanCtx, set, dctx, internalRequest, 0, spanName, ""); err != nil {
			return nil, err
		}
		if queryRequest.GetAgg() == nil {
			return p.executeRowsStream(ctx, set, queryRequest, nil)
		}
		return p.executeAggStream(ctx, set, queryRequest, nil)
	}

	// Multi-group path: one scan per group so each data node returns frames whose schema reflects only its local
	// group's column layout.
	partialSchemas, scanErr := p.openGroupScans(ctx, scanCtx, set, dctx, queryRequest, groups)
	if scanErr != nil {
		return nil, scanErr
	}
	if queryRequest.GetAgg() == nil {
		return p.executeRowsMultiGroup(ctx, set, queryRequest)
	}
	// Multi-group agg: reduce every group's partials into one layout derived from the groups' schemas, not from
	// whichever groups returned rows.
	target, targetErr := multiGroupAggTarget(partialSchemas, len(groups), queryRequest.GetTagProjection())
	if targetErr != nil {
		err = fmt.Errorf("vec distributed plan: %w", targetErr)
		return nil, err
	}
	return p.executeAggStream(ctx, set, queryRequest, target)
}

// streamsBuckets reports whether the request is a time-bucketed group-by that is reduced bucket by bucket: a
// single-group aggregation over a part-scan measure without Top. Index-mode measures have no ascending-timestamp
// guarantee, and Top and the multi-group schema merge need every partial, so those reduce after the scan.
func (p *DistributedPlan) streamsBuckets(req *measurev1.QueryRequest) bool {
	return req.GetAgg() != nil && req.GetGroupBy().GetTimeBucket() != nil && req.GetTop() == nil &&
		len(req.GetGroups()) <= 1 && len(p.measureSchemas) > 0 && !p.measureSchemas[0].GetIndexMode()
}

// openGroupScans opens one scan per group. For aggregation queries it returns the partial layout each group emits.
func (p *DistributedPlan) openGroupScans(ctx, scanCtx context.Context, set *scanSet, dctx executor.DistributedExecutionContext,
	queryRequest *measurev1.QueryRequest, groups []string,
) ([]*vectorized.BatchSchema, error) {
	var partialSchemas []*vectorized.BatchSchema
	for groupIdx, groupName := range groups {
		nodeRequest := p.cloneNodeRequest(dctx)
		nodeRequest.Groups = []string{groupName}
		// Filter TagProjection and FieldProjection to only include columns that
		// exist in this group's schema. Data nodes reject tags/fields that are
		// not defined in their local schema, so the liaison must not send
		// columns belonging to a different group's schema.
		var groupSchema *databasev1.Measure
		if groupIdx < len(p.measureSchemas) {
			groupSchema = p.measureSchemas[groupIdx]
		}
		if groupSchema != nil {
			nodeRequest.TagProjection = intersectTagProjection(nodeRequest.GetTagProjection(), groupSchema)
			nodeRequest.FieldProjection = intersectFieldProjection(nodeRequest.GetFieldProjection(), groupSchema)
		}
		if queryRequest.GetAgg() != nil && groupSchema != nil {
			// The partial layout this group's data nodes emit for nodeRequest.
			partialSchema, partialErr := planGroupPartialSchema(nodeRequest, groupSchema)
			if partialErr != nil {
				return nil, fmt.Errorf("vec distributed plan: plan group %s partials: %w", groupName, partialErr)
			}
			partialSchemas = append(partialSchemas, partialSchema)
		}
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		if err := p.openScan(ctx, scanCtx, set, dctx, internalRequest, groupIdx, fmt.Sprintf("broadcast-per-group-%s", groupName), groupName); err != nil {
			return nil, err
		}
	}
	return partialSchemas, nil
}

// executeAggStream reduces the partials of the data nodes. A non-nil target is the layout every partial is
// reshaped onto first (multi-group queries).
func (p *DistributedPlan) executeAggStream(ctx context.Context, set *scanSet, req *measurev1.QueryRequest, target *vectorized.BatchSchema) (*BatchResult, error) {
	keyTagFamily, keyTagNames := distributedGroupByTagKey(req.GetGroupBy())
	bucketed := req.GetGroupBy().GetTimeBucket() != nil
	aggFunc, aggErr := distributedAggFunc(req.GetAgg().GetFunction())
	if aggErr != nil {
		return nil, aggErr
	}
	aggSpecs := []vmeasure.AggReduceSpec{{OutputName: aggOutputName(req.GetAgg()), Func: aggFunc}}
	var topSpec *vmeasure.ReduceTopSpec
	if top := req.GetTop(); top != nil {
		topSpec = &vmeasure.ReduceTopSpec{FieldName: top.GetFieldName(), N: int(top.GetNumber()), Asc: top.GetFieldValueSort() == modelv1.Sort_SORT_ASC}
	}
	tracker := vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024)
	newReducer := func() *vmeasure.PartialReducer {
		return vmeasure.NewPartialReducer(target, keyTagFamily, keyTagNames, bucketed, aggSpecs, p.cfg.BatchSize, tracker)
	}
	if p.streamsBuckets(req) {
		return p.executeBucketStream(ctx, set, req, func() *vmeasure.PartialReducer {
			return vmeasure.NewPartialReducer(nil, keyTagFamily, keyTagNames, true, aggSpecs, p.cfg.BatchSize, tracker)
		})
	}
	reduceSpan, reduceSpanCtx := startTraceSpan(ctx, "reduce-raw-frames")
	set.summaryCtx = reduceSpanCtx
	reducer := newReducer()
	defer reducer.Close()
	var frames int
	for _, src := range set.sources {
		for {
			batch, nextErr := src.next(ctx)
			if nextErr != nil {
				return nil, p.failReduce(reduceSpan, nextErr)
			}
			if batch == nil {
				break
			}
			frames++
			// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
			consumeErr := reducer.Consume(batch)
			batch.ReleaseOwners()
			if consumeErr != nil {
				return nil, p.failReduce(reduceSpan, fmt.Errorf("vec distributed plan: reduce raw frames: %w", consumeErr))
			}
		}
	}
	// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
	batches, aggValuePath, reduceErr := reducer.Finish()
	if reduceErr != nil {
		addTraceTag(reduceSpan, tracelabels.TagAggValuePath, string(aggValuePath))
		return nil, p.failReduce(reduceSpan, fmt.Errorf("vec distributed plan: reduce raw frames: %w", reduceErr))
	}
	addTraceTagf(reduceSpan, tracelabels.TagFramesIn, "%d", frames)
	addTraceTagf(reduceSpan, tracelabels.TagRowsOut, "%d", batchRows(batches))
	addTraceTagf(reduceSpan, tracelabels.TagGroupsOut, "%d", batchRows(batches))
	addTraceTag(reduceSpan, tracelabels.TagAggValuePath, string(aggValuePath))
	stopTraceSpan(reduceSpan)
	if topSpec != nil && topSpec.N > 0 && len(batches) > 0 {
		topSpan, _ := startTraceSpan(ctx, "apply-top-to-reduce")
		rowsIn := batchRows(batches)
		// nolint:contextcheck // pure in-memory top selection; no cancelable I/O downstream
		topped, topErr := vmeasure.ApplyTopToReduce(batches, *topSpec, p.cfg.BatchSize)
		if topErr != nil {
			if topSpan != nil {
				topSpan.Error(topErr)
				stopTraceSpan(topSpan)
			}
			return nil, fmt.Errorf("vec distributed plan: top reduced frames: %w", topErr)
		}
		rowsOut := batchRows(topped)
		addTraceTagf(topSpan, tracelabels.TagTopN, "%d", topSpec.N)
		addTraceTagf(topSpan, tracelabels.TagTopAsc, "%t", topSpec.Asc)
		addTraceTagf(topSpan, tracelabels.TagRowsIn, "%d", rowsIn)
		addTraceTagf(topSpan, tracelabels.TagRowsOut, "%d", rowsOut)
		addTraceTagf(topSpan, tracelabels.TagDroppedRows, "%d", rowsIn-rowsOut)
		addTraceTag(topSpan, tracelabels.TagDropReason, "top")
		stopTraceSpan(topSpan)
		batches = topped
	}
	return p.batchResultOf(ctx, set, batches, req, nil)
}

func (p *DistributedPlan) failReduce(span *query.Span, err error) error {
	if span != nil {
		span.Error(err)
		stopTraceSpan(span)
	}
	return err
}

// executeBucketStream returns the result of a bucket-by-bucket reduction, which is pulled as the data nodes stream.
func (p *DistributedPlan) executeBucketStream(ctx context.Context, set *scanSet, req *measurev1.QueryRequest,
	template func() *vmeasure.PartialReducer,
) (*BatchResult, error) {
	reduceSpan, reduceSpanCtx := startTraceSpan(ctx, "reduce-raw-frames")
	set.summaryCtx = reduceSpanCtx
	source, err := newBucketReduceSource(ctx, set, template)
	if err != nil {
		return nil, p.failReduce(reduceSpan, fmt.Errorf("vec distributed plan: reduce raw frames: %w", err))
	}
	if source.OutputSchema() == nil {
		// No node sent a row.
		_ = source.Close()
		stopTraceSpan(reduceSpan)
		return p.batchResultOf(ctx, set, nil, req, nil)
	}
	result := p.sourceResult(ctx, set, source, req, source.OutputSchema())
	result.onClose = append(result.onClose, func() {
		addTraceTagf(reduceSpan, tracelabels.TagGroupsOut, "%d", source.bucketsOut)
		addTraceTag(reduceSpan, tracelabels.TagAggValuePath, string(vmeasure.AggValuePathTyped))
		addTraceTag(reduceSpan, "bucket_watermark", "true")
		stopTraceSpan(reduceSpan)
	})
	return result, nil
}

// executeRowsStream merges the rows of the data nodes of a single-group request.
func (p *DistributedPlan) executeRowsStream(ctx context.Context, set *scanSet, req *measurev1.QueryRequest, _ *vectorized.BatchSchema) (*BatchResult, error) {
	inputs, schema, err := p.primeRows(ctx, set, nil)
	if err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		return p.batchResultOf(ctx, set, nil, req, nil)
	}
	if schema.SeriesIDIndex() < 0 || schema.TimestampIndex() < 0 || schema.VersionIndex() < 0 {
		for _, in := range inputs {
			in.first.ReleaseOwners()
		}
		return nil, fmt.Errorf("vec distributed plan: merge rows: row merge requires series-id, timestamp, and version columns")
	}
	spec := p.rowsSpec(ctx, req, p.measureSchemas[0].GetIndexMode())
	return p.mergeRows(ctx, set, req, inputs, schema, sameLayoutReshape(schema), spec, nil)
}

// executeRowsMultiGroup is the multi-group non-agg row merge path. It builds the merged BatchSchema by unioning
// all per-group schemas, then runs the k-way heap merger over the frames of every group decoded into it.
func (p *DistributedPlan) executeRowsMultiGroup(ctx context.Context, set *scanSet, req *measurev1.QueryRequest) (*BatchResult, error) {
	schemaSpan, _ := startTraceSpan(ctx, "build-multi-group-schema")
	addTraceTagf(schemaSpan, tracelabels.TagGroupsIn, "%d", len(p.measureSchemas))
	mergedSchema, schemaErr := BuildMultiGroupBatchSchema(p.measureSchemas, req)
	if schemaErr != nil {
		if schemaSpan != nil {
			schemaSpan.Error(schemaErr)
			stopTraceSpan(schemaSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: build multi-group schema: %w", schemaErr)
	}
	if mergedSchema != nil {
		addTraceTagf(schemaSpan, tracelabels.TagSchemaCols, "%d", len(mergedSchema.Columns))
	}
	addTraceTagf(schemaSpan, tracelabels.TagSchemaDegraded, "%t", false)
	stopTraceSpan(schemaSpan)
	if mergedSchema == nil {
		return p.batchResultOf(ctx, set, nil, req, nil)
	}
	inputs, _, err := p.primeRows(ctx, set, mergedSchema)
	if err != nil {
		return nil, err
	}
	// IndexMode is true when ALL groups are index-mode. A mixed-mode configuration is treated as non-index-mode
	// to avoid over-suppressing rows from non-index-mode groups.
	indexMode := len(p.measureSchemas) > 0
	for _, ms := range p.measureSchemas {
		if !ms.GetIndexMode() {
			indexMode = false
			break
		}
	}
	spec := p.rowsSpec(ctx, req, indexMode)
	if len(inputs) == 0 {
		return p.batchResultOf(ctx, set, nil, req, mergedSchema)
	}
	return p.mergeRows(ctx, set, req, inputs, mergedSchema, layoutReshape(mergedSchema, true), spec, mergedSchema)
}

// primeRows reads the first chunk of every node: the layout of the merged rows is known only from them. For a
// single group the layout is the union of the nodes' layouts; with a merged schema it is that schema.
func (p *DistributedPlan) primeRows(ctx context.Context, set *scanSet, merged *vectorized.BatchSchema) ([]rowStreamInput, *vectorized.BatchSchema, error) {
	inputs := make([]rowStreamInput, 0, len(set.sources))
	releaseInputs := func() {
		for _, in := range inputs {
			if in.first != nil {
				in.first.ReleaseOwners()
			}
		}
	}
	for _, src := range set.sources {
		first, err := src.next(ctx)
		if err != nil {
			releaseInputs()
			return nil, nil, err
		}
		if first == nil {
			continue
		}
		inputs = append(inputs, rowStreamInput{src: src, first: first, source: src.index, group: src.group})
	}
	if merged != nil || len(inputs) == 0 {
		return inputs, merged, nil
	}
	sources := make([]decodedBatchSource, 0, len(inputs))
	for _, in := range inputs {
		sources = append(sources, decodedBatchSource{batch: in.first, source: in.source, group: in.group})
	}
	unioned, unionErr := unionDistributedRowFrameSchemas(sources)
	if unionErr != nil {
		releaseInputs()
		return nil, nil, unionErr
	}
	return inputs, unioned, nil
}

// rowsSpec is the merge configuration of a rows request.
func (p *DistributedPlan) rowsSpec(ctx context.Context, req *measurev1.QueryRequest, indexMode bool) distributedRowsSpec {
	spec := distributedRowsSpec{
		Desc:          req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
		IndexMode:     indexMode,
		BatchSize:     p.cfg.BatchSize,
		Tracker:       vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024),
		OrderByColIdx: -1,
		// The seen-set of the index-mode merge lives as long as the query: it is charged to the query lease.
		ChargeSeen: func(bytes uint64) error { return query.Charge(ctx, bytes) },
	}
	if p.orderByTag != nil {
		spec.OrderByFamily = p.orderByTag.family
		spec.OrderByTagName = p.orderByTag.tag
	}
	return spec
}

// mergeRows merges the primed inputs. Raw GroupBy and Top need every row, so they collect the merge and run as
// they always did; every other rows request is returned as a lazy source that pulls the data nodes as it is read.
func (p *DistributedPlan) mergeRows(ctx context.Context, set *scanSet, req *measurev1.QueryRequest, inputs []rowStreamInput,
	schema *vectorized.BatchSchema, reshape func(*vectorized.RecordBatch) (*vectorized.RecordBatch, error), spec distributedRowsSpec,
	resultSchema *vectorized.BatchSchema,
) (*BatchResult, error) {
	mergeSpan, mergeSpanCtx := startTraceSpan(ctx, "merge-distributed-rows")
	set.summaryCtx = mergeSpanCtx
	addTraceTagf(mergeSpan, tracelabels.TagSourcesIn, "%d", len(inputs))
	merger, mergeErr := newRowMerger(ctx, inputs, schema, reshape, spec)
	if mergeErr != nil {
		if mergeSpan != nil {
			mergeSpan.Error(mergeErr)
			stopTraceSpan(mergeSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: merge rows: %w", mergeErr)
	}
	finishMergeSpan := func() {
		addTraceTagf(mergeSpan, tracelabels.TagRowsOut, "%d", merger.rows)
		addTraceTagf(mergeSpan, tracelabels.TagOrderByColIdx, "%d", spec.OrderByColIdx)
		addTraceTag(mergeSpan, tracelabels.TagOrderByFamily, spec.OrderByFamily)
		addTraceTag(mergeSpan, tracelabels.TagOrderByTag, spec.OrderByTagName)
		addTraceTagf(mergeSpan, tracelabels.TagDesc, "%t", spec.Desc)
		addTraceTagf(mergeSpan, tracelabels.TagIndexMode, "%t", spec.IndexMode)
		stopTraceSpan(mergeSpan)
	}
	groupBy, top := req.GetGroupBy(), req.GetTop()
	if groupBy == nil && top == nil {
		result := p.sourceResult(ctx, set, merger, req, schema)
		result.onClose = append(result.onClose, finishMergeSpan)
		return result, nil
	}
	batches, drainErr := drainRowMerger(ctx, merger)
	if drainErr != nil {
		if mergeSpan != nil {
			mergeSpan.Error(drainErr)
			stopTraceSpan(mergeSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: merge rows: %w", drainErr)
	}
	finishMergeSpan()
	tracker := spec.Tracker
	// Phase 5: raw GroupBy liaison pass. Runs after the k-way heap merge so
	// the stream is already (OrderBy/ts, sid)-sorted, and before BatchTop so
	// that BatchTop never sees two rows from the same group (which would
	// mean both could rank in top-N while only one should survive).
	//
	// Row-vs-vec Top+GroupBy semantic divergence (architect-verified, Phase 6):
	//   Row path (pkg/query/logical/measure/measure_analyzer.go:129-147):
	//     Top sees ALL rows first, then GroupBy surfaces the first-seen row per
	//     group from the top-N result set.
	//   Vec path (here):
	//     GroupByFirst sees all rows and retains the first-seen row per group,
	//     then BatchTop ranks those per-group representatives.
	// For typical queries these produce identical output: users expect to rank
	// groups by a representative value, and the first-seen row per group is the
	// natural representative in both paths. A query shape where the two orderings
	// differ would be visible only if the Top.FieldName differs across rows within a
	// group, which is atypical in time-series measures that aggregate to a scalar per
	// series per window.
	if groupBy != nil {
		var gbErr error
		before := batches
		// nolint:contextcheck // pure in-memory dedup; no cancelable I/O downstream
		batches, gbErr = applyBatchGroupByFirstToRows(batches, groupBy, p.cfg.BatchSize, tracker)
		releaseReplaced(before, batches)
		if gbErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply group-by to rows: %w", gbErr)
		}
	}
	if top != nil {
		var topErr error
		before := batches
		// nolint:contextcheck // pure in-memory top selection; no cancelable I/O downstream
		batches, topErr = applyBatchTopToRows(batches, top, p.cfg.BatchSize)
		releaseReplaced(before, batches)
		if topErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply top to rows: %w", topErr)
		}
	}
	return p.batchResultOf(ctx, set, batches, req, resultSchema)
}

// sourceResult wraps a pull source as the result of the query; closing the result closes the source and the scans.
func (p *DistributedPlan) sourceResult(ctx context.Context, set *scanSet, source vectorized.PullOperator, req *measurev1.QueryRequest,
	schema *vectorized.BatchSchema,
) *BatchResult {
	vmeasure.RecordBatchResultOpened()
	return &BatchResult{
		source: &lendingSource{inner: source}, schema: schema, batchSize: p.cfg.BatchSize, plan: p, request: req,
		tracker: vectorized.QueryMemoryTracker(ctx, int64(p.cfg.QueryMemoryMiB)*1024*1024),
		onClose: []func(){set.close},
	}
}

// batchResultOf wraps collected batches as the result of the query and hands the scans to it.
func (p *DistributedPlan) batchResultOf(ctx context.Context, set *scanSet, batches []*vectorized.RecordBatch,
	req *measurev1.QueryRequest, schema *vectorized.BatchSchema,
) (*BatchResult, error) {
	result, err := p.batchResult(ctx, batches, req, schema)
	if err != nil {
		return nil, err
	}
	result.onClose = append(result.onClose, set.close)
	return result, nil
}

// applyBatchTopToRows runs a BatchTop operator over the merged row batches,
// returning the top-N rows by top.FieldName. The schema is derived from the
// first non-nil batch; if there are no batches the input is returned unchanged.
// Loud-failure: if top.FieldName does not resolve on the merged schema's field
// columns, an error is returned rather than silently passing through.
//
// batches is normally the batchSize-paginated output of mergeDistributedRows
// / mergeDistributedRowsMulti, so in practice no single b here exceeds
// batchSize rows. But this function does not control its caller's batching
// discipline, and BatchTop.Consume shares BatchAggregation's exposure to the
// design/0.12.0/limit-after-aggregation §5 uint16 wraparound (both drive
// their per-row loop from activeIndices), so a batch larger than batchSize
// is chunked via vectorized.ConsumeChunked exactly like ReducePartialBatches,
// rather than assumed away.
func applyBatchTopToRows(batches []*vectorized.RecordBatch, top *measurev1.QueryRequest_Top, batchSize int) ([]*vectorized.RecordBatch, error) {
	if top == nil || top.GetNumber() <= 0 || len(batches) == 0 {
		return batches, nil
	}
	var schema *vectorized.BatchSchema
	for _, b := range batches {
		if b != nil && b.Schema != nil {
			schema = b.Schema
			break
		}
	}
	if schema == nil {
		return batches, nil
	}
	fieldIdx, ok := schema.FieldIndex(top.GetFieldName())
	if !ok {
		return nil, fmt.Errorf("top field %s not found in schema", top.GetFieldName())
	}
	asc := top.GetFieldValueSort() == modelv1.Sort_SORT_ASC
	topOp := vmeasure.NewBatchTop(schema, fieldIdx, int(top.GetNumber()), asc, batchSize)
	defer topOp.Close()
	if initErr := topOp.Init(context.Background()); initErr != nil {
		return nil, fmt.Errorf("applyBatchTopToRows: init: %w", initErr)
	}
	var scratch *vectorized.RecordBatch
	for idx, b := range batches {
		if b == nil || b.Len == 0 {
			continue
		}
		if b.ActiveLen() <= batchSize {
			if consumeErr := topOp.Consume(context.Background(), b); consumeErr != nil {
				return nil, fmt.Errorf("applyBatchTopToRows: consume batch %d: %w", idx, consumeErr)
			}
			continue
		}
		if scratch == nil {
			scratch = vectorized.NewRecordBatch(schema, batchSize)
		}
		if consumeErr := vectorized.ConsumeChunked(topOp, scratch, b, batchSize); consumeErr != nil {
			return nil, fmt.Errorf("applyBatchTopToRows: consume batch %d: %w", idx, consumeErr)
		}
	}
	if finalErr := topOp.Finalize(context.Background()); finalErr != nil {
		return nil, fmt.Errorf("applyBatchTopToRows: finalize: %w", finalErr)
	}
	var out []*vectorized.RecordBatch
	for {
		nb, nextErr := topOp.NextBatch(context.Background())
		if nextErr != nil {
			return nil, fmt.Errorf("applyBatchTopToRows: next: %w", nextErr)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out, nil
}

// applyBatchGroupByFirstToRows runs a BatchGroupByFirst operator over the
// merged row batches, retaining only the first-seen row per group. This is
// the liaison-side pass for raw GroupBy (GroupBy without Agg): data nodes
// already emit one row per group via the per-node BatchGroupByFirst inserted
// by the single-node analyzer (BuildOperators → NewBatchGroupByFirst). The
// liaison re-applies GroupByFirst across the per-node partials to collapse any
// residual duplicates that arise when two data nodes happen to hold the same
// group's first-seen row (which can occur under replicated or rebalanced
// shards). "First" is defined by insertion order in the k-way heap-merged
// stream — which is already deterministically sorted by (OrderBy/ts, sid) —
// so the output is stable across nodes.
//
// GroupBy tag names are taken from req.GroupBy.TagProjection (first family,
// v1 single-family limitation). Tag indices are resolved on the merged schema.
// Loud-failure: if any GroupBy tag name is not present in the merged schema an
// error is returned so the caller can surface it rather than silently dropping
// the column.
//
// When req.GroupBy is nil the function is a no-op and returns batches unchanged.
//
// batches is normally the batchSize-paginated output of mergeDistributedRows
// / mergeDistributedRowsMulti, so in practice no single b here exceeds
// batchSize rows. But this function does not control its caller's batching
// discipline, and BatchGroupBy.Consume shares BatchAggregation's exposure to
// the design/0.12.0/limit-after-aggregation §5 uint16 wraparound (both drive
// their per-row loop from activeIndices), so a batch larger than batchSize is
// chunked via vectorized.ConsumeChunked exactly like ReducePartialBatches,
// rather than assumed away.
func applyBatchGroupByFirstToRows(
	batches []*vectorized.RecordBatch,
	groupBy *measurev1.QueryRequest_GroupBy,
	batchSize int,
	tracker *vectorized.MemoryTracker,
) ([]*vectorized.RecordBatch, error) {
	if groupBy == nil || len(batches) == 0 {
		return batches, nil
	}
	families := groupBy.GetTagProjection().GetTagFamilies()
	if len(families) == 0 || len(families[0].GetTags()) == 0 {
		return batches, nil
	}
	family := families[0].GetName()
	tagNames := families[0].GetTags()

	var schema *vectorized.BatchSchema
	for _, b := range batches {
		if b != nil && b.Schema != nil {
			schema = b.Schema
			break
		}
	}
	if schema == nil {
		return batches, nil
	}

	// Resolve GroupBy tag column indices on the merged schema.
	keyIndices := make([]int, 0, len(tagNames))
	for _, tagName := range tagNames {
		found := false
		for colIdx, def := range schema.Columns {
			if def.Role == vectorized.RoleTag && def.TagFamily == family && def.Name == tagName {
				keyIndices = append(keyIndices, colIdx)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("groupby tag %s not found in schema", tagName)
		}
	}

	pool := vectorized.NewBatchPool(schema, batchSize)
	const groupByEntrySize int64 = 512
	gbOp := vmeasure.NewBatchGroupByFirst(schema, keyIndices, pool, batchSize, tracker, groupByEntrySize)
	defer gbOp.Close()
	if initErr := gbOp.Init(context.Background()); initErr != nil {
		return nil, fmt.Errorf("applyBatchGroupByFirstToRows: init: %w", initErr)
	}
	var scratch *vectorized.RecordBatch
	for batchIdx, b := range batches {
		if b == nil || b.Len == 0 {
			continue
		}
		if b.ActiveLen() <= batchSize {
			if consumeErr := gbOp.Consume(context.Background(), b); consumeErr != nil {
				return nil, fmt.Errorf("applyBatchGroupByFirstToRows: consume batch %d: %w", batchIdx, consumeErr)
			}
			continue
		}
		if scratch == nil {
			scratch = vectorized.NewRecordBatch(schema, batchSize)
		}
		if consumeErr := vectorized.ConsumeChunked(gbOp, scratch, b, batchSize); consumeErr != nil {
			return nil, fmt.Errorf("applyBatchGroupByFirstToRows: consume batch %d: %w", batchIdx, consumeErr)
		}
	}
	if finalErr := gbOp.Finalize(context.Background()); finalErr != nil {
		return nil, fmt.Errorf("applyBatchGroupByFirstToRows: finalize: %w", finalErr)
	}
	var out []*vectorized.RecordBatch
	for {
		nb, nextErr := gbOp.NextBatch(context.Background())
		if nextErr != nil {
			return nil, fmt.Errorf("applyBatchGroupByFirstToRows: next: %w", nextErr)
		}
		if nb == nil {
			break
		}
		out = append(out, nb)
	}
	return out, nil
}

func (p *DistributedPlan) batchResult(ctx context.Context, batches []*vectorized.RecordBatch,
	req *measurev1.QueryRequest, schema *vectorized.BatchSchema,
) (*BatchResult, error) {
	if schema == nil {
		for _, batch := range batches {
			if batch != nil && batch.Schema != nil {
				schema = batch.Schema
				break
			}
		}
	}
	if schema == nil {
		vecPlan, analyzeErr := Analyze(req, p.measureSchemas[0], vmeasure.AggModeAll, nil)
		if analyzeErr != nil {
			releaseBatches(batches)
			return nil, fmt.Errorf("vec distributed plan: analyze empty output: %w", analyzeErr)
		}
		schema = vecPlan.Schema()
	}
	return newBatchResult(ctx, p, batches, req, schema)
}

// calibratedTopWithoutAggLimit computes the per-node row Limit for a
// Top-without-Agg request so the liaison's global BatchTop always sees the
// true global Top-N winner regardless of cardinality skew across nodes.
//
// Formula rationale (worst-case proof):
//
//	Let N = top.Number, G = nGroups (number of request Groups).
//	Each data node may hold at most all N global winners in a single group,
//	while every other group contributes zero rows. To guarantee the liaison
//	sees all N winners the per-node Limit must be ≥ N for every group.
//
//	With G groups the simple safe bound is: perNodeLimit = 2*N + N*(G-1)/G.
//	  - Single-group (G≤1): 2*N — a 2× safety margin above the theoretical
//	    minimum (N) absorbs clock skew and partial shards without MaxUint32.
//	  - Multi-group (G>1): 2*N + N*(G-1)/G. As G→∞ this approaches 3*N,
//	    so the total per-node response across all G groups is bounded by
//	    G * 3*N rows (not G * MaxUint32). The extra N*(G-1)/G term ensures
//	    that even when one group monopolises all N slots the per-node cap is
//	    still large enough: the worst single-group contribution is N rows,
//	    which fits within 2*N. Other groups need at most N rows each, and the
//	    formula's aggregate across all groups exceeds G*N, covering the global
//	    winner under any skew distribution.
//	  - Hard floor: max(2*N, 2) so requests with N=0 or N=1 don't produce
//	    a zero or one-row cap that drops the single winner.
//
// Engineering ceiling: the formula output is capped at perNodeSafetyBound
// (500_000 rows) before the uint32 overflow guard. Rationale: each data node
// in a typical 4 GB deployment can safely materialize ~500K int64 rows
// (~4 MB) per GroupBy+Top response without memory pressure. Any N large
// enough to push 3*N beyond 500_000 (i.e. N > ~166_667) is far outside
// normal operational use; capping at 500_000 prevents a pathological
// Top.Number (e.g. MaxInt32) from turning the per-node limit into a de-facto
// unbounded scan and exhausting data-node heap. The correctness argument
// still holds for realistic queries: real Top.Number values (1–10_000) are
// well below the ceiling and so see the un-capped formula output.
//
// When top is nil or top.Number == 0 the function returns math.MaxUint32
// (the original Phase 4 fallback) because there is no N to calibrate against.
const perNodeSafetyBound uint64 = 500_000

func calibratedTopWithoutAggLimit(top *measurev1.QueryRequest_Top, nGroups int) uint32 {
	if top == nil || top.GetNumber() <= 0 {
		return math.MaxUint32
	}
	n := uint64(top.GetNumber())
	g := max(uint64(nGroups), 1)
	// perNodeLimit = 2*N + N*(G-1)/G  (integer division; always ≥ 2*N)
	perNode := max(2*n+n*(g-1)/g, 2)
	// Engineering ceiling: cap before the MaxUint32 overflow guard so
	// pathological N values never produce a per-node response large enough
	// to OOM a data node. See docstring for the reasoning.
	perNode = min(perNode, perNodeSafetyBound)
	// Belt-and-suspenders: cap at MaxUint32 for the uint32 cast.
	if perNode > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(perNode)
}

// aggOutputName returns the name of the AggModeMap partial's value column
// for agg — the target's name (design §5.2), whichever of field_name /
// tag_name is set. This must agree with BuildOperators' aggTargetName so
// the liaison's AggReduceSpec.OutputName binds to the column the data node
// actually emitted.
func aggOutputName(agg *measurev1.QueryRequest_Aggregation) string {
	if tagName := agg.GetTagName(); tagName != "" {
		return tagName
	}
	return agg.GetFieldName()
}

// distributedGroupByTagKey extracts the GroupBy key tag family and names
// from req.GroupBy.TagProjection (first family, v1 single-family
// limitation — same convention as applyBatchGroupByFirstToRows). The family
// must travel with the names: tag-family validation does not reject the
// same tag name in two families (design doc §5.1), so a bare name is
// ambiguous on a valid schema and the liaison-side reduce must resolve key
// columns the same (family, name) way the node-side GroupBy already does
// (plan.go's lookupGroupByKeyIndices, distributed.go's
// applyBatchGroupByFirstToRows).
func distributedGroupByTagKey(groupBy *measurev1.QueryRequest_GroupBy) (family string, names []string) {
	if groupBy == nil || groupBy.GetTagProjection() == nil {
		return "", nil
	}
	families := groupBy.GetTagProjection().GetTagFamilies()
	if len(families) == 0 {
		return "", nil
	}
	return families[0].GetName(), append([]string(nil), families[0].GetTags()...)
}

func distributedAggFunc(fn modelv1.AggregationFunction) (vmeasure.AggFunc, error) {
	switch fn {
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM:
		return vmeasure.AggSum, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT:
		return vmeasure.AggCount, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MIN:
		return vmeasure.AggMin, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MAX:
		return vmeasure.AggMax, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_MEAN:
		return vmeasure.AggMean, nil
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_UNSPECIFIED:
		return 0, fmt.Errorf("vec distributed plan: aggregation function is unspecified")
	case modelv1.AggregationFunction_AGGREGATION_FUNCTION_COUNT_DISTINCT:
		// The liaison reduces COUNT_DISTINCT partials with ordinary SUM:
		// each node's partial is already a local exact distinct count
		// (design §7.3), and summing disjoint per-shard counts is exactly
		// the decomposability condition's guarantee (§7.4) — no new
		// reduce path, no new wire type.
		return vmeasure.AggSum, nil
	}
	return 0, fmt.Errorf("vec distributed plan: unknown aggregation function %v", fn)
}

// String returns a concise plan rendering for tracing.
func (p *DistributedPlan) String() string {
	parts := []string{"vec-distributed"}
	if p.queryTemplate.GetAgg() != nil {
		parts = append(parts, "agg")
	}
	if p.queryTemplate.GetGroupBy() != nil {
		parts = append(parts, "group-by")
	}
	if p.queryTemplate.GetTop() != nil {
		parts = append(parts, "top")
	}
	return strings.Join(parts, ":")
}

type batchSliceSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	idx     int
}

func (s *batchSliceSource) Init(context.Context) error { return nil }

func (s *batchSliceSource) OutputSchema() *vectorized.BatchSchema { return s.schema }

func (s *batchSliceSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	for s.idx < len(s.batches) {
		batch := s.batches[s.idx]
		s.idx++
		if batch == nil || batch.Len == 0 {
			continue
		}
		return batch, nil
	}
	return nil, nil
}

// Close releases the owners of every batch the pipeline did not consume to the end (a limit
// that closed early leaves some behind). Releasing is idempotent, so batches the downstream
// already recycled are unaffected.
func (s *batchSliceSource) Close() error {
	releaseBatches(s.batches)
	return nil
}

// releaseBatches drops the references each batch holds on borrowed memory.
func releaseBatches(batches []*vectorized.RecordBatch) {
	for _, b := range batches {
		if b != nil {
			b.ReleaseOwners()
		}
	}
}

// releaseReplaced releases old after an operator replaced it with replacement, unless the
// operator passed the batches through unchanged. The operators that replace batches copy
// every cell they keep, so nothing in replacement refers to old's borrowed memory.
func releaseReplaced(old, replacement []*vectorized.RecordBatch) {
	if len(old) > 0 && len(replacement) > 0 && old[0] == replacement[0] {
		return
	}
	releaseBatches(old)
}

// lendingSource lends each batch of its source until the next pull or Close, as BatchResult promises, so a lazy
// source gives back what a batch borrows as soon as the consumer has moved on.
type lendingSource struct {
	inner vectorized.PullOperator
	lent  *vectorized.RecordBatch
}

func (l *lendingSource) Init(ctx context.Context) error { return l.inner.Init(ctx) }

func (l *lendingSource) OutputSchema() *vectorized.BatchSchema { return l.inner.OutputSchema() }

func (l *lendingSource) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if l.lent != nil {
		l.lent.ReleaseOwners()
		l.lent = nil
	}
	batch, err := l.inner.NextBatch(ctx)
	l.lent = batch
	return batch, err
}

func (l *lendingSource) Close() error {
	if l.lent != nil {
		l.lent.ReleaseOwners()
		l.lent = nil
	}
	return l.inner.Close()
}
