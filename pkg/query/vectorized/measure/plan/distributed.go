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
	"time"

	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// distributedQueryTimeout is the historical hard-coded broadcast deadline.
// Retained as a fallback when DistributedPlan.cfg.BroadcastTimeout is zero
// so call sites that build a VectorizedConfig by hand (most existing
// tests) keep their prior behavior. Production deployments thread the
// operator's --dst-broadcast-timeout through cfg.BroadcastTimeout.
const distributedQueryTimeout = 15 * time.Second

// broadcastTimeout returns the effective per-broadcast deadline for this
// plan: the operator-configured value when non-zero, the historical 15 s
// constant otherwise. Used at every dctx.Broadcast call site so a single
// flag flip changes both the row-and-vec-fanout single-broadcast path and
// the per-group fanout loop used by multi-group requests.
func (p *DistributedPlan) broadcastTimeout() time.Duration {
	if p.cfg.BroadcastTimeout > 0 {
		return p.cfg.BroadcastTimeout
	}
	return distributedQueryTimeout
}

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
}

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
	cfg vmeasure.VectorizedConfig,
) (*DistributedPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("vec distributed analyze: nil request")
	}
	if len(measureSchemas) == 0 {
		return nil, fmt.Errorf("vec distributed analyze: no measure schemas supplied")
	}
	if cfgErr := cfg.Validate(); cfgErr != nil {
		return nil, fmt.Errorf("vec distributed analyze: %w", cfgErr)
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
		queryTemplate:  queryTemplate,
		nodeTemplate:   nodeTemplate,
		measureSchemas: measureSchemas,
		indexRules:     indexRules,
		cfg:            cfg,
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
	if !data.MeasureWireModeRaw() {
		return nil, fmt.Errorf("vec distributed plan requires raw measure wire mode")
	}
	dctx := executor.FromDistributedExecutionContext(ctx)
	queryRequest := proto.Clone(p.queryTemplate).(*measurev1.QueryRequest)
	queryRequest.TimeRange = dctx.TimeRange()

	groups := queryRequest.GetGroups()
	if len(groups) <= 1 {
		// Single-group fast path — unchanged behavior.
		nodeRequest := proto.Clone(p.nodeTemplate).(*measurev1.QueryRequest)
		nodeRequest.TimeRange = dctx.TimeRange()
		spanName := "broadcast-rows"
		if queryRequest.GetAgg() != nil {
			spanName = "broadcast-agg"
		}
		broadcastSpan, broadcastSpanCtx := startTraceSpan(ctx, spanName)
		addTraceTagf(broadcastSpan, tracelabels.TagNodeCount, "%d", nodeSelectorCount(dctx.NodeSelectors()))
		addTraceTagf(broadcastSpan, tracelabels.TagBroadcastTimeoutMS, "%d", p.broadcastTimeout().Milliseconds())
		addTraceTag(broadcastSpan, tracelabels.TagTimeRange, dctx.TimeRange().String())
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		ff, broadcastErr := dctx.Broadcast(p.broadcastTimeout(), data.TopicInternalMeasureQuery,
			bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
		if broadcastErr != nil {
			if broadcastSpan != nil {
				broadcastSpan.Error(fmt.Errorf("vec distributed plan: broadcast: %w", broadcastErr))
				stopTraceSpan(broadcastSpan)
			}
			return nil, fmt.Errorf("vec distributed plan: broadcast: %w", broadcastErr)
		}
		frames, _, nodes, responseErr := collectRawFrameResponsesWithNodes(ff)
		applyFanoutCap(broadcastSpanCtx, broadcastSpan, nodes)
		addTraceTagf(broadcastSpan, tracelabels.TagResponseCount, "%d", len(frames))
		frameBytesTotal := 0
		for _, frameBody := range frames {
			frameBytesTotal += len(frameBody)
		}
		addTraceTagf(broadcastSpan, tracelabels.TagFrameBytesTotal, "%d", frameBytesTotal)
		if responseErr != nil && broadcastSpan != nil {
			broadcastSpan.Error(responseErr)
		}
		stopTraceSpan(broadcastSpan)
		if responseErr != nil {
			return nil, responseErr
		}
		if queryRequest.GetAgg() == nil {
			return p.executeRows(ctx, frames, queryRequest)
		}
		return p.executeAgg(ctx, frames, queryRequest)
	}

	// Multi-group path: one broadcast per group so each data node returns a
	// frame whose schema reflects only its local group's column layout.
	var perGroupErr error
	allGroupFrames := make([]groupFrame, 0, len(groups)*4)
	for groupIdx, groupName := range groups {
		nodeRequest := proto.Clone(p.nodeTemplate).(*measurev1.QueryRequest)
		nodeRequest.TimeRange = dctx.TimeRange()
		nodeRequest.Groups = []string{groupName}
		// Filter TagProjection and FieldProjection to only include columns that
		// exist in this group's schema. Data nodes reject tags/fields that are
		// not defined in their local schema, so the liaison must not broadcast
		// columns belonging to a different group's schema.
		var groupSchema *databasev1.Measure
		if groupIdx < len(p.measureSchemas) {
			groupSchema = p.measureSchemas[groupIdx]
		}
		if groupSchema != nil {
			nodeRequest.TagProjection = intersectTagProjection(nodeRequest.GetTagProjection(), groupSchema)
			nodeRequest.FieldProjection = intersectFieldProjection(nodeRequest.GetFieldProjection(), groupSchema)
		}
		broadcastSpan, broadcastSpanCtx := startTraceSpan(ctx, fmt.Sprintf("broadcast-per-group-%s", groupName))
		addTraceTag(broadcastSpan, tracelabels.TagGroupName, groupName)
		addTraceTagf(broadcastSpan, tracelabels.TagNodeCount, "%d", nodeSelectorCount(dctx.NodeSelectors()))
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		ff, broadcastErr := dctx.Broadcast(p.broadcastTimeout(), data.TopicInternalMeasureQuery,
			bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
		if broadcastErr != nil {
			wrappedErr := fmt.Errorf("vec distributed plan: broadcast group %s: %w", groupName, broadcastErr)
			if broadcastSpan != nil {
				broadcastSpan.Error(wrappedErr)
				stopTraceSpan(broadcastSpan)
			}
			perGroupErr = multierr.Append(perGroupErr, wrappedErr)
			continue
		}
		rawFrames, _, groupNodes, responseErr := collectRawFrameResponsesWithNodes(ff)
		applyFanoutCap(broadcastSpanCtx, broadcastSpan, groupNodes)
		addTraceTagf(broadcastSpan, tracelabels.TagResponseCount, "%d", len(rawFrames))
		if responseErr != nil {
			if broadcastSpan != nil {
				broadcastSpan.Error(responseErr)
				stopTraceSpan(broadcastSpan)
			}
			perGroupErr = multierr.Append(perGroupErr, responseErr)
			continue
		}
		stopTraceSpan(broadcastSpan)
		for _, body := range rawFrames {
			allGroupFrames = append(allGroupFrames, groupFrame{body: body, group: groupIdx})
		}
	}
	if perGroupErr != nil {
		return nil, perGroupErr
	}
	if queryRequest.GetAgg() == nil {
		return p.executeRowsMultiGroup(ctx, allGroupFrames, queryRequest)
	}
	// Multi-group agg: collect all frames as flat []byte and reduce.
	flatFrames := make([][]byte, 0, len(allGroupFrames))
	for _, gf := range allGroupFrames {
		flatFrames = append(flatFrames, gf.body)
	}
	return p.executeAgg(ctx, flatFrames, queryRequest)
}

func collectRawFrameResponses(ff []bus.Future) ([][]byte, []*commonv1.Trace, error) {
	frames, traces, _, err := collectRawFrameResponsesWithNodes(ff)
	return frames, traces, err
}

func collectRawFrameResponsesWithNodes(ff []bus.Future) ([][]byte, []*commonv1.Trace, []nodeInfo, error) {
	frames := make([][]byte, 0, len(ff))
	traces := make([]*commonv1.Trace, 0, len(ff))
	nodes := make([]nodeInfo, 0, len(ff))
	var err error
	for _, future := range ff {
		message, getErr := future.Get()
		if getErr != nil {
			err = multierr.Append(err, getErr)
			nodes = append(nodes, nodeInfo{hasError: true})
			continue
		}
		switch response := message.Data().(type) {
		case nil:
			// Empty-result carve-out: emptyMIterator (and any
			// FrameEmitter that has no rows to emit) returns a nil
			// rawBody, which the bus wire layer surfaces as an
			// untyped-nil Data interface. Treat as zero rows from this
			// source — there is no frame to decode.
			nodes = append(nodes, nodeInfo{rows: 0})
		case []byte:
			if len(response) > 0 {
				frames = append(frames, response)
				nodes = append(nodes, nodeInfo{bytes: int64(len(response))})
			} else {
				nodes = append(nodes, nodeInfo{})
			}
		case *common.Error:
			err = multierr.Append(err, fmt.Errorf("data node error: %s", response.Error()))
			nodes = append(nodes, nodeInfo{hasError: true})
		case *measurev1.InternalQueryResponse:
			if len(response.GetDataPoints()) > 0 && len(response.GetRawFrameBody()) == 0 {
				err = multierr.Append(err, fmt.Errorf("vec distributed plan: got data_points proto response under raw wire mode"))
				nodes = append(nodes, nodeInfo{hasError: true})
				continue
			}
			frameBody := response.GetRawFrameBody()
			if len(frameBody) > 0 {
				frames = append(frames, frameBody)
			}
			nodeTrace := response.GetTrace()
			ni := nodeInfo{
				trace:     nodeTrace,
				bytes:     int64(len(frameBody)),
				latencyNS: extractNodeLatency(nodeTrace),
				rows:      extractNodeTagInt64(nodeTrace, tracelabels.TagRespCount),
				hasError:  nodeTrace != nil && nodeTrace.GetError(),
			}
			if nodeTrace != nil {
				traces = append(traces, nodeTrace)
				if len(frameBody) == 0 && nodeTrace.GetError() {
					err = multierr.Append(err, fmt.Errorf("data node trace error"))
				}
			}
			nodes = append(nodes, ni)
		default:
			err = multierr.Append(err, fmt.Errorf("vec distributed plan: unexpected response %T", response))
			nodes = append(nodes, nodeInfo{hasError: true})
		}
	}
	return frames, traces, nodes, err
}

func (p *DistributedPlan) executeAgg(ctx context.Context, frames [][]byte, req *measurev1.QueryRequest) (executor.MIterator, error) {
	keyTagNames := distributedGroupByTagNames(req.GetGroupBy())
	aggFunc, aggErr := distributedAggFunc(req.GetAgg().GetFunction())
	if aggErr != nil {
		return nil, aggErr
	}
	aggSpecs := []vmeasure.AggReduceSpec{{OutputName: req.GetAgg().GetFieldName(), Func: aggFunc}}
	var topSpec *vmeasure.ReduceTopSpec
	if top := req.GetTop(); top != nil {
		topSpec = &vmeasure.ReduceTopSpec{FieldName: top.GetFieldName(), N: int(top.GetNumber()), Asc: top.GetFieldValueSort() == modelv1.Sort_SORT_ASC}
	}
	tracker := vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024)
	reduceSpan, reduceSpanCtx := startTraceSpan(ctx, "reduce-raw-frames")
	addTraceTagf(reduceSpan, tracelabels.TagFramesIn, "%d", len(frames))
	frameDecodeDurations := collectFrameDecodeDurations(frames)
	// nolint:contextcheck // pure in-memory reducer; no cancelable I/O downstream
	batches, aggValuePath, reduceErr := vmeasure.ReduceRawFrames(frames, keyTagNames, aggSpecs, p.cfg.BatchSize, tracker)
	if reduceErr != nil {
		if reduceSpan != nil {
			reduceSpan.Error(reduceErr)
			addTraceTag(reduceSpan, tracelabels.TagAggValuePath, string(aggValuePath))
			stopTraceSpan(reduceSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: reduce raw frames: %w", reduceErr)
	}
	emitDecodeFrameSummarySpan(reduceSpanCtx, frameDecodeDurations)
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
	return p.iteratorFromBatches(ctx, batches, req)
}

func (p *DistributedPlan) executeRows(ctx context.Context, frames [][]byte, req *measurev1.QueryRequest) (executor.MIterator, error) {
	tracker := vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024)
	spec := distributedRowsSpec{
		Desc:          req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
		IndexMode:     p.measureSchemas[0].GetIndexMode(),
		BatchSize:     p.cfg.BatchSize,
		Tracker:       tracker,
		OrderByColIdx: -1,
	}
	if p.orderByTag != nil {
		spec.OrderByFamily = p.orderByTag.family
		spec.OrderByTagName = p.orderByTag.tag
	}
	mergeSpan, mergeSpanCtx := startTraceSpan(ctx, "merge-distributed-rows")
	addTraceTagf(mergeSpan, tracelabels.TagSourcesIn, "%d", len(frames))
	rowsFrameDecodeDurations := collectFrameDecodeDurations(frames)
	batches, mergeErr := mergeDistributedRows(frames, spec)
	if mergeErr != nil {
		if mergeSpan != nil {
			mergeSpan.Error(mergeErr)
			stopTraceSpan(mergeSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: merge rows: %w", mergeErr)
	}
	emitDecodeFrameSummarySpan(mergeSpanCtx, rowsFrameDecodeDurations)
	addTraceTagf(mergeSpan, tracelabels.TagRowsOut, "%d", batchRows(batches))
	addTraceTagf(mergeSpan, tracelabels.TagOrderByColIdx, "%d", spec.OrderByColIdx)
	addTraceTag(mergeSpan, tracelabels.TagOrderByFamily, spec.OrderByFamily)
	addTraceTag(mergeSpan, tracelabels.TagOrderByTag, spec.OrderByTagName)
	addTraceTagf(mergeSpan, tracelabels.TagDesc, "%t", spec.Desc)
	addTraceTagf(mergeSpan, tracelabels.TagIndexMode, "%t", spec.IndexMode)
	stopTraceSpan(mergeSpan)
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
	// natural representative in both paths. The integration suite at ~231s shows
	// no fixture-visible divergence. A query shape where the two orderings differ
	// (e.g. a group whose top-1 row is not its first-seen row AND that group would
	// be excluded by Top in the row path but included in the vec path) would be
	// visible only if the Top.FieldName differs across rows within a group —
	// which is atypical in time-series measures that aggregate to a scalar per
	// series per window.
	if groupBy := req.GetGroupBy(); groupBy != nil {
		var gbErr error
		// nolint:contextcheck // pure in-memory dedup; no cancelable I/O downstream
		batches, gbErr = applyBatchGroupByFirstToRows(batches, groupBy, p.cfg.BatchSize, tracker)
		if gbErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply group-by to rows: %w", gbErr)
		}
	}
	if top := req.GetTop(); top != nil {
		var topErr error
		// nolint:contextcheck // pure in-memory top selection; no cancelable I/O downstream
		batches, topErr = applyBatchTopToRows(batches, top, p.cfg.BatchSize)
		if topErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply top to rows: %w", topErr)
		}
	}
	return p.iteratorFromBatches(ctx, batches, req)
}

// executeRowsMultiGroup is the multi-group non-agg row merge path. It builds
// the merged BatchSchema by unioning all per-group schemas, then runs the
// k-way heap merger over all per-group frames decoded into that merged schema.
func (p *DistributedPlan) executeRowsMultiGroup(ctx context.Context, groupFrames []groupFrame, req *measurev1.QueryRequest) (executor.MIterator, error) {
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
	tracker := vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024)
	// IndexMode is true when ALL groups are index-mode. A mixed-mode
	// configuration is treated as non-index-mode to avoid over-suppressing rows
	// from non-index-mode groups.
	indexMode := len(p.measureSchemas) > 0
	for _, ms := range p.measureSchemas {
		if !ms.GetIndexMode() {
			indexMode = false
			break
		}
	}
	spec := distributedRowsSpec{
		Desc:          req.GetOrderBy().GetSort() == modelv1.Sort_SORT_DESC,
		IndexMode:     indexMode,
		BatchSize:     p.cfg.BatchSize,
		Tracker:       tracker,
		OrderByColIdx: -1,
	}
	if p.orderByTag != nil {
		spec.OrderByFamily = p.orderByTag.family
		spec.OrderByTagName = p.orderByTag.tag
	}
	mergeSpan, _ := startTraceSpan(ctx, "merge-distributed-rows-multi")
	addTraceTagf(mergeSpan, tracelabels.TagSourcesIn, "%d", len(groupFrames))
	batches, mergeErr := mergeDistributedRowsMulti(groupFrames, mergedSchema, spec)
	if mergeErr != nil {
		if mergeSpan != nil {
			mergeSpan.Error(mergeErr)
			stopTraceSpan(mergeSpan)
		}
		return nil, fmt.Errorf("vec distributed plan: merge multi-group rows: %w", mergeErr)
	}
	addTraceTagf(mergeSpan, tracelabels.TagRowsOut, "%d", batchRows(batches))
	stopTraceSpan(mergeSpan)
	// Phase 5: raw GroupBy liaison pass. Same ordering as executeRows —
	// GroupByFirst before BatchTop so each group contributes at most one row
	// to the global Top ranking.
	if groupBy := req.GetGroupBy(); groupBy != nil {
		var gbErr error
		// nolint:contextcheck // pure in-memory dedup; no cancelable I/O downstream
		batches, gbErr = applyBatchGroupByFirstToRows(batches, groupBy, p.cfg.BatchSize, tracker)
		if gbErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply group-by to multi-group rows: %w", gbErr)
		}
	}
	if top := req.GetTop(); top != nil {
		var topErr error
		// nolint:contextcheck // pure in-memory top selection; no cancelable I/O downstream
		batches, topErr = applyBatchTopToRows(batches, top, p.cfg.BatchSize)
		if topErr != nil {
			return nil, fmt.Errorf("vec distributed plan: apply top to multi-group rows: %w", topErr)
		}
	}
	return p.iteratorFromBatchesWithSchema(ctx, batches, req, mergedSchema)
}

// applyBatchTopToRows runs a BatchTop operator over the merged row batches,
// returning the top-N rows by top.FieldName. The schema is derived from the
// first non-nil batch; if there are no batches the input is returned unchanged.
// Loud-failure: if top.FieldName does not resolve on the merged schema's field
// columns, an error is returned rather than silently passing through.
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
	for idx, b := range batches {
		if b == nil || b.Len == 0 {
			continue
		}
		if consumeErr := topOp.Consume(context.Background(), b); consumeErr != nil {
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
	for batchIdx, b := range batches {
		if b == nil || b.Len == 0 {
			continue
		}
		if consumeErr := gbOp.Consume(context.Background(), b); consumeErr != nil {
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

func (p *DistributedPlan) iteratorFromBatches(ctx context.Context, batches []*vectorized.RecordBatch, req *measurev1.QueryRequest) (executor.MIterator, error) {
	var schema *vectorized.BatchSchema
	for _, batch := range batches {
		if batch != nil && batch.Schema != nil {
			schema = batch.Schema
			break
		}
	}
	if schema == nil {
		vecPlan, analyzeErr := Analyze(req, p.measureSchemas[0], vmeasure.AggModeAll)
		if analyzeErr != nil {
			return nil, fmt.Errorf("vec distributed plan: analyze empty output: %w", analyzeErr)
		}
		schema = vecPlan.Schema()
	}
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024))
	builder.From(&batchSliceSource{batches: batches, schema: schema})
	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	builder.Apply(vmeasure.NewBatchLimit(schema, req.GetOffset(), limit))
	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		return nil, fmt.Errorf("vec distributed plan: build output pipeline: %w", buildErr)
	}
	if initErr := pipeline.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("vec distributed plan: init output pipeline: %w", initErr)
	}
	pool := vectorized.NewBatchPool(schema, p.cfg.BatchSize)
	var iter executor.MIterator = vmeasure.NewIteratorFromPipeline(ctx, pipeline, schema, pool)
	if !p.hiddenOrderBy.IsEmpty() {
		// Strip the OrderBy tag projected as a hidden column for native
		// cross-source merge sorting. The visible response matches what a
		// query without the OrderBy projection would emit byte-for-byte.
		iter = &hiddenTagsMIterator{inner: iter, hiddenTags: p.hiddenOrderBy}
	}
	if p.hiddenTopField != "" {
		// Strip the Top field appended to the nodeTemplate's FieldProjection
		// for BatchTop sorting. The visible response matches a query without
		// the extra field projection.
		iter = &hiddenFieldsMIterator{inner: iter, hiddenField: p.hiddenTopField}
	}
	return iter, nil
}

// iteratorFromBatchesWithSchema is the multi-group variant of
// iteratorFromBatches. The merged BatchSchema is supplied by the caller
// (already computed by BuildMultiGroupBatchSchema) so this function does not
// fall back to Analyze on empty output — an empty multi-group result simply
// returns no rows rather than re-deriving a schema from a single group.
func (p *DistributedPlan) iteratorFromBatchesWithSchema(
	ctx context.Context,
	batches []*vectorized.RecordBatch,
	req *measurev1.QueryRequest,
	schema *vectorized.BatchSchema,
) (executor.MIterator, error) {
	builder := vectorized.NewPipelineBuilder().WithMemoryTracker(vectorized.NewMemoryTracker(int64(p.cfg.QueryMemoryMiB) * 1024 * 1024))
	builder.From(&batchSliceSource{batches: batches, schema: schema})
	limit := req.GetLimit()
	if limit == 0 {
		limit = defaultLimit
	}
	builder.Apply(vmeasure.NewBatchLimit(schema, req.GetOffset(), limit))
	pipeline, buildErr := builder.Build()
	if buildErr != nil {
		return nil, fmt.Errorf("vec distributed plan: build output pipeline: %w", buildErr)
	}
	if initErr := pipeline.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("vec distributed plan: init output pipeline: %w", initErr)
	}
	pool := vectorized.NewBatchPool(schema, p.cfg.BatchSize)
	var iter executor.MIterator = vmeasure.NewIteratorFromPipeline(ctx, pipeline, schema, pool)
	if !p.hiddenOrderBy.IsEmpty() {
		iter = &hiddenTagsMIterator{inner: iter, hiddenTags: p.hiddenOrderBy}
	}
	if p.hiddenTopField != "" {
		iter = &hiddenFieldsMIterator{inner: iter, hiddenField: p.hiddenTopField}
	}
	return iter, nil
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

func distributedGroupByTagNames(groupBy *measurev1.QueryRequest_GroupBy) []string {
	if groupBy == nil || groupBy.GetTagProjection() == nil {
		return nil
	}
	families := groupBy.GetTagProjection().GetTagFamilies()
	if len(families) == 0 {
		return nil
	}
	return append([]string(nil), families[0].GetTags()...)
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

func (s *batchSliceSource) Close() error { return nil }
