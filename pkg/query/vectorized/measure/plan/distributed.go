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
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

const distributedQueryTimeout = 15 * time.Second

// SupportsDistributedRows reports whether a non-aggregation request can use
// the native vectorized distributed row merge. Phase 2 of vec-distributed-
// full-native lifts the OrderBy.IndexRuleName != "" gate: data nodes emit
// per-shard pre-sorted by the OrderBy field and the liaison's k-way heap
// merger now compares on the OrderBy column instead of timestamp when an
// index rule is present.
func SupportsDistributedRows(req *measurev1.QueryRequest) bool {
	if req == nil || req.GetAgg() != nil || req.GetGroupBy() != nil || req.GetTop() != nil {
		return false
	}
	return true
}

// DistributedPlan is the vectorized liaison-side distributed measure plan.
// It consumes data-node raw frame bodies as []byte values from RawFrameCodec,
// decodes them into vectorized batches, and runs the liaison operators without
// routing through the row-compatible logical distributedPlan.
type DistributedPlan struct {
	queryTemplate   *measurev1.QueryRequest
	nodeTemplate    *measurev1.QueryRequest
	measureSchemas  []*databasev1.Measure
	indexRules      [][]*databasev1.IndexRule
	orderByTag      *resolvedOrderByTag
	hiddenOrderBy   logical.HiddenTagSet
	cfg             vmeasure.VectorizedConfig
}

// AnalyzeDistributed builds the vectorized distributed liaison plan.
// measureSchemas is the per-group slice of Measure schemas (one entry per
// req.Groups element). indexRules is the corresponding per-group slice of
// index rule sets (ec.GetIndexRules() for each group). Both slices must be
// in request-group order. Single-group callers may pass a length-1 slice for
// each (the existing behaviour is preserved byte-for-byte).
//
// When indexRules is nil or empty and req.OrderBy.IndexRuleName is non-empty,
// the resolver surfaces an "index rule X not found" error byte-equivalent to
// the row path.
func AnalyzeDistributed(req *measurev1.QueryRequest, measureSchemas []*databasev1.Measure, indexRules [][]*databasev1.IndexRule, cfg vmeasure.VectorizedConfig) (*DistributedPlan, error) {
	if req == nil {
		return nil, fmt.Errorf("vec distributed analyze: nil request")
	}
	if len(measureSchemas) == 0 {
		return nil, fmt.Errorf("vec distributed analyze: no measure schemas supplied")
	}
	if cfgErr := cfg.Validate(); cfgErr != nil {
		return nil, fmt.Errorf("vec distributed analyze: %w", cfgErr)
	}
	if req.GetAgg() == nil && !SupportsDistributedRows(req) {
		return nil, fmt.Errorf("vec distributed analyze: unsupported non-aggregation scan requires row distributed merge")
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
	nodeTemplate.Top = nil
	if nodeTemplate.GetAgg() == nil {
		nodeTemplate.GroupBy = nil
	}
	if hadTop && nodeTemplate.GetAgg() != nil {
		nodeTemplate.Limit = math.MaxUint32
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
// behaviour). For multi-group requests, one broadcast is issued per group,
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
		// Single-group fast path — unchanged behaviour.
		nodeRequest := proto.Clone(p.nodeTemplate).(*measurev1.QueryRequest)
		nodeRequest.TimeRange = dctx.TimeRange()
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		ff, broadcastErr := dctx.Broadcast(distributedQueryTimeout, data.TopicInternalMeasureQuery,
			bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
		if broadcastErr != nil {
			return nil, fmt.Errorf("vec distributed plan: broadcast: %w", broadcastErr)
		}
		frames, responseErr := collectRawFrameResponses(ff)
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
		internalRequest := &measurev1.InternalQueryRequest{Request: nodeRequest, AggReturnPartial: queryRequest.GetAgg() != nil}
		ff, broadcastErr := dctx.Broadcast(distributedQueryTimeout, data.TopicInternalMeasureQuery,
			bus.NewMessageWithNodeSelectors(bus.MessageID(dctx.TimeRange().Begin.Nanos), dctx.NodeSelectors(), dctx.TimeRange(), internalRequest))
		if broadcastErr != nil {
			perGroupErr = multierr.Append(perGroupErr, fmt.Errorf("vec distributed plan: broadcast group %s: %w", groupName, broadcastErr))
			continue
		}
		rawFrames, responseErr := collectRawFrameResponses(ff)
		if responseErr != nil {
			perGroupErr = multierr.Append(perGroupErr, responseErr)
			continue
		}
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

func collectRawFrameResponses(ff []bus.Future) ([][]byte, error) {
	frames := make([][]byte, 0, len(ff))
	var err error
	for _, future := range ff {
		message, getErr := future.Get()
		if getErr != nil {
			err = multierr.Append(err, getErr)
			continue
		}
		switch response := message.Data().(type) {
		case nil:
			// Empty-result carve-out: emptyMIterator (and any
			// FrameEmitter that has no rows to emit) returns a nil
			// rawBody, which the bus wire layer surfaces as an
			// untyped-nil Data interface. Treat as zero rows from this
			// source — there is no frame to decode.
		case []byte:
			if len(response) > 0 {
				frames = append(frames, response)
			}
		case *common.Error:
			err = multierr.Append(err, fmt.Errorf("data node error: %s", response.Error()))
		case *measurev1.InternalQueryResponse:
			err = multierr.Append(err, fmt.Errorf("vec distributed plan: got proto response under raw wire mode"))
		default:
			err = multierr.Append(err, fmt.Errorf("vec distributed plan: unexpected response %T", response))
		}
	}
	return frames, err
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
	batches, reduceErr := vmeasure.ReduceRawFrames(frames, keyTagNames, aggSpecs, p.cfg.BatchSize, tracker)
	if reduceErr != nil {
		return nil, fmt.Errorf("vec distributed plan: reduce raw frames: %w", reduceErr)
	}
	if topSpec != nil && topSpec.N > 0 && len(batches) > 0 {
		topped, topErr := vmeasure.ApplyTopToReduce(batches, *topSpec, p.cfg.BatchSize)
		if topErr != nil {
			return nil, fmt.Errorf("vec distributed plan: top reduced frames: %w", topErr)
		}
		batches = topped
	}
	return p.iteratorFromBatches(ctx, batches, req)
}

func (p *DistributedPlan) executeRows(ctx context.Context, frames [][]byte, req *measurev1.QueryRequest) (executor.MIterator, error) {
	if !SupportsDistributedRows(req) {
		return nil, fmt.Errorf("vec distributed plan: unsupported non-aggregation scan requires row distributed merge")
	}
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
	batches, mergeErr := mergeDistributedRows(frames, spec)
	if mergeErr != nil {
		return nil, fmt.Errorf("vec distributed plan: merge rows: %w", mergeErr)
	}
	return p.iteratorFromBatches(ctx, batches, req)
}

// executeRowsMultiGroup is the multi-group non-agg row merge path. It builds
// the merged BatchSchema by unioning all per-group schemas, then runs the
// k-way heap merger over all per-group frames decoded into that merged schema.
func (p *DistributedPlan) executeRowsMultiGroup(ctx context.Context, groupFrames []groupFrame, req *measurev1.QueryRequest) (executor.MIterator, error) {
	mergedSchema, schemaErr := BuildMultiGroupBatchSchema(p.measureSchemas, req)
	if schemaErr != nil {
		return nil, fmt.Errorf("vec distributed plan: build multi-group schema: %w", schemaErr)
	}
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
	batches, mergeErr := mergeDistributedRowsMulti(groupFrames, mergedSchema, spec)
	if mergeErr != nil {
		return nil, fmt.Errorf("vec distributed plan: merge multi-group rows: %w", mergeErr)
	}
	return p.iteratorFromBatchesWithSchema(ctx, batches, req, mergedSchema)
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
	return iter, nil
}

// iteratorFromBatchesWithSchema is the multi-group variant of
// iteratorFromBatches. The merged BatchSchema is supplied by the caller
// (already computed by BuildMultiGroupBatchSchema) so this function does not
// fall back to Analyze on empty output — an empty multi-group result simply
// returns no rows rather than re-deriving a schema from a single group.
func (p *DistributedPlan) iteratorFromBatchesWithSchema(ctx context.Context, batches []*vectorized.RecordBatch, req *measurev1.QueryRequest, schema *vectorized.BatchSchema) (executor.MIterator, error) {
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
	return iter, nil
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
	batches []*vectorized.RecordBatch
	schema  *vectorized.BatchSchema
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
