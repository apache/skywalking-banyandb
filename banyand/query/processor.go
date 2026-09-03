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

package query

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"golang.org/x/exp/slices"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_measure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	logical_trace "github.com/apache/skywalking-banyandb/pkg/query/logical/trace"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	vecplan "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/plan"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

const (
	moduleName = "query"
)

var (
	_ bus.MessageListener            = (*streamQueryProcessor)(nil)
	_ bus.MessageListener            = (*measureQueryProcessor)(nil)
	_ bus.MessageListener            = (*measureInternalQueryProcessor)(nil)
	_ bus.MessageListener            = (*traceQueryProcessor)(nil)
	_ executor.TraceExecutionContext = trace.Trace(nil)
)

type streamQueryProcessor struct {
	streamService stream.Service
	*queryService
	*bus.UnImplementedHealthyListener
	// distributed is true on a distributed data node, whose query response is
	// sent over the wire to a remote liaison that decodes the native columnar
	// frame. It is false in standalone, where the local grpc service consumes the
	// response directly and expects the proto body — so the frame must not be
	// emitted there.
	distributed bool
}

func (p *streamQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	queryCriteria, ok := message.Data().(*streamv1.QueryRequest)
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a query request")
	}
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()
	var metadata []*commonv1.Metadata
	var schemas []logical.Schema
	var ecc []executor.StreamExecutionContext
	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, err := p.streamService.Stream(meta)
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to get execution context for stream %s: %v", meta.GetName(), err))
			return
		}
		ecc = append(ecc, ec)
		s, err := logical_stream.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if err != nil {
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to build schema for stream %s: %v", meta.GetName(), err))
			return
		}
		schemas = append(schemas, s)
		metadata = append(metadata, meta)
	}

	plan, err := logical_stream.Analyze(queryCriteria, metadata, schemas, ecc)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for stream %s: %v", queryCriteria.GetName(), err))
		return
	}

	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}
	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
		span.Tag(tracelabels.TagPlan, plan.String())
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *streamv1.QueryResponse:
				span.Tag(tracelabels.TagRespCount, fmt.Sprintf("%d", len(d.Elements)))
				span.Stop()
				d.Trace = tracer.ToProto()
			case *common.Error:
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
		}()
	}

	// Vec dispatch is the only stream execution path; the row path was removed in
	// 0.12.0. Every analyzed plan shape must therefore be vec-eligible, so a decline
	// is a hard error rather than a silent proto fallback (same no-silent-fallback
	// discipline as measure and trace). When tracing is on we MUST return a proto
	// QueryResponse (it carries common.v1.Trace); the frame emit is gated on tracer == nil.
	handled, vecResp := p.tryStreamVecDispatch(ctx, plan, queryCriteria, tracer != nil)
	plan.(executor.StreamCloser).Close()
	if !handled {
		p.log.Error().Str("plan", plan.String()).RawJSON("req", logger.Proto(queryCriteria)).Msg("no vectorized execution path for the query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("no vectorized execution path for stream %s", queryCriteria.GetName()))
		return
	}
	resp = vecResp
	return
}

// tryStreamVecDispatch runs the native columnar (vec) path for a stream query.
// It returns (false, nil) when the plan is not vec-eligible, which the caller
// treats as a hard error. On success it returns (true, resp) where resp is:
//   - a []byte columnar frame body, when the process is a distributed data node
//     with the raw wire mode on and tracing off (traced == false);
//   - a *streamv1.QueryResponse with materialized Elements, for standalone, for a
//     traced query (the frame has no trace channel), or when the wire mode is off.
//
// Any error is surfaced as a *common.Error response (handled=true), matching the
// measure/trace no-silent-fallback discipline.
func (p *streamQueryProcessor) tryStreamVecDispatch(ctx context.Context, plan logical.Plan,
	queryCriteria *streamv1.QueryRequest, traced bool,
) (bool, bus.Message) {
	now := time.Now().UnixNano()

	// Multi-group query (*limit → *mergePlan): run the vec scan+pipeline per group,
	// then cross-group merge via the SAME primitives the row mergePlan.Execute uses
	// (MergeGroupElements) and apply the outer offset:offset+limit slice. The
	// cross-group merge is element-level (proto Elements), so this always takes the
	// proto egress — the liaison merges proto/frame bodies interchangeably.
	if merge, ok := logical_stream.VecMergeExecutable(plan); ok {
		return p.tryVecMergeDispatch(ctx, merge, queryCriteria, now)
	}

	vecExec := logical_stream.VecExecutable(plan)
	if vecExec == nil {
		return false, bus.Message{}
	}
	batches, schema, execErr := vecExec.ExecuteVectorized(ctx)
	if execErr != nil {
		p.log.Error().Err(execErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the vectorized query plan")
		return true, bus.NewMessage(bus.MessageID(now), common.NewError("execute the vectorized query plan for stream %s: %v", queryCriteria.GetName(), execErr))
	}

	// A criteria query carries a per-element tag filter (VecTagFilter) that must be
	// applied to materialized Elements (byte-identical to the row tagFilterPlan). The
	// frame path operates on columnar batches and has no element-level filter stage,
	// so a filter query forces the proto egress; the liaison merges proto Elements
	// and frame bodies interchangeably (mixed-mode), so this stays correct.
	tagFilter, hiddenTags, filterSchema, hasFilter := logical_stream.VecTagFilter(plan)

	// Data-node native wire mode (flag-on, no tracing): emit a columnar frame body
	// so the send path passes it through and the liaison decodes it. The liaison
	// applies the global offset/limit slice, so the data node emits the whole
	// (already per-node-capped) batch set — no slice here.
	if !hasFilter && !vecExec.HidesOrderTag() && streamVecEmitAsFrame(p.distributed, data.StreamWireModeRaw(), traced) {
		merged, mergeErr := mergeStreamBatches(schema, batches)
		if mergeErr != nil {
			p.log.Error().Err(mergeErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to merge the vectorized stream batches")
			return true, bus.NewMessage(bus.MessageID(now), common.NewError("merge the vectorized stream batches for stream %s: %v", queryCriteria.GetName(), mergeErr))
		}
		frameBytes, encErr := streamframe.Encode(merged)
		if encErr != nil {
			p.log.Error().Err(encErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to encode the vectorized stream frame")
			return true, bus.NewMessage(bus.MessageID(now), common.NewError("encode the vectorized stream frame for stream %s: %v", queryCriteria.GetName(), encErr))
		}
		return true, bus.NewMessage(bus.MessageID(now), frameBytes)
	}

	// Standalone (or traced, or wire mode off): materialize Elements in process and
	// apply the client offset:offset+limit slice the row *limit node would apply.
	elements, buildErr := stream.BuildElementsFromBatches(batches, vecExec.ProjectionTags())
	if buildErr != nil {
		p.log.Error().Err(buildErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to materialize vectorized stream elements")
		return true, bus.NewMessage(bus.MessageID(now), common.NewError("materialize vectorized stream elements for stream %s: %v", queryCriteria.GetName(), buildErr))
	}
	// Criteria query: apply the SAME per-element tagFilter.Match + hidden-tag strip
	// as the row tagFilterPlan.Execute, BEFORE the outer offset:offset+limit slice
	// (row order: scan → merge → distinct → filter → limit). The element set handed
	// to the filter already matches row's, because the merge capped (or deliberately
	// did not cap) according to the scan's order type — see scanResumesAcrossPulls.
	if hasFilter {
		filtered, filterErr := applyStreamTagFilter(elements, tagFilter, hiddenTags, filterSchema)
		if filterErr != nil {
			p.log.Error().Err(filterErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to apply the vectorized stream tag filter")
			return true, bus.NewMessage(bus.MessageID(now), common.NewError("apply the vectorized stream tag filter for stream %s: %v", queryCriteria.GetName(), filterErr))
		}
		elements = filtered
	}
	if offset, limit, ok := logical_stream.VecOffsetLimit(plan); ok {
		elements = sliceStreamElements(elements, offset, limit)
	}
	return true, bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Elements: elements})
}

// tryVecMergeDispatch runs the vec path for a multi-group query (*limit →
// *mergePlan). For EACH group it runs the vec scan → BuildElementsFromBatches →
// (if the group carries criteria) the SAME per-element tagFilter.Match + hidden-tag
// strip the row tagFilterPlan.Execute applies. It then cross-group merges via the
// shared MergeGroupElements (the exact comparableElement/sortableElements +
// sort.NewItemIter path the row mergePlan.Execute uses) and applies the outer
// offset:offset+limit slice. Emits proto Elements (element-level merge/filter); the
// liaison merges proto/frame bodies interchangeably.
func (p *streamQueryProcessor) tryVecMergeDispatch(ctx context.Context, merge *logical_stream.VecMerge,
	queryCriteria *streamv1.QueryRequest, now int64,
) (bool, bus.Message) {
	perGroup := make([][]*streamv1.Element, 0, len(merge.Groups))
	for _, group := range merge.Groups {
		batches, _, execErr := group.Scan.ExecuteVectorized(ctx)
		if execErr != nil {
			p.log.Error().Err(execErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the vectorized stream group")
			return true, bus.NewMessage(bus.MessageID(now), common.NewError("execute the vectorized query plan for stream %s: %v", queryCriteria.GetName(), execErr))
		}
		elements, buildErr := stream.BuildElementsFromBatches(batches, group.Scan.ProjectionTags())
		if buildErr != nil {
			p.log.Error().Err(buildErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to materialize vectorized stream group elements")
			return true, bus.NewMessage(bus.MessageID(now), common.NewError("materialize vectorized stream elements for stream %s: %v", queryCriteria.GetName(), buildErr))
		}
		if group.HasFilter {
			filtered, filterErr := applyStreamTagFilter(elements, group.TagFilter, group.HiddenTags, group.FilterSchema)
			if filterErr != nil {
				p.log.Error().Err(filterErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to apply the vectorized stream group tag filter")
				return true, bus.NewMessage(bus.MessageID(now), common.NewError("apply the vectorized stream tag filter for stream %s: %v", queryCriteria.GetName(), filterErr))
			}
			elements = filtered
		}
		perGroup = append(perGroup, elements)
	}
	merged := logical_stream.MergeGroupElements(perGroup, merge.SortByTime, merge.SortTagSpec, merge.Desc)
	merged = sliceStreamElements(merged, merge.Offset, merge.Limit)
	return true, bus.NewMessage(bus.MessageID(now), &streamv1.QueryResponse{Elements: merged})
}

// streamVecEmitAsFrame is the data-node emit gate: emit a native columnar frame
// ([]byte) only on a distributed data node with the raw wire mode on and tracing
// OFF. A traced query MUST return a *streamv1.QueryResponse proto (it carries
// common.v1.Trace, which the frame has no channel for) — so tracing forces the
// proto egress. Standalone (!distributed) and wire-mode-off also take proto.
func streamVecEmitAsFrame(distributed, wireModeRaw, traced bool) bool {
	return distributed && wireModeRaw && !traced
}

// mergeStreamBatches concatenates the active rows of several columnar batches into
// a single batch so the frame carries one contiguous columnar block. Selection is
// respected via the batch schema's active-row semantics at frame encode; here we
// simply flatten Selection into a fresh dense batch.
func mergeStreamBatches(schema *vectorized.BatchSchema, batches []*vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
	total := 0
	for _, b := range batches {
		if b != nil {
			total += b.ActiveLen()
		}
	}
	out := vectorized.NewRecordBatch(schema, total)
	for _, b := range batches {
		if b == nil || b.ActiveLen() == 0 {
			continue
		}
		if appendErr := vectorized.AppendActiveRows(out, b); appendErr != nil {
			return nil, appendErr
		}
	}
	return out, nil
}

// applyStreamTagFilter keeps only the elements whose tag families satisfy the
// criteria tag filter, then strips the filter-only (hidden) tags from the kept
// elements — mirroring the row tagFilterPlan.Execute per-element loop exactly so
// the vec result is byte-identical.
func applyStreamTagFilter(elements []*streamv1.Element, tagFilter logical.TagFilter,
	hiddenTags logical.HiddenTagSet, s logical.Schema,
) ([]*streamv1.Element, error) {
	filtered := make([]*streamv1.Element, 0, len(elements))
	for _, e := range elements {
		ok, matchErr := tagFilter.Match(logical.TagFamilies(e.TagFamilies), s)
		if matchErr != nil {
			return nil, matchErr
		}
		if ok {
			e.TagFamilies = hiddenTags.StripHiddenTags(e.TagFamilies)
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

// sliceStreamElements applies the client offset:offset+limit window, matching the
// row *limit.Execute egress slice.
func sliceStreamElements(elements []*streamv1.Element, offset, limit uint32) []*streamv1.Element {
	start := int(offset)
	if start >= len(elements) {
		return []*streamv1.Element{}
	}
	end := start + int(limit)
	if end > len(elements) {
		end = len(elements)
	}
	return elements[start:end]
}

type measureQueryProcessor struct {
	measureService measure.Service
	*queryService
	*bus.UnImplementedHealthyListener
}

// measureExecutionContext holds the common execution context for measure queries.
type measureExecutionContext struct {
	ml       *logger.Logger
	ecc      []executor.MeasureExecutionContext
	metadata []*commonv1.Metadata
	schemas  []logical.Schema
}

// buildMeasureContext builds the execution context for a measure query.
func buildMeasureContext(measureService measure.Service, log *logger.Logger, queryCriteria *measurev1.QueryRequest, logPrefix string) (*measureExecutionContext, error) {
	var metadata []*commonv1.Metadata
	var schemas []logical.Schema
	var ecc []executor.MeasureExecutionContext
	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, ecErr := measureService.Measure(meta)
		if ecErr != nil {
			return nil, fmt.Errorf("fail to get execution context for measure %s: %w", meta.GetName(), ecErr)
		}
		s, schemaErr := logical_measure.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if schemaErr != nil {
			return nil, fmt.Errorf("fail to build schema for measure %s: %w", meta.GetName(), schemaErr)
		}
		ecc = append(ecc, ec)
		schemas = append(schemas, s)
		metadata = append(metadata, meta)
	}
	ml := log.Named(logPrefix, queryCriteria.Groups[0], queryCriteria.Name)
	return &measureExecutionContext{
		metadata: metadata,
		schemas:  schemas,
		ecc:      ecc,
		ml:       ml,
	}, nil
}

// vecExecutionContext is the optional capability the production
// MeasureExecutionContext implementation exposes to surface the active
// VectorizedConfig and the *databasev1.Measure schema needed by the vec
// dispatch. Production satisfies it; tests that swap the row executor
// can leave it unimplemented to keep dispatch dormant.
type vecExecutionContext interface {
	executor.MeasureExecutionContext
	VectorizedConfig() vmeasure.VectorizedConfig
	GetSchema() *databasev1.Measure
}

// executeMeasurePlan executes the measure query plan and returns the iterator.
//
// The second return value is the rendered plan string used by the caller
// for tracing — abstracted to a string so the vec subsystem (which does
// not produce a logical.Plan) can participate.
func executeMeasurePlan(
	ctx context.Context,
	queryCriteria *measurev1.QueryRequest,
	mctx *measureExecutionContext,
	emitPartial bool,
) (executor.MIterator, string, error) {
	mit, planStr, dispatchErr := dispatchMeasure(ctx, queryCriteria, mctx, emitPartial)
	if dispatchErr != nil {
		return nil, "", fmt.Errorf("fail to dispatch the query request for measure %s: %w", queryCriteria.GetName(), dispatchErr)
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.Str("plan", planStr).Msg("vec query plan")
	}
	return mit, planStr, nil
}

// dispatchMeasure is the thin adapter from measureExecutionContext into
// the vec dispatch inputs. Multi-measure queries (len(ecc) > 1) are
// handled per-group: each group is dispatched individually and the
// per-group iterators are merged through the shared cross-group ordering
// stack in logical_measure.MergeGroupMIterators (G9f.1, reusing
// sortableDataPoints + sortedMIterator so the cross-group order and
// version dedup are reproduced by construction, not by parallel
// reimplementation).
//
// Distributed Map-mode GroupBy+Agg (emitPartial=true) routes to vec via
// AggModeMap (G9f.2): vecplan.Dispatch receives emitPartial and the
// BatchAggregation operator emits typed-column partials. As of G9f.4 the
// data-node side handles distributed Top-over-Agg too — the vec plan emits
// Scan → GroupByAgg(Map) → Top → Limit per node, BatchTop sorts on the
// partial value column, and the liaison's distributed plan dedupes and
// applies the global Top across the per-node partial top-Ns.
func dispatchMeasure(
	ctx context.Context,
	queryCriteria *measurev1.QueryRequest,
	mctx *measureExecutionContext,
	emitPartial bool,
) (executor.MIterator, string, error) {
	vecs := make([]vecExecutionContext, len(mctx.ecc))
	for groupIdx, ec := range mctx.ecc {
		vec, ok := ec.(vecExecutionContext)
		if !ok {
			// Production storage always satisfies vecExecutionContext. A
			// group that does not is a botched rollout and must fail loud.
			return nil, "", fmt.Errorf("vec dispatch: group %d execution context not vec-capable (rollout skew?)", groupIdx)
		}
		vecs[groupIdx] = vec
	}
	if len(mctx.ecc) == 1 {
		return vecplan.Dispatch(ctx, queryCriteria, mctx.metadata[0], vecs[0].GetSchema(),
			mctx.schemas[0], vecs[0], vecs[0].VectorizedConfig(), emitPartial, false)
	}
	// Multi-measure projection validation: a tag/field is valid if it
	// resolves in ANY group's schema (mirrors the mergeSchema(ss) union).
	// Running the per-group validation inside Dispatch would reject the
	// schema-evolution case where one group added a tag/field the others
	// lack (multi_group_new_tag_field integration fixture). We pre-validate
	// here against the union once, then skip the per-group validation
	// inside Dispatch.
	measureSchemas := make([]*databasev1.Measure, len(vecs))
	for i, v := range vecs {
		measureSchemas[i] = v.GetSchema()
	}
	if projErr := vecplan.ValidateMultiGroupProjection(queryCriteria, mctx.schemas, measureSchemas); projErr != nil {
		return nil, "", projErr
	}
	iters := make([]executor.MIterator, 0, len(mctx.ecc))
	planStrs := make([]string, 0, len(mctx.ecc))
	closeOpened := func() {
		for _, opened := range iters {
			_ = opened.Close()
		}
	}
	for groupIdx, vec := range vecs {
		mit, planStr, dispatchErr := vecplan.Dispatch(ctx, queryCriteria,
			mctx.metadata[groupIdx], vec.GetSchema(), mctx.schemas[groupIdx], vec, vec.VectorizedConfig(), emitPartial, true)
		if dispatchErr != nil {
			closeOpened()
			return nil, "", dispatchErr
		}
		iters = append(iters, mit)
		planStrs = append(planStrs, planStr)
	}
	order, orderErr := logical_measure.ResolveCrossGroupMergeOrder(queryCriteria, mctx.schemas)
	if orderErr != nil {
		closeOpened()
		return nil, "", orderErr
	}
	merged := logical_measure.MergeGroupMIterators(iters, order)
	return merged, fmt.Sprintf("vec-multi-measure%v", planStrs), nil
}

// collectInternalDataPoints collects InternalDataPoints from the iterator.
func collectInternalDataPoints(mIterator executor.MIterator) []*measurev1.InternalDataPoint {
	result := make([]*measurev1.InternalDataPoint, 0)
	for mIterator.Next() {
		current := mIterator.Current()
		if len(current) > 0 {
			result = append(result, current[0])
		}
	}
	return result
}

func (p *measureQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	queryCriteria, ok := message.Data().(*measurev1.QueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	resp = p.executeQuery(ctx, queryCriteria)
	return
}

func (p *measureQueryProcessor) executeQuery(ctx context.Context, queryCriteria *measurev1.QueryRequest) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			p.log.Error().Interface("err", recoverErr).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	mctx, buildErr := buildMeasureContext(p.measureService, p.log, queryCriteria, "measure")
	if buildErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", buildErr))
		return
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received a query event")
	}

	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
	}
	mIterator, planStr, execErr := executeMeasurePlan(ctx, queryCriteria, mctx, false)
	if execErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", execErr))
		return
	}
	iteratorClosed := false
	closeIterator := func() {
		if iteratorClosed {
			return
		}
		iteratorClosed = true
		if closeErr := mIterator.Close(); closeErr != nil {
			mctx.ml.Error().Err(closeErr).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}
	defer closeIterator()

	if span != nil {
		span.Tag(tracelabels.TagPlan, planStr)
		defer func() {
			data := resp.Data()
			switch d := data.(type) {
			case *measurev1.QueryResponse:
				closeIterator()
				span.Tag(tracelabels.TagRespCount, fmt.Sprintf("%d", len(d.DataPoints)))
				span.Stop()
				d.Trace = tracer.ToProto()
			case *common.Error:
				closeIterator()
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.QueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
		}()
	}

	var result []*measurev1.DataPoint
	func() {
		var r int
		if tracer != nil {
			iterSpan, _ := tracer.StartSpan(ctx, "iterator")
			defer func() {
				iterSpan.Tag(tracelabels.TagRounds, fmt.Sprintf("%d", r))
				iterSpan.Tag(tracelabels.TagSize, fmt.Sprintf("%d", len(result)))
				iterSpan.Stop()
			}()
		}
		for mIterator.Next() {
			r++
			current := mIterator.Current()
			if len(current) > 0 {
				result = append(result, current[0].GetDataPoint())
			}
		}
	}()

	qr := &measurev1.QueryResponse{DataPoints: result}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("ret", logger.Proto(qr)).Msg("got a measure")
	}
	resp = bus.NewMessage(bus.MessageID(now), qr)
	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", len(result)).Msg("measure slow query")
		}
	}
	return
}

type measureInternalQueryProcessor struct {
	measureService measure.Service
	*queryService
	*bus.UnImplementedHealthyListener
	metricSvc observability.MetricsRegistry
}

func (p *measureInternalQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	internalRequest, ok := message.Data().(*measurev1.InternalQueryRequest)
	n := time.Now()
	now := n.UnixNano()
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	queryCriteria := internalRequest.GetRequest()
	if queryCriteria == nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("query request is nil"))
		return
	}
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			p.log.Error().Interface("err", recoverErr).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	mctx, buildErr := buildMeasureContext(p.measureService, p.log, queryCriteria, "internal-measure")
	if buildErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", buildErr))
		return
	}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("req", logger.Proto(queryCriteria)).Msg("received an internal query event")
	}

	var tracer *query.Tracer
	var span *query.Span
	if queryCriteria.Trace {
		tracer, ctx = query.NewTracer(ctx, n.Format(time.RFC3339Nano))
		span, ctx = tracer.StartSpan(ctx, "data-%s", p.queryService.nodeID)
	}
	mIterator, planStr, execErr := executeMeasurePlan(ctx, queryCriteria, mctx, internalRequest.GetAggReturnPartial())
	if execErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", execErr))
		return
	}
	iteratorClosed := false
	closeIterator := func() {
		if iteratorClosed {
			return
		}
		iteratorClosed = true
		if closeErr := mIterator.Close(); closeErr != nil {
			mctx.ml.Error().Err(closeErr).Dur("latency", time.Since(n)).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to close the query plan")
		}
	}
	defer closeIterator()

	// Raw-frame wire-emit path under flag-on. No-fall-through directive:
	// data.MeasureWireModeRaw() means the cluster wire codec is
	// RawFrameCodec, so the data-node Rev MUST emit a raw frame body
	// (or a nil empty body — the codec carve-out). A proto body would
	// be silently rejected by the liaison's RawFrameCodec.Unmarshal
	// bad-magic guard.
	//
	// Iterators encapsulate their own drain + encode via FrameEmitter:
	//
	//   - vectorizedMIterator drains the vec Pipeline directly
	//     (throughput-optimal: no proto materialization, columnar
	//     end-to-end).
	//   - emptyMIterator emits a nil body (codec empty-result carve-out).
	//   - hiddenTagsMIterator drains via Next/Current (its strip stays
	//     the source of truth) and reverse-serializes.
	//   - sortedMIterator drains via Next/Current (its cross-group sort
	//     + version dedup stay the source of truth) and reverse-
	//     serializes.
	//
	if data.MeasureWireModeRaw() {
		if span != nil {
			span.Tag(tracelabels.TagPlan, planStr)
			span.Tag(tracelabels.TagRespKind, "raw-frame")
		}
		emitter, ok := mIterator.(vmeasure.FrameEmitter)
		if !ok {
			emitErr := fmt.Errorf("vec wire mode (flag-on) requires FrameEmitter iterator; got %T — this iterator type has no wire-emit implementation", mIterator)
			if span != nil {
				closeIterator()
				span.Error(emitErr)
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalQueryResponse{Trace: tracer.ToProto()})
				return
			}
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", emitErr))
			return
		}
		rawBody, drainErr := emitter.EmitFrame(ctx)
		if drainErr != nil {
			emitErr := fmt.Errorf("vec raw-frame emit: %w", drainErr)
			if span != nil {
				closeIterator()
				span.Error(emitErr)
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalQueryResponse{Trace: tracer.ToProto()})
				return
			}
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("%v", emitErr))
			return
		}
		if span != nil {
			closeIterator()
			span.Tagf(tracelabels.TagBytesOut, "%d", len(rawBody))
			span.Stop()
			resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalQueryResponse{RawFrameBody: rawBody, Trace: tracer.ToProto()})
		} else {
			resp = bus.NewMessage(bus.MessageID(now), rawBody)
		}
		if p.slowQuery > 0 {
			latency := time.Since(n)
			if latency > p.slowQuery {
				p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_bytes", len(rawBody)).Msg("internal measure slow query (raw frame)")
			}
		}
		return
	}

	if span != nil {
		span.Tag(tracelabels.TagPlan, planStr)
		defer func() {
			respData := resp.Data()
			switch d := respData.(type) {
			case *measurev1.InternalQueryResponse:
				closeIterator()
				span.Tag(tracelabels.TagRespCount, fmt.Sprintf("%d", len(d.DataPoints)))
				span.Stop()
				d.Trace = tracer.ToProto()
			case *common.Error:
				closeIterator()
				span.Error(errors.New(d.Error()))
				span.Stop()
				resp = bus.NewMessage(bus.MessageID(now), &measurev1.InternalQueryResponse{Trace: tracer.ToProto()})
			default:
				panic("unexpected data type")
			}
		}()
	}

	result := collectInternalDataPoints(mIterator)

	qr := &measurev1.InternalQueryResponse{DataPoints: result}
	if e := mctx.ml.Debug(); e.Enabled() {
		e.RawJSON("ret", logger.Proto(qr)).Msg("got an internal measure response")
	}
	resp = bus.NewMessage(bus.MessageID(now), qr)
	if !queryCriteria.Trace && p.slowQuery > 0 {
		latency := time.Since(n)
		if latency > p.slowQuery {
			p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", len(result)).Msg("internal measure slow query")
		}
	}
	return
}

type traceQueryProcessor struct {
	traceService trace.Service
	*queryService
	*bus.UnImplementedHealthyListener
	// distributed is true on a distributed data node, whose query response is
	// sent over the wire to a remote liaison that decodes the native columnar
	// frame. It is false in standalone, where the local grpc service consumes the
	// response directly and expects the proto body — so the frame must not be
	// emitted there.
	distributed bool
}

func (p *traceQueryProcessor) Rev(ctx context.Context, message bus.Message) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	queryCriteria, ok := message.Data().(*tracev1.QueryRequest)
	if !ok {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("invalid event data type"))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().RawJSON("criteria", logger.Proto(queryCriteria)).Msg("received a trace query request")
	}
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	resp = p.executeQuery(ctx, queryCriteria)
	return
}

type traceExecutionPlan struct {
	traceIDTagNames   []string
	spanIDTagNames    []string
	timestampTagNames []string
	metadata          []*commonv1.Metadata
	schemas           []logical.Schema
	executionContexts []trace.Trace
}

func (p *traceQueryProcessor) setupTraceExecutionPlan(queryCriteria *tracev1.QueryRequest) (*traceExecutionPlan, *common.Error) {
	plan := &traceExecutionPlan{
		metadata:          make([]*commonv1.Metadata, 0, len(queryCriteria.Groups)),
		schemas:           make([]logical.Schema, 0, len(queryCriteria.Groups)),
		executionContexts: make([]trace.Trace, 0, len(queryCriteria.Groups)),
		traceIDTagNames:   make([]string, 0, len(queryCriteria.Groups)),
		spanIDTagNames:    make([]string, 0, len(queryCriteria.Groups)),
		timestampTagNames: make([]string, 0, len(queryCriteria.Groups)),
	}

	for i := range queryCriteria.Groups {
		meta := &commonv1.Metadata{
			Name:  queryCriteria.Name,
			Group: queryCriteria.Groups[i],
		}
		ec, err := p.traceService.Trace(meta)
		if err != nil {
			return nil, common.NewError("fail to get execution context for trace %s: %v", meta.GetName(), err)
		}

		s, err := logical_trace.BuildSchema(ec.GetSchema(), ec.GetIndexRules())
		if err != nil {
			return nil, common.NewError("fail to build schema for trace %s: %v", meta.GetName(), err)
		}

		// Validate tag name consistency
		if errMsg := p.validateTagNames(plan, ec, meta); errMsg != nil {
			return nil, errMsg
		}

		plan.executionContexts = append(plan.executionContexts, ec)
		plan.schemas = append(plan.schemas, s)
		plan.metadata = append(plan.metadata, meta)
		plan.traceIDTagNames = append(plan.traceIDTagNames, ec.GetSchema().GetTraceIdTagName())
		plan.spanIDTagNames = append(plan.spanIDTagNames, ec.GetSchema().GetSpanIdTagName())
		plan.timestampTagNames = append(plan.timestampTagNames, ec.GetSchema().GetTimestampTagName())
	}

	return plan, nil
}

func (p *traceQueryProcessor) validateTagNames(plan *traceExecutionPlan, ec trace.Trace, meta *commonv1.Metadata) *common.Error {
	if len(plan.traceIDTagNames) > 0 && plan.traceIDTagNames[0] != ec.GetSchema().GetTraceIdTagName() {
		return common.NewError("trace id tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.traceIDTagNames[0], ec.GetSchema().GetTraceIdTagName())
	}
	if len(plan.spanIDTagNames) > 0 && plan.spanIDTagNames[0] != ec.GetSchema().GetSpanIdTagName() {
		return common.NewError("span id tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.spanIDTagNames[0], ec.GetSchema().GetSpanIdTagName())
	}
	if len(plan.timestampTagNames) > 0 && plan.timestampTagNames[0] != ec.GetSchema().GetTimestampTagName() {
		return common.NewError("timestamp tag name mismatch for trace %s: %s != %s",
			meta.GetName(), plan.timestampTagNames[0], ec.GetSchema().GetTimestampTagName())
	}
	return nil
}

type traceMonitor struct {
	tracer *query.Tracer
	span   *query.Span
}

func (p *traceQueryProcessor) setupTraceMonitor(ctx context.Context, queryCriteria *tracev1.QueryRequest,
	plan logical.Plan, startTime time.Time,
) (context.Context, *traceMonitor) {
	if !queryCriteria.Trace {
		return ctx, nil
	}

	tracer, newCtx := query.NewTracer(ctx, startTime.Format(time.RFC3339Nano))
	span, newCtx := tracer.StartSpan(newCtx, "data-%s", p.queryService.nodeID)
	span.Tag(tracelabels.TagPlan, plan.String())

	return newCtx, &traceMonitor{
		tracer: tracer,
		span:   span,
	}
}

func (tm *traceMonitor) finishTrace(resp *bus.Message, messageID int64) {
	if tm == nil {
		return
	}

	data := resp.Data()
	switch d := data.(type) {
	case *tracev1.InternalQueryResponse:
		tm.span.Tag("resp_count", fmt.Sprintf("%d", len(d.InternalTraces)))
		tm.span.Stop()
		d.TraceQueryResult = tm.tracer.ToProto()
	case *common.Error:
		tm.span.Error(errors.New(d.Error()))
		tm.span.Stop()
		*resp = bus.NewMessage(bus.MessageID(messageID), &tracev1.QueryResponse{TraceQueryResult: tm.tracer.ToProto()})
	default:
		panic("unexpected data type")
	}
}

func (p *traceQueryProcessor) processTraceResults(resultIterator iter.Iterator[model.TraceResult],
	queryCriteria *tracev1.QueryRequest, execPlan *traceExecutionPlan,
) ([]*tracev1.InternalTrace, error) {
	var traces []*tracev1.InternalTrace

	// Build tag inclusion maps for each group
	traceIDInclusionMap := make(map[int]bool)
	spanIDInclusionMap := make(map[int]bool)
	for i, tagName := range execPlan.traceIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			traceIDInclusionMap[i] = true
		}
	}
	for i, tagName := range execPlan.spanIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			spanIDInclusionMap[i] = true
		}
	}

	for {
		result, hasNext := resultIterator.Next()
		if !hasNext {
			break
		}
		if result.Error != nil {
			return nil, result.Error
		}
		if result.TID == "" {
			// Skip spans without trace ID
			continue
		}

		// Create a trace for this result
		trace := &tracev1.InternalTrace{
			TraceId: result.TID,
			Key:     result.Key,
			Spans:   make([]*tracev1.Span, 0, len(result.Spans)),
		}
		// Convert each span in the trace result
		for i, spanBytes := range result.Spans {
			traceTags := p.buildTraceTags(&result, queryCriteria, execPlan, i, traceIDInclusionMap, spanIDInclusionMap)

			span := &tracev1.Span{
				Tags:   traceTags,
				Span:   spanBytes,
				SpanId: result.SpanIDs[i],
			}
			trace.Spans = append(trace.Spans, span)
		}
		traces = append(traces, trace)
	}

	return traces, nil
}

func (p *traceQueryProcessor) buildTraceTags(result *model.TraceResult, queryCriteria *tracev1.QueryRequest, execPlan *traceExecutionPlan,
	spanIndex int, traceIDInclusionMap, spanIDInclusionMap map[int]bool,
) []*modelv1.Tag {
	var traceTags []*modelv1.Tag

	// Create trace tags from the result
	if result.Tags != nil && len(queryCriteria.TagProjection) > 0 {
		for _, tag := range result.Tags {
			if !slices.Contains(queryCriteria.TagProjection, tag.Name) {
				continue
			}
			if spanIndex < len(tag.Values) {
				traceTags = append(traceTags, &modelv1.Tag{
					Key:   tag.Name,
					Value: tag.Values[spanIndex],
				})
			}
		}
	}

	// Use group index to select traceIDTagName
	if traceIDInclusionMap[result.GroupIndex] && result.TID != "" {
		traceTags = append(traceTags, &modelv1.Tag{
			Key: execPlan.traceIDTagNames[result.GroupIndex],
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: result.TID,
					},
				},
			},
		})
	}

	// Add span ID tag to each span if it should be included
	// Use group index to select spanIDTagName
	if spanIDInclusionMap[result.GroupIndex] && spanIndex < len(result.SpanIDs) {
		traceTags = append(traceTags, &modelv1.Tag{
			Key: execPlan.spanIDTagNames[result.GroupIndex],
			Value: &modelv1.TagValue{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: result.SpanIDs[spanIndex],
					},
				},
			},
		})
	}

	return traceTags
}

// buildFrameTraceResults mirrors processTraceResults+buildTraceTags but outputs
// columnar []model.TraceResult (TID/Key/Spans/SpanIDs plus one TagValue column
// per span) for the native columnar wire frame. It replicates the projected-tag
// filter and the traceID/spanID tag augmentation so the liaison reconstructs the
// same spans the proto path would have produced.
func (p *traceQueryProcessor) buildFrameTraceResults(resultIterator iter.Iterator[model.TraceResult],
	queryCriteria *tracev1.QueryRequest, execPlan *traceExecutionPlan,
) ([]model.TraceResult, error) {
	traceIDInclusionMap := make(map[int]bool)
	spanIDInclusionMap := make(map[int]bool)
	for i, tagName := range execPlan.traceIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			traceIDInclusionMap[i] = true
		}
	}
	for i, tagName := range execPlan.spanIDTagNames {
		if slices.Contains(queryCriteria.TagProjection, tagName) {
			spanIDInclusionMap[i] = true
		}
	}

	var results []model.TraceResult
	for {
		result, hasNext := resultIterator.Next()
		if !hasNext {
			break
		}
		if result.Error != nil {
			return nil, result.Error
		}
		if result.TID == "" {
			continue
		}

		spanCount := len(result.Spans)
		frameResult := model.TraceResult{
			TID:     result.TID,
			Key:     result.Key,
			Spans:   result.Spans,
			SpanIDs: result.SpanIDs,
		}
		if result.Tags != nil && len(queryCriteria.TagProjection) > 0 {
			for _, tag := range result.Tags {
				if !slices.Contains(queryCriteria.TagProjection, tag.Name) {
					continue
				}
				frameResult.Tags = append(frameResult.Tags, tag)
			}
		}
		if traceIDInclusionMap[result.GroupIndex] {
			values := make([]*modelv1.TagValue, spanCount)
			tidValue := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: result.TID}}}
			for spanIdx := range values {
				values[spanIdx] = tidValue
			}
			frameResult.Tags = append(frameResult.Tags, model.Tag{Name: execPlan.traceIDTagNames[result.GroupIndex], Values: values})
		}
		if spanIDInclusionMap[result.GroupIndex] {
			values := make([]*modelv1.TagValue, spanCount)
			for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
				if spanIdx < len(result.SpanIDs) {
					values[spanIdx] = &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: result.SpanIDs[spanIdx]}}}
				} else {
					values[spanIdx] = pbv1.NullTagValue
				}
			}
			frameResult.Tags = append(frameResult.Tags, model.Tag{Name: execPlan.spanIDTagNames[result.GroupIndex], Values: values})
		}
		results = append(results, frameResult)
	}

	return results, nil
}

func (p *traceQueryProcessor) logSlowQuery(queryCriteria *tracev1.QueryRequest, traces []*tracev1.InternalTrace, startTime time.Time) {
	spanCount := 0
	for _, trace := range traces {
		spanCount += len(trace.Spans)
	}
	p.logSlowTraceQuery(queryCriteria, spanCount, startTime)
}

func (p *traceQueryProcessor) logSlowFrameQuery(queryCriteria *tracev1.QueryRequest, results []model.TraceResult, startTime time.Time) {
	spanCount := 0
	for resultIdx := range results {
		spanCount += len(results[resultIdx].Spans)
	}
	p.logSlowTraceQuery(queryCriteria, spanCount, startTime)
}

func (p *traceQueryProcessor) logSlowTraceQuery(queryCriteria *tracev1.QueryRequest, spanCount int, startTime time.Time) {
	if queryCriteria.Trace || p.slowQuery <= 0 {
		return
	}
	latency := time.Since(startTime)
	if latency <= p.slowQuery {
		return
	}
	p.log.Warn().Dur("latency", latency).RawJSON("req", logger.Proto(queryCriteria)).Int("resp_count", spanCount).Msg("trace slow query")
}

func (p *traceQueryProcessor) executeQuery(ctx context.Context, queryCriteria *tracev1.QueryRequest) (resp bus.Message) {
	n := time.Now()
	now := n.UnixNano()
	defer func() {
		if err := recover(); err != nil {
			p.log.Error().Interface("err", err).RawJSON("req", logger.Proto(queryCriteria)).Str("stack", string(debug.Stack())).Msg("panic")
			resp = bus.NewMessage(bus.MessageID(time.Now().UnixNano()), common.NewError("panic"))
		}
	}()

	execPlan, setupErr := p.setupTraceExecutionPlan(queryCriteria)
	if setupErr != nil {
		resp = bus.NewMessage(bus.MessageID(now), setupErr)
		return
	}
	traceExecContexts := make([]executor.TraceExecutionContext, len(execPlan.executionContexts))
	for i, ec := range execPlan.executionContexts {
		traceExecContexts[i] = ec
	}

	plan, err := logical_trace.Analyze(queryCriteria, execPlan.metadata, execPlan.schemas, traceExecContexts,
		execPlan.traceIDTagNames, execPlan.spanIDTagNames, execPlan.timestampTagNames)
	if err != nil {
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("fail to analyze the query request for trace %s: %v", queryCriteria.GetName(), err))
		return
	}
	if p.log.Debug().Enabled() {
		p.log.Debug().Str("plan", plan.String()).Msg("query plan")
	}

	ctx, traceMonitor := p.setupTraceMonitor(ctx, queryCriteria, plan, n)
	if traceMonitor != nil {
		defer traceMonitor.finishTrace(&resp, now)
	}

	te := plan.(executor.TraceExecutable)
	defer te.Close()
	resultIterator, err := te.Execute(ctx)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to execute the trace query plan")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("execute the query plan for trace %s: %v", queryCriteria.GetName(), err))
		return
	}

	// Native wire mode (no tracing): emit a columnar frame body so the send path
	// passes it through as opaque bytes and the liaison decodes it without the
	// protobuf message-slice/oneof machinery. The tracing path keeps the proto
	// body (the traceMonitor defer needs *InternalQueryResponse).
	if p.distributed && traceMonitor == nil {
		results, buildErr := p.buildFrameTraceResults(resultIterator, queryCriteria, execPlan)
		if buildErr != nil {
			p.log.Error().Err(buildErr).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to process trace results")
			resp = bus.NewMessage(bus.MessageID(now), common.NewError("process trace results for trace %s: %v", queryCriteria.GetName(), buildErr))
			return
		}
		resp = bus.NewMessage(bus.MessageID(now), logical_trace.EncodeTraceResultFrame(results))
		p.logSlowFrameQuery(queryCriteria, results, n)
		return
	}

	traces, err := p.processTraceResults(resultIterator, queryCriteria, execPlan)
	if err != nil {
		p.log.Error().Err(err).RawJSON("req", logger.Proto(queryCriteria)).Msg("fail to process trace results")
		resp = bus.NewMessage(bus.MessageID(now), common.NewError("process trace results for trace %s: %v", queryCriteria.GetName(), err))
		return
	}

	resp = bus.NewMessage(bus.MessageID(now), &tracev1.InternalQueryResponse{InternalTraces: traces})

	p.logSlowQuery(queryCriteria, traces, n)
	return
}
