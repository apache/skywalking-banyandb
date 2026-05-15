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
	"sync/atomic"

	"github.com/pkg/errors"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Process-wide observability counters for G8e parity testing. Tests
// assert HandledCount > 0 to prove dispatch fires (vs silently falling
// through), and FellThroughCount > 0 to prove the row path still serves
// queries Dispatch is not yet ready to take.
//
// Counters are best-effort: under concurrent queries the deltas are
// accurate, but a test reading them across a workload may observe
// updates from unrelated queries. Snapshot via Load() before the test
// workload and compute the delta.
var (
	handledCount     atomic.Int64
	fellThroughCount atomic.Int64
)

// HandledCount returns the cumulative number of vec dispatch successes
// observed by this process.
func HandledCount() int64 { return handledCount.Load() }

// FellThroughCount returns the cumulative number of times Dispatch
// declined to handle a request (returned handled=false, err=nil) in
// this process.
func FellThroughCount() int64 { return fellThroughCount.Load() }

// Dispatch is the G8d top-level entry into the vec measure subsystem.
//
// Called from banyand/query/processor.go before the row-path Analyze runs.
// When the request is eligible for the vec subsystem, Dispatch:
//
//  1. Analyzes the request into a VecPlan via plan.Analyze (G8b)
//  2. Resolves the index.Query + entity table the storage layer needs
//     (using inverted.BuildQuery / BuildIndexModeQuery — the same helpers
//     the deprecated row path uses; the logical.Schema parameter threads
//     through unchanged)
//  3. Calls ec.Query(ctx, opts) to obtain the MeasureQueryResult
//  4. Wraps the result as a vec PullOperator (BatchSourceFromBatchResult
//     fast path when available; BatchScan fallback otherwise) and installs
//     it on the leaf Scan node
//  5. Executes the plan via plan.Execute (G8c) to return an MIterator
//
// Returns (iter, planStr, true, nil) when the request is handled; the
// caller MUST return that iterator and skip the row plan. Returns
// (nil, "", false, nil) when the request is NOT eligible — the caller
// should fall through to the row path. Returns (nil, "", true, err)
// when the request was eligible but execution failed; the caller must
// surface the error rather than fall through (the storage query may
// have already touched state).
//
// Eligibility gate (v1):
//   - cfg.Enabled must be true
//   - request may carry GroupBy and/or Agg in any combination (group+agg,
//     scalar reduce, raw GroupBy); plan.Analyze auto-extends the
//     projection so the keys / agg field always resolve
//   - request may carry Top: the analyzer emits Scan → Top → Limit
//     (or Scan → GroupByAgg → Top → Limit) and BatchTop reproduces the
//     row path's top-N (G9a)
//   - request must carry TimeRange (storage requires a bounded window)
//   - hidden criteria tags (criteria tags absent from the projection)
//     are projected for storage-side filtering, then stripped at egress
//     by hiddenTagsMIterator so the wire format is byte-identical
//   - measureSchema and logicalSchema must be non-nil
func Dispatch(
	ctx context.Context,
	req *measurev1.QueryRequest,
	metadata *commonv1.Metadata,
	measureSchema *databasev1.Measure,
	logicalSchema logical.Schema,
	ec executor.MeasureExecutionContext,
	cfg measure.VectorizedConfig,
) (iter executor.MIterator, planStr string, handled bool, err error) {
	defer func() {
		// Errors are surfaced as-is; only count clean handled / fall-
		// through outcomes so observability matches the caller's
		// branching contract.
		if err != nil {
			return
		}
		if handled {
			handledCount.Add(1)
		} else {
			fellThroughCount.Add(1)
		}
	}()
	if !cfg.Enabled {
		return nil, "", false, nil
	}
	if req == nil {
		return nil, "", false, nil
	}
	// Top is handled by the vec subsystem: plan.Analyze emits
	// Scan → Top → Limit (or Scan → GroupByAgg → Top → Limit) and
	// BatchTop reproduces the row path's top-N (G9a).
	// GroupBy and Agg are handled by the vec subsystem in all three
	// shapes — group+agg, scalar reduce (Agg only), raw GroupBy (GroupBy
	// only). plan.Analyze auto-extends the projection so the GroupBy keys
	// and Agg field always materialize a column, so there is no
	// projection-coverage gate here anymore (G9b).
	// G9c #9: a nil TimeRange is NOT a fall-through. The row path does not
	// reject it — parseFields feeds criteria.GetTimeRange().GetBegin().AsTime()
	// into the index scan, and a nil *timestamppb.Timestamp resolves to the
	// Unix epoch (1970-01-01T00:00:00Z). The query then runs over the
	// degenerate [epoch, epoch] window and yields an empty result. The
	// scan.Params.TimeRange construction below mirrors that exactly
	// (req.GetTimeRange().GetBegin().AsTime() == epoch when TimeRange is
	// nil), so dispatch produces the row path's canonical empty response
	// directly instead of borrowing it.

	// Defensive nil guards on the runtime context. These should not fire
	// in production paths — buildMeasureContext populates all of them —
	// but a defensive fallthrough is safer than a nil dereference.
	if measureSchema == nil || logicalSchema == nil || ec == nil || metadata == nil {
		return nil, "", false, nil
	}

	// G9c #11: projection validation. The row path's Analyze rejects
	// unknown projection names via ValidateProjectionTags /
	// ValidateProjectionFields and surfaces a descriptive error
	// (test fixtures with WantErr=true assert it). Dispatch reproduces
	// that canonical error byte-for-byte and returns handled=true so the
	// caller surfaces it rather than falling through.
	if projErr := validateProjectionParity(req, logicalSchema, measureSchema); projErr != nil {
		return nil, "", true, projErr
	}

	// Hidden-tag detection: criteria may reference tags that are NOT in
	// the projection (they're needed only as filter inputs). Such tags
	// are projected so storage can evaluate the criteria, then stripped
	// at egress by hiddenTagsMIterator so the wire bytes stay identical
	// to a query without hidden criteria tags (the row path does the
	// same in unresolvedIndexScan.Analyze + resultMIterator).
	projectedTagNames := projectedNames(req.GetTagProjection())
	entityList := logicalSchema.EntityList()
	entityMap := make(map[string]int, len(entityList))
	entity := make([]*modelv1.TagValue, len(entityList))
	for idx, e := range entityList {
		entityMap[e] = idx
		entity[idx] = pbv1.AnyTagValue
	}
	familyNames := make([]string, 0, len(measureSchema.GetTagFamilies()))
	for _, tf := range measureSchema.GetTagFamilies() {
		familyNames = append(familyNames, tf.GetName())
	}
	hidden, hiddenExtras := logical.CollectHiddenCriteriaTags(
		req.GetCriteria(), projectedTagNames, entityMap, logicalSchema,
		func() []string { return familyNames },
	)
	// analyzeReq carries the hidden tags in its TagProjection so the Scan
	// materializes them for storage-side filtering; req itself is left
	// unchanged for the index.Query / opts wiring below.
	analyzeReq := augmentRequestWithHiddenTags(req, hiddenExtras)

	indexOrder, orderErr := resolveOrderBy(req.GetOrderBy(), logicalSchema)
	if orderErr != nil {
		return nil, "", true, orderErr
	}

	// Resolve the index.Query + entities the same way the row path does
	// in unresolvedIndexScan.Analyze.
	var query index.Query
	var entities [][]*modelv1.TagValue
	var qErr error
	if measureSchema.GetIndexMode() {
		query, qErr = inverted.BuildIndexModeQuery(metadata.GetName(), req.GetCriteria(), logicalSchema)
	} else {
		query, entities, _, qErr = inverted.BuildQuery(req.GetCriteria(), logicalSchema, entityMap, entity)
	}
	if qErr != nil {
		return nil, "", true, fmt.Errorf("vec dispatch: build query: %w", qErr)
	}

	// Build the structural plan tree from analyzeReq so the Scan's
	// BatchSchema + opts.TagProjection carry the hidden criteria tags.
	p, analyzeErr := Analyze(analyzeReq, measureSchema)
	if analyzeErr != nil {
		return nil, "", true, fmt.Errorf("vec dispatch: analyze: %w", analyzeErr)
	}
	scan := locateScan(p)
	if scan == nil {
		return nil, "", true, fmt.Errorf("vec dispatch: plan missing Scan node")
	}
	tr := timestamp.NewInclusiveTimeRange(
		req.GetTimeRange().GetBegin().AsTime(),
		req.GetTimeRange().GetEnd().AsTime(),
	)
	scan.Params.TimeRange = &tr
	scan.Params.Query = query
	scan.Params.Entities = entities

	// Execute the storage query. The vec source is constructed from the
	// returned MeasureQueryResult and threaded into the Scan node.
	//
	// GroupBy/Agg must be threaded into opts so banyand/measure/query.go
	// rebuilds result.batchSchema with the same native typed columns the
	// analyzer baked into scan.BatchSchema. Mismatched halves (one side
	// native, the other passthrough) surface as a type-assert panic in
	// BatchAggregation.fold or a TypedColumn[T] mismatch in
	// BatchSourceFromBatchResult.appendColumnRange.
	opts := model.MeasureQueryOptions{
		Name:            metadata.GetName(),
		TimeRange:       scan.Params.TimeRange,
		Entities:        entities,
		Query:           query,
		Order:           indexOrder,
		GroupBy:         scan.Params.GroupBy,
		Agg:             scan.Params.Agg,
		TagProjection:   scan.Params.TagProjection,
		FieldProjection: scan.Params.FieldProjection,
	}
	result, queryErr := ec.Query(ctx, opts)
	if queryErr != nil {
		return nil, "", true, fmt.Errorf("vec dispatch: query measure: %w", queryErr)
	}
	if result == nil {
		// G9c #13: a typed-nil result is the row path's canonical empty
		// response. The row iterator (resultMIterator{result: nil}) reports
		// Next()==false immediately and Close()==nil, so the client
		// observes an empty []*measurev1.InternalDataPoint. Emit the same
		// empty MIterator directly with handled=true instead of borrowing
		// the row machinery. Hidden-tag egress strip (below) is
		// intentionally skipped here: an empty result has no DataPoints,
		// so there is nothing to strip.
		return emptyMIterator{}, p.String(), true, nil
	}

	pool := vectorized.NewBatchPool(scan.BatchSchema, cfg.BatchSize)
	var source vectorized.PullOperator
	if br, ok := result.(model.MeasureBatchResult); ok {
		source = measure.NewBatchSourceFromBatchResult(br, scan.BatchSchema, pool, cfg.BatchSize)
	} else {
		source = measure.NewBatchScan(result, scan.BatchSchema, pool, cfg.BatchSize)
	}
	scan.Source = source

	iter, execErr := Execute(ctx, p, cfg)
	if execErr != nil {
		// Execute closes the pipeline on Build/Init failure, which
		// closes the source, which releases result. No extra Release
		// here.
		return nil, "", true, fmt.Errorf("vec dispatch: execute: %w", execErr)
	}
	if !hidden.IsEmpty() {
		// Strip the projected-for-filtering hidden tags before
		// serialization so the wire bytes match a query without them.
		iter = &hiddenTagsMIterator{inner: iter, hiddenTags: hidden}
	}
	return iter, p.String(), true, nil
}

// resolveOrderBy mirrors the row path's PushDownOrder optimizer rule.
// Empty index rule + UNSPECIFIED sort yields (nil, nil) so dispatch
// leaves opts.Order unset, matching the row path's no-order default.
// ParseOrderBy errors on an unknown index rule or one with NoSort=true
// — surface that error so dispatch reports handled=true rather than
// silently retrying the row path, which would produce the same error
// downstream.
func resolveOrderBy(reqOrder *modelv1.QueryOrder, schema logical.Schema) (*index.OrderBy, error) {
	if reqOrder == nil {
		return nil, nil
	}
	parsed, err := logical.ParseOrderBy(schema, reqOrder.GetIndexRuleName(), reqOrder.GetSort())
	if err != nil {
		return nil, fmt.Errorf("vec dispatch: parse order_by: %w", err)
	}
	if parsed == nil {
		return nil, nil
	}
	out := &index.OrderBy{Sort: parsed.Sort, Index: parsed.Index, Type: index.OrderByTypeIndex}
	if parsed.Index == nil {
		out.Type = index.OrderByTypeTime
	}
	return out, nil
}

// locateScan walks a vec plan tree to find the leaf Scan node. Today there
// is exactly one Scan per plan (multi-measure merge is a G8 follow-up).
func locateScan(p VecPlan) *Scan {
	if s, ok := p.(*Scan); ok {
		return s
	}
	for _, c := range p.Children() {
		if s := locateScan(c); s != nil {
			return s
		}
	}
	return nil
}

// projectedNames flattens a TagProjection into the {tagName -> struct{}}
// set used by logical.CollectHiddenCriteriaTags.
func projectedNames(tp *modelv1.TagProjection) map[string]struct{} {
	out := make(map[string]struct{})
	if tp == nil {
		return out
	}
	for _, tf := range tp.GetTagFamilies() {
		for _, t := range tf.GetTags() {
			out[t] = struct{}{}
		}
	}
	return out
}

// validateProjectionParity reproduces, byte-for-byte, the projection
// errors the row path's logical_measure.Analyze raises so dispatch can
// surface the canonical WantErr=true message directly instead of falling
// through. It mirrors the row path exactly:
//
//   - Tags are validated before fields (measure_analyzer.go:110-119).
//   - Tag projection is checked only when non-empty; each projected tag
//     (families in order, tags in order) is looked up schema-wide via the
//     TagSpec registry — the logical.Schema equivalent of CommonSchema's
//     TagSpecMap. The first miss returns errors.Wrap(ErrTagNotDefined,
//     tagName), identical to CommonSchema.ValidateProjectionTags
//     (schema.go:175): "<tagName>: tag is not defined".
//   - Field projection is checked only when non-empty; the first name
//     absent from the Measure schema's fields returns errors.Errorf(
//     "field %s not found in schema", field), identical to
//     measure.schema.ValidateProjectionFields (measure/schema.go:77).
//
// A nil error means every projected name resolves, so dispatch proceeds.
func validateProjectionParity(req *measurev1.QueryRequest, logicalSchema logical.Schema, m *databasev1.Measure) error {
	if tp := req.GetTagProjection(); tp != nil {
		for _, reqFamily := range tp.GetTagFamilies() {
			for _, name := range reqFamily.GetTags() {
				if logicalSchema.FindTagSpecByName(name) == nil {
					return errors.Wrap(logical.ErrTagNotDefined, name)
				}
			}
		}
	}
	if fp := req.GetFieldProjection(); fp != nil && len(fp.GetNames()) > 0 {
		// The row path's m.fieldMap is built from md.GetFields() in
		// logical_measure.BuildSchema, so the Measure schema's field set
		// is the authoritative lookup the row path's
		// ValidateProjectionFields consults.
		known := make(map[string]struct{}, len(m.GetFields()))
		for _, fs := range m.GetFields() {
			known[fs.GetName()] = struct{}{}
		}
		for _, name := range fp.GetNames() {
			if _, ok := known[name]; !ok {
				return errors.Errorf("field %s not found in schema", name)
			}
		}
	}
	return nil
}

// emptyMIterator is the vec equivalent of the row path's
// resultMIterator{result: nil}: Next reports no rows, Current is never
// reached, and Close is a no-op error. Dispatch returns it for the
// canonical empty response (G9c #13).
type emptyMIterator struct{}

func (emptyMIterator) Next() bool { return false }

func (emptyMIterator) Current() []*measurev1.InternalDataPoint { return nil }

func (emptyMIterator) Close() error { return nil }
