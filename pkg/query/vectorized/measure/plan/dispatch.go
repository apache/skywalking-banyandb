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
//   - request must NOT carry GroupBy or Agg (column-type bridging at the
//     scan source still pending; see executor.go's TODO(G8d))
//   - request must NOT carry Top (BatchTop's single-heap semantic differs
//     from the row path's per-timestamp top-N)
//   - request must carry TimeRange (storage requires a bounded window)
//   - request must NOT have hidden criteria tags (those need an egress
//     strip wrapper that v1 dispatch does not implement)
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
	if req.GetGroupBy() != nil || req.GetAgg() != nil || req.GetTop() != nil {
		// G8d.2 will lift GroupBy/Agg once the scan-source column type
		// bridging is in place. Top awaits per-timestamp partitioning of
		// BatchTop.
		return nil, "", false, nil
	}
	if req.GetOrderBy() != nil {
		// The row path resolves order_by via the PushDownOrder optimizer
		// rule (logical.NewPushDownOrder applied after Analyze). The vec
		// dispatch does not invoke those rules, so it would silently
		// drop OrderBy and return unsorted rows. Fall through until
		// dispatch threads order_by into model.MeasureQueryOptions.Order.
		return nil, "", false, nil
	}
	if req.GetTimeRange() == nil {
		return nil, "", false, nil
	}
	// Defensive nil guards on the runtime context. These should not fire
	// in production paths — buildMeasureContext populates all of them —
	// but a defensive fallthrough is safer than a nil dereference.
	if measureSchema == nil || logicalSchema == nil || ec == nil || metadata == nil {
		return nil, "", false, nil
	}

	// Projection validation. The row path's Analyze rejects unknown
	// projection names via ValidateProjectionTags / ValidateProjectionFields
	// and surfaces a descriptive error. Dispatch falls through so the
	// row path produces that canonical error (test fixtures with
	// WantErr=true depend on it).
	if !projectionsExistInSchema(req, measureSchema) {
		return nil, "", false, nil
	}

	// Hidden-tag detection: criteria may reference tags that are NOT in
	// the projection (they're needed only as filter inputs). The row
	// path strips them at egress via hiddenTagsMIterator. v1 dispatch
	// does not implement that strip yet, so fall through when present.
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
	hidden, _ := logical.CollectHiddenCriteriaTags(
		req.GetCriteria(), projectedTagNames, entityMap, logicalSchema,
		func() []string { return familyNames },
	)
	if !hidden.IsEmpty() {
		return nil, "", false, nil
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

	// Build the structural plan tree.
	p, analyzeErr := Analyze(req, measureSchema)
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
	opts := model.MeasureQueryOptions{
		Name:            metadata.GetName(),
		TimeRange:       scan.Params.TimeRange,
		Entities:        entities,
		Query:           query,
		TagProjection:   scan.Params.TagProjection,
		FieldProjection: scan.Params.FieldProjection,
	}
	result, queryErr := ec.Query(ctx, opts)
	if queryErr != nil {
		return nil, "", true, fmt.Errorf("vec dispatch: query measure: %w", queryErr)
	}
	if result == nil {
		// Match the row path's typed-nil handling: an empty query result
		// flows through the row iterator as a no-op. Falling back lets
		// that machinery surface the empty response unchanged.
		return nil, "", false, nil
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
	return iter, p.String(), true, nil
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

// projectionsExistInSchema returns false if any tag (in any requested tag
// family) or field name in the request's projection is absent from the
// Measure schema. Callers use the result as an eligibility gate: missing
// names route through the row path, which surfaces a descriptive error
// via logical_measure.Analyze.
func projectionsExistInSchema(req *measurev1.QueryRequest, m *databasev1.Measure) bool {
	if tp := req.GetTagProjection(); tp != nil {
		for _, reqFamily := range tp.GetTagFamilies() {
			schemaFamily := findSchemaTagFamily(m, reqFamily.GetName())
			if schemaFamily == nil {
				return false
			}
			known := make(map[string]struct{}, len(schemaFamily.GetTags()))
			for _, ts := range schemaFamily.GetTags() {
				known[ts.GetName()] = struct{}{}
			}
			for _, name := range reqFamily.GetTags() {
				if _, ok := known[name]; !ok {
					return false
				}
			}
		}
	}
	if fp := req.GetFieldProjection(); fp != nil && len(fp.GetNames()) > 0 {
		known := make(map[string]struct{}, len(m.GetFields()))
		for _, fs := range m.GetFields() {
			known[fs.GetName()] = struct{}{}
		}
		for _, name := range fp.GetNames() {
			if _, ok := known[name]; !ok {
				return false
			}
		}
	}
	return true
}

// findSchemaTagFamily returns the schema-defined tag family with the
// given name, or nil if no such family exists.
func findSchemaTagFamily(m *databasev1.Measure, name string) *databasev1.TagFamilySpec {
	for _, tf := range m.GetTagFamilies() {
		if tf.GetName() == name {
			return tf
		}
	}
	return nil
}
