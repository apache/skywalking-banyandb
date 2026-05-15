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
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logicalmeasure "github.com/apache/skywalking-banyandb/pkg/query/logical/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

func dispatchCfg(enabled bool) measure.VectorizedConfig {
	return measure.VectorizedConfig{Enabled: enabled, BatchSize: 1024, QueryMemoryMiB: 16}
}

func bareReq() *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Name:            "demo",
		Groups:          []string{"default"},
		TagProjection:   projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(time.Unix(0, 0)),
			End:   timestamppb.New(time.Unix(0, 1_000_000)),
		},
	}
}

// TestDispatch_NotEnabled_FallsThrough verifies cfg.Enabled=false returns
// (nil, "", false, nil) immediately, before any other check.
func TestDispatch_NotEnabled_FallsThrough(t *testing.T) {
	iter, planStr, handled, err := Dispatch(context.Background(),
		bareReq(), nil, nil, nil, nil, dispatchCfg(false))
	if err != nil {
		t.Fatalf("disabled config should not error: %v", err)
	}
	if handled {
		t.Fatal("disabled config must not handle the request")
	}
	if iter != nil || planStr != "" {
		t.Fatalf("disabled config: iter/planStr must be zero, got %v / %q", iter, planStr)
	}
}

// dispatchSchemaFixture builds the (measureSchema, logicalSchema,
// metadata, fakeEC) tuple the post-G9 ReachesEcQuery tests share. fakeEC
// returns (nil, nil) so dispatch falls through after ec.Query (empty
// result); what each test asserts is that ec.Query was reached at all,
// proving the eligibility gate admitted the request.
func dispatchSchemaFixture(t *testing.T) (*databasev1.Measure, logical.Schema, *commonv1.Metadata, *fakeEC) {
	t.Helper()
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	return measureSchema, logicalSchema, &commonv1.Metadata{Name: "demo", Group: "default"}, &fakeEC{}
}

// TestDispatch_RawGroupBy_ReachesEcQuery confirms G9b: GroupBy without
// Agg (raw grouping) is now handled by the vec subsystem rather than
// falling through to the row path.
func TestDispatch_RawGroupBy_ReachesEcQuery(t *testing.T) {
	measureSchema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)

	req := bareReq()
	req.GroupBy = &measurev1.QueryRequest_GroupBy{
		TagProjection: projTagProj(),
		FieldName:     fieldValue,
	}
	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("raw GroupBy must not error before ec.Query: %v", err)
	}
	if !ec.called {
		t.Fatal("raw GroupBy (no Agg) must reach ec.Query post-G9b, not fall through")
	}
	if !handled {
		t.Fatal("raw GroupBy reached ec.Query (empty result) — dispatch must report handled=true")
	}
}

// TestDispatch_ScalarReduce_ReachesEcQuery confirms G9b: Agg without
// GroupBy (scalar reduce) is now handled by the vec subsystem.
func TestDispatch_ScalarReduce_ReachesEcQuery(t *testing.T) {
	measureSchema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)

	req := bareReq()
	req.Agg = &measurev1.QueryRequest_Aggregation{
		Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		FieldName: fieldValue,
	}
	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("scalar reduce must not error before ec.Query: %v", err)
	}
	if !ec.called {
		t.Fatal("scalar reduce (Agg, no GroupBy) must reach ec.Query post-G9b, not fall through")
	}
	if !handled {
		t.Fatal("scalar reduce reached ec.Query (empty result) — dispatch must report handled=true")
	}
}

// TestDispatch_GroupByAggUncoveredProjection_ReachesEcQuery confirms
// G9b's projection auto-coverage: when GroupBy keys or the Agg field are
// absent from the request's projection, plan.Analyze extends the
// projection so the request is admitted instead of falling through.
func TestDispatch_GroupByAggUncoveredProjection_ReachesEcQuery(t *testing.T) {
	cases := []struct {
		mutate func(*measurev1.QueryRequest)
		name   string
	}{
		{
			name: "groupby_tag_not_in_projection",
			mutate: func(req *measurev1.QueryRequest) {
				// GroupBy references region but TagProjection only carries svc.
				req.GroupBy = &measurev1.QueryRequest_GroupBy{
					TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
						{Name: "default", Tags: []string{"region"}},
					}},
					FieldName: fieldValue,
				}
				req.Agg = &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: fieldValue,
				}
			},
		},
		{
			name: "agg_field_not_in_projection",
			mutate: func(req *measurev1.QueryRequest) {
				req.GroupBy = &measurev1.QueryRequest_GroupBy{
					TagProjection: projTagProj(),
					FieldName:     fieldValue,
				}
				req.Agg = &measurev1.QueryRequest_Aggregation{
					Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
					FieldName: fieldValue,
				}
				// Strip the value field from FieldProjection; G9b auto-coverage re-adds it.
				req.FieldProjection = nil
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			measureSchema, logicalSchema, metadata, ec := dispatchSchemaFixture(t)
			req := bareReq()
			c.mutate(req)
			_, _, handled, err := Dispatch(context.Background(),
				req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
			if err == nil && !handled {
				t.Fatal("auto-covered projection must reach ec.Query (handled=true), not fall through")
			}
			if err != nil {
				t.Fatalf("auto-covered projection must not error: %v", err)
			}
			if !ec.called {
				t.Fatal("uncovered GroupBy/Agg projection must reach ec.Query post-G9b (auto-coverage)")
			}
		})
	}
}

// TestDispatch_Top_ReachesEcQuery confirms G9a removed the Top gate:
// req.Top no longer triggers an eligibility fall-through. The request
// proceeds to ec.Query, where the analyzer has emitted
// Scan → Top → Limit. fakeEC returns nil so dispatch falls through after
// ec.Query (the empty-result branch) — what matters is that ec.Query was
// invoked at all, proving the Top gate no longer rejects the request.
func TestDispatch_Top_ReachesEcQuery(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.Top = &measurev1.QueryRequest_Top{
		Number:         5,
		FieldName:      fieldValue,
		FieldValueSort: modelv1.Sort_SORT_DESC,
	}

	iter, planStr, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("Top request must not error before ec.Query: %v", err)
	}
	if !ec.called {
		t.Fatalf("Top request must reach ec.Query (G9a removed the Top gate); "+
			"got iter=%v planStr=%q handled=%v", iter, planStr, handled)
	}
}

// TestDispatch_OrderBy_ReachesEcQuery confirms dispatch resolves
// req.OrderBy via logical.ParseOrderBy and threads it into
// MeasureQueryOptions.Order, instead of falling through to the row
// path. fakeEC returns nil so dispatch falls through after ec.Query
// — what matters is that ec.Query was reached at all, proving the
// OrderBy gate no longer rejects the request.
func TestDispatch_OrderBy_ReachesEcQuery(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.OrderBy = &modelv1.QueryOrder{
		Sort: modelv1.Sort_SORT_DESC,
	}

	iter, planStr, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("OrderBy must not error before ec.Query: %v", err)
	}
	if !ec.called {
		t.Fatalf("OrderBy must reach ec.Query (no longer falls through); "+
			"got iter=%v planStr=%q handled=%v", iter, planStr, handled)
	}
}

// TestDispatch_OrderBy_UnknownIndexRule_BubblesUpError covers the
// error branch dispatch added when threading OrderBy through
// logical.ParseOrderBy: an unknown index rule name must surface as a
// dispatch error with handled=true so the caller does not silently
// retry the row path (which would produce the same canonical error).
func TestDispatch_OrderBy_UnknownIndexRule_BubblesUpError(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.OrderBy = &modelv1.QueryOrder{
		IndexRuleName: "no_such_index_rule",
		Sort:          modelv1.Sort_SORT_ASC,
	}

	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err == nil {
		t.Fatal("unknown OrderBy index rule must surface as a dispatch error")
	}
	if !handled {
		t.Fatal("unknown OrderBy index rule must report handled=true so caller does not re-try row path")
	}
	if ec.called {
		t.Fatal("unknown OrderBy index rule must error before ec.Query is invoked")
	}
}

// TestDispatch_UnknownTagProjection_SurfacesCanonicalError covers G9c
// #11: the row path rejects unknown tags via ValidateProjectionTags with
// errors.Wrap(ErrTagNotDefined, tagName). Dispatch reproduces that exact
// message and returns handled=true so the caller surfaces it (the
// WantErr=true fixtures depend on it) instead of borrowing the row path.
func TestDispatch_UnknownTagProjection_SurfacesCanonicalError(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: "default", Tags: []string{"ghost"}},
	}}
	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err == nil {
		t.Fatal("unknown tag in projection must surface the canonical row-path error")
	}
	// Byte-identical to logical.CommonSchema.ValidateProjectionTags
	// (pkg/query/logical/schema.go:175): errors.Wrap(ErrTagNotDefined, "ghost").
	const wantMsg = "ghost: tag is not defined"
	if err.Error() != wantMsg {
		t.Fatalf("error message parity: want %q, got %q", wantMsg, err.Error())
	}
	if !handled {
		t.Fatal("projection error must report handled=true so caller surfaces it (no row-path retry)")
	}
	if ec.called {
		t.Fatal("ec.Query must not be invoked when projection is invalid")
	}
}

// TestDispatch_UnknownFieldProjection_SurfacesCanonicalError is the
// field-side counterpart: byte-identical to
// measure.schema.ValidateProjectionFields (measure/schema.go:77).
func TestDispatch_UnknownFieldProjection_SurfacesCanonicalError(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{"ghost"}}
	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err == nil {
		t.Fatal("unknown field in projection must surface the canonical row-path error")
	}
	const wantMsg = "field ghost not found in schema"
	if err.Error() != wantMsg {
		t.Fatalf("error message parity: want %q, got %q", wantMsg, err.Error())
	}
	if !handled {
		t.Fatal("projection error must report handled=true so caller surfaces it (no row-path retry)")
	}
	if ec.called {
		t.Fatal("ec.Query must not be invoked when projection is invalid")
	}
}

// TestDispatch_TagValidatedBeforeField mirrors the row path's ordering
// (measure_analyzer.go validates tags before fields): when both
// projections are unknown, the tag error wins.
func TestDispatch_TagValidatedBeforeField(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{}

	req := bareReq()
	req.TagProjection = &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
		{Name: "default", Tags: []string{"ghost"}},
	}}
	req.FieldProjection = &measurev1.QueryRequest_FieldProjection{Names: []string{"phantom"}}
	_, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err == nil || err.Error() != "ghost: tag is not defined" {
		t.Fatalf("tag error must take precedence over field error; got %v", err)
	}
	if !handled {
		t.Fatal("projection parity error must report handled=true (caller must not retry row path)")
	}
}

// TestDispatch_NoTimeRange_EmptyResultParity covers G9c #9: a nil
// TimeRange is NOT rejected by the row path. parseFields feeds
// criteria.GetTimeRange().GetBegin().AsTime() (nil → Unix epoch) into the
// scan, the query runs over [epoch, epoch], and the client observes an
// empty response. Dispatch reproduces that exact behavior directly:
// ec.Query is invoked (over the epoch window) and the canonical empty
// MIterator is emitted with handled=true.
func TestDispatch_NoTimeRange_EmptyResultParity(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.TimeRange = nil
	iter, _, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("nil TimeRange must not error (row path does not reject it): %v", err)
	}
	if !handled {
		t.Fatal("nil TimeRange must be handled (row path produces an empty result, not a fall-through)")
	}
	if !ec.called {
		t.Fatal("ec.Query must be invoked over the epoch window (row-path parity)")
	}
	if iter == nil {
		t.Fatal("expected an empty MIterator, got nil")
	}
	if iter.Next() {
		t.Fatal("empty-result MIterator must report Next()==false")
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatalf("empty-result MIterator Close must be nil (row-path parity): %v", closeErr)
	}
}

// TestDispatch_NilRuntimeContext_FallsThrough covers the defensive guard
// against nil ec / schema / metadata. These should not arise in
// production but a fallthrough is safer than a nil dereference.
func TestDispatch_NilRuntimeContext_FallsThrough(t *testing.T) {
	_, _, handled, err := Dispatch(context.Background(),
		bareReq(), nil, nil, nil, nil, dispatchCfg(true))
	if err != nil {
		t.Fatalf("nil runtime ctx must not error, got %v", err)
	}
	if handled {
		t.Fatal("nil runtime ctx must fall through")
	}
}

// fakeEC is a stub MeasureExecutionContext that records its Query call
// and returns a configured (result, error) pair.
type fakeEC struct {
	wantResult model.MeasureQueryResult
	wantErr    error
	lastOpts   model.MeasureQueryOptions
	called     bool
}

func (f *fakeEC) Query(_ context.Context, opts model.MeasureQueryOptions) (model.MeasureQueryResult, error) {
	f.called = true
	f.lastOpts = opts
	return f.wantResult, f.wantErr
}

// TestDispatch_EmptyResult_CanonicalEmptyIterator covers G9c #13: an
// eligible request reaches ec.Query, ec returns (nil, nil) (the row
// path's typed-nil empty result). Dispatch emits the canonical empty
// MIterator (Next()==false, Close()==nil) with handled=true — the same
// empty []*measurev1.InternalDataPoint the row iterator
// (resultMIterator{result: nil}) would surface. This also confirms the
// index.Query construction and Analyze invocation complete without error
// against a real logical.Schema.
func TestDispatch_EmptyResult_CanonicalEmptyIterator(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	iter, _, handled, err := Dispatch(context.Background(),
		bareReq(), metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("dispatch must not error on empty result: %v", err)
	}
	if !handled {
		t.Fatal("empty result must be handled (canonical empty response, not a fall-through)")
	}
	if iter == nil {
		t.Fatal("expected an empty MIterator, got nil")
	}
	if iter.Next() {
		t.Fatal("empty-result MIterator must report Next()==false")
	}
	if closeErr := iter.Close(); closeErr != nil {
		t.Fatalf("empty-result MIterator Close must be nil (row-path parity): %v", closeErr)
	}
	if !ec.called {
		t.Fatal("ec.Query must be invoked before the empty-result decision")
	}
	if ec.lastOpts.Name != "demo" {
		t.Fatalf("opts.Name: want demo, got %q", ec.lastOpts.Name)
	}
	if ec.lastOpts.TimeRange == nil {
		t.Fatal("opts.TimeRange must be set from req.TimeRange")
	}
}

// TestDispatch_Counters_TrackFellThroughCalls confirms the
// FellThroughCount counter increments on every non-error fallthrough.
// HandledCount must not move when dispatch declines. This is the unit-
// level half of the G8e parity-gate observability — integration runs
// assert HandledCount > 0 after replaying the measure/topn cases.
func TestDispatch_Counters_TrackFellThroughCalls(t *testing.T) {
	startHandled := HandledCount()
	startFellThrough := FellThroughCount()

	// Post-G9 the Top / GroupBy / nil-TimeRange shapes are all handled by
	// the vec subsystem, so these requests fall through only via the
	// PERMANENT nil-runtime-context guard (Dispatch is called with all-nil
	// schema/ec/metadata in the loop below). The counter is still
	// exercised on the clean (non-error) fall-through path.
	gbReq := bareReq()
	gbReq.GroupBy = &measurev1.QueryRequest_GroupBy{TagProjection: projTagProj(), FieldName: fieldValue}
	noTimeReq := bareReq()
	noTimeReq.TimeRange = nil

	for _, req := range []*measurev1.QueryRequest{gbReq, noTimeReq, bareReq() /* nil ec */} {
		_, _, handled, dispatchErr := Dispatch(context.Background(),
			req, nil, nil, nil, nil, dispatchCfg(true))
		if dispatchErr != nil {
			t.Fatalf("fallthrough must not error: %v", dispatchErr)
		}
		if handled {
			t.Fatal("test expected fallthrough; got handled=true")
		}
	}

	if got := HandledCount() - startHandled; got != 0 {
		t.Fatalf("HandledCount delta: want 0, got %d", got)
	}
	if got := FellThroughCount() - startFellThrough; got != 3 {
		t.Fatalf("FellThroughCount delta: want 3, got %d", got)
	}
}

// TestDispatch_GroupByAggCovered_ReachesEcQuery confirms the dispatch
// gate admits GroupBy+Agg requests whose projection covers both the
// GroupBy keys and the Agg field. fakeEC returns nil so dispatch falls
// through after ec.Query (matching the empty-result branch) — what
// matters is that ec.Query was invoked at all, proving the eligibility
// gate let the request through.
func TestDispatch_GroupByAggCovered_ReachesEcQuery(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	req := bareReq()
	req.GroupBy = &measurev1.QueryRequest_GroupBy{
		TagProjection: projTagProj(),
		FieldName:     fieldValue,
	}
	req.Agg = &measurev1.QueryRequest_Aggregation{
		Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		FieldName: fieldValue,
	}

	iter, planStr, handled, err := Dispatch(context.Background(),
		req, metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("covered GroupBy+Agg must not error before ec.Query: %v", err)
	}
	if !ec.called {
		// Surface the other return values for debugging when the gate
		// regression resurfaces — they're all zero-valued today because
		// ec.Query returned (nil, nil) and dispatch fell through.
		t.Fatalf("covered GroupBy+Agg must reach ec.Query (dispatch gate); "+
			"got iter=%v planStr=%q handled=%v", iter, planStr, handled)
	}
}

// TestAugmentRequestWithHiddenTags_AppendsFamiliesAfterVisible covers the
// G9d projection-extension mechanism: hidden criteria tags (grouped by
// family) are appended AFTER the visible projection so the visible tags
// keep their projected order, the caller's request is never mutated, and
// an empty extras slice returns the request unchanged (no clone).
func TestAugmentRequestWithHiddenTags_AppendsFamiliesAfterVisible(t *testing.T) {
	req := bareReq() // TagProjection: default=[svc]
	extras := [][]*logical.Tag{
		{logical.NewTag("default", "region")},
		{logical.NewTag("extra", "zone")},
	}
	got := augmentRequestWithHiddenTags(req, extras)
	if got == req {
		t.Fatal("augment must return a clone when extras are present")
	}
	if len(req.GetTagProjection().GetTagFamilies()) != 1 ||
		req.GetTagProjection().GetTagFamilies()[0].GetTags()[0] != tagSvc {
		t.Fatalf("caller's req.TagProjection must be untouched, got %+v", req.GetTagProjection())
	}
	fams := got.GetTagProjection().GetTagFamilies()
	if len(fams) != 3 {
		t.Fatalf("want 3 families (1 visible + 2 hidden), got %d: %+v", len(fams), fams)
	}
	if fams[0].GetName() != "default" || fams[0].GetTags()[0] != tagSvc {
		t.Fatalf("visible family must stay first, got %+v", fams[0])
	}
	if fams[1].GetName() != "default" || fams[1].GetTags()[0] != "region" {
		t.Fatalf("first hidden family wrong, got %+v", fams[1])
	}
	if fams[2].GetName() != "extra" || fams[2].GetTags()[0] != "zone" {
		t.Fatalf("second hidden family wrong, got %+v", fams[2])
	}
	if same := augmentRequestWithHiddenTags(req, nil); same != req {
		t.Fatal("augment with no extras must return the original request")
	}
}

// TestHiddenTagsMIterator_StripsHiddenTagsFromCurrent proves the egress
// wrapper removes exactly the hidden tags from each Current() DataPoint,
// leaving visible tags untouched — the same contract as the row path's
// hiddenTagsMIterator, which is what keeps the wire bytes identical.
func TestHiddenTagsMIterator_StripsHiddenTagsFromCurrent(t *testing.T) {
	build := func() []*measurev1.InternalDataPoint {
		return []*measurev1.InternalDataPoint{{
			DataPoint: &measurev1.DataPoint{
				TagFamilies: []*modelv1.TagFamily{{
					Name: "default",
					Tags: []*modelv1.Tag{
						{Key: tagSvc, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "a"}}}},
						{Key: "region", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "us"}}}},
					},
				}},
			},
		}}
	}
	hidden := logical.NewHiddenTagSet()
	hidden.Add("region")
	it := &hiddenTagsMIterator{inner: &stubMIterator{rows: build()}, hiddenTags: hidden}
	if !it.Next() {
		t.Fatal("Next must advance once")
	}
	cur := it.Current()
	if len(cur) != 1 || len(cur[0].DataPoint.TagFamilies) != 1 {
		t.Fatalf("expected one family after strip, got %+v", cur)
	}
	tags := cur[0].DataPoint.TagFamilies[0].Tags
	if len(tags) != 1 || tags[0].Key != tagSvc {
		t.Fatalf("only visible tag 'svc' must remain, got %+v", tags)
	}
	if it.Close() != nil {
		t.Fatal("Close must propagate inner Close (nil)")
	}
}

// stubMIterator is a one-shot executor.MIterator over a fixed row slice.
type stubMIterator struct {
	rows []*measurev1.InternalDataPoint
	pos  int
}

func (s *stubMIterator) Next() bool {
	s.pos++
	return s.pos <= len(s.rows)
}

func (s *stubMIterator) Current() []*measurev1.InternalDataPoint {
	if s.pos < 1 || s.pos > len(s.rows) {
		return nil
	}
	return s.rows[s.pos-1 : s.pos]
}

func (s *stubMIterator) Close() error { return nil }

// TestDispatch_QueryError_BubblesUp covers the error propagation when
// the storage query itself fails. Dispatch must report (nil, "", true,
// err) so the caller surfaces the error rather than re-trying the row
// path.
func TestDispatch_QueryError_BubblesUp(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	wantErr := context.DeadlineExceeded
	ec := &fakeEC{wantErr: wantErr}

	_, _, handled, err := Dispatch(context.Background(),
		bareReq(), metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err == nil {
		t.Fatal("ec.Query error must surface as a dispatch error")
	}
	if !handled {
		t.Fatal("ec.Query error must report handled=true so caller does not re-try row path")
	}
}
