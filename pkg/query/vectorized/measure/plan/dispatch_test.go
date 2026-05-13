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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
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

// TestDispatch_GroupBy_FallsThrough covers the column-bridging gate.
func TestDispatch_GroupBy_FallsThrough(t *testing.T) {
	req := bareReq()
	req.GroupBy = &measurev1.QueryRequest_GroupBy{
		TagProjection: projTagProj(),
		FieldName:     "value",
	}
	req.Agg = &measurev1.QueryRequest_Aggregation{
		Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		FieldName: "value",
	}
	_, _, handled, err := Dispatch(context.Background(),
		req, nil, nil, nil, nil, dispatchCfg(true))
	if err != nil {
		t.Fatalf("GroupBy fallthrough must not error: %v", err)
	}
	if handled {
		t.Fatal("GroupBy+Agg must fall through to row path in G8d.1")
	}
}

// TestDispatch_Agg_FallsThrough covers Agg-without-GroupBy (which the
// analyzer would reject, but the dispatch gate fires before Analyze).
func TestDispatch_Agg_FallsThrough(t *testing.T) {
	req := bareReq()
	req.Agg = &measurev1.QueryRequest_Aggregation{
		Function:  modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM,
		FieldName: "value",
	}
	_, _, handled, err := Dispatch(context.Background(),
		req, nil, nil, nil, nil, dispatchCfg(true))
	if err != nil {
		t.Fatalf("Agg fallthrough must not error: %v", err)
	}
	if handled {
		t.Fatal("Agg must fall through to row path in G8d.1")
	}
}

// TestDispatch_Top_FallsThrough covers the per-timestamp top-N gap.
func TestDispatch_Top_FallsThrough(t *testing.T) {
	req := bareReq()
	req.Top = &measurev1.QueryRequest_Top{
		Number:         5,
		FieldName:      "value",
		FieldValueSort: modelv1.Sort_SORT_DESC,
	}
	_, _, handled, err := Dispatch(context.Background(),
		req, nil, nil, nil, nil, dispatchCfg(true))
	if err != nil {
		t.Fatalf("Top fallthrough must not error: %v", err)
	}
	if handled {
		t.Fatal("Top must fall through (BatchTop semantics differ from row TopN)")
	}
}

// TestDispatch_NoTimeRange_FallsThrough covers the bounded-window
// requirement.
func TestDispatch_NoTimeRange_FallsThrough(t *testing.T) {
	req := bareReq()
	req.TimeRange = nil
	_, _, handled, err := Dispatch(context.Background(),
		req, nil, nil, nil, nil, dispatchCfg(true))
	if err != nil {
		t.Fatalf("no-TimeRange fallthrough must not error: %v", err)
	}
	if handled {
		t.Fatal("missing TimeRange must fall through")
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

// TestDispatch_EmptyResult_FallsThrough exercises the full eligibility
// path: an eligible request reaches ec.Query, ec returns (nil, nil)
// (empty range), Dispatch reports fallthrough so the row path can surface
// the empty response. This also confirms the index.Query construction
// and Analyze invocation complete without error against a real
// logical.Schema.
func TestDispatch_EmptyResult_FallsThrough(t *testing.T) {
	measureSchema := testMeasureSchema()
	// nolint:staticcheck // SA1019 — row-path BuildSchema is the only schema builder until G8 replaces it.
	logicalSchema, schemaErr := logicalmeasure.BuildSchema(measureSchema, nil)
	if schemaErr != nil {
		t.Fatalf("BuildSchema: %v", schemaErr)
	}
	metadata := &commonv1.Metadata{Name: "demo", Group: "default"}
	ec := &fakeEC{wantResult: nil, wantErr: nil}

	iter, planStr, handled, err := Dispatch(context.Background(),
		bareReq(), metadata, measureSchema, logicalSchema, ec, dispatchCfg(true))
	if err != nil {
		t.Fatalf("dispatch must not error on empty result: %v", err)
	}
	if handled {
		t.Fatal("empty result must fall through to row path")
	}
	if iter != nil || planStr != "" {
		t.Fatalf("expect zero outputs on fallthrough, got iter=%v planStr=%q", iter, planStr)
	}
	if !ec.called {
		t.Fatal("ec.Query must be invoked before fallthrough decision")
	}
	if ec.lastOpts.Name != "demo" {
		t.Fatalf("opts.Name: want demo, got %q", ec.lastOpts.Name)
	}
	if ec.lastOpts.TimeRange == nil {
		t.Fatal("opts.TimeRange must be set from req.TimeRange")
	}
}

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
