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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	logical_stream "github.com/apache/skywalking-banyandb/pkg/query/logical/stream"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

// The apache/skywalking#14067 oracle, hand-calculated in the issue body and NOT
// derived from the removed row engine:
//
//	ID=A, order=1, state=closed, service=old
//	ID=A, order=2, state=open,   service=new
//	ID=B, order=3, state=open,   service=other
//
// criteria state=open, ascending index order on `sequence`, projection `service`
// ⇒ A/new then B/other, and neither `state` nor `sequence` may reach the client.
const (
	oracleFamily      = "searchable"
	oracleClientTag   = "service"
	oracleCriteriaTag = "state"
	// oracleOrderTag is the ordering tag the client never projects, so the scan
	// adds it to its own request to build the OrderKey — the second hidden-tag
	// source, and the one HidesOrderTag() reduces to a bool.
	oracleOrderTag  = "sequence"
	oracleIndexRule = "by-sequence"
	oracleStateWant = "open"
	oracleStateFail = "closed"
	// oracleElementA repeats across two rows, so the distinct stage must pick a
	// winner. #1331 fixed that contract as filter-first: the closed row loses.
	oracleElementA = 1
	oracleElementB = 2
)

// oracleVecSource replays one pre-built batch as an executor.StreamVecScanSource,
// so ExecuteVectorized runs without a data node. Copied from the #14056 harness in
// pkg/query/logical/stream/stream_plan_topn_pushdown_test.go, which is unexported.
type oracleVecSource struct {
	schema  *vectorized.BatchSchema
	batches []*vectorized.RecordBatch
	pos     int
}

func (s *oracleVecSource) Schema() *vectorized.BatchSchema { return s.schema }

func (s *oracleVecSource) NextBatch(context.Context) (*vectorized.RecordBatch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	batch := s.batches[s.pos]
	s.pos++
	return batch, nil
}

func (s *oracleVecSource) Release() {}

// oracleExecContext is the two-method StreamExecutionContext the analyzer stores
// on the scan. It ignores the query options: the replayed corpus IS the scan result.
type oracleExecContext struct {
	src *oracleVecSource
}

func (e *oracleExecContext) QueryVectorized(context.Context, model.StreamQueryOptions) (executor.StreamVecScanSource, error) {
	return e.src, nil
}

func (e *oracleExecContext) VectorizedConfig() vstream.VectorizedConfig {
	return vstream.DefaultConfig()
}

// oracleStreamSchema declares all three tags. Only `service` is projected by the
// request, so the analyzer appends `state` as a criteria-only hidden tag and the
// scan appends `sequence` for the OrderKey.
func oracleStreamSchema() *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: "oracle", Group: "test"},
		Entity:   &databasev1.Entity{TagNames: []string{oracleClientTag}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: oracleFamily,
			Tags: []*databasev1.TagSpec{
				{Name: oracleClientTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: oracleCriteriaTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: oracleOrderTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
}

func oracleIndexRules() []*databasev1.IndexRule {
	return []*databasev1.IndexRule{{
		Metadata: &commonv1.Metadata{Name: oracleIndexRule},
		Tags:     []string{oracleOrderTag},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	}}
}

func oracleStr(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

// oracleBatchSchema is the schema the scan executes against: the given tag columns
// plus the synthetic OrderKey column an index-order scan always carries.
//
// The pipeline validates batch.Schema by POINTER identity, so every caller must
// build this ONCE and hand the same pointer to the source and to its batches.
func oracleBatchSchema(tagNames []string) *vectorized.BatchSchema {
	return vstream.BuildStreamBatchSchema(
		[]model.TagProjection{{Family: oracleFamily, Names: tagNames}},
		oracleFamily, oracleOrderTag,
	)
}

// oracleRow is one line of the issue's oracle table.
type oracleRow struct {
	sequence string
	state    string
	service  string
	elemID   uint64
}

// cell returns the row's value for a tag name, so one corpus builder serves every
// projection shape the arms need.
func (r oracleRow) cell(tagName string) string {
	switch tagName {
	case oracleClientTag:
		return r.service
	case oracleCriteriaTag:
		return r.state
	case oracleOrderTag:
		return r.sequence
	default:
		panic("oracleRow: unknown tag " + tagName)
	}
}

func oracleRows() []oracleRow {
	return []oracleRow{
		{elemID: oracleElementA, sequence: "001", state: oracleStateFail, service: "old"},
		{elemID: oracleElementA, sequence: "002", state: oracleStateWant, service: "new"},
		{elemID: oracleElementB, sequence: "003", state: oracleStateWant, service: "other"},
	}
}

// oracleCorpusBatch materializes oracleRows() in ascending OrderKey order. The keys
// are zero-padded so lexicographic byte order equals the numeric sequence order.
func oracleCorpusBatch(schema *vectorized.BatchSchema, tagNames []string) *vectorized.RecordBatch {
	rows := oracleRows()
	batch := vectorized.NewRecordBatch(schema, len(rows))
	tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	keyCol := batch.Columns[schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte])
	tagCols := make([]*vectorized.TypedColumn[*modelv1.TagValue], 0, len(tagNames))
	for _, name := range tagNames {
		tagIdx, ok := schema.TagIndex(oracleFamily, name)
		if !ok {
			panic("oracleCorpusBatch: tag " + name + " absent from the batch schema")
		}
		tagCols = append(tagCols, batch.Columns[tagIdx].(*vectorized.TypedColumn[*modelv1.TagValue]))
	}
	for rowIdx, row := range rows {
		tsCol.Append(int64(rowIdx + 1))
		elemCol.Append(vstream.ElementIDToColumn(row.elemID))
		seriesCol.Append(vstream.SeriesIDToColumn(1))
		keyCol.Append([]byte(row.sequence))
		for colIdx, name := range tagNames {
			tagCols[colIdx].Append(oracleStr(row.cell(name)))
		}
		batch.Len++
	}
	return batch
}

// oracleRequest builds the index-ordered request. clientTags is the client
// projection and withCriteria adds the state=open condition.
//
// The oracle arm projects `service` alone, so BOTH gate terms block it: a criteria
// sets hasFilter, and an unprojected ordering tag sets HidesOrderTag. The control
// arm projects `service` and `sequence` with no criteria, which clears both terms
// and emits a frame on origin/main already.
func oracleRequest(clientTags []string, withCriteria bool) *streamv1.QueryRequest {
	req := &streamv1.QueryRequest{
		Name:   "oracle",
		Groups: []string{"test"},
		TimeRange: &modelv1.TimeRange{
			Begin: &timestamppb.Timestamp{Seconds: 0},
			End:   &timestamppb.Timestamp{Seconds: 1 << 20},
		},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{
			Name: oracleFamily,
			Tags: clientTags,
		}}},
		OrderBy: &modelv1.QueryOrder{IndexRuleName: oracleIndexRule, Sort: modelv1.Sort_SORT_ASC},
		Limit:   10,
	}
	if withCriteria {
		req.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
			Name:  oracleCriteriaTag,
			Op:    modelv1.Condition_BINARY_OP_EQ,
			Value: oracleStr(oracleStateWant),
		}}}
	}
	return req
}

// newOracleProcessor returns a data-node stream processor with the raw wire mode
// on and tracing off — the only combination streamVecEmitAsFrame admits.
func newOracleProcessor(t *testing.T) *streamQueryProcessor {
	t.Helper()
	prev := data.StreamWireModeRaw()
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(prev) })
	return &streamQueryProcessor{
		distributed:  true,
		queryService: &queryService{log: logger.GetLogger("test-oracle"), nodeID: "test-node"},
	}
}

// newOraclePlan runs the production analyzer over the request, so the
// *limit → *tagFilterPlan → *localIndexScan shape and the hasFilter decision are
// the real ones rather than a struct literal written to match.
// executedTags must list the tag columns the ANALYZED scan requests, which is the
// client projection plus any criteria-only tag plus the ordering tag.
func newOraclePlan(t *testing.T, req *streamv1.QueryRequest, executedTags []string) logical.Plan {
	t.Helper()
	sch, err := logical_stream.BuildSchema(oracleStreamSchema(), oracleIndexRules())
	require.NoError(t, err)
	batchSchema := oracleBatchSchema(executedTags)
	ec := &oracleExecContext{src: &oracleVecSource{
		schema:  batchSchema,
		batches: []*vectorized.RecordBatch{oracleCorpusBatch(batchSchema, executedTags)},
	}}
	plan, err := logical_stream.Analyze(req, []*commonv1.Metadata{oracleStreamSchema().GetMetadata()},
		[]logical.Schema{sch}, []executor.StreamExecutionContext{ec})
	require.NoError(t, err)
	return plan
}

// TestStreamVecDispatch_FilteredQueryEmitsFrame is the apache/skywalking#14067 R4
// acceptance assertion, and it FAILS on origin/main by construction. The gate at
// processor.go:244 reads `!hasFilter && !vecExec.HidesOrderTag() && ...`, and the
// analyzer sets a *tagFilterPlan for every surviving criteria, so a filtered query
// can never reach the frame emit today. Both leading terms must go.
//
// Boundary: the scan source is a replayed batch, not a real part. Real-part
// correctness is already covered by banyand/stream/query_vectorized_parity_test.go,
// so this test deliberately does not re-cover it. What it does cover is the
// processor's egress decision and the columnar hidden-tag strip.
func TestStreamVecDispatch_FilteredQueryEmitsFrame(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, true)
	// The analyzer appends `state` for the criteria, and the scan appends `sequence`
	// for the OrderKey, so the executed schema carries all three tag columns.
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag})

	handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
	require.True(t, handled, "the filtered index-order shape must be vec-eligible")

	body, ok := resp.Data().([]byte)
	require.True(t, ok, "a filtered query emitted %T, not a columnar frame body", resp.Data())

	batch, err := streamframe.Decode(body)
	require.NoError(t, err)
	require.Equal(t, 2, batch.ActiveLen(), "the oracle keeps A/new and B/other")

	// R3: neither the criteria-only tag nor the ordering tag may reach the client.
	_, hasState := batch.Schema.TagIndex(oracleFamily, oracleCriteriaTag)
	require.False(t, hasState, "the criteria-only tag %q leaked into the frame", oracleCriteriaTag)
	_, hasSequence := batch.Schema.TagIndex(oracleFamily, oracleOrderTag)
	require.False(t, hasSequence, "the hidden ordering tag %q leaked into the frame", oracleOrderTag)

	// R3: the internal ordering key survives, because the liaison still merges on it.
	require.NotEqual(t, -1, batch.Schema.OrderKeyIndex(), "the OrderKey column must survive the strip")

	serviceIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
	require.True(t, ok, "the projected tag %q must survive the strip", oracleClientTag)
	serviceData := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
	idData := batch.Columns[batch.Schema.ElementIDIndex()].(*vectorized.TypedColumn[int64]).Data()

	var gotServices []string
	var gotIDs []uint64
	for _, rowIdx := range oracleActiveRows(batch) {
		gotServices = append(gotServices, serviceData[rowIdx].GetStr().GetValue())
		gotIDs = append(gotIDs, vstream.ColumnToElementID(idData[rowIdx]))
	}
	// Filter-first duplicate winner: element A resolves to the `open` row, not the
	// `closed` row that sorts ahead of it.
	require.Equal(t, []string{"new", "other"}, gotServices)
	require.Equal(t, []uint64{oracleElementA, oracleElementB}, gotIDs)
}

// TestStreamVecDispatch_UnfilteredQueryEmitsFrame is the control arm. It passes on
// origin/main, so a failure here means the harness broke, not the gate.
func TestStreamVecDispatch_UnfilteredQueryEmitsFrame(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag, oracleOrderTag}, false)
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleOrderTag})

	handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
	require.True(t, handled)
	_, ok := resp.Data().([]byte)
	require.True(t, ok, "an unfiltered query emitted %T, not a columnar frame body", resp.Data())
}

// TestStreamVecDispatch_HiddenOrderTagEmitsFrame is the apache/skywalking#14067 R3
// assertion for the SECOND hidden-tag source, taken on its own: an UNFILTERED
// index-order query whose sort tag the client never projects.
//
// Before the strip landed, the gate sent this shape to the proto egress, because a
// frame rebuilt from the batch schema would have carried the sort tag. Now
// mergeStreamBatches drops that column by name, so the frame is legal. No criteria
// is involved, which is what isolates the ordering tag from the filter work.
func TestStreamVecDispatch_HiddenOrderTagEmitsFrame(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, false)
	// No criteria, so the analyzer appends nothing; the scan still appends `sequence`
	// for the OrderKey.
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleOrderTag})

	handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
	require.True(t, handled)
	body, ok := resp.Data().([]byte)
	require.True(t, ok, "a hidden-order-tag query emitted %T, not a columnar frame body", resp.Data())

	batch, err := streamframe.Decode(body)
	require.NoError(t, err)
	// Three rows in, two out: the distinct stage keeps one row for each ElementID, and
	// element A repeats. With no criteria the first row in ascending order wins, so A
	// resolves to `old` — the opposite winner from the filtered arm, which is exactly
	// what the filter-first contract means.
	require.Equal(t, 2, batch.ActiveLen(), "distinct keeps one row for each ElementID")

	_, hasSequence := batch.Schema.TagIndex(oracleFamily, oracleOrderTag)
	require.False(t, hasSequence, "the hidden ordering tag %q leaked into the frame", oracleOrderTag)
	_, hasService := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
	require.True(t, hasService, "the projected tag %q must survive the strip", oracleClientTag)
	require.NotEqual(t, -1, batch.Schema.OrderKeyIndex(), "the OrderKey column must survive the strip")

	serviceIdx, _ := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
	serviceData := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
	var gotServices []string
	for _, rowIdx := range oracleActiveRows(batch) {
		gotServices = append(gotServices, serviceData[rowIdx].GetStr().GetValue())
	}
	require.Equal(t, []string{"old", "other"}, gotServices)
}

// oracleActiveRows returns the batch's active row indices, honoring Selection.
func oracleActiveRows(batch *vectorized.RecordBatch) []int {
	if batch.Selection == nil {
		rows := make([]int, 0, batch.Len)
		for rowIdx := 0; rowIdx < batch.Len; rowIdx++ {
			rows = append(rows, rowIdx)
		}
		return rows
	}
	rows := make([]int, 0, len(batch.Selection))
	for _, sel := range batch.Selection {
		rows = append(rows, int(sel))
	}
	return rows
}
