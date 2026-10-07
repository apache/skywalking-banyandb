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

	"github.com/apache/skywalking-banyandb/api/common"
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
// orderTag is "" for a timestamp-order scan, which carries no OrderKey column.
func oracleBatchSchema(tagNames []string, orderTag string) *vectorized.BatchSchema {
	family := oracleFamily
	if orderTag == "" {
		family = ""
	}
	return vstream.BuildStreamBatchSchema(
		[]model.TagProjection{{Family: oracleFamily, Names: tagNames}},
		family, orderTag,
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
	return oracleBatchFromRows(schema, tagNames, oracleRows())
}

// oracleCorpusBatches splits the oracle rows into batches of rowsPerBatch, so a test
// can prove the egress works across batch boundaries and not only inside one batch.
func oracleCorpusBatches(schema *vectorized.BatchSchema, tagNames []string, rowsPerBatch int) []*vectorized.RecordBatch {
	rows := oracleRows()
	var batches []*vectorized.RecordBatch
	for start := 0; start < len(rows); start += rowsPerBatch {
		end := min(start+rowsPerBatch, len(rows))
		batches = append(batches, oracleBatchFromRows(schema, tagNames, rows[start:end]))
	}
	return batches
}

func oracleBatchFromRows(schema *vectorized.BatchSchema, tagNames []string, rows []oracleRow) *vectorized.RecordBatch {
	batch := vectorized.NewRecordBatch(schema, len(rows))
	tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
	elemCol := batch.Columns[schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
	seriesCol := batch.Columns[schema.SeriesIDIndex()].(*vectorized.TypedColumn[int64])
	var keyCol *vectorized.TypedColumn[[]byte]
	if schema.OrderKeyIndex() >= 0 {
		keyCol = batch.Columns[schema.OrderKeyIndex()].(*vectorized.TypedColumn[[]byte])
	}
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
		if keyCol != nil {
			keyCol.Append([]byte(row.sequence))
		}
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
// indexRule is "" for a timestamp-order request, which takes no pre-merge filter.
func oracleRequest(clientTags []string, withCriteria bool, indexRule string) *streamv1.QueryRequest {
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
		OrderBy: &modelv1.QueryOrder{IndexRuleName: indexRule, Sort: modelv1.Sort_SORT_ASC},
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

// newOracleStandaloneProcessor returns a standalone stream processor. Standalone
// answers the client directly, so it always takes the proto egress — that is the arm
// where the per-element hidden-tag strip still has to happen.
func newOracleStandaloneProcessor(t *testing.T) *streamQueryProcessor {
	t.Helper()
	return &streamQueryProcessor{
		distributed:  false,
		queryService: &queryService{log: logger.GetLogger("test-oracle-standalone"), nodeID: "test-node"},
	}
}

// oracleProtoServices reads the projected tag out of a proto response and asserts no
// element carries a hidden tag.
func oracleProtoServices(t *testing.T, elements []*streamv1.Element) []string {
	t.Helper()
	services := make([]string, 0, len(elements))
	for _, element := range elements {
		var found bool
		for _, family := range element.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				require.NotEqual(t, oracleCriteriaTag, tag.GetKey(),
					"the criteria-only tag survived the per-element strip")
				require.NotEqual(t, oracleOrderTag, tag.GetKey(),
					"the hidden ordering tag reached the client")
				if tag.GetKey() == oracleClientTag {
					services = append(services, tag.GetValue().GetStr().GetValue())
					found = true
				}
			}
		}
		require.True(t, found, "the projected tag %q is missing from an element", oracleClientTag)
	}
	return services
}

// TestStreamVecDispatch_FilteredIndexOrderProtoStillStrips is the
// apache/skywalking#14067 R1 guard. The element-level tagFilter.Match is gone for an
// index-order query, because the scan already ran that same filter on the columns
// ahead of the merge. The hidden-tag STRIP is not redundant, so it stays — and this
// test fails if the R1 change takes it away with the match.
//
// It runs standalone, which forces the proto egress, so the per-element path is the
// one under test. It also pins the oracle's offset/limit answer.
func TestStreamVecDispatch_FilteredIndexOrderProtoStillStrips(t *testing.T) {
	executedTags := []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}

	t.Run("whole window", func(t *testing.T) {
		p := newOracleStandaloneProcessor(t)
		req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
		plan := newOraclePlan(t, req, executedTags, oracleOrderTag)

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled)
		response, ok := resp.Data().(*streamv1.QueryResponse)
		require.True(t, ok, "standalone emitted %T, not a proto response", resp.Data())
		require.Equal(t, []string{"new", "other"}, oracleProtoServices(t, response.GetElements()))
	})

	t.Run("offset 1 limit 1", func(t *testing.T) {
		p := newOracleStandaloneProcessor(t)
		req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
		req.Offset, req.Limit = 1, 1
		plan := newOraclePlan(t, req, executedTags, oracleOrderTag)

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled)
		response, ok := resp.Data().(*streamv1.QueryResponse)
		require.True(t, ok, "standalone emitted %T, not a proto response", resp.Data())
		// The issue's hand-calculated answer for offset 1, limit 1.
		require.Equal(t, []string{"other"}, oracleProtoServices(t, response.GetElements()))
	})
}

// newOraclePlan runs the production analyzer over the request, so the
// *limit → *tagFilterPlan → *localIndexScan shape and the hasFilter decision are
// the real ones rather than a struct literal written to match.
// executedTags must list the tag columns the ANALYZED scan requests, which is the
// client projection plus any criteria-only tag plus the ordering tag.
func newOraclePlan(t *testing.T, req *streamv1.QueryRequest, executedTags []string, orderTag string) logical.Plan {
	t.Helper()
	return newOracleBatchedPlan(t, req, executedTags, orderTag, len(oracleRows()))
}

// newOracleBatchedPlan is newOraclePlan with the corpus split across batches of
// rowsPerBatch rows.
func newOracleBatchedPlan(t *testing.T, req *streamv1.QueryRequest, executedTags []string,
	orderTag string, rowsPerBatch int,
) logical.Plan {
	t.Helper()
	plan, _ := newOraclePlanWithCorpus(t, req, executedTags, orderTag, rowsPerBatch)
	return plan
}

// newOraclePlanWithCorpus also returns the batches the fake source will replay, so a
// test can mutate a cell — marking it null, for instance — before the dispatch runs.
func newOraclePlanWithCorpus(t *testing.T, req *streamv1.QueryRequest, executedTags []string,
	orderTag string, rowsPerBatch int,
) (logical.Plan, []*vectorized.RecordBatch) {
	t.Helper()
	sch, err := logical_stream.BuildSchema(oracleStreamSchema(), oracleIndexRules())
	require.NoError(t, err)
	batchSchema := oracleBatchSchema(executedTags, orderTag)
	corpus := oracleCorpusBatches(batchSchema, executedTags, rowsPerBatch)
	ec := &oracleExecContext{src: &oracleVecSource{schema: batchSchema, batches: corpus}}
	plan, err := logical_stream.Analyze(req, []*commonv1.Metadata{oracleStreamSchema().GetMetadata()},
		[]logical.Schema{sch}, []executor.StreamExecutionContext{ec})
	require.NoError(t, err)
	return plan, corpus
}

// newOracleGroupsPlan analyzes req against one fake execution context for each group,
// each replaying its own single-batch corpus, and returns those batches so an arm can
// mutate a cell before the dispatch runs.
//
// One group gives the single-scan shape the other tests use. TWO groups give the
// *limit → *mergePlan shape, which the analyzer builds only from more than one
// metadata + schema + execution context, so the multi-group dispatch cannot be
// reached any other way. req.Groups must name one group for each row set, which this
// also checks.
//
// The rows are explicit because the issue's three-row table cannot express either
// caller: the groups have to be distinguishable by value, and the null-criteria arm
// needs a second surviving element to compare against.
func newOracleGroupsPlan(t *testing.T, req *streamv1.QueryRequest, executedTags []string,
	orderTag string, groups ...[]oracleRow,
) (logical.Plan, []*vectorized.RecordBatch) {
	t.Helper()
	require.Len(t, req.GetGroups(), len(groups), "the request must name one group for each row set")
	metadata := make([]*commonv1.Metadata, 0, len(groups))
	schemas := make([]logical.Schema, 0, len(groups))
	ecc := make([]executor.StreamExecutionContext, 0, len(groups))
	batches := make([]*vectorized.RecordBatch, 0, len(groups))
	for groupIdx, rows := range groups {
		sch, err := logical_stream.BuildSchema(oracleStreamSchema(), oracleIndexRules())
		require.NoError(t, err)
		// Each group gets its OWN batch schema pointer. The pipeline validates
		// batch.Schema by pointer identity, so one shared pointer would let a
		// cross-group mix-up pass instead of failing on it.
		batchSchema := oracleBatchSchema(executedTags, orderTag)
		batch := oracleBatchFromRows(batchSchema, executedTags, rows)
		md := oracleStreamSchema().GetMetadata()
		md.Group = req.GetGroups()[groupIdx]
		metadata = append(metadata, md)
		schemas = append(schemas, sch)
		ecc = append(ecc, &oracleExecContext{
			src: &oracleVecSource{schema: batchSchema, batches: []*vectorized.RecordBatch{batch}},
		})
		batches = append(batches, batch)
	}
	plan, err := logical_stream.Analyze(req, metadata, schemas, ecc)
	require.NoError(t, err)
	return plan, batches
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
	req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
	// The analyzer appends `state` for the criteria, and the scan appends `sequence`
	// for the OrderKey, so the executed schema carries all three tag columns.
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}, oracleOrderTag)

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
	req := oracleRequest([]string{oracleClientTag, oracleOrderTag}, false, oracleIndexRule)
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleOrderTag}, oracleOrderTag)

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
//
// NO PRODUCTION QUERY REACHES THIS SHAPE TODAY, and the test pins it anyway. The frame
// egress fires only when p.distributed is true, which exactly one production site sets
// (pkg/cmdsetup/data.go:79), so a stream request on that path always arrives from the
// liaison. The liaison's distributedPlan.Execute copies the client projection into its
// query template verbatim, and its analyzer narrows the schema to that projection
// (pkg/query/logical/stream/stream_plan_distributed.go:70) BEFORE it looks the sort tag
// up (:113) — an unprojected sort tag fails there with `tag <name> not found`, so the
// plan never exists. The strip is therefore defense for a shape the liaison rejects
// today, required by apache/skywalking#14067 R3, and apache/skywalking#14070 may make
// it live when it widens frame output to the multi-group and distributed callers. Keep
// the test and every assertion in it.
func TestStreamVecDispatch_HiddenOrderTagEmitsFrame(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, false, oracleIndexRule)
	// No criteria, so the analyzer appends nothing; the scan still appends `sequence`
	// for the OrderKey.
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleOrderTag}, oracleOrderTag)

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

// TestStreamVecDispatch_TimeOrderFilterStaysBehindTheCap is the
// apache/skywalking#14067 R2 guard. A timestamp-order scan caps BEFORE it filters, so
// its filter has to stay after the cap or the merge fills the limit from rows the row
// path never saw. Moving that filter from the elements onto the columns must not
// change the answer, under-fill included.
//
// The oracle here is the PROTO egress, whose element filter this change does not
// touch. Asserting frame == proto therefore pins "R2 changed nothing" directly,
// rather than restating a hand-calculated count that could drift with the fixture.
func TestStreamVecDispatch_TimeOrderFilterStaysBehindTheCap(t *testing.T) {
	// Timestamp order: no index rule, so scanFromInput pushes no filter down and the
	// executed schema carries no OrderKey column.
	executedTags := []string{oracleClientTag, oracleCriteriaTag}

	protoReq := oracleRequest([]string{oracleClientTag}, true, "")
	protoPlan := newOraclePlan(t, protoReq, executedTags, "")
	protoCriteria, hasFilter := logical_stream.VecTagFilter(protoPlan)
	require.True(t, hasFilter)
	require.False(t, protoCriteria.PreMerged,
		"a timestamp-order scan must NOT push the filter down, or this test proves nothing")

	standalone := newOracleStandaloneProcessor(t)
	handled, protoResp := standalone.tryStreamVecDispatch(context.Background(), protoPlan, protoReq, false)
	require.True(t, handled)
	response, ok := protoResp.Data().(*streamv1.QueryResponse)
	require.True(t, ok, "standalone emitted %T, not a proto response", protoResp.Data())
	wantServices := oracleProtoServices(t, response.GetElements())
	// Guard against a vacuous pass: two empty lists compare equal and prove nothing.
	require.NotEmpty(t, wantServices, "the proto oracle returned nothing, so the parity check is empty")
	t.Logf("timestamp-order oracle, from the untouched proto egress: %v", wantServices)

	// Same query on a data node. The columnar filter replaces the element filter.
	dataNode := newOracleProcessor(t)
	frameReq := oracleRequest([]string{oracleClientTag}, true, "")
	framePlan := newOraclePlan(t, frameReq, executedTags, "")
	handled, frameResp := dataNode.tryStreamVecDispatch(context.Background(), framePlan, frameReq, false)
	require.True(t, handled)
	body, ok := frameResp.Data().([]byte)
	require.True(t, ok, "a filtered timestamp-order query emitted %T, not a columnar frame body", frameResp.Data())

	batch, err := streamframe.Decode(body)
	require.NoError(t, err)
	_, hasState := batch.Schema.TagIndex(oracleFamily, oracleCriteriaTag)
	require.False(t, hasState, "the criteria-only tag %q leaked into the frame", oracleCriteriaTag)

	serviceIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
	require.True(t, ok)
	serviceData := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
	var gotServices []string
	for _, rowIdx := range oracleActiveRows(batch) {
		gotServices = append(gotServices, serviceData[rowIdx].GetStr().GetValue())
	}
	require.Equal(t, wantServices, gotServices,
		"the columnar filter changed the timestamp-order answer, so the under-fill moved")
}

// TestStreamVecDispatch_CancelledFilteredQueryFailsClosed is the
// apache/skywalking#14067 batch-ownership box for cancellation. A canceled filtered
// query must surface an error, not a short frame: a frame that silently dropped the
// rows the merge never produced would read as a complete result on the liaison.
//
// Cancellation is honored inside the pipeline, at SortedMerge.NextBatch, which
// returns its in-flight batch to the pool before erroring. So the processor never
// receives that batch, and filterStreamBatches never runs on it.
func TestStreamVecDispatch_CancelledFilteredQueryFailsClosed(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}, oracleOrderTag)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	handled, resp := p.tryStreamVecDispatch(ctx, plan, req, false)
	require.True(t, handled, "a canceled query is still handled, as an error response")

	_, isFrame := resp.Data().([]byte)
	require.False(t, isFrame, "a canceled query emitted a frame, which would look complete")
	_, isProto := resp.Data().(*streamv1.QueryResponse)
	require.False(t, isProto, "a canceled query emitted a result response")

	queryErr, ok := resp.Data().(*common.Error)
	require.True(t, ok, "a canceled query emitted %T, not an error", resp.Data())
	require.Contains(t, queryErr.Error(), "execute the vectorized query plan",
		"the error must name the execution stage the cancellation hit")
}

// TestFilterStreamBatches_ForeignSchemaFailsClosed pins the columnar filter's own
// error path. The operator rejects a batch whose schema is not the one it indexed its
// columns against, and filterStreamBatches must return that error rather than leave a
// half-filtered batch for the encode step.
func TestFilterStreamBatches_ForeignSchemaFailsClosed(t *testing.T) {
	sch, err := logical_stream.BuildSchema(oracleStreamSchema(), oracleIndexRules())
	require.NoError(t, err)
	tagFilter, err := logical.BuildSimpleTagFilter(&modelv1.Criteria{Exp: &modelv1.Criteria_Condition{
		Condition: &modelv1.Condition{
			Name:  oracleCriteriaTag,
			Op:    modelv1.Condition_BINARY_OP_EQ,
			Value: oracleStr(oracleStateWant),
		},
	}})
	require.NoError(t, err)

	executedTags := []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}
	projection := []model.TagProjection{{Family: oracleFamily, Names: executedTags}}
	indexed := oracleBatchSchema(executedTags, oracleOrderTag)
	// A second builder call gives an equal schema with a DIFFERENT pointer, which is
	// exactly the identity mismatch the operator guards against.
	foreign := oracleBatchSchema(executedTags, oracleOrderTag)
	batch := oracleCorpusBatch(foreign, executedTags)
	before := batch.ActiveLen()

	filterErr := filterStreamBatches(context.Background(), indexed, projection,
		logical_stream.VecCriteriaFilter{TagFilter: tagFilter, Schema: sch}, []*vectorized.RecordBatch{batch})
	require.Error(t, filterErr, "a foreign batch schema must fail, not filter silently")
	require.Equal(t, before, batch.ActiveLen(), "the rejected batch must keep its selection")
}

// TestStreamVecDispatch_FilteredQueryAcrossBatches is the "tested across batches" box.
// The oracle corpus arrives as three single-row batches instead of one, so the merge,
// the pre-merge filter and the columnar strip each cross a batch boundary. The answer
// must not change.
func TestStreamVecDispatch_FilteredQueryAcrossBatches(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
	plan := newOracleBatchedPlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag},
		oracleOrderTag, 1)

	handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
	require.True(t, handled)
	body, ok := resp.Data().([]byte)
	require.True(t, ok, "a multi-batch filtered query emitted %T, not a columnar frame body", resp.Data())

	batch, err := streamframe.Decode(body)
	require.NoError(t, err)
	require.Equal(t, 2, batch.ActiveLen())
	_, hasState := batch.Schema.TagIndex(oracleFamily, oracleCriteriaTag)
	require.False(t, hasState, "the criteria-only tag %q leaked into the frame", oracleCriteriaTag)
	require.Equal(t, []string{"new", "other"}, oracleFrameServices(t, batch),
		"three single-row batches must give the same answer as one three-row batch")
}

// TestStreamVecDispatch_TracedFilteredQueryStaysProto is the "traced responses retain
// tracing information" box, taken through the dispatch rather than through the emit
// predicate alone. A frame carries no field for common.v1.Trace, so tracing must keep
// forcing the proto egress even now that a filtered query is otherwise eligible.
func TestStreamVecDispatch_TracedFilteredQueryStaysProto(t *testing.T) {
	p := newOracleProcessor(t)
	req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
	plan := newOraclePlan(t, req, []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}, oracleOrderTag)

	// traced=true is the only difference from TestStreamVecDispatch_FilteredQueryEmitsFrame.
	handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, true)
	require.True(t, handled)
	_, isFrame := resp.Data().([]byte)
	require.False(t, isFrame, "a traced query emitted a frame, so its trace channel is lost")
	response, ok := resp.Data().(*streamv1.QueryResponse)
	require.True(t, ok, "a traced query emitted %T, not a proto response", resp.Data())
	require.Equal(t, []string{"new", "other"}, oracleProtoServices(t, response.GetElements()),
		"the traced proto answer must match the untraced frame answer")
}

// TestStreamVecDispatch_NullTagValueSurvivesTheColumnStrip is the "null behavior" box.
//
// A column tracks nullness in a validity bitmap that is independent of the cell it
// holds, and the two legitimately disagree: AppendColumnRange copies the source value
// unconditionally and only then marks the destination row null, so a null row keeps a
// stale pointer. The columnar strip copies columns with that same helper, so this test
// proves the strip carries the BITMAP and not just the cell, and that the frame codec
// round-trips it.
//
// Marking the cell without clearing it is the shape that produces silently wrong
// output rather than a crash. The same trap is pinned for the element egress in
// pkg/query/vectorized/stream/egress_null_test.go.
func TestStreamVecDispatch_NullTagValueSurvivesTheColumnStrip(t *testing.T) {
	executedTags := []string{oracleClientTag, oracleCriteriaTag, oracleOrderTag}
	// Row 2 is element B, which survives the criteria. Nulling its projected tag does
	// not change the filter, which matches on `state`.
	const nulledRow = 2

	nullServiceOnRowB := func(t *testing.T, corpus []*vectorized.RecordBatch) {
		t.Helper()
		require.Len(t, corpus, 1, "this arm expects one batch, so the row index is absolute")
		batch := corpus[0]
		serviceIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
		require.True(t, ok)
		serviceCol := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
		serviceCol.MarkNullAt(nulledRow)
		require.True(t, serviceCol.IsNull(nulledRow))
		require.NotNil(t, serviceCol.Data()[nulledRow],
			"precondition: the stale pointer must still be present, or the test proves nothing")
	}

	t.Run("frame carries the null", func(t *testing.T) {
		p := newOracleProcessor(t)
		req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
		plan, corpus := newOraclePlanWithCorpus(t, req, executedTags, oracleOrderTag, len(oracleRows()))
		nullServiceOnRowB(t, corpus)

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled)
		body, ok := resp.Data().([]byte)
		require.True(t, ok, "the data node emitted %T, not a columnar frame body", resp.Data())

		batch, err := streamframe.Decode(body)
		require.NoError(t, err)
		require.Equal(t, 2, batch.ActiveLen())

		serviceIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
		require.True(t, ok, "the projected tag column must survive the strip")
		serviceCol := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
		rows := oracleActiveRows(batch)
		require.Len(t, rows, 2)
		// Row 0 is element A, which keeps its value. Row 1 is element B, now null.
		require.False(t, serviceCol.IsNull(rows[0]), "element A must keep its value")
		require.Equal(t, "new", serviceCol.Data()[rows[0]].GetStr().GetValue())
		require.True(t, serviceCol.IsNull(rows[1]),
			"the strip or the frame codec lost the validity bit, so a null reads as a value")
	})

	t.Run("proto carries the same null", func(t *testing.T) {
		p := newOracleStandaloneProcessor(t)
		req := oracleRequest([]string{oracleClientTag}, true, oracleIndexRule)
		plan, corpus := newOraclePlanWithCorpus(t, req, executedTags, oracleOrderTag, len(oracleRows()))
		nullServiceOnRowB(t, corpus)

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled)
		response, ok := resp.Data().(*streamv1.QueryResponse)
		require.True(t, ok, "standalone emitted %T, not a proto response", resp.Data())
		require.Len(t, response.GetElements(), 2)

		values := make([]*modelv1.TagValue, 0, 2)
		for _, element := range response.GetElements() {
			for _, family := range element.GetTagFamilies() {
				for _, tag := range family.GetTags() {
					if tag.GetKey() == oracleClientTag {
						values = append(values, tag.GetValue())
					}
				}
			}
		}
		require.Len(t, values, 2)
		require.Equal(t, "new", values[0].GetStr().GetValue(), "element A must keep its value")
		require.IsType(t, &modelv1.TagValue_Null{}, values[1].GetValue(),
			"element B must read as a null tag, not as the stale cell value")
	})
}

// oracleMultiGroupRows is one corpus for each group of the multi-group arms. The two
// groups carry DIFFERENT service values, so a returned element names the group it came
// from, and each group holds one row that FAILS the criteria, so a group that skipped
// its filter shows up as a row that does not match.
//
// The sequence values are globally ordered across the groups, so the index-order
// cross-group merge has one unambiguous answer. The timestamps are per-batch
// (oracleBatchFromRows numbers from 1), and the two surviving rows sit at DIFFERENT
// row offsets, so the timestamp-order merge is unambiguous too — in the opposite
// order, which is what proves the merge sorted rather than concatenated.
func oracleMultiGroupRows() ([]oracleRow, []oracleRow) {
	return []oracleRow{
			{elemID: 1, sequence: "001", state: oracleStateFail, service: "a-closed"},
			{elemID: 2, sequence: "002", state: oracleStateWant, service: "a-open"},
		}, []oracleRow{
			{elemID: 3, sequence: "003", state: oracleStateWant, service: "b-open"},
			{elemID: 4, sequence: "004", state: oracleStateFail, service: "b-closed"},
		}
}

// oracleMultiGroupFailServices are the service values of the rows the criteria must
// reject. No returned element may carry one.
func oracleMultiGroupFailServices() []string { return []string{"a-closed", "b-closed"} }

// TestStreamVecDispatch_MultiGroupFilteredDispatch covers the multi-group vec dispatch
// — *limit → *mergePlan → per-group *tagFilterPlan → *localIndexScan, the shape
// tryStreamVecDispatch hands to tryVecMergeDispatch. No other test in the repo reaches
// it, so forcing every group's criteria to be skipped (PreMerged = true inside
// VecMergeExecutable) kept the whole suite green. This test fails under that mutation.
//
// The multi-group path merges ELEMENTS across groups, so it always emits
// *streamv1.QueryResponse and never a frame. Both arms therefore run on a data node
// with the raw wire mode on, which pins that too.
func TestStreamVecDispatch_MultiGroupFilteredDispatch(t *testing.T) {
	// The schema's own group plus a second one. Only the count drives the merge shape;
	// the names reach the fake execution context, which ignores them.
	groupNames := []string{"test", "test-b"}
	rowsA, rowsB := oracleMultiGroupRows()

	// TIMESTAMP order is the arm the mutation breaks: it pushes no filter down, so
	// every group's criteria has to run on that group's elements.
	t.Run("timestamp order filters each group", func(t *testing.T) {
		p := newOracleProcessor(t)
		req := oracleRequest([]string{oracleClientTag}, true, "")
		req.Groups = groupNames
		// No ordering tag, so no OrderKey column; the analyzer appends `state` for the
		// criteria in each group.
		plan, _ := newOracleGroupsPlan(t, req, []string{oracleClientTag, oracleCriteriaTag}, "", rowsA, rowsB)

		merge, ok := logical_stream.VecMergeExecutable(plan)
		require.True(t, ok, "two groups must give the multi-group vec shape")
		require.Len(t, merge.Groups, 2)
		require.True(t, merge.SortByTime, "an index-rule-less order must merge on time")
		for groupIdx, group := range merge.Groups {
			require.True(t, group.HasFilter, "group %d lost its criteria", groupIdx)
		}

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled, "the multi-group filtered shape must be vec-eligible")
		response, ok := resp.Data().(*streamv1.QueryResponse)
		require.True(t, ok, "the multi-group path emitted %T, not a proto response", resp.Data())

		services := oracleProtoServices(t, response.GetElements())
		// The discriminating assertion, and it comes FIRST so that it is the one a
		// skipped criteria reports: a group that did not filter returns rows that do not
		// match the criteria.
		for _, service := range services {
			require.NotContains(t, oracleMultiGroupFailServices(), service,
				"an element that fails the criteria came back, so a group skipped its filter")
		}
		// b-open sits at row offset 0 of its batch and a-open at offset 1, so ascending
		// timestamp order puts b-open first.
		require.Equal(t, []string{"b-open", "a-open"}, services)

		// The fixture guard sits BEHIND the answer deliberately. A timestamp-order group
		// must not push its filter down, or the per-element filter this arm targets never
		// runs — but checking that first would abort on a forced PreMerged before the
		// answer assertions above could catch it, which is the whole point of them.
		for groupIdx, group := range merge.Groups {
			require.False(t, group.Criteria.PreMerged,
				"group %d pushed its filter down, so this arm proves nothing", groupIdx)
		}
	})

	// INDEX order covers the other branch of the same loop: the scan filtered the
	// columns ahead of the merge, so the group owes only the criteria-tag strip.
	t.Run("index order strips the criteria tag", func(t *testing.T) {
		p := newOracleProcessor(t)
		// The merger narrows the merged schema to the client projection and only THEN
		// looks the sort tag up (stream_plan_merge.go:59 then :91), so a multi-group index-order
		// query must PROJECT its sort tag or the plan does not analyze at all. The
		// liaison narrows the same way — see TestStreamVecDispatch_HiddenOrderTagEmitsFrame.
		req := oracleRequest([]string{oracleClientTag, oracleOrderTag}, true, oracleIndexRule)
		req.Groups = groupNames
		plan, _ := newOracleGroupsPlan(t, req,
			[]string{oracleClientTag, oracleOrderTag, oracleCriteriaTag}, oracleOrderTag, rowsA, rowsB)

		merge, ok := logical_stream.VecMergeExecutable(plan)
		require.True(t, ok, "two groups must give the multi-group vec shape")
		require.Len(t, merge.Groups, 2)
		require.False(t, merge.SortByTime, "an index-rule order must merge on the sort tag")
		for groupIdx, group := range merge.Groups {
			require.True(t, group.HasFilter, "group %d lost its criteria", groupIdx)
			require.True(t, group.Criteria.PreMerged,
				"group %d must push its filter onto the columns, which is the strip-only branch", groupIdx)
		}

		handled, resp := p.tryStreamVecDispatch(context.Background(), plan, req, false)
		require.True(t, handled)
		response, ok := resp.Data().(*streamv1.QueryResponse)
		require.True(t, ok, "the multi-group path emitted %T, not a proto response", resp.Data())

		// oracleProtoServices is not usable here: this arm PROJECTS the ordering tag, so
		// `sequence` legitimately reaches the client and only `state` must be gone.
		services := make([]string, 0, len(response.GetElements()))
		for _, element := range response.GetElements() {
			for _, family := range element.GetTagFamilies() {
				for _, tag := range family.GetTags() {
					require.NotEqual(t, oracleCriteriaTag, tag.GetKey(),
						"the criteria-only tag survived the per-group strip")
					if tag.GetKey() == oracleClientTag {
						services = append(services, tag.GetValue().GetStr().GetValue())
					}
				}
			}
		}
		// Ascending `sequence` across both groups: 002 then 003.
		require.Equal(t, []string{"a-open", "b-open"}, services)
		for _, service := range services {
			require.NotContains(t, oracleMultiGroupFailServices(), service,
				"an element that fails the criteria came back, so a group skipped its filter")
		}
	})
}

// TestStreamVecDispatch_NullCriteriaCellFailsBothFilters is the null case for the
// column the criteria READS. TestStreamVecDispatch_NullTagValueSurvivesTheColumnStrip
// nulls a PROJECTED tag, which cannot change a filter decision; a null in the criteria
// column can.
//
// Two independent readers substitute pbv1.NullTagValue for a null cell: the columnar
// tagRowAccessor the criteria filter uses (pkg/query/vectorized/stream/tag_filter.go)
// and the element egress (pkg/query/vectorized/stream/egress.go). Both read the
// validity bitmap, not the cell, so a null criteria cell must fail an equality
// criteria on both — the cell here keeps its stale matching pointer, which is what a
// reader trusting Data() alone would wrongly let through.
//
// Honest scope: only the TIMESTAMP arm compares the two readers against each other
// (the frame path filters the columns, the proto path filters the elements). An
// index-order query filters on the columns for BOTH egresses, so that arm pins the
// columnar reader and the strip, not a cross-reader agreement.
func TestStreamVecDispatch_NullCriteriaCellFailsBothFilters(t *testing.T) {
	// Both rows match the criteria, so nulling the first row's criteria cell is the ONLY
	// reason it can be missing from the answer. The second row keeps the answer
	// non-empty, so the frame/proto comparison cannot pass vacuously.
	rows := []oracleRow{
		{elemID: oracleElementA, sequence: "001", state: oracleStateWant, service: "nulled"},
		{elemID: oracleElementB, sequence: "002", state: oracleStateWant, service: "kept"},
	}
	const nulledRow = 0

	nullTheCriteriaCell := func(t *testing.T, batch *vectorized.RecordBatch) {
		t.Helper()
		stateIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleCriteriaTag)
		require.True(t, ok, "the criteria column must be in the executed schema")
		stateCol := batch.Columns[stateIdx].(*vectorized.TypedColumn[*modelv1.TagValue])
		stateCol.MarkNullAt(nulledRow)
		require.True(t, stateCol.IsNull(nulledRow))
		require.Equal(t, oracleStateWant, stateCol.Data()[nulledRow].GetStr().GetValue(),
			"precondition: the stale MATCHING value must still be present, or the test proves nothing")
	}

	for _, arm := range []struct {
		name      string
		indexRule string
		orderTag  string
	}{
		{name: "index order, the columnar filter reads the null", indexRule: oracleIndexRule, orderTag: oracleOrderTag},
		{name: "timestamp order, the element filter reads the null", indexRule: "", orderTag: ""},
	} {
		t.Run(arm.name, func(t *testing.T) {
			executedTags := []string{oracleClientTag, oracleCriteriaTag}
			if arm.orderTag != "" {
				executedTags = append(executedTags, arm.orderTag)
			}

			dataNode := newOracleProcessor(t)
			frameReq := oracleRequest([]string{oracleClientTag}, true, arm.indexRule)
			framePlan, frameCorpus := newOracleGroupsPlan(t, frameReq, executedTags, arm.orderTag, rows)
			nullTheCriteriaCell(t, frameCorpus[0])

			handled, frameResp := dataNode.tryStreamVecDispatch(context.Background(), framePlan, frameReq, false)
			require.True(t, handled)
			body, ok := frameResp.Data().([]byte)
			require.True(t, ok, "the data node emitted %T, not a columnar frame body", frameResp.Data())
			batch, err := streamframe.Decode(body)
			require.NoError(t, err)
			gotFrame := oracleFrameServices(t, batch)

			standalone := newOracleStandaloneProcessor(t)
			protoReq := oracleRequest([]string{oracleClientTag}, true, arm.indexRule)
			protoPlan, protoCorpus := newOracleGroupsPlan(t, protoReq, executedTags, arm.orderTag, rows)
			nullTheCriteriaCell(t, protoCorpus[0])

			handled, protoResp := standalone.tryStreamVecDispatch(context.Background(), protoPlan, protoReq, false)
			require.True(t, handled)
			response, ok := protoResp.Data().(*streamv1.QueryResponse)
			require.True(t, ok, "standalone emitted %T, not a proto response", protoResp.Data())
			gotProto := oracleProtoServices(t, response.GetElements())

			// Guard against a vacuous pass: two empty lists compare equal and prove nothing.
			require.NotEmpty(t, gotProto, "the proto answer is empty, so the parity check is empty")
			require.Equal(t, []string{"kept"}, gotProto,
				"the null criteria cell must fail the equality, so only the other row survives")
			require.Equal(t, gotProto, gotFrame,
				"the columnar reader and the element reader disagree about a null criteria cell")
		})
	}
}

// oracleFrameServices reads the projected tag out of a decoded frame batch.
func oracleFrameServices(t *testing.T, batch *vectorized.RecordBatch) []string {
	t.Helper()
	serviceIdx, ok := batch.Schema.TagIndex(oracleFamily, oracleClientTag)
	require.True(t, ok, "the projected tag %q must survive the strip", oracleClientTag)
	serviceData := batch.Columns[serviceIdx].(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
	services := make([]string, 0, batch.ActiveLen())
	for _, rowIdx := range oracleActiveRows(batch) {
		services = append(services, serviceData[rowIdx].GetStr().GetValue())
	}
	return services
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
