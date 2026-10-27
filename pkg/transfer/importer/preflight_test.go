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

package importer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// dryRun runs a dry-run import of dir through the given liaisons of f.
func dryRun(t *testing.T, f *fakeLiaison, dir string, liaisons ...*cluster.Liaison) importRun {
	t.Helper()
	opts := defaultImport(t, dir)
	opts.DryRun = true
	return runImportVia(t, f, liaisons, opts)
}

// assertRefusedUnwritten checks a preflight refusal: exit 2, nothing written or sent.
func assertRefusedUnwritten(t *testing.T, f *fakeLiaison, run importRun) {
	t.Helper()
	assert.Equal(t, exitcode.ExitPreflight, run.code(), "%v\n%s", run.err, run.out)
	assert.Empty(t, f.writeLog())
	assert.Empty(t, f.receivedBatches())
	assert.Contains(t, run.out, "RESULT    refused, nothing written (exit 2)")
}

func TestPreflightLeavesCreatedGroupsUnrouted(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	var asked []string
	f.routeErr = func(k *transferv1.TargetsRequest) error {
		asked = append(asked, k.GetGroup())
		return nil
	}
	run := dryRun(t, f, writeDataArtifact(t), li)
	require.NoError(t, run.err, run.out)
	assert.ElementsMatch(t, []string{"sw_metric", "sw_prop", "sw_stream"}, asked, "a created group is asked only to see that the liaison serves Targets")
	assert.NotContains(t, run.out, "WARNINGS", "a created group's unknown placement is no warning")
}

func TestPreflightRefusesARoutingError(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	f.routeErr = func(k *transferv1.TargetsRequest) error {
		if k.GetGroup() == "sw_metric" {
			return status.Error(codes.FailedPrecondition, "stage hot of group sw_metric has no matching data node")
		}
		return nil
	}
	run := dryRun(t, f, writeDataArtifact(t), li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "measure/sw_metric/hot/20261016/shard-0: the target route cannot be written to: liaison "+li.Addr+
		": stage hot of group sw_metric has no matching data node")
}

func TestPreflightRefusesInactiveNodes(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	f.inactive = []string{"data-b"}
	run := dryRun(t, f, writeDataArtifact(t), li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "data node(s) [data-b] of the route are not active")
}

func TestPreflightRefusesADeletingGroup(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	f.deleting = map[string]bool{"sw_stream": true}
	run := dryRun(t, f, writeDataArtifact(t), li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "group sw_stream is being deleted")
}

func TestPreflightRefusesLiaisonsThatDisagree(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	other := f.serveAs(t, "other", true)
	f.targetsHook = func(via string, resp *transferv1.TargetsResponse) {
		if via == "other" {
			resp.Copies++
		}
	}
	run := dryRun(t, f, writeDataArtifact(t), li, other)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "liaisons disagree: "+li.Addr+" routes stage=hot shards=2 copies=1")
	assert.Contains(t, run.out, other.Addr+" routes stage=hot shards=2 copies=2")

	f.targetsHook = nil
	run = dryRun(t, f, writeDataArtifact(t), li, other)
	require.NoError(t, run.err, "liaisons that agree pass")
}

func TestPreflightRefusesALiaisonWithoutImportService(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	old := f.serveAs(t, "old", false)
	run := dryRun(t, f, writeDataArtifact(t), li, old)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "liaison "+old.Addr+" cannot serve the import: ImportService.Targets answered Unimplemented")
}

// measureFixtureFile is the data fixture's measure file.
const measureFixtureFile = "nodes/data-a/measure/sw_metric/seg-20261016/shard-0-00001.csv"

// lowerRecvLimit lowers a receive limit to 4 KiB for the test.
func lowerRecvLimit(t *testing.T, limit *int) {
	t.Helper()
	saved := *limit
	*limit = 4 << 10
	t.Cleanup(func() { *limit = saved })
}

func TestPreflightRefusesBadRows(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	dir := writeDataArtifactWith(t,
		cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "1", "default.entity_id": "e1", "cpm::_field.value": "5"},
		cells{"_name": "cpm", "_timestamp": cellTime(time.Minute), "_version": "0", "default.entity_id": "e2", "cpm::_field.value": "7"},
		cells{"_name": "cpm", "_timestamp": csvfmt.EncodeTimestamp(dataTime.UnixNano() + 1), "_version": "3", "default.entity_id": "e3"},
	)
	run := dryRun(t, f, dir, li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "row check: file "+measureFixtureFile+" record 2: artifact is corrupt: _version of measure cpm is null or 0")
	assert.Contains(t, run.out, "row check: file "+measureFixtureFile+" record 3: invalid timestamp: time is not millisecond precision")
	assert.Contains(t, run.out, "row check: 2 row(s) of 1 work item(s) fail")
	assert.Contains(t, run.out, "ROWS      checked 8 row(s) of 3 work item(s) read-only")

	realRun := runImport(t, f, li, defaultImport(t, dir))
	assert.Equal(t, run.out, realRun.out, "the real run prints the same refusal")
	assert.Equal(t, exitcode.ExitPreflight, realRun.code())
	assert.Empty(t, f.writeLog())
}

func TestPreflightRefusesAnOversizedRowAndWarnsOverBatch(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	lowerRecvLimit(t, &liaisonMaxRecvBytes)
	dir := writeDataArtifactWith(t,
		cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "1", "default.entity_id": "e1", "default.tier": strings.Repeat("x", 2<<10)},
		cells{"_name": "cpm", "_timestamp": cellTime(time.Minute), "_version": "1", "default.entity_id": "e2", "default.tier": strings.Repeat("x", 8<<10)},
	)
	opts := defaultImport(t, dir)
	opts.DryRun = true
	opts.WriteBatchBytes = 64
	run := runImportVia(t, f, []*cluster.Liaison{li}, opts)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "row check: file "+measureFixtureFile+" record 2: the row is 8")
	assert.Contains(t, run.out, "exceeds the 4096 bytes a liaison receives by default (--max-recv-msg-size)")
	assert.Contains(t, run.out, "1 row(s) are larger than --write-batch-bytes 64 (largest 2")
	assert.Contains(t, run.out, "record 2)\n", "the largest row is the oversized one")
}

func TestPreflightRefusesARowLargerThanADataNodeReceives(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	lowerRecvLimit(t, &dataNodeMaxRecvBytes)
	dir := writeDataArtifactWith(t,
		cells{"_name": "cpm", "_timestamp": cellTime(0), "_version": "1", "default.entity_id": "e1", "default.tier": "gold"},
		cells{"_name": "cpm", "_timestamp": cellTime(time.Minute), "_version": "1", "default.entity_id": "e2", "default.tier": strings.Repeat("x", 8<<10)},
	)
	run := dryRun(t, f, dir, li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "row check: file "+measureFixtureFile+" record 2: the row is 8")
	assert.Contains(t, run.out, "exceeds the 4096 bytes a data node receives by default (--max-recv-msg-size of its queue server)")
}

func TestPreflightWarnsAboutABatchLargerThanALiaisonReceives(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	lowerRecvLimit(t, &liaisonMaxRecvBytes)
	opts := defaultImport(t, writeDataArtifact(t))
	opts.DryRun = true
	opts.WriteBatchBytes = 8 << 10
	run := runImportVia(t, f, []*cluster.Liaison{li}, opts)
	require.NoError(t, run.err, "an oversized batch setting is only a warning: %s", run.out)
	assert.Contains(t, run.out, "--write-batch-bytes 8192 exceeds the 4096 bytes a liaison receives by default (--max-recv-msg-size)")
}

func TestPreflightWarnsAboutNodesBehindTheSchema(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	mirrorFake(f, dataSchemas())
	f.revisionAnswer = func(int) *schemav1.AwaitRevisionAppliedResponse {
		return &schemav1.AwaitRevisionAppliedResponse{Laggards: []*schemav1.NodeLaggard{{Node: "data-b", CurrentModRevision: 5}}}
	}
	run := dryRun(t, f, writeDataArtifact(t), li)
	require.NoError(t, run.err, "a lagging node is only a warning")
	assert.Contains(t, run.out, "schema barrier: data-b has applied schema revision 5, not yet 600")
	assert.Equal(t, 1, f.revProbes)
}

func TestRunImportPrintsWarningsOnlyTheRecheckFinds(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	// The empty target has no revision to probe; after the schema write it has.
	f.revisionAnswer = func(int) *schemav1.AwaitRevisionAppliedResponse {
		return &schemav1.AwaitRevisionAppliedResponse{Laggards: []*schemav1.NodeLaggard{{Node: "data-b", CurrentModRevision: 1}}}
	}
	run := runImport(t, f, li, defaultImport(t, writeDataArtifact(t)))
	require.NoError(t, run.err, run.log)
	assert.NotContains(t, run.out, "schema barrier")
	assert.Contains(t, run.log, "WARN (re-check after the schema write) schema barrier: data-b")
}

func TestDiffSchemasPredictsTheTopNResultMeasure(t *testing.T) {
	art := dataSchemas()
	foreign := testTopNResult("sw_metric")
	foreign.TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_STRING
	art.Measures[ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}] = foreign
	d := DiffSchemas(art, NewTargetState(), defaultOpts())
	topN := actionOf(t, d, "measure/sw_metric/"+csvfmt.TopNResultMeasure)
	assert.Equal(t, DispositionConflict, topN.Disposition)
	assert.Contains(t, topN.Reason, "the target creates with the group")
	assert.Equal(t, []FieldDiff{{Path: "_topN.direction", Class: ClassShape, Artifact: "TAG_TYPE_STRING", Target: "TAG_TYPE_INT"}}, topN.Diffs)

	compatible := DiffSchemas(dataSchemas(), NewTargetState(), defaultOpts())
	assert.Equal(t, DispositionOK, actionOf(t, compatible, "measure/sw_metric/"+csvfmt.TopNResultMeasure).Disposition, "a compatible _top_n_result is OK")
	predicted := PredictState(NewTargetState(), compatible.Actions)
	assert.NotNil(t, predicted.Measures[ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}], "a created measure group gets its _top_n_result")
}

func TestRunImportRefusesAForeignTopNResult(t *testing.T) {
	f := newFakeLiaison()
	li := f.serve(t)
	s := dataSchemas()
	s.Measures[ResourceKey{"sw_metric", csvfmt.TopNResultMeasure}].TagFamilies[0].Tags[3].Type = databasev1.TagType_TAG_TYPE_INT
	run := dryRun(t, f, writeDataArtifactSchemas(t, s), li)
	assertRefusedUnwritten(t, f, run)
	assert.Contains(t, run.out, "measure/sw_metric/_top_n_result conflicts with the target")
}

// checkBuilder is a row builder of the data fixture's group of catalog, over target.
func checkBuilder(t *testing.T, catalog commonv1.Catalog, group string, target *Schemas) *rowBuilder {
	t.Helper()
	return newRowBuilder(layoutOf(t, target, catalog, group), target, catalog, group)
}

func TestCheckRowAppliesTheLiaisonChecks(t *testing.T) {
	s := dataSchemas()
	s.Groups["sw_trace"] = testGroup("sw_trace", commonv1.Catalog_CATALOG_TRACE)
	s.Traces[ResourceKey{"sw_trace", "span"}] = testTrace("sw_trace", "span")
	ok := timestamppb.New(dataTime)
	subMilli := timestamppb.New(dataTime.Add(time.Nanosecond))
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	tsTag := func(ts *timestamppb.Timestamp) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: ts}}
	}
	families := []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{str("a"), nullTag}}}
	stream := checkBuilder(t, commonv1.Catalog_CATALOG_STREAM, "sw_stream", s)
	measure := checkBuilder(t, commonv1.Catalog_CATALOG_MEASURE, "sw_metric", s)
	trace := checkBuilder(t, commonv1.Catalog_CATALOG_TRACE, "sw_trace", s)
	prop := checkBuilder(t, commonv1.Catalog_CATALOG_PROPERTY, "sw_prop", s)
	point := func(version int64, ts *timestamppb.Timestamp) *transferv1.ImportRowsRequest_MeasureRow {
		return &transferv1.ImportRowsRequest_MeasureRow{Name: "cpm", Point: &measurev1.DataPointValue{Timestamp: ts, TagFamilies: families, Version: version}}
	}
	span := func(tags ...*modelv1.TagValue) *transferv1.ImportRowsRequest_TraceRow {
		return &transferv1.ImportRowsRequest_TraceRow{Name: "span", Tags: tags}
	}
	property := func(group, id string, deleteTime int64) *transferv1.ImportRowsRequest_PropertyRow {
		return &transferv1.ImportRowsRequest_PropertyRow{
			Property: &propertyv1.Property{Metadata: &commonv1.Metadata{Group: group, Name: "ui"}, Id: id}, DeleteTime: deleteTime,
		}
	}
	cases := []struct {
		b    *rowBuilder
		row  proto.Message
		want string
	}{
		{stream, &transferv1.ImportRowsRequest_StreamRow{Name: "log", Element: &streamv1.ElementValue{Timestamp: ok, TagFamilies: families}}, ""},
		{stream, &transferv1.ImportRowsRequest_StreamRow{Name: "log"}, "element is required"},
		{stream, &transferv1.ImportRowsRequest_StreamRow{Name: "log", Element: &streamv1.ElementValue{Timestamp: subMilli, TagFamilies: families}}, "millisecond"},
		{stream, &transferv1.ImportRowsRequest_StreamRow{Name: "log", Element: &streamv1.ElementValue{Timestamp: ok}}, "entity tag svc of log is missing"},
		{measure, point(1, ok), ""},
		{measure, point(-1, ok), ""},
		{measure, point(0, ok), "must not be 0"},
		{measure, point(1, nil), "time is empty"},
		{measure, &transferv1.ImportRowsRequest_MeasureRow{Name: "cpm"}, "point is required"},
		{trace, span(str("t"), str("s"), tsTag(ok)), ""},
		{trace, span(str("t"), str("s")), "has 3 tags, the row carries 2"},
		{trace, span(str(""), str("s"), tsTag(ok)), "trace id tag trace_id must be a non-empty string"},
		{trace, span(nullTag, str("s"), tsTag(ok)), "trace id tag trace_id must be a non-empty string"},
		{trace, span(str("t"), nullTag, tsTag(ok)), "span id tag span_id must be a non-empty string"},
		{trace, span(str("t"), str("s"), nullTag), "timestamp tag ts must not be null"},
		{trace, span(str("t"), str("s"), tsTag(subMilli)), "millisecond"},
		{prop, property("sw_prop", "p1", 0), ""},
		{prop, property("other", "p1", 0), "property group other is not the stream's group sw_prop"},
		{prop, property("sw_prop", "", 0), "property name and id are required"},
		{prop, property("sw_prop", "p1", -1), "delete time must not be negative"},
		{prop, &transferv1.ImportRowsRequest_PropertyRow{Property: &propertyv1.Property{}}, "property metadata is required"},
	}
	for i, c := range cases {
		err := c.b.checkRow(c.row)
		if c.want == "" {
			assert.NoError(t, err, "case %d", i)
			continue
		}
		assert.ErrorContains(t, err, c.want, "case %d", i)
	}
}

func TestCheckRowRefusesAnEntityTagWithoutColumn(t *testing.T) {
	art := dataSchemas()
	target := dataSchemas()
	m := target.Measures[ResourceKey{"sw_metric", "cpm"}]
	m.TagFamilies[0].Tags = append(m.TagFamilies[0].Tags, &databasev1.TagSpec{Name: "region", Type: databasev1.TagType_TAG_TYPE_STRING})
	m.Entity.TagNames = append(m.Entity.TagNames, "region")
	b := newRowBuilder(layoutOf(t, art, commonv1.Catalog_CATALOG_MEASURE, "sw_metric"), target, commonv1.Catalog_CATALOG_MEASURE, "sw_metric")
	row := &transferv1.ImportRowsRequest_MeasureRow{Name: "cpm", Point: &measurev1.DataPointValue{
		Timestamp: timestamppb.New(dataTime), Version: 1,
		TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{nullTag, nullTag, nullTag}}},
	}}
	assert.ErrorContains(t, b.checkRow(row), "entity tag region of cpm has no column in the artifact")
}
