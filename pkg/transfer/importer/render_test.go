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
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

var update = flag.Bool("update", false, "rewrite the golden files of the render tests")

func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(path, got, 0o600))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func render(t *testing.T, format string, p *Plan) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, Render(&buf, format, p))
	return buf.Bytes()
}

func TestRenderGolden(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	checkGolden(t, "plan.table.golden", render(t, "table", p))
	checkGolden(t, "plan.json.golden", render(t, "json", p))
	checkGolden(t, "plan.yaml.golden", render(t, "yaml", p))
}

func TestRenderGoldenRefused(t *testing.T) {
	in := decideInputs(t, Flags{ForceTTL: true})
	art := in.Artifact
	target := mirrorTarget(&art.Schemas)
	tm := target.Measures[ResourceKey{"sw_metric", "cpm"}]
	tm.TagFamilies[0].Tags[1].Type = databasev1.TagType_TAG_TYPE_INT
	target.Groups["sw_stream"].ResourceOpts.ShardNum = 4
	d := DiffSchemas(&art.Schemas, target, SchemaOptions{
		IncludeSchema: true, Rules: []config.ConflictRule{{Match: "group/sw_stream", Policy: config.ConflictPolicyIgnore}},
	})
	in.Actions = d.Actions
	in.Target.Groups = PredictState(target, d.Actions).Groups
	p := Decide(in)
	require.True(t, p.Refused())
	checkGolden(t, "plan-refused.table.golden", render(t, "table", p))
}

// gatesPlan is a plan refused for a blocker and a row that fails the row check, with a route that
// cannot be written to and a segment past the retention imported by --force-ttl.
func gatesPlan(t *testing.T) *Plan {
	t.Helper()
	in := decideInputs(t, Flags{ForceTTL: true})
	in.Target.RouteIssues = map[RouteKey]string{
		NewRouteKey("sw_metric", "warm"): "liaison liaison-dst:17912: data node(s) [dst-warm1] of the route are not active",
	}
	in.Target.Blockers = []string{"liaison liaison-b:17912 cannot serve the import: ImportService.Targets answered Unimplemented: unknown service"}
	in.Target.Notes = []string{"schema barrier: dst-warm1 has applied schema revision 5, not yet 9; rows routed to it may fail until it catches up"}
	p := Decide(in)
	key := ""
	for _, e := range p.Entries {
		if e.Item.Catalog == stream {
			key = e.Item.Key()
		}
	}
	require.NotEmpty(t, key)
	file := "nodes/data-a/stream/sw_stream/seg-20261018/shard-0-00001.csv.gz"
	ApplyRowCheck(p, &RowCheck{
		Bad:              []BadRow{{Item: key, File: file, Record: 7, Reason: "invalid timestamp: time is not millisecond precision"}},
		BadItems:         map[string]uint64{key: 1},
		Largest:          RowPos{File: file, Record: 3, Bytes: 5 << 20},
		LargestOverBatch: RowPos{File: file, Record: 3, Bytes: 5 << 20},
		Rows:             2300, BadRows: 1, OverBatchRows: 1, Items: len(p.Entries),
	}, 4<<20)
	require.True(t, p.Refused())
	return p
}

func TestRenderGoldenGates(t *testing.T) {
	p := gatesPlan(t)
	checkGolden(t, "plan-gates.table.golden", render(t, "table", p))
}

// A standalone target has no data nodes to route to, so a group the import creates shows no
// target nodes rather than ones routed after its schema is written.
func TestRenderStandaloneShowsNoAfterSchemaTargets(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	p.Header.Standalone = true
	out := string(render(t, "table", p))
	assert.NotContains(t, out, "after-schema")
}

func TestResultOfASchemaOnlyPlanPromisesNoData(t *testing.T) {
	p := Decide(decideInputs(t, Flags{SchemaOnly: true}))
	require.NotEmpty(t, p.Entries, "the data work items are still decided")
	writes := 0
	for _, a := range p.Schema {
		if a.Disposition == DispositionCreate || a.Disposition == DispositionOverwrite {
			writes++
		}
	}
	require.Positive(t, writes)
	got := result(p)
	assert.Equal(t, fmt.Sprintf("schema only: %d resource(s) to create or overwrite, no data sent", writes), got)
	assert.NotContains(t, got, "work item")
}

func TestRenderIsIndependentOfNow(t *testing.T) {
	in := decideInputs(t, Flags{ForceTTL: true})
	a := render(t, "table", Decide(in))
	in.Now = in.Now.Add(time.Hour)
	assert.Equal(t, string(a), string(render(t, "table", Decide(in))))
}

func TestNewestTimeOfSegmentFiles(t *testing.T) {
	m := decideInputs(t, Flags{}).Artifact.Manifest
	m.Source.Nodes = []manifest.Node{{ID: "data-a", Dir: "data-a", Timezone: "Asia/Shanghai"}}
	segmentUnit := func(stage string) manifest.Unit {
		return manifest.Unit{
			Catalog: "MEASURE", Group: "sw_metric", Stage: stage, Segment: "20261010",
			Sources: []manifest.UnitSource{{Node: "data-a", SegmentFiles: []manifest.File{dataFile("x")}}},
		}
	}
	m.Units = []manifest.Unit{segmentUnit("warm")}
	items, err := BuildWorkItems(m)
	require.NoError(t, err)
	require.Len(t, items, 1)
	newest, err := newestTime(&items[0])
	require.NoError(t, err)
	// The warm segment interval is 3d: 2026-10-10T00:00+08:00 + 3d, minus one nanosecond.
	assert.Equal(t, time.Date(2026, 10, 12, 15, 59, 59, 999999999, time.UTC).UnixNano(), newest)

	m.Units = []manifest.Unit{segmentUnit("frozen")}
	items, err = BuildWorkItems(m)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = newestTime(&items[0])
	assert.ErrorContains(t, err, `the manifest has no stage "frozen" for group sw_metric`)
}

func TestRenderTimeRangeOfIndexModeOnlyGroup(t *testing.T) {
	in := decideInputs(t, Flags{})
	m := in.Artifact.Manifest
	m.Source.Nodes = []manifest.Node{{ID: "data-a", Dir: "data-a", Timezone: "Asia/Shanghai"}}
	m.Groups = []manifest.Group{{Name: "sw_metric", Catalog: "MEASURE", IndexMode: true, Stages: []manifest.Stage{{Name: "hot", SegmentInterval: "1d", TTL: "7d"}}}}
	m.Units = []manifest.Unit{{
		Catalog: "MEASURE", Group: "sw_metric", Stage: "hot", Segment: "20261016",
		Sources: []manifest.UnitSource{{
			Node: "data-a", SegmentFiles: []manifest.File{dataFile("nodes/data-a/measure/sw_metric/seg-20261016/segment-00001.csv.gz")},
			RowCounts: map[string]uint64{"idx": 5}, TotalCount: 5,
		}},
	}}
	p := Decide(in)
	require.Len(t, p.Entries, 1)
	lo, hi := p.Entries[0].Item.TimeRange()
	assert.Equal(t, time.Date(2026, 10, 15, 16, 0, 0, 0, time.UTC).UnixNano(), lo)
	assert.Equal(t, time.Date(2026, 10, 16, 15, 59, 59, 999999999, time.UTC).UnixNano(), hi)
	var row string
	for _, l := range strings.Split(string(render(t, "table", p)), "\n") {
		if strings.HasPrefix(l, "measure") {
			row = l
		}
	}
	assert.True(t, strings.HasSuffix(row, "10-15 ~ 10-16"), row)
}

// A segment item shows the time range of the rows the export recorded for its unit, not the
// segment bounds it pushes with, so a group's row keeps the range of its data.
func TestRenderTimeRangeOfRecordedSegmentRows(t *testing.T) {
	in := decideInputs(t, Flags{})
	m := in.Artifact.Manifest
	m.Source.Nodes = []manifest.Node{{ID: "data-a", Dir: "data-a", Timezone: "UTC"}}
	m.Groups = []manifest.Group{{Name: "sw_metric", Catalog: "MEASURE", IndexMode: true, Stages: []manifest.Stage{{Name: "hot", SegmentInterval: "7d", TTL: "30d"}}}}
	rowsLo := time.Date(2026, 10, 18, 1, 0, 0, 0, time.UTC).UnixNano()
	rowsHi := time.Date(2026, 10, 18, 2, 0, 0, 0, time.UTC).UnixNano()
	m.Units = []manifest.Unit{{
		Catalog: "MEASURE", Group: "sw_metric", Stage: "hot", Segment: "20261016",
		MinTimestamp: rowsLo, MaxTimestamp: rowsHi,
		Sources: []manifest.UnitSource{{
			Node: "data-a", SegmentFiles: []manifest.File{dataFile("nodes/data-a/measure/sw_metric/seg-20261016/segment-00001.csv.gz")},
			RowCounts: map[string]uint64{"idx": 5}, TotalCount: 5,
		}},
	}}
	p := Decide(in)
	require.Len(t, p.Entries, 1)
	lo, _ := p.Entries[0].Item.TimeRange()
	assert.Equal(t, time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC).UnixNano(), lo, "the push still spans the segment")
	var row string
	for _, l := range strings.Split(string(render(t, "table", p)), "\n") {
		if strings.HasPrefix(l, "measure") {
			row = l
		}
	}
	assert.True(t, strings.HasSuffix(row, "10-18 ~ 10-18"), row)
}

// The schema is summarized per group; the problems name the missing resources' groups in one
// sentence and every conflict with its differences and the rule that accepts it.
func TestRenderSchemaPerGroupAndItsProblems(t *testing.T) {
	missing := "the target has no such resource"
	p := &Plan{Schema: []SchemaAction{
		{Kind: "group", Group: "a", Match: "group/a", Disposition: DispositionMissing, Reason: missing},
		{Kind: "measure", Group: "a", Name: "m", Match: "measure/a/m", Disposition: DispositionMissing, Reason: missing},
		{Kind: "group", Group: "b", Match: "group/b", Disposition: DispositionCreate},
		{
			Kind: "measure", Group: "c", Name: "x", Match: "measure/c/x", Disposition: DispositionConflict, Reason: "policy=error (default)",
			Diffs: []FieldDiff{{Path: "f", Artifact: "INT", Target: "STRING"}},
		},
		{Kind: "group", Group: "c", Match: "group/c", Disposition: DispositionOK},
		{Kind: "measure", Group: "c", Name: csvfmt.TopNResultMeasure, Match: "measure/c/" + csvfmt.TopNResultMeasure, Disposition: DispositionOK},
	}}
	var buf bytes.Buffer
	renderSchema(&buf, p)
	renderProblems(&buf, p)
	assert.Equal(t, "\nSCHEMA\n"+
		"  GROUP  RESOURCES  RESULT\n"+
		"  a      2          2 missing\n"+
		"  b      1          1 create\n"+
		"  c      2          1 conflict, 1 ok\n"+
		"\nPROBLEMS\n"+
		"  - 2 resource(s) of group(s) a do not exist on the target, and the import does not create them "+
		"(--include-schema=false, or the artifact has no schema)\n"+
		"  - measure/c/x conflicts with the target: policy=error (default); f is INT in the artifact, STRING on the target; "+
		"to accept it, add the schemaConflictPolicy rule \"match: measure/c/x\" with policy ignore or overwrite\n", buf.String())
}

// A resource the import writes rows for without applying its schema is counted as not applied,
// after the other results of its group.
func TestRenderSchemaCountsTheSchemaNotApplied(t *testing.T) {
	notApplied := "not applied (--include-schema=false, or the artifact has no schema); the shared columns have the same types"
	p := &Plan{Schema: []SchemaAction{
		{Kind: "group", Group: "a", Match: "group/a", Disposition: DispositionOK, Reason: notApplied},
		{Kind: "measure", Group: "a", Name: "m", Match: "measure/a/m", Disposition: DispositionOK, Reason: notApplied},
		{Kind: "measure", Group: "a", Name: "x", Match: "measure/a/x", Disposition: DispositionConflict, Reason: "--include-schema=false, but ..."},
		{Kind: "group", Group: "b", Match: "group/b", Disposition: DispositionOK, Reason: notApplied},
	}}
	var buf bytes.Buffer
	renderSchema(&buf, p)
	assert.Equal(t, "\nSCHEMA\n"+
		"  GROUP  RESOURCES  RESULT\n"+
		"  a      3          1 conflict, 2 not applied\n"+
		"  b      1          1 not applied\n", buf.String())
}

func renderLeftOf(t *testing.T, p *Plan) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, renderResumed(&buf, "table", p))
	return buf.String()
}

// okSchema marks the schema resources of groups as ok.
func okSchema(p *Plan, groups ...string) {
	for i := range p.Schema {
		if len(groups) == 0 || slices.Contains(groups, p.Schema[i].Group) {
			p.Schema[i].Disposition, p.Schema[i].Reason = DispositionOK, ""
		}
	}
}

// A resume lists only what is left: one row per (catalog, group, target stage) with its work
// items, rows, size, target nodes and time range. The schema, the problems, a STATUS column and
// the after-schema note come only when something is not ok, and nothing when nothing is left.
func TestRenderLeft(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	checkGolden(t, "left-created-schema.table.golden", []byte(renderLeftOf(t, p)))

	ok := Decide(decideInputs(t, Flags{}))
	okSchema(ok)
	ok.Rejected, ok.Warnings, ok.GateDetails, ok.Errors, ok.Blockers = nil, nil, nil, nil, nil
	for i := range ok.Entries {
		ok.Entries[i].TargetNodes, ok.Entries[i].TTLExpired = []string{"data-a"}, false
	}
	out := renderLeftOf(t, ok)
	checkGolden(t, "left-ok.table.golden", []byte(out))
	for _, absent := range []string{"TARGET    ", "SOURCE    ", "TOTAL", "SCHEMA", "PROBLEMS", "NOTE", "STATUS", "RESULT", "ROWS      checked"} {
		assert.NotContains(t, out, absent)
	}

	// A refused row, a blocker and the warnings; of the schema only the group that mixes resources
	// that are ok with ones the import creates is listed.
	gated := gatesPlan(t)
	okSchema(gated, "sw_prop", "sw_stream")
	for i := range gated.Schema {
		if gated.Schema[i].Group == "sw_metric" && gated.Schema[i].Kind == "group" {
			gated.Schema[i].Disposition = DispositionOK
		}
	}
	out = renderLeftOf(t, gated)
	checkGolden(t, "left-gates.table.golden", []byte(out))
	assert.Contains(t, out, "refused")
	assert.NotContains(t, out, "\nRESULT    ", "a table with rows has no result line")

	assert.Empty(t, renderLeftOf(t, &Plan{Header: p.Header}))
	for _, format := range []string{"json", "yaml"} {
		var buf bytes.Buffer
		require.NoError(t, renderResumed(&buf, format, p))
		assert.Equal(t, string(render(t, format, p)), buf.String(), "%s renders the whole plan", format)
	}
}

// A row that is not ok shows the STATUS column even when every schema resource is ok, and the
// SCHEMA table stays out.
func TestRenderLeftStatusWithoutASchemaTable(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	okSchema(p)
	p.Warnings, p.GateDetails = nil, nil
	require.NotEmpty(t, p.Entries)
	p.Entries[0].TTLExpired = true
	out := renderLeftOf(t, p)
	assert.Contains(t, out, "STATUS")
	assert.Contains(t, out, "expired")
	assert.NotContains(t, out, "SCHEMA")
}

// A plan refused for its schema, a blocker or an error with nothing left to push still says why:
// the problems and the result, as the dry run of an unfinished import with an empty remainder.
func TestRenderLeftOfARefusedPlanWithNothingLeft(t *testing.T) {
	p := &Plan{Header: Decide(decideInputs(t, Flags{})).Header, Blockers: []string{"liaison liaison-b:17912 cannot serve the import"}}
	require.True(t, p.Refused())
	out := renderLeftOf(t, p)
	assert.Contains(t, out, "PROBLEMS\n  - liaison liaison-b:17912 cannot serve the import\n")
	assert.Contains(t, out, "RESULT    refused, nothing written (exit 2)")
	assert.NotContains(t, out, "CATALOG")

	p = &Plan{Header: p.Header, Warnings: []string{"group g: the target has no stage cold"}}
	out = renderLeftOf(t, p)
	assert.Contains(t, out, "warning: group g: the target has no stage cold")
	assert.NotContains(t, out, "RESULT", "a warning alone refuses nothing")
}

// A work item the progress file records as complete is not left, whatever a gate says of it now.
func TestRemainingDropsTheRejectionsOfCompleteItems(t *testing.T) {
	p := Decide(decideInputs(t, Flags{}))
	require.NotEmpty(t, p.Rejected, "the default plan rejects a segment past the retention")
	state := newProgress(csvArtifact(), testBindings())
	state.Done[p.Rejected[0].Item.Key()] = &itemRecord{Complete: true}
	left := remaining(p, state)
	assert.Len(t, left.Rejected, len(p.Rejected)-1)
	assert.Len(t, p.Rejected, len(p.Rejected), "the plan itself is left as it was")
	for _, r := range left.Rejected {
		assert.NotEqual(t, p.Rejected[0].Item.Key(), r.Item.Key())
	}
}
