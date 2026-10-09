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

package exporter

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

func nodes(names ...string) []*databasev1.Node {
	out := make([]*databasev1.Node, 0, len(names))
	for _, n := range names {
		out = append(out, &databasev1.Node{Metadata: &commonv1.Metadata{Name: n}})
	}
	return out
}

// sampleReport has four registered nodes: a and b answered the dry run, c and d were
// reported unreachable.
func sampleReport() *Report {
	cluster := &ClusterInfo{DataNodes: nodes("a", "b", "c", "d")}
	result := &PlanResult{
		Frames: unitFrames(
			unitFrame("a", "", "m", 1_700_000_000_000_000_000, 1_700_086_400_000_000_000, 5),
			unitFrame("b", "", "m", 1_700_050_000_000_000_000, 1_700_100_000_000_000_000, 6),
		),
		UnreachableNodes: []string{"c", "d"},
		AnsweredNodes:    []string{"a", "b"},
	}
	return BuildReport(cluster, result)
}

func TestBuildReport_NodesGapAndMultiSource(t *testing.T) {
	rep := sampleReport()
	if got := rep.MissingNodes; len(got) != 2 || got[0] != "c" || got[1] != "d" {
		t.Fatalf("missing nodes = %v, want [c d] (the unreachable nodes)", got)
	}
	if got := rep.DataNodes; len(got) != 4 || got[0] != "a" || got[3] != "d" {
		t.Fatalf("data nodes = %v, want answered + unreachable [a b c d]", got)
	}
	if len(rep.MultiSource) != 1 || rep.MultiSource[0].Key != "measure/m/hot/20260930/shard-0" || len(rep.MultiSource[0].Sources) != 2 {
		t.Fatalf("multi-source = %+v", rep.MultiSource)
	}
	wantSources := []Source{
		{Node: "a", Rows: 5, MinTimestamp: 1_700_000_000_000_000_000, MaxTimestamp: 1_700_086_400_000_000_000},
		{Node: "b", Rows: 6, MinTimestamp: 1_700_050_000_000_000_000, MaxTimestamp: 1_700_100_000_000_000_000},
	}
	if !slices.Equal(rep.MultiSource[0].Sources, wantSources) {
		t.Fatalf("sources must be sorted by node with each node's rows and range: %+v", rep.MultiSource[0].Sources)
	}
	if len(rep.Rows) != 2 || rep.Rows[0].Node != "a" || rep.Rows[0].Stage != "hot" || rep.Rows[0].EstRows != 5 ||
		rep.Rows[0].CompressedBytes != 110 || rep.Rows[0].UncompressedBytes != 300 || rep.Rows[0].Segments != 1 || rep.Rows[0].Parts != 1 {
		t.Fatalf("rows = %+v", rep.Rows)
	}
	if node, size := rep.LargestNodeCompressedBytes(); node != "a" || size != 110 {
		t.Fatalf("largest node = %s %d, want a 110 (ties go to the first node)", node, size)
	}
}

func TestBuildReport_NodesComeFromTheLiaisonNotThePreflight(t *testing.T) {
	// e registered after the preflight and was reported unreachable: it is a gap and joins
	// the data-node list; d left the registry before the plan and is not counted. b answered
	// but was also reported unreachable (its frames were dropped), so it is listed once.
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a", "b", "d")}, &PlanResult{AnsweredNodes: []string{"a", "b"}, UnreachableNodes: []string{"b", "e"}})
	if got := rep.MissingNodes; len(got) != 2 || got[0] != "b" || got[1] != "e" {
		t.Fatalf("missing nodes = %v, want [b e]", got)
	}
	if got := rep.DataNodes; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "e" {
		t.Fatalf("data nodes = %v, want [a b e]", got)
	}
}

func TestBuildReport_ShardsAreDistinctPerRow(t *testing.T) {
	second := unitFrame("a", "", "m", 1, 2, 1)
	segmentOf(second).Unit.SegmentSuffix = "20261001"
	third := unitFrame("a", "", "m", 1, 2, 1)
	segmentOf(third).Unit.ShardIds = []uint32{1}
	segmentOf(third).Shards[0].ShardId = 1
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a")}, &PlanResult{AnsweredNodes: []string{"a"}, Frames: unitFrames(
		unitFrame("a", "", "m", 1, 2, 1), second, third,
	)})
	if len(rep.Rows) != 1 || rep.Rows[0].Segments != 3 || rep.Rows[0].Shards != 2 || rep.Rows[0].Parts != 3 {
		t.Fatalf("SHARDS must count distinct shard ids (0 and 1), SEGMENTS every unit: %+v", rep.Rows)
	}
	if len(rep.MultiSource) != 0 {
		t.Fatalf("one node is never a multi-source: %+v", rep.MultiSource)
	}
}

func TestBuildReport_SegmentLevelRawBytesOnlyForIndexMode(t *testing.T) {
	indexMode := unitFrame("a", "", "m", 1, 2, 0)
	segmentOf(indexMode).SegmentLevel.DocCount = 7
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a")}, &PlanResult{AnsweredNodes: []string{"a"}, Frames: unitFrames(
		unitFrame("a", "", "plain", 1, 2, 1), indexMode,
	)})
	if rep.Rows[0].Group != "m" || rep.Rows[0].UncompressedBytes != 310 || rep.Rows[0].EstRows != 7 {
		t.Fatalf("index-mode group counts the sidx bytes as raw: %+v", rep.Rows[0])
	}
	if rep.Rows[1].Group != "plain" || rep.Rows[1].UncompressedBytes != 300 || rep.Rows[1].CompressedBytes != 110 {
		t.Fatalf("a regular group rebuilds its series index; sidx bytes are compressed only: %+v", rep.Rows[1])
	}
}

// propertyFrame is a `units` frame with one property group of two shards on node.
func propertyFrame(node string) *transferv1.UnitFrame {
	return &transferv1.UnitFrame{NodeId: node, Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Property{
		Property: &transferv1.PropertyInventory{Group: "ui", Shards: []*transferv1.PropertyShardStat{
			{ShardId: 0, EstimatedBytes: 2048, DocCount: 12},
			{ShardId: 2, EstimatedBytes: 1024, DocCount: 3},
		}},
	}}}}
}

func TestBuildReport_PropertyRows(t *testing.T) {
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a", "b")}, &PlanResult{AnsweredNodes: []string{"a", "b"}, Frames: []*transferv1.UnitFrame{
		propertyFrame("a"), propertyFrame("b"),
	}})
	if len(rep.MultiSource) != 0 {
		t.Fatalf("every property node holds the whole group; that is not a multi-source: %+v", rep.MultiSource)
	}
	if len(rep.Rows) != 2 {
		t.Fatalf("rows = %+v", rep.Rows)
	}
	row := rep.Rows[0]
	if row.Catalog != "property" || row.Group != "ui" || row.Stage != "hot" || row.Segments != 0 || row.Parts != 0 || row.Shards != 2 {
		t.Fatalf("a property row resolves its stage like any group, has no segments or parts and counts its shards: %+v", row)
	}
	if row.EstRows != 15 || row.CompressedBytes != 3072 || row.UncompressedBytes != 3072 || row.MinTimestamp != 0 || row.MaxTimestamp != 0 {
		t.Fatalf("a property row sums doc_count and estimated_bytes on both sides and has no time range: %+v", row)
	}
	var buf bytes.Buffer
	if err := Render(&buf, "table", rep); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "a     property  ui     hot    -         2       -      15        3.0 KiB / 3.0 KiB   -") {
		t.Fatalf("property rows render their stage and - for SEGMENTS and PARTS:\n%s", out)
	}
	if multiSourceKey("measure", "m", "hot", "20260930", 3) != "measure/m/hot/20260930/shard-3" {
		t.Fatal("the multi-source key is catalog/group/stage/segment/shard")
	}
}

func TestBuildReport_AllAnswered(t *testing.T) {
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a")}, &PlanResult{AnsweredNodes: []string{"a"}})
	if len(rep.MissingNodes) != 0 || len(rep.Rows) != 0 {
		t.Fatalf("an answered node without data is not a gap: %+v", rep)
	}
}

func TestRender_Table(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, "table", sampleReport()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"NODES", "⚠", "[c d]",
		"MULTI-SRC 1 unit(s)", "measure/m/hot/20260930/shard-0",
		"NODE  CATALOG  GROUP  STAGE  SEGMENTS  SHARDS  PARTS  EST-ROWS  EST-SIZE(comp/raw)  TIME-RANGE",
		"a     measure  m      hot", "110 B / 300 B", "11-14 ~ 11-15",
		"MULTI-SRC 1 unit(s)", "measure/m/hot/20260930/shard-0  <- a(5 rows, 11-14 ~ 11-15) + b(6 rows, 11-15 ~ 11-16)",
		"SNAPSHOT  each data node pins its own snapshot; the largest node (a) needs about 110 B; check df there first",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("table output lacks %q:\n%s", want, out)
		}
	}
}

func TestRender_JSONAndYAML(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, "json", sampleReport()); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Rows         []Row    `json:"rows"`
		MissingNodes []string `json:"missingNodes"`
	}
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Rows) != 2 || decoded.Rows[0].Node != "a" || len(decoded.MissingNodes) != 2 {
		t.Fatalf("json = %+v", decoded)
	}
	buf.Reset()
	if err := Render(&buf, "yaml", sampleReport()); err != nil {
		t.Fatal(err)
	}
	var fromYAML map[string]any
	if err := yaml.Unmarshal(buf.Bytes(), &fromYAML); err != nil {
		t.Fatal(err)
	}
	if _, ok := fromYAML["rows"]; !ok {
		t.Fatalf("yaml lacks rows:\n%s", buf.String())
	}
	if err := Render(&buf, "xml", sampleReport()); err == nil {
		t.Fatal("unknown format must be rejected")
	}
}

func TestHumanUnits(t *testing.T) {
	for n, want := range map[uint64]string{
		999: "999", 1_000: "1.0K", 999_949: "999.9K", 999_999: "1.0M", 118_200_000: "118.2M", 1_400_000_000: "1.4G", 999_999_999_999: "1.0T",
	} {
		if got := humanCount(n); got != want {
			t.Fatalf("humanCount(%d) = %q, want %q", n, got, want)
		}
	}
	for n, want := range map[uint64]string{
		0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 19_649_000_000: "18.3 GiB", 1 << 40: "1.0 TiB",
	} {
		if got := humanBytes(n); got != want {
			t.Fatalf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
	if timeRange(0, 0) != "-" {
		t.Fatal("an empty range renders as a dash")
	}
	if got := timeRange(1_700_000_000_000_000_000, 1_700_086_400_000_000_000); got != "11-14 ~ 11-15" {
		t.Fatalf("same year renders MM-DD: %q", got)
	}
	if got := timeRange(1_700_000_000_000_000_000, 1_704_067_200_000_000_000); got != "2023-11-14 ~ 2024-01-01" {
		t.Fatalf("different years render the year: %q", got)
	}
}

func TestRender_ZeroRowsDoesNotPanic(t *testing.T) {
	rep := &Report{
		DataNodes:     []string{"node1"},
		AnsweredNodes: []string{"node1"},
	}
	var buf bytes.Buffer
	if err := Render(&buf, "table", rep); err != nil {
		t.Fatalf("Render with zero rows must not error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "NODE") {
		t.Fatalf("table must include the header line: %q", out)
	}
	if !strings.Contains(out, "SNAPSHOT") {
		t.Fatalf("table must include the SNAPSHOT summary line: %q", out)
	}
}

func TestBuildReport_IndexModeSegmentIsNotAMultiSourceUnit(t *testing.T) {
	indexMode := func(node string, docs uint64) *transferv1.UnitFrame {
		frame := unitFrame(node, "", "sw_metadata", 0, 0, 0)
		seg := segmentOf(frame)
		seg.Shards, seg.Unit.ShardIds = nil, nil
		seg.SegmentLevel.DocCount = docs
		return frame.GetUnits()
	}
	// Two nodes holding the same index-mode segment may be replicas or different shards;
	// the plan cannot tell, so the segment is never listed.
	rep := BuildReport(&ClusterInfo{DataNodes: nodes("a", "b")}, &PlanResult{AnsweredNodes: []string{"a", "b"}, Frames: []*transferv1.UnitFrame{
		indexMode("b", 9), indexMode("a", 7),
	}})
	if len(rep.MultiSource) != 0 {
		t.Fatalf("an index-mode segment-level unit must not be a multi-source: %+v", rep.MultiSource)
	}
	var rows uint64
	for _, row := range rep.Rows {
		rows += row.EstRows
	}
	if rows != 16 {
		t.Fatalf("index-mode doc counts must still reach the table rows, got %d", rows)
	}
	var buf bytes.Buffer
	if err := Render(&buf, "table", rep); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "MULTI-SRC") {
		t.Fatalf("table must not print MULTI-SRC:\n%s", buf.String())
	}
}

func TestBuildReport_TimeRangeMergesShardsAndSkipsEmptyOnes(t *testing.T) {
	frame := unitFrame("a", "", "m", 300, 400, 1)
	seg := segmentOf(frame)
	seg.Shards = append(seg.Shards,
		&transferv1.ShardStat{ShardId: 1, MinTimestamp: 100, MaxTimestamp: 200, TotalCount: 1},
		&transferv1.ShardStat{ShardId: 2}, // no flushed timestamps: must not pull the minimum to 0
		&transferv1.ShardStat{ShardId: 3, MinTimestamp: 350, MaxTimestamp: 900, TotalCount: 1},
	)
	rep := BuildReport(&ClusterInfo{}, &PlanResult{AnsweredNodes: []string{"a"}, Frames: unitFrames(frame)})
	if row := rep.Rows[0]; row.MinTimestamp != 100 || row.MaxTimestamp != 900 || row.Shards != 4 {
		t.Fatalf("time range must span every non-empty shard: %+v", row)
	}
}

// A shard with a maximum but no minimum is skipped whatever its position, so the merged
// range does not depend on the shard order.
func TestBuildReport_TimeRangeIsOrderIndependent(t *testing.T) {
	ranges := func(order []*transferv1.ShardStat) (int64, int64) {
		frame := unitFrame("a", "", "m", 300, 400, 1)
		seg := segmentOf(frame)
		seg.Shards = append(seg.Shards, order...)
		row := BuildReport(&ClusterInfo{}, &PlanResult{AnsweredNodes: []string{"a"}, Frames: unitFrames(frame)}).Rows[0]
		return row.MinTimestamp, row.MaxTimestamp
	}
	noMin := &transferv1.ShardStat{ShardId: 1, MaxTimestamp: 5000, TotalCount: 1}
	early := &transferv1.ShardStat{ShardId: 2, MinTimestamp: 100, MaxTimestamp: 200, TotalCount: 1}
	minA, maxA := ranges([]*transferv1.ShardStat{noMin, early})
	minB, maxB := ranges([]*transferv1.ShardStat{early, noMin})
	if minA != 100 || maxA != 400 || minA != minB || maxA != maxB {
		t.Fatalf("merge must ignore the shard without a minimum in any order: [%d,%d] vs [%d,%d]", minA, maxA, minB, maxB)
	}
}

func TestRender_NodesLine(t *testing.T) {
	for name, tc := range map[string]struct {
		cluster *ClusterInfo
		want    string
	}{
		"standalone":   {cluster: &ClusterInfo{Standalone: true}, want: "NODES     standalone process [a]\n"},
		"all answered": {cluster: &ClusterInfo{}, want: "NODES     1 data node(s) [a], all answered\n"},
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Render(&buf, "table", BuildReport(tc.cluster, &PlanResult{AnsweredNodes: []string{"a"}})); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(buf.String(), tc.want) {
				t.Fatalf("got:\n%s\nwant prefix %q", buf.String(), tc.want)
			}
		})
	}
}

func TestBuildReport_JSONHasNoNull(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, "json", BuildReport(&ClusterInfo{}, &PlanResult{})); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "null") {
		t.Fatalf("every list must render as [], got:\n%s", buf.String())
	}
}

func TestReport_LargestNodeCompressedBytes(t *testing.T) {
	rep := &Report{Rows: []Row{
		{Node: "a", CompressedBytes: 300}, {Node: "b", CompressedBytes: 200}, {Node: "b", CompressedBytes: 200}, {Node: "c", CompressedBytes: 50},
	}}
	if node, size := rep.LargestNodeCompressedBytes(); node != "b" || size != 400 {
		t.Fatalf("the budget is the largest per-node subtotal, not a row or the total: %s %d", node, size)
	}
	var buf bytes.Buffer
	if err := Render(&buf, "table", &Report{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "the largest node (-) needs about 0 B") {
		t.Fatalf("an empty report has no largest node:\n%s", buf.String())
	}
}

// failingWriter accepts writes until one contains marker, then fails that write and every
// later one.
type failingWriter struct {
	err    error
	marker string
	failed bool
}

func (w *failingWriter) Write(p []byte) (int, error) {
	if w.failed || strings.Contains(string(p), w.marker) {
		w.failed = true
		return 0, w.err
	}
	return len(p), nil
}

// A failure while writing the closing SNAPSHOT line, which comes after the table flush, is
// returned rather than reported as a successful render.
func TestRender_TableReturnsTheSnapshotLineWriteError(t *testing.T) {
	broken := errors.New("broken pipe")
	err := Render(&failingWriter{err: broken, marker: "SNAPSHOT"}, "table", sampleReport())
	if !errors.Is(err, broken) {
		t.Fatalf("want the SNAPSHOT write error, got %v", err)
	}
}
