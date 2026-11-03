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
	"fmt"
	"maps"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// segmentInv is a segment unit of the Plan with the given shard statistics.
func segmentInv(catalog commonv1.Catalog, grp, suffix string, shards ...*transferv1.ShardStat) *transferv1.UnitInventory {
	ids := make([]uint32, 0, len(shards))
	for _, s := range shards {
		ids = append(ids, s.GetShardId())
	}
	return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
		Unit:           &transferv1.SegmentUnit{Catalog: catalog, Group: grp, SegmentSuffix: suffix, ShardIds: ids},
		SegmentVersion: "1.3.0",
		Shards:         shards,
	}}}
}

// shardStat is the Plan statistics of one shard; est is the compressed estimate, the raw one is 4x.
func shardStat(id uint32, minTS, maxTS int64, est uint64, parts ...*transferv1.PartStat) *transferv1.ShardStat {
	var total uint64
	for _, p := range parts {
		total += p.GetTotalCount()
	}
	return &transferv1.ShardStat{
		ShardId: id, MinTimestamp: minTS, MaxTimestamp: maxTS,
		EstimatedCompressedBytes: est, EstimatedUncompressedBytes: 4 * est,
		PartsCount: uint32(len(parts)), TotalCount: total, Parts: parts,
	}
}

func partStat(id uint64, minTS, maxTS int64, total uint64) *transferv1.PartStat {
	return &transferv1.PartStat{Id: id, MinTimestamp: minTS, MaxTimestamp: maxTS, TotalCount: total}
}

func propertyInv(grp string, shards ...*transferv1.PropertyShardStat) *transferv1.UnitInventory {
	return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Property{Property: &transferv1.PropertyInventory{Group: grp, Shards: shards}}}
}

// shardFile is a received file of one shard; path is relative to the node's subtree.
func shardFile(node, path string, shard uint32, size uint64, rows map[string]uint64) receivedFile {
	return receivedFile{
		file:      manifest.File{Path: nodesDir + "/" + manifest.NodeDir(node) + "/" + path, Bytes: size, CRC32: manifest.FormatCRC32(uint32(size)), EntryCount: 1},
		shardID:   shard,
		rowCounts: rows,
	}
}

// segmentFile is a received segment-level file. Its shard id stays 0, as the node leaves it.
func segmentFile(node, path string, size uint64, rows map[string]uint64) receivedFile {
	f := shardFile(node, path, 0, size, rows)
	f.segmentLevel = true
	return f
}

const (
	unitStream   = "stream/sw_stream/seg-20261004"
	unitMeasure  = "measure/sw_metric/seg-20261004"
	unitIndex    = "measure/sw_idx/seg-20261005"
	unitReplica  = "stream/sw_rep/seg-20261006"
	unitProperty = "property/ui_template"
)

// countsPlan is a Plan of two nodes:
//   - sw_stream: one stream segment; data-a holds shards 0 and 1, data-b shard 2, each shard
//     with several parts;
//   - sw_metric: one measure segment; data-a holds shard 0, data-b shard 1;
//   - sw_idx: an index-mode measure segment on data-a, its rows in segment-level files;
//   - sw_rep: a stream segment whose shard 0 is replicated on both nodes;
//   - ui_template: a property group held by data-a in stage warm and by data-b in stage hot.
//
// data-b's frame comes first and data-a's units come in two frames of different stages.
func countsPlan() *PlanResult {
	return &PlanResult{Frames: []*transferv1.UnitFrame{
		{NodeId: nodeB, Stage: "hot", Units: []*transferv1.UnitInventory{
			segmentInv(commonv1.Catalog_CATALOG_STREAM, "sw_stream", "20261004",
				shardStat(2, 900, 1800, 2048, partStat(31, 900, 1300, 40), partStat(32, 1300, 1800, 45))),
			segmentInv(commonv1.Catalog_CATALOG_MEASURE, "sw_metric", "20261004",
				shardStat(1, 1050, 1950, 512, partStat(51, 1050, 1950, 40))),
			segmentInv(commonv1.Catalog_CATALOG_STREAM, "sw_rep", "20261006",
				shardStat(0, 2000, 2500, 256, partStat(61, 2000, 2500, 25))),
			propertyInv("ui_template", &transferv1.PropertyShardStat{ShardId: 2, EstimatedBytes: 300, DocCount: 7}),
		}},
		{NodeId: nodeA, Stage: "warm", Units: []*transferv1.UnitInventory{
			propertyInv("ui_template",
				&transferv1.PropertyShardStat{ShardId: 0, EstimatedBytes: 1000, DocCount: 30},
				&transferv1.PropertyShardStat{ShardId: 1, EstimatedBytes: 500, DocCount: 12}),
		}},
		{NodeId: nodeA, Stage: "", Units: []*transferv1.UnitInventory{
			segmentInv(commonv1.Catalog_CATALOG_STREAM, "sw_stream", "20261004",
				shardStat(0, 1000, 1900, 4096, partStat(11, 1000, 1500, 100), partStat(12, 1400, 1900, 50)),
				shardStat(1, 1100, 2000, 1024, partStat(21, 1100, 2000, 70))),
			segmentInv(commonv1.Catalog_CATALOG_MEASURE, "sw_metric", "20261004",
				shardStat(0, 1000, 2000, 1024, partStat(41, 1000, 1600, 50), partStat(42, 1500, 2000, 30))),
			func() *transferv1.UnitInventory {
				inv := segmentInv(commonv1.Catalog_CATALOG_MEASURE, "sw_idx", "20261005",
					shardStat(0, 3000, 3500, 0), shardStat(1, 3100, 3900, 0))
				inv.GetSegment().SegmentLevel = &transferv1.SidxStat{EstimatedBytes: 2048, DocCount: 90}
				return inv
			}(),
			segmentInv(commonv1.Catalog_CATALOG_STREAM, "sw_rep", "20261006",
				shardStat(0, 2000, 2500, 256, partStat(61, 2000, 2500, 25))),
		}},
	}}
}

// countsFiles are the files every unit received, by node and unit label. The stream files
// count every row of their parts; the measure files count fewer rows than the parts, as a
// measure export drops the duplicated data points.
func countsFiles() map[string]map[string][]receivedFile {
	return map[string]map[string][]receivedFile{
		nodeA: {
			unitStream: {
				shardFile(nodeA, "stream/sw_stream/seg-20261004/shard-0-00001.csv.gz", 0, 700, map[string]uint64{"service_log": 80, "segment": 20}),
				shardFile(nodeA, "stream/sw_stream/seg-20261004/shard-0-00002.csv.gz", 0, 300, map[string]uint64{"service_log": 40, "segment": 10}),
				shardFile(nodeA, "stream/sw_stream/seg-20261004/shard-1-00001.csv.gz", 1, 200, map[string]uint64{"service_log": 30}),
				shardFile(nodeA, "stream/sw_stream/seg-20261004/shard-1-00002.csv.gz", 1, 150, map[string]uint64{"service_log": 25, "segment": 15}),
			},
			unitMeasure: {
				shardFile(nodeA, "measure/sw_metric/seg-20261004/shard-0-00001.csv.gz", 0, 500, map[string]uint64{"service_cpm": 30, "endpoint_cpm": 20}),
				shardFile(nodeA, "measure/sw_metric/seg-20261004/shard-0-00002.csv.gz", 0, 100, map[string]uint64{"service_cpm": 15}),
			},
			unitIndex: {
				segmentFile(nodeA, "measure/sw_idx/seg-20261005/segment-00001.csv.gz", 900, map[string]uint64{"endpoint_traffic": 50, "service_traffic": 10}),
				segmentFile(nodeA, "measure/sw_idx/seg-20261005/segment-00002.csv.gz", 400, map[string]uint64{"endpoint_traffic": 30}),
			},
			unitReplica: {
				shardFile(nodeA, "stream/sw_rep/seg-20261006/shard-0-00001.csv.gz", 0, 111, map[string]uint64{"service_log": 25}),
			},
			unitProperty: {
				shardFile(nodeA, "property/ui_template/shard-0-00001.csv.gz", 0, 600, map[string]uint64{"ui_template": 20}),
				shardFile(nodeA, "property/ui_template/shard-0-00002.csv.gz", 0, 250, map[string]uint64{"ui_template": 10}),
				shardFile(nodeA, "property/ui_template/shard-1-00001.csv.gz", 1, 310, map[string]uint64{"ui_template": 12}),
			},
		},
		nodeB: {
			unitStream: {
				shardFile(nodeB, "stream/sw_stream/seg-20261004/shard-2-00001.csv.gz", 2, 400, map[string]uint64{"segment": 40}),
				shardFile(nodeB, "stream/sw_stream/seg-20261004/shard-2-00002.csv.gz", 2, 250, map[string]uint64{"segment": 5, "service_log": 20}),
				shardFile(nodeB, "stream/sw_stream/seg-20261004/shard-2-00003.csv.gz", 2, 50, map[string]uint64{"service_log": 20}),
			},
			unitMeasure: {
				shardFile(nodeB, "measure/sw_metric/seg-20261004/shard-1-00001.csv.gz", 1, 333, map[string]uint64{"service_cpm": 40}),
			},
			unitReplica: {
				shardFile(nodeB, "stream/sw_rep/seg-20261006/shard-0-00001.csv.gz", 0, 112, map[string]uint64{"service_log": 25}),
			},
			unitProperty: {
				shardFile(nodeB, "property/ui_template/shard-2-00001.csv.gz", 2, 120, map[string]uint64{"ui_template": 7}),
			},
		},
	}
}

// countsElapsed is the time each unit took, by node and unit label; sw_rep took none.
var countsElapsed = map[string]map[string]time.Duration{
	nodeA: {
		unitStream: 20 * time.Second, unitMeasure: 59*time.Second + 949*time.Millisecond,
		unitIndex: 59*time.Second + 950*time.Millisecond, unitProperty: time.Minute,
	},
	nodeB: {unitStream: 89*time.Second + 500*time.Millisecond, unitMeasure: 45*time.Second + 500*time.Millisecond, unitProperty: 2040 * time.Millisecond},
}

// countsJobs runs buildJobs on countsPlan and gives every unit its files, its time and, for
// data-b's stream unit, a warning, as the workers would.
func countsJobs(t *testing.T) []*nodeJob {
	t.Helper()
	jobs, err := buildJobs(countsPlan())
	if err != nil {
		t.Fatal(err)
	}
	files := countsFiles()
	attached := 0
	for _, job := range jobs {
		for _, u := range job.units {
			u.files = files[job.nodeID][u.label()]
			u.elapsed = countsElapsed[job.nodeID][u.label()]
			if job.nodeID == nodeB && u.label() == unitStream {
				u.warnings = []string{"shard 2 changed while it was read"}
			}
			attached += len(u.files)
		}
	}
	if attached != 18 {
		t.Fatalf("the jobs hold %d files, want all 18", attached)
	}
	return jobs
}

// countsManifest is the manifest of countsJobs.
func countsManifest(t *testing.T) (*manifest.Manifest, []*nodeJob) {
	t.Helper()
	jobs := countsJobs(t)
	m := baseManifest(transferv1.Compress_COMPRESS_GZIP, []*commonv1.Group{
		group("sw_stream", commonv1.Catalog_CATALOG_STREAM), group("sw_metric", commonv1.Catalog_CATALOG_MEASURE),
		group("sw_idx", commonv1.Catalog_CATALOG_MEASURE), group("sw_rep", commonv1.Catalog_CATALOG_STREAM),
		group("ui_template", commonv1.Catalog_CATALOG_PROPERTY),
	}, nil)
	cluster := &ClusterInfo{DataNodes: []*databasev1.Node{
		{Metadata: &commonv1.Metadata{Name: nodeA}, TzName: "Asia/Shanghai"},
		{Metadata: &commonv1.Metadata{Name: nodeB}, TzName: "UTC"},
	}}
	fillManifest(m, cluster, jobs)
	return m, jobs
}

// unitOf returns the manifest unit of (catalog, group, segment) and fails without exactly one.
func unitOf(t *testing.T, m *manifest.Manifest, catalog, grp, segment string) manifest.Unit {
	t.Helper()
	var found []manifest.Unit
	for _, u := range m.Units {
		if u.Catalog == catalog && u.Group == grp && u.Segment == segment {
			found = append(found, u)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one unit %s/%s/%s, got %d: %+v", catalog, grp, segment, len(found), found)
	}
	return found[0]
}

// shardSummary renders the counts of one manifest shard.
func shardSummary(s manifest.Shard) string {
	parts := make([]string, 0, len(s.Parts))
	for _, p := range s.Parts {
		parts = append(parts, fmt.Sprintf("%d[%d,%d]=%d", p.ID, p.MinTimestamp, p.MaxTimestamp, p.TotalCount))
	}
	return fmt.Sprintf("shard %d [%d,%d] est %d/%d docs %d rows %v total %d parts %v files %d",
		s.Shard, s.MinTimestamp, s.MaxTimestamp, s.EstimatedCompressedBytes, s.EstimatedUncompressedBytes, s.DocCount,
		s.RowCounts, s.TotalCount, parts, len(s.Files))
}

// checkSources compares every source's node, shards and segment-level counts with want, one
// string per source: the node, its segment-level rows and total, then one line per shard.
func checkSources(t *testing.T, u manifest.Unit, want []string) {
	t.Helper()
	got := make([]string, 0, len(u.Sources))
	for _, src := range u.Sources {
		lines := []string{fmt.Sprintf("%s segment-level rows %v total %d files %d", src.Node, src.RowCounts, src.TotalCount, len(src.SegmentFiles))}
		for _, s := range src.Shards {
			lines = append(lines, shardSummary(s))
		}
		got = append(got, strings.Join(lines, "\n"))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources of %s/%s/%s:\ngot\n%s\nwant\n%s", u.Catalog, u.Group, u.Segment,
			strings.Join(got, "\n--\n"), strings.Join(want, "\n--\n"))
	}
}

// sourceRows is the number of rows a source exported: its segment-level rows and its shards'.
func sourceRows(src manifest.UnitSource) uint64 {
	n := src.TotalCount
	for _, s := range src.Shards {
		n += s.TotalCount
	}
	return n
}

// unitRows sums sourceRows over the unit's sources.
func unitRows(u manifest.Unit) uint64 {
	var n uint64
	for _, src := range u.Sources {
		n += sourceRows(src)
	}
	return n
}

// buildJobs folds both of data-a's frames into one job, keeps data-b's units in its frame
// order and normalizes the empty stage.
func TestBuildJobs_FoldsFramesPerNodeInNodeOrder(t *testing.T) {
	jobs, err := buildJobs(countsPlan())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, job := range jobs {
		for _, u := range job.units {
			got = append(got, job.nodeID+" "+job.dir+" "+u.stage+" "+u.label())
		}
	}
	want := []string{
		nodeA + " data-a-17912-d20a13e5 warm " + unitProperty,
		nodeA + " data-a-17912-d20a13e5 hot " + unitStream,
		nodeA + " data-a-17912-d20a13e5 hot " + unitMeasure,
		nodeA + " data-a-17912-d20a13e5 hot " + unitIndex,
		nodeA + " data-a-17912-d20a13e5 hot " + unitReplica,
		nodeB + " data-b-17912-9378f780 hot " + unitStream,
		nodeB + " data-b-17912-9378f780 hot " + unitMeasure,
		nodeB + " data-b-17912-9378f780 hot " + unitReplica,
		nodeB + " data-b-17912-9378f780 hot " + unitProperty,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("jobs:\ngot  %q\nwant %q", got, want)
	}
}

// A stream unit of several shards on two nodes, each shard with rolled files: every shard
// counts the rows of its own files, copies its parts from the Plan, and the sources and the
// unit add up.
func TestFillManifest_RolledFilesOfManyShardsAndNodes(t *testing.T) {
	m, _ := countsManifest(t)
	if len(m.Units) != 5 {
		t.Fatalf("want 5 units, got %d", len(m.Units))
	}
	u := unitOf(t, m, "STREAM", "sw_stream", "20261004")
	if u.Stage != "hot" || u.SegmentVersion != "1.3.0" || u.MinTimestamp != 900 || u.MaxTimestamp != 2000 {
		t.Fatalf("unit header: %+v", u)
	}
	checkSources(t, u, []string{
		nodeA + " segment-level rows map[] total 0 files 0\n" +
			"shard 0 [1000,1900] est 4096/16384 docs 0 rows map[segment:30 service_log:120] total 150 parts [11[1000,1500]=100 12[1400,1900]=50] files 2\n" +
			"shard 1 [1100,2000] est 1024/4096 docs 0 rows map[segment:15 service_log:55] total 70 parts [21[1100,2000]=70] files 2",
		nodeB + " segment-level rows map[] total 0 files 0\n" +
			"shard 2 [900,1800] est 2048/8192 docs 0 rows map[segment:45 service_log:40] total 85 parts [31[900,1300]=40 32[1300,1800]=45] files 3",
	})
	if a, b := sourceRows(u.Sources[0]), sourceRows(u.Sources[1]); a != 220 || b != 85 || unitRows(u) != 305 {
		t.Fatalf("rows per source %d, %d and per unit %d; want 220, 85 and 305", a, b, unitRows(u))
	}
	// The files keep their order of arrival within a shard.
	if p := u.Sources[0].Shards[0].Files[1].Path; p != "nodes/data-a-17912-d20a13e5/stream/sw_stream/seg-20261004/shard-0-00002.csv.gz" {
		t.Fatalf("second file of shard 0: %s", p)
	}

	mu := unitOf(t, m, "MEASURE", "sw_metric", "20261004")
	checkSources(t, mu, []string{
		nodeA + " segment-level rows map[] total 0 files 0\n" +
			"shard 0 [1000,2000] est 1024/4096 docs 0 rows map[endpoint_cpm:20 service_cpm:45] total 65 parts [41[1000,1600]=50 42[1500,2000]=30] files 2",
		nodeB + " segment-level rows map[] total 0 files 0\n" +
			"shard 1 [1050,1950] est 512/2048 docs 0 rows map[service_cpm:40] total 40 parts [51[1050,1950]=40] files 1",
	})
	if unitRows(mu) != 105 {
		t.Fatalf("measure unit rows %d, want 105", unitRows(mu))
	}

	wantNodes := []manifest.Node{
		{ID: nodeA, Dir: "data-a-17912-d20a13e5", Timezone: "Asia/Shanghai"},
		{ID: nodeB, Dir: "data-b-17912-9378f780", Timezone: "UTC"},
	}
	if !reflect.DeepEqual(m.Source.Nodes, wantNodes) {
		t.Fatalf("source nodes %+v", m.Source.Nodes)
	}
	wantWarnings := []string{schemaSkippedWarning, "node " + nodeB + " unit " + unitStream + ": shard 2 changed while it was read"}
	if !reflect.DeepEqual(m.Warnings, wantWarnings) {
		t.Fatalf("warnings %q", m.Warnings)
	}
}

// An index-mode segment counts its segment-level files on the source; its shards keep the
// Plan statistics but count no row, so no row is counted twice.
func TestFillManifest_IndexModeSegmentFilesCountOnTheSource(t *testing.T) {
	m, _ := countsManifest(t)
	u := unitOf(t, m, "MEASURE", "sw_idx", "20261005")
	if u.MinTimestamp != 3000 || u.MaxTimestamp != 3900 || len(u.Sources) != 1 {
		t.Fatalf("unit: %+v", u)
	}
	checkSources(t, u, []string{
		nodeA + " segment-level rows map[endpoint_traffic:80 service_traffic:10] total 90 files 2\n" +
			"shard 0 [3000,3500] est 0/0 docs 0 rows map[] total 0 parts [] files 0\n" +
			"shard 1 [3100,3900] est 0/0 docs 0 rows map[] total 0 parts [] files 0",
	})
	src := u.Sources[0]
	if !maps.Equal(src.RowCounts, map[string]uint64{"endpoint_traffic": 80, "service_traffic": 10}) || sourceRows(src) != 90 {
		t.Fatalf("source rows %v total %d, sum %d; want 90", src.RowCounts, src.TotalCount, sourceRows(src))
	}
	if got := []string{src.SegmentFiles[0].Path, src.SegmentFiles[1].Path}; got[0] != "nodes/data-a-17912-d20a13e5/measure/sw_idx/seg-20261005/segment-00001.csv.gz" ||
		got[1] != "nodes/data-a-17912-d20a13e5/measure/sw_idx/seg-20261005/segment-00002.csv.gz" {
		t.Fatalf("segment files %v", got)
	}
}

// A property group held by two nodes in different stages is one unit, with one source per
// node, the stage of the first source in node order (not in frame order) and each shard's
// docCount from the Plan.
func TestFillManifest_PropertyGroupAcrossStagesIsOneUnit(t *testing.T) {
	m, _ := countsManifest(t)
	u := unitOf(t, m, "PROPERTY", "ui_template", "")
	if u.Stage != "warm" || u.SegmentVersion != "" || u.MinTimestamp != 0 || u.MaxTimestamp != 0 {
		t.Fatalf("unit header: %+v", u)
	}
	checkSources(t, u, []string{
		nodeA + " segment-level rows map[] total 0 files 0\n" +
			"shard 0 [0,0] est 1000/1000 docs 30 rows map[ui_template:30] total 30 parts [] files 2\n" +
			"shard 1 [0,0] est 500/500 docs 12 rows map[ui_template:12] total 12 parts [] files 1",
		nodeB + " segment-level rows map[] total 0 files 0\n" +
			"shard 2 [0,0] est 300/300 docs 7 rows map[ui_template:7] total 7 parts [] files 1",
	})
	if unitRows(u) != 49 {
		t.Fatalf("property unit rows %d, want 49", unitRows(u))
	}
}

// The same shard exported from two replicas stays two sources, each with its own files and
// counts; the importer decides which one to use.
func TestFillManifest_ReplicatedShardKeepsBothSources(t *testing.T) {
	m, _ := countsManifest(t)
	u := unitOf(t, m, "STREAM", "sw_rep", "20261006")
	checkSources(t, u, []string{
		nodeA + " segment-level rows map[] total 0 files 0\n" +
			"shard 0 [2000,2500] est 256/1024 docs 0 rows map[service_log:25] total 25 parts [61[2000,2500]=25] files 1",
		nodeB + " segment-level rows map[] total 0 files 0\n" +
			"shard 0 [2000,2500] est 256/1024 docs 0 rows map[service_log:25] total 25 parts [61[2000,2500]=25] files 1",
	})
	if a, b := u.Sources[0].Shards[0].Files[0], u.Sources[1].Shards[0].Files[0]; a.Path == b.Path || a.Bytes != 111 || b.Bytes != 112 {
		t.Fatalf("each replica keeps its own file: %+v %+v", a, b)
	}
	if unitRows(u) != 50 {
		t.Fatalf("replicated unit rows %d, want 50 (both copies)", unitRows(u))
	}
}

// elapsedOf sums the units' time as exportSession does.
func elapsedOf(jobs []*nodeJob) map[string]time.Duration {
	elapsed := map[string]time.Duration{}
	for _, job := range jobs {
		for _, u := range job.units {
			elapsed[elapsedKey(job.nodeID, catalogLabel(u.catalog()), u.group())] += u.elapsed
		}
	}
	return elapsed
}

// fieldsOf splits every line of the summary into its whitespace-separated cells.
func fieldsOf(s string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		out = append(out, strings.Join(strings.Fields(line), " "))
	}
	return out
}

// countsSummary is the summary of countsManifest with countsElapsed and a total of 3m4.96s.
var countsSummary = []string{
	"NODE CATALOG GROUP STAGE UNITS FILES ROWS SIZE TIME",
	nodeA + " measure sw_idx hot 1 2 90 1.3 KiB 1m0s",
	nodeA + " measure sw_metric hot 1 2 65 600 B 59.9s",
	nodeA + " property ui_template warm 1 3 42 1.1 KiB 1m0s",
	nodeA + " stream sw_rep hot 1 1 25 111 B 0.0s",
	nodeA + " stream sw_stream hot 1 4 220 1.3 KiB 20.0s",
	nodeB + " measure sw_metric hot 1 1 40 333 B 45.5s",
	nodeB + " property ui_template warm 1 1 7 120 B 2.0s",
	nodeB + " stream sw_rep hot 1 1 25 112 B 0.0s",
	nodeB + " stream sw_stream hot 1 3 85 700 B 1m30s",
	"",
	"export written to /out: 5 unit(s), 18 file(s), 5.7 KiB, 2 warning(s) in 3m5s",
}

// renderSummary prints rows, files and bytes per (node, catalog, group, stage), takes each
// row's time from elapsed by elapsedKey with the manifest's catalog label, shows 0.0s for a
// group whose units took no time, and adds up the totals line.
func TestRenderSummary_CountsAndTimesOfTheManifest(t *testing.T) {
	m, jobs := countsManifest(t)
	elapsed := elapsedOf(jobs)
	if d, ok := elapsed[elapsedKey(nodeA, "STREAM", "sw_rep")]; !ok || d != 0 {
		t.Fatalf("sw_rep on data-a took %v (present %v), want 0", d, ok)
	}
	if _, ok := elapsed[elapsedKey(nodeA, "stream", "sw_rep")]; ok {
		t.Fatal("elapsed is keyed by the manifest catalog label, not the lower-case name")
	}
	var out bytes.Buffer
	if err := renderSummary(&out, "/out", m, elapsed, 3*time.Minute+4960*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := fieldsOf(out.String()); !reflect.DeepEqual(got, countsSummary) {
		t.Fatalf("summary:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(countsSummary, "\n"))
	}

	// Without any elapsed entry every row shows no time, and the counts do not change.
	out.Reset()
	if err := renderSummary(&out, "/out", m, nil, 0); err != nil {
		t.Fatal(err)
	}
	for i, line := range fieldsOf(out.String()) {
		want := countsSummary[i]
		if i > 0 && i < len(countsSummary)-2 {
			want = want[:strings.LastIndex(want, " ")] + " 0.0s"
		}
		if i == len(countsSummary)-1 {
			want = strings.TrimSuffix(want, "3m5s") + "0.0s"
		}
		if line != want {
			t.Fatalf("line %d without elapsed: %q, want %q", i, line, want)
		}
	}
}

// Durations round to a tenth of a second below a minute and to whole seconds from a minute
// on: 59.95s rounds up into the minute.
func TestFormatDuration_AroundTheMinute(t *testing.T) {
	for _, c := range []struct {
		want string
		d    time.Duration
	}{
		{d: 59*time.Second + 900*time.Millisecond, want: "59.9s"},
		{d: 59*time.Second + 949*time.Millisecond, want: "59.9s"},
		{d: 59*time.Second + 950*time.Millisecond, want: "1m0s"},
		{d: time.Minute, want: "1m0s"},
		{d: time.Minute + 449*time.Millisecond, want: "1m0s"},
		{d: time.Minute + 450*time.Millisecond, want: "1m1s"},
		{d: 89*time.Second + 500*time.Millisecond, want: "1m30s"},
	} {
		if got := formatDuration(c.d); got != c.want {
			t.Fatalf("formatDuration(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

// The manifest of all the units above reads back unchanged, renders the same summary, and
// its per-shard totals agree with the parts' totals for a stream unit and are at most them
// for a measure unit.
func TestManifestCounts_SurviveWriteAndRead(t *testing.T) {
	m, jobs := countsManifest(t)
	path := filepath.Join(t.TempDir(), manifest.FileName)
	if err := manifest.Write(path, m); err != nil {
		t.Fatal(err)
	}
	got, err := manifest.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("read back differs:\ngot  %+v\nwant %+v", got, m)
	}
	var out bytes.Buffer
	if err = renderSummary(&out, "/out", got, elapsedOf(jobs), 3*time.Minute+4960*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if lines := fieldsOf(out.String()); !reflect.DeepEqual(lines, countsSummary) {
		t.Fatalf("summary of the read manifest:\n%s", strings.Join(lines, "\n"))
	}

	checked := map[string]int{}
	for _, u := range got.Units {
		for _, src := range u.Sources {
			for _, s := range src.Shards {
				var parts, rows uint64
				for _, p := range s.Parts {
					parts += p.TotalCount
				}
				for _, n := range s.RowCounts {
					rows += n
				}
				if rows != s.TotalCount {
					t.Fatalf("%s/%s %s shard %d: rowCounts add up to %d, totalCount is %d", u.Catalog, u.Group, src.Node, s.Shard, rows, s.TotalCount)
				}
				switch u.Catalog {
				case "STREAM":
					if s.TotalCount != parts {
						t.Fatalf("stream %s %s shard %d: totalCount %d, parts %d", u.Group, src.Node, s.Shard, s.TotalCount, parts)
					}
				case "MEASURE":
					if s.TotalCount > parts {
						t.Fatalf("measure %s %s shard %d: totalCount %d above the parts' %d", u.Group, src.Node, s.Shard, s.TotalCount, parts)
					}
				}
				checked[u.Catalog]++
			}
		}
	}
	if want := map[string]int{"STREAM": 5, "MEASURE": 4, "PROPERTY": 3}; !maps.Equal(checked, want) {
		t.Fatalf("checked shards %v, want %v", checked, want)
	}
}
