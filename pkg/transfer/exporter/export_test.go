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
	"bufio"
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/schemasync"
)

const (
	sessID = "feedfeed"
	nodeA  = "data-a:17912"
	nodeB  = "data-b:17912"
)

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	b  bytes.Buffer
	mu sync.Mutex
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func md(group, name string, rev int64) *commonv1.Metadata {
	return &commonv1.Metadata{Group: group, Name: name, ModRevision: rev}
}

func group(name string, catalog commonv1.Catalog) *commonv1.Group {
	day := func(n uint32) *commonv1.IntervalRule {
		return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
	}
	return &commonv1.Group{Metadata: &commonv1.Metadata{Name: name}, Catalog: catalog, ResourceOpts: &commonv1.ResourceOpts{
		ShardNum: 2, SegmentInterval: day(1), Ttl: day(7), Replicas: 1,
		Stages: []*commonv1.LifecycleStage{{Name: "warm", ShardNum: 1, SegmentInterval: day(3), Ttl: day(30)}},
	}}
}

// exportFake is a liaison with a measure, a stream and a property group: data-a holds a
// measure segment (shard 0), a stream segment (shards 0, 1) and property shards 0-1; data-b
// holds a replica of the measure segment.
func exportFake() *fakeLiaison {
	f := newFakeLiaison()
	f.dataNodes[0].TzName = "Asia/Shanghai"
	f.groups = []*commonv1.Group{
		group("sw_metric", commonv1.Catalog_CATALOG_MEASURE),
		group("sw_record", commonv1.Catalog_CATALOG_STREAM),
		group("ui", commonv1.Catalog_CATALOG_PROPERTY),
		group("unselected", commonv1.Catalog_CATALOG_TRACE),
	}
	f.schema.measures = []*databasev1.Measure{
		{Metadata: md("sw_metric", "service_cpm", 7)},
		{Metadata: md("sw_metric", "service_idx", 8), IndexMode: true},
		// A node encodes no column of the internal Top-N measure, so it reports no revision of it.
		{Metadata: md("sw_metric", csvfmt.TopNResultMeasure, 9)},
	}
	f.schema.streams = []*databasev1.Stream{{Metadata: md("sw_record", "log", 3)}}
	f.schema.properties = []*databasev1.Property{{Metadata: md("ui", "template", 5)}}
	f.schema.traces = []*databasev1.Trace{{Metadata: md("unselected", "segment", 9)}}
	f.schema.rules = []*databasev1.IndexRule{{Metadata: md("sw_metric", "r1", 1)}}
	f.schema.bindings = []*databasev1.IndexRuleBinding{{Metadata: md("sw_record", "b1", 1)}}
	f.schema.topNs = []*databasev1.TopNAggregation{{Metadata: md("sw_metric", "top", 1)}}

	measureA := unitFrame(nodeA, "", "sw_metric", 100, 200, 4)
	segmentOf(measureA).Shards[0].Parts = []*transferv1.PartStat{{Id: 1, MinTimestamp: 100, MaxTimestamp: 200, TotalCount: 4}}
	stream := unitFrame(nodeA, "", "sw_record", 50, 300, 6)
	streamSeg := segmentOf(stream)
	streamSeg.Unit.Catalog = commonv1.Catalog_CATALOG_STREAM
	streamSeg.Unit.ShardIds = []uint32{0, 1}
	streamSeg.Shards = append(streamSeg.Shards, &transferv1.ShardStat{ShardId: 1, MinTimestamp: 60, MaxTimestamp: 300, TotalCount: 3})
	stream.GetUnits().Units = append(stream.GetUnits().Units, &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Property{
		Property: &transferv1.PropertyInventory{Group: "ui", Shards: []*transferv1.PropertyShardStat{
			{ShardId: 0, DocCount: 2, EstimatedBytes: 10}, {ShardId: 1, DocCount: 2, EstimatedBytes: 10},
		}},
	}})
	measureB := unitFrame(nodeB, "", "sw_metric", 120, 220, 4)
	f.planFrames = []*transferv1.PlanResponse{measureA, stream, measureB}
	return f
}

// testOptions are the settings of a test export plus where it prints.
type testOptions struct {
	Log      io.Writer
	Out      io.Writer
	Terminal func() (int, int)
	Confirm  func(context.Context, string) bool
	config.Export
}

func exportOptions(t *testing.T, _ *fakeLiaison) (testOptions, *syncBuffer) {
	t.Helper()
	log := &syncBuffer{}
	return testOptions{Log: log, Out: log, Export: config.Export{
		Selectors: []*transferv1.Selector{
			{Catalog: commonv1.Catalog_CATALOG_MEASURE}, {Catalog: commonv1.Catalog_CATALOG_STREAM}, {Catalog: commonv1.Catalog_CATALOG_PROPERTY},
		},
		Output:           filepath.Join(t.TempDir(), "out"),
		Format:           transferv1.Format_FORMAT_CSV,
		Compress:         transferv1.Compress_COMPRESS_NONE,
		MaxFileSize:      512 << 20,
		Parallelism:      8,
		NodeRateLimit:    1 << 20,
		NodeRowRateLimit: 50000,
		NodeIdleTimeout:  10 * time.Second,
		IncludeSchema:    true,
	}}, log
}

// envFor serves every fake as a liaison, the first being the primary, and returns the
// environment of an export through them.
func envFor(t *testing.T, opts testOptions, fakes ...*fakeLiaison) Env {
	t.Helper()
	liaisons := &Liaisons{}
	for _, f := range fakes {
		addr := serveLiaison(t, f)
		liaisons.All = append(liaisons.All, &Liaison{Addr: addr, Conn: dial(t, addr)})
	}
	return Env{
		Log: opts.Log, Out: opts.Out, Terminal: opts.Terminal, Confirm: opts.Confirm, Liaisons: liaisons, Groups: fakes[0].groups,
		Cluster: &ClusterInfo{Liaison: liaisons.All[0].Addr, DataNodes: fakes[0].dataNodes},
	}
}

func runExport(t *testing.T, f *fakeLiaison, opts testOptions) error {
	t.Helper()
	return runExportVia(t, opts, f)
}

func runExportVia(t *testing.T, opts testOptions, fakes ...*fakeLiaison) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return RunExport(ctx, envFor(t, opts, fakes...), opts.Export)
}

// shortHeartbeat makes the export heartbeat tick every d for one test.
func shortHeartbeat(t *testing.T, d time.Duration) {
	saved := heartbeatInterval
	heartbeatInterval = d
	t.Cleanup(func() { heartbeatInterval = saved })
}

func requireExit(t *testing.T, err error, code int, contains string) {
	t.Helper()
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != code || !strings.Contains(err.Error(), contains) {
		t.Fatalf("want exit %d containing %q, got %v", code, contains, err)
	}
}

// requireKept checks a failed run kept the session: no release call and the release hint.
func requireKept(t *testing.T, f *fakeLiaison, log *syncBuffer) {
	t.Helper()
	if got := f.releasedIDs(); len(got) != 0 {
		t.Fatalf("a failed export must keep the session, released %v", got)
	}
	if !strings.Contains(log.String(), "export session feedfeed kept; release it with: bydbctl data release-session --id feedfeed") {
		t.Fatalf("the release hint is missing:\n%s", log)
	}
}

func checkFile(t *testing.T, root string, f manifest.File) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f.Path)))
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(body)) != f.Bytes || manifest.FormatCRC32(crc32.ChecksumIEEE(body)) != f.CRC32 {
		t.Fatalf("%s: %d bytes %s on disk, manifest says %d %s", f.Path, len(body), manifest.FormatCRC32(crc32.ChecksumIEEE(body)), f.Bytes, f.CRC32)
	}
}

func TestRunExport_WritesFilesAndAConsistentManifest(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	checkManifestHeader(t, m)
	checkManifestUnits(t, m, opts.Output)
	if got := f.releasedIDs(); len(got) != 1 || got[0] != sessID {
		t.Fatalf("a finished export releases its session, released %v", got)
	}
	checkExportRequests(t, f)
	for _, want := range []string{
		"export session feedfeed created",
		"rate limits: node 1048576 bytes/s, 50000 rows/s; liaison 0 bytes/s per liaison (0 = unlimited)",
		"  " + nodeA + "  [3/3] property/ui started: 2 shard(s), 4 part(s)\n",
		"✓ " + nodeA + "  done: 3 unit(s), 5 file(s), 10 rows, ",
		"✓ " + nodeB + "  done: 1 unit(s), 1 file(s), 2 rows, ",
		"export written to",
	} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q:\n%s", want, log)
		}
	}
	_ = filepath.WalkDir(opts.Output, func(p string, _ os.DirEntry, _ error) error {
		if strings.HasSuffix(p, ".tmp") {
			t.Fatalf("leftover %s", p)
		}
		return nil
	})
}

func checkManifestHeader(t *testing.T, m *manifest.Manifest) {
	t.Helper()
	if m.Format != "csv" || m.Compress != "none" || m.ArtifactVersion != manifest.ArtifactVersion || m.SchemaOnly {
		t.Fatalf("header = %+v", m)
	}
	nodes := m.Source.Nodes
	if len(nodes) != 2 || nodes[0].ID != nodeA || nodes[0].Dir != manifest.NodeDir(nodeA) || nodes[0].Timezone != "Asia/Shanghai" {
		t.Fatalf("source nodes = %+v", nodes)
	}
	hot := manifest.Stage{Name: "hot", ShardNum: 2, SegmentInterval: "1d", TTL: "7d", Replicas: 1}
	if len(m.Groups) != 3 || m.Groups[0].Name != "sw_metric" || !m.Groups[0].IndexMode || m.Groups[1].IndexMode ||
		len(m.Groups[0].Stages) != 2 || m.Groups[0].Stages[0] != hot || m.Groups[0].Stages[1].Name != "warm" {
		t.Fatalf("groups = %+v", m.Groups)
	}
	if len(m.SchemaFiles) != len(schemasync.Kinds) {
		t.Fatalf("schema files = %+v", m.SchemaFiles)
	}
}

// checkManifestUnits asserts every shard's rowCounts add up to its totalCount and every
// registered file (data and schema) exists with the registered size and CRC-32.
func checkManifestUnits(t *testing.T, m *manifest.Manifest, root string) {
	t.Helper()
	if len(m.Units) != 3 {
		t.Fatalf("units = %+v", m.Units)
	}
	files := 0
	for _, u := range m.Units {
		for _, src := range u.Sources {
			for _, sh := range src.Shards {
				var sum uint64
				for _, n := range sh.RowCounts {
					sum += n
				}
				if sum != sh.TotalCount || sh.TotalCount == 0 || len(sh.Files) != 1 {
					t.Fatalf("unit %s/%s node %s shard %d: rowCounts %v totalCount %d files %d", u.Catalog, u.Group, src.Node, sh.Shard, sh.RowCounts, sh.TotalCount, len(sh.Files))
				}
				for _, file := range sh.Files {
					checkFile(t, root, file)
					files++
				}
			}
		}
	}
	if files != 6 {
		t.Fatalf("want 6 files (measure x2, stream x2, property x2), got %d", files)
	}
	measure := m.Units[0]
	if measure.Catalog != "MEASURE" || measure.Segment != "20260930" || measure.SegmentVersion != "1.5.0" || len(measure.Sources) != 2 ||
		measure.MinTimestamp != 100 || measure.MaxTimestamp != 220 || len(measure.Sources[0].Shards[0].Parts) != 1 {
		t.Fatalf("measure unit = %+v", measure)
	}
	if prop := m.Units[1]; prop.Catalog != "PROPERTY" || prop.Segment != "" || prop.Sources[0].Shards[1].DocCount != 2 {
		t.Fatalf("property unit = %+v", prop)
	}
	for _, sf := range m.SchemaFiles {
		checkFile(t, root, sf)
	}
}

func checkExportRequests(t *testing.T, f *fakeLiaison) {
	t.Helper()
	if len(f.exportInits) != 2 {
		t.Fatalf("one Export stream per node, got %d", len(f.exportInits))
	}
	for _, in := range f.exportInits {
		if in.GetSessionId() != sessID || in.GetFormat() != transferv1.Format_FORMAT_CSV || in.GetMaxFileSize() != 512<<20 ||
			in.GetRateLimit().GetBytesPerSecond() != 1<<20 || in.GetRateLimit().GetRowsPerSecond() != 50000 {
			t.Fatalf("init = %+v", in)
		}
	}
	if units := f.exportUnits[nodeA]; len(units) != 3 || units[2].GetProperty().GetGroup() != "ui" || len(units[2].GetProperty().GetShardIds()) != 2 {
		t.Fatalf("data-a units = %v", units)
	}
}

// scripted runs fn for every unit request instead of the default files.
func scripted(fn func(f *fakeLiaison, init *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error) func(
	*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer,
) error {
	return func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if err := sendReady(init, stream); err != nil {
			return err
		}
		for {
			req, err := f.recvUnit(init, stream)
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if err = fn(f, init, req, stream); err != nil {
				return err
			}
		}
	}
}

// tamper alters the first file of every unit before it is sent.
func tamper(change func(*fakeFile)) func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
	return scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		files := f.filesFor(req)
		change(&files[0])
		return sendUnit(stream, files, nil)
	})
}

func onlyNodeA(f *fakeLiaison) {
	f.planFrames = f.planFrames[:1]
	f.dataNodes = f.dataNodes[:1]
}

func TestRunExport_CRCMismatchIsNotRenamed(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = tamper(func(ff *fakeFile) { ff.badCRC = true })
	opts, log := exportOptions(t, f)
	err := runExport(t, f, opts)
	requireExit(t, err, ExitRuntime, "is corrupt")
	final := filepath.Join(opts.Output, "nodes", manifest.NodeDir(nodeA), "measure", "sw_metric", "seg-20260930", "shard-0-00001.csv")
	if _, statErr := os.Stat(final); !os.IsNotExist(statErr) {
		t.Fatalf("a corrupt file must not be renamed: %v", statErr)
	}
	if _, statErr := os.Stat(final + ".tmp"); statErr != nil {
		t.Fatalf("the .tmp is left for inspection: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(statErr) {
		t.Fatal("a failed export writes no manifest")
	}
	requireKept(t, f, log)
}

func TestRunExport_RejectsUnsafePaths(t *testing.T) {
	for _, p := range []string{
		"measure/sw_metric/../../../evil.csv",
		"/etc/evil.csv",
		"stream/sw_metric/seg-1/shard-0-00001.csv",
		"measure/other/seg-1/shard-0-00001.csv",
		`measure/sw_metric/a\..\b.csv`,
		"measure/sw_metric",
	} {
		f := exportFake()
		onlyNodeA(f)
		f.exportScript = tamper(func(ff *fakeFile) { ff.path = p })
		opts, log := exportOptions(t, f)
		requireExit(t, runExport(t, f, opts), ExitRuntime, "relative path")
		requireKept(t, f, log)
		if _, err := os.Stat(filepath.Join(filepath.Dir(opts.Output), "evil.csv")); !os.IsNotExist(err) {
			t.Fatalf("%s escaped the output directory", p)
		}
	}
	if clean, err := safeRelativePath("measure/sw_metric/./seg-1//a.csv", "measure/sw_metric/"); err != nil || clean != "measure/sw_metric/seg-1/a.csv" {
		t.Fatalf("a redundant path is cleaned: %q %v", clean, err)
	}
}

func TestRunExport_ExistingTargetIsRejected(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		files := f.filesFor(req)
		return sendUnit(stream, append(files, files[0]), nil)
	})
	opts, _ := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "already exists")
}

// The layout is built from every resource of the group, so a resource the node does not
// know, or one it knows but the exported schema lacks, shifts the columns.
func TestRunExport_LayoutResourceSetMismatchFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		revs   map[string]int64
		expect string
	}{
		{
			"node lacks a resource",
			map[string]int64{"service_cpm": 7},
			"sw_metric/service_idx changed during export: the node encoded measure/sw_metric/seg-20260930/shard-0-00001.csv " +
				"with mod revision missing, the exported schema has 8",
		},
		{
			"node has an extra resource",
			map[string]int64{"service_cpm": 7, "service_idx": 8, "a_new": 1},
			"sw_metric/a_new changed during export: the node encoded measure/sw_metric/seg-20260930/shard-0-00001.csv " +
				"with mod revision 1, the exported schema has missing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := exportFake()
			onlyNodeA(f)
			f.exportScript = tamper(func(ff *fakeFile) { ff.revs = tc.revs })
			opts, log := exportOptions(t, f)
			requireExit(t, runExport(t, f, opts), ExitRuntime, tc.expect)
			requireKept(t, f, log)
		})
	}
}

func TestRunExport_SchemaChangeDuringExportFails(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = tamper(func(ff *fakeFile) { ff.revs = map[string]int64{"service_cpm": 99} })
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "schema of measure sw_metric/service_cpm changed during export")
	requireKept(t, f, log)
}

func TestRunExport_RefusesANonEmptyOutputDir(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	if err := os.MkdirAll(opts.Output, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(opts.Output, "old"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	requireExit(t, runExport(t, f, opts), ExitUsage, "is not empty")
	if f.lastPlan() != nil {
		t.Fatal("nothing may be planned for a non-empty output directory")
	}
	// An existing empty directory is fine.
	f2 := exportFake()
	opts2, log := exportOptions(t, f2)
	if err := os.MkdirAll(opts2.Output, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runExport(t, f2, opts2); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
}

func TestRunExport_OccupiedClusterExits2(t *testing.T) {
	f := exportFake()
	f.listed = listedOn("b0b0")
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitPreflight, "give --preempt to take it over")
	if !strings.Contains(log.String(), "export session feedfeed created") || strings.Contains(log.String(), "kept") {
		t.Fatalf("a refused create was rolled back by the liaison; nothing is kept:\n%s", log)
	}
	// The schema it wrote is removed, so the same command with --preempt can run next.
	if err := CheckOutputDir(opts.Output); err != nil {
		t.Fatalf("the output directory must be empty again: %v", err)
	}
}

// An operator asked about the occupant who answers yes runs the export as with --preempt; one
// who answers no gets the refusal and an empty output directory.
func TestRunExport_AsksBeforeTakingOver(t *testing.T) {
	for _, yes := range []bool{true, false} {
		f := exportFake()
		f.listed = listedOn("b0b0")
		opts, log := exportOptions(t, f)
		var question string
		opts.Confirm = func(_ context.Context, q string) bool {
			question = q
			return yes
		}
		err := runExport(t, f, opts)
		if !strings.Contains(question, "session b0b0") || !strings.HasSuffix(question, "[y/N] ") {
			t.Fatalf("question: %q", question)
		}
		if yes {
			if err != nil || !strings.Contains(log.String(), "preempted the existing export session(s) [b0b0") {
				t.Fatalf("a yes exports and warns about the preemption: %v\n%s", err, log)
			}
			continue
		}
		requireExit(t, err, ExitPreflight, "left as they are; give --preempt to take them over")
		if checkErr := CheckOutputDir(opts.Output); checkErr != nil {
			t.Fatalf("a no leaves the output directory empty: %v", checkErr)
		}
	}
}

// Ctrl-C while the takeover question waits is an interruption, not a declined takeover.
func TestRunExport_InterruptedAtTheQuestion(t *testing.T) {
	f := exportFake()
	f.listed = listedOn("b0b0")
	opts, _ := exportOptions(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts.Confirm = func(context.Context, string) bool {
		cancel()
		return false
	}
	err := RunExport(ctx, envFor(t, opts, f), opts.Export)
	requireExit(t, err, ExitRuntime, "export interrupted before its session was created")
	if checkErr := CheckOutputDir(opts.Output); checkErr != nil {
		t.Fatalf("an interrupted export leaves the output directory empty: %v", checkErr)
	}
}

func TestRunExport_PreemptWarns(t *testing.T) {
	f := exportFake()
	f.listed = listedOn("b0b0")
	opts, log := exportOptions(t, f)
	opts.Preempt = true
	if err := runExport(t, f, opts); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "preempted the existing export session(s) [b0b0") {
		t.Fatalf("the preemption must be warned about:\n%s", log)
	}
}

// blockUntilCanceled answers ready and then waits for the client to go away.
func blockUntilCanceled(_ *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
	if err := sendReady(init, stream); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestRunExport_HeartbeatLostAborts(t *testing.T) {
	f := exportFake()
	f.hbCode = codes.FailedPrecondition
	f.exportScript = blockUntilCanceled
	opts, log := exportOptions(t, f)
	shortHeartbeat(t, 20*time.Millisecond)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "export session feedfeed lost")
	requireKept(t, f, log)
}

func TestRunExport_FailFastCancelsOtherWorkers(t *testing.T) {
	f := exportFake()
	f.exportScript = func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if init.GetTargetNode() == nodeB {
			return blockUntilCanceled(f, init, stream)
		}
		if err := sendReady(init, stream); err != nil {
			return err
		}
		return status.Error(codes.Internal, "snapshot unit missing (injected)")
	}
	opts, log := exportOptions(t, f)
	start := time.Now()
	requireExit(t, runExport(t, f, opts), ExitRuntime, "snapshot unit missing")
	if time.Since(start) > 10*time.Second {
		t.Fatal("the failure of one node must cancel the others")
	}
	requireKept(t, f, log)
	deadline := time.Now().Add(5 * time.Second)
	for {
		f.mu.Lock()
		canceled := f.canceled[nodeB]
		f.mu.Unlock()
		if canceled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the other node's stream was not canceled")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// bigFile streams one file of chunks x size bytes for the first unit.
func bigFile(chunks, size int, beforeStart time.Duration) func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
	return scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		time.Sleep(beforeStart)
		file := f.filesFor(req)[0]
		chunk := bytes.Repeat([]byte("x"), size)
		send := func(r *transferv1.ExportResponse) error { return stream.Send(r) }
		if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: &transferv1.UnitStarted{}}}); err != nil {
			return err
		}
		if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{RelativePath: file.path}}}); err != nil {
			return err
		}
		crc := crc32.NewIEEE()
		for i := 0; i < chunks; i++ {
			_, _ = crc.Write(chunk)
			if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: chunk}}}); err != nil {
				return err
			}
		}
		if err := send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: &transferv1.FileFinished{
			Bytes: uint64(chunks * size), Crc32: crc.Sum32(), RowCounts: file.rows, SchemaModRevisions: file.revs,
		}}}); err != nil {
			return err
		}
		return send(&transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitFinished{UnitFinished: &transferv1.UnitFinished{}}})
	})
}

// The liaison rate limit is not enforced by the client: every stream's Init carries it,
// rounded, for the liaison to apply.
func TestRunExport_InitCarriesTheLiaisonRate(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	opts.LiaisonRateLimit = 1<<20 + 0.4
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if len(f.exportInits) != 2 {
		t.Fatalf("inits = %v", f.exportInits)
	}
	for _, in := range f.exportInits {
		if in.GetLiaisonBytesPerSecond() != 1<<20 {
			t.Fatalf("init = %v", in)
		}
	}
	if !strings.Contains(log.String(), "liaison 1048576 bytes/s per liaison") {
		t.Fatalf("the requested rates are printed once:\n%s", log)
	}
}

func TestRunExport_IdleTimeoutFires(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = bigFile(1, 10, time.Second)
	opts, log := exportOptions(t, f)
	opts.NodeIdleTimeout = 200 * time.Millisecond
	requireExit(t, runExport(t, f, opts), ExitRuntime, "--node-idle-timeout")
	requireKept(t, f, log)
}

func TestRunExport_RetriesABusyNode(t *testing.T) {
	origBase := exportRetryBase
	exportRetryBase = 10 * time.Millisecond
	t.Cleanup(func() { exportRetryBase = origBase })
	f := exportFake()
	onlyNodeA(f)
	var calls int
	f.exportScript = func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		f.mu.Lock()
		calls++
		n := calls
		f.mu.Unlock()
		if n <= 2 {
			return status.Error(codes.ResourceExhausted, "busy (injected)")
		}
		return defaultExport(f, init, stream)
	}
	opts, log := exportOptions(t, f)
	opts.SendRetryTimeout = time.Minute
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if calls != 3 || !strings.Contains(log.String(), "is busy") {
		t.Fatalf("calls = %d\n%s", calls, log)
	}
	// Without a retry window the refusal is final.
	f2 := exportFake()
	onlyNodeA(f2)
	f2.exportScript = func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
		return status.Error(codes.ResourceExhausted, "busy (injected)")
	}
	opts2, _ := exportOptions(t, f2)
	requireExit(t, runExport(t, f2, opts2), ExitRuntime, "busy")
}

func TestRunExport_SchemaOnly(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	opts.SchemaOnly = true
	if err := runExport(t, f, opts); err != nil {
		t.Fatal(err)
	}
	if f.lastPlan() != nil || len(f.exportInits) != 0 {
		t.Fatal("a schema-only export creates no session and streams no data")
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !m.SchemaOnly || len(m.Units) != 0 || len(m.SchemaFiles) != len(schemasync.Kinds) || len(m.Groups) != 3 {
		t.Fatalf("manifest = %+v", m)
	}
}

func TestRunExport_NativeIsNotImplemented(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	opts.Format = transferv1.Format_FORMAT_NATIVE
	requireExit(t, runExport(t, f, opts), ExitUsage, "native export format is not implemented yet")
}

func TestSchemaSync_ListsOnlyTheKindsOfEachCatalog(t *testing.T) {
	f := exportFake()
	conn := dial(t, serveLiaison(t, f))
	dir := t.TempDir()
	groups := SelectedGroups(f.groups, []*transferv1.Selector{
		{Catalog: commonv1.Catalog_CATALOG_MEASURE}, {Catalog: commonv1.Catalog_CATALOG_PROPERTY, Groups: []string{"ui"}},
	})
	res, err := schemasync.Export(context.Background(), conn, groups, dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"measure/sw_metric", "topNAggregation/sw_metric", "indexRule/sw_metric", "indexRuleBinding/sw_metric", "property/ui"}
	if got := f.schema.calls(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("list calls = %v, want %v", got, want)
	}
	if rev, ok := res.GroupRevisions(commonv1.Catalog_CATALOG_MEASURE, "sw_metric")["service_cpm"]; !ok || rev != 7 {
		t.Fatalf("mod revision = %d %v", rev, ok)
	}
	if _, ok := res.GroupRevisions(commonv1.Catalog_CATALOG_STREAM, "sw_metric")["service_cpm"]; ok {
		t.Fatal("the catalog is part of the key")
	}
	if !slices.ContainsFunc(res.Measures, func(m *databasev1.Measure) bool {
		return m.GetMetadata().GetGroup() == "sw_metric" && m.GetMetadata().GetName() == "service_idx" && m.GetIndexMode()
	}) {
		t.Fatal("the decoded measures are kept")
	}
	lines := map[string][]string{}
	for _, sf := range res.Files {
		fh, openErr := os.Open(filepath.Join(dir, filepath.FromSlash(sf.Path)))
		if openErr != nil {
			t.Fatal(openErr)
		}
		info, _ := fh.Stat()
		if info.Mode().Perm() != 0o600 || uint64(info.Size()) != sf.Bytes {
			t.Fatalf("%s: mode %v size %d, want 0600 and %d", sf.Path, info.Mode().Perm(), info.Size(), sf.Bytes)
		}
		sc := bufio.NewScanner(fh)
		for sc.Scan() {
			lines[sf.Path] = append(lines[sf.Path], sc.Text())
		}
		_ = fh.Close()
	}
	if info, _ := os.Stat(filepath.Join(dir, schemasync.Dir)); info.Mode().Perm() != 0o700 {
		t.Fatalf("schema dir mode %v", info.Mode().Perm())
	}
	if len(lines["schema/group.jsonl"]) != 2 || len(lines["schema/measure.jsonl"]) != 3 || len(lines["schema/stream.jsonl"]) != 0 {
		t.Fatalf("lines = %v", lines)
	}
	// Unpopulated fields are emitted: index_mode=false is still present.
	if !strings.Contains(lines["schema/measure.jsonl"][0], `"indexMode":false`) && !strings.Contains(lines["schema/measure.jsonl"][0], `"indexMode": false`) {
		t.Fatalf("unpopulated fields must be emitted: %s", lines["schema/measure.jsonl"][0])
	}
}

// A cancellation during the retry back-off reports why the export was canceled, not the
// busy answer of the attempt before it.
func TestRunExport_CancelDuringRetryBackoffReportsTheCause(t *testing.T) {
	origBase := exportRetryBase
	exportRetryBase = time.Minute
	t.Cleanup(func() { exportRetryBase = origBase })
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
		return status.Error(codes.ResourceExhausted, "busy (injected)")
	}
	opts, log := exportOptions(t, f)
	opts.SendRetryTimeout = time.Hour
	env := envFor(t, opts, f)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	interrupted := errors.New("interrupted by the operator (injected)")
	//panicdiag:allow-rawgo test helper that cancels the export once the retry is logged
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for !strings.Contains(log.String(), "is busy") && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		cancel(interrupted)
	}()
	start := time.Now()
	err := RunExport(ctx, env, opts.Export)
	if !errors.Is(err, interrupted) || !strings.Contains(err.Error(), "node "+nodeA) || strings.Contains(err.Error(), "busy (injected)") {
		t.Fatalf("want the cancellation cause with node context, got %v", err)
	}
	if time.Since(start) > 10*time.Second {
		t.Fatal("the cancellation must interrupt the back-off")
	}
	requireKept(t, f, log)
}

// A stream that fails in the middle of a file leaves only the .tmp of that file.
func TestRunExport_StreamErrorMidFileLeavesOnlyTheTmp(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		file := f.filesFor(req)[0]
		for _, r := range []*transferv1.ExportResponse{
			{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: &transferv1.UnitStarted{}}},
			{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{RelativePath: file.path}}},
			{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: file.content[:4]}}},
			{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: file.content[4:8]}}},
		} {
			if err := stream.Send(r); err != nil {
				return err
			}
		}
		return status.Error(codes.Internal, "part read failed (injected)")
	})
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "part read failed")
	final := filepath.Join(opts.Output, "nodes", manifest.NodeDir(nodeA), "measure", "sw_metric", "seg-20260930", "shard-0-00001.csv")
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("an interrupted file must not get its final name: %v", err)
	}
	if body, err := os.ReadFile(final + ".tmp"); err != nil || len(body) != 8 {
		t.Fatalf("the .tmp keeps what was received: %d bytes, %v", len(body), err)
	}
	if _, err := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(err) {
		t.Fatal("a failed export writes no manifest")
	}
	requireKept(t, f, log)
}

// A plan with no unit (only the summary frame) is a successful, empty export.
func TestRunExport_EmptyPlan(t *testing.T) {
	f := exportFake()
	f.planFrames = nil
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Units) != 0 || len(m.Source.Nodes) != 0 || len(m.SchemaFiles) != len(schemasync.Kinds) {
		t.Fatalf("manifest = %+v", m)
	}
	if got := f.releasedIDs(); len(got) != 1 || got[0] != sessID {
		t.Fatalf("an empty export releases its session, released %v", got)
	}
	if len(f.exportInits) != 0 || !strings.Contains(log.String(), "0 unit(s), 0 file(s)") {
		t.Fatalf("nothing is streamed: %d inits\n%s", len(f.exportInits), log)
	}
}

// --include-schema=false leaves the schema alone: no registry call beyond the group list,
// no schema files, no revision check, and a manifest warning about the unknown index mode.
func TestRunExport_WithoutSchema(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	// Revisions that disagree with the registry would fail an export with the schema.
	f.exportScript = tamper(func(ff *fakeFile) { ff.revs = map[string]int64{"service_cpm": 99} })
	opts, log := exportOptions(t, f)
	opts.IncludeSchema = false
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if calls := f.schema.calls(); len(calls) != 0 {
		t.Fatalf("no schema registry may be listed, got %v", calls)
	}
	if _, err := os.Stat(filepath.Join(opts.Output, schemasync.Dir)); !os.IsNotExist(err) {
		t.Fatalf("no schema directory is written: %v", err)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.SchemaFiles) != 0 || len(m.Groups) != 3 || m.Groups[0].Name != "sw_metric" || m.Groups[0].IndexMode {
		t.Fatalf("manifest = %+v", m)
	}
	if len(m.Warnings) == 0 || m.Warnings[0] != schemaSkippedWarning || !strings.Contains(log.String(), "WARN "+schemaSkippedWarning) {
		t.Fatalf("warnings = %v\n%s", m.Warnings, log)
	}
	opts2, _ := exportOptions(t, f)
	opts2.IncludeSchema, opts2.SchemaOnly = false, true
	requireExit(t, runExport(t, exportFake(), opts2), ExitUsage, "--include-schema=false")
}

// The manifest labels an unspecified compression as the csv default gzip.
func TestCompressLabel(t *testing.T) {
	if compressLabel(transferv1.Compress_COMPRESS_UNSPECIFIED) != "gzip" || compressLabel(transferv1.Compress_COMPRESS_NONE) != "none" {
		t.Fatal("compressLabel never reports none for an unspecified compression")
	}
}

// The workers and the heartbeat watcher may print at once; plain writers must still be safe,
// even one shared by the log and the output (run with -race).
func TestRunExport_LogWriterNeedsNoLocking(t *testing.T) {
	f := exportFake()
	opts, _ := exportOptions(t, f)
	var plain bytes.Buffer
	opts.Log = &plain
	opts.Out = &plain
	shortHeartbeat(t, time.Millisecond)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, plain.String())
	}
	for _, want := range []string{"✓ " + nodeA + "  done: ", "✓ " + nodeB + "  done: "} {
		if !strings.Contains(plain.String(), "\n"+want) {
			t.Fatalf("log lacks the whole line %q:\n%s", want, plain.String())
		}
	}
}

// The progress and the summary go to the output, warnings and hints to the log.
func TestRunExport_ProgressOnTheOutputWarningsOnTheLog(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	out := &syncBuffer{}
	opts.Out = out
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s%s", err, log, out)
	}
	for _, want := range []string{"export session feedfeed created\n", "exporting 2 node(s)", " started: ", "✓ " + nodeA + "  done: ", "export written to"} {
		if !strings.Contains(out.String(), want) || strings.Contains(log.String(), want) {
			t.Fatalf("%q belongs to the output only\nout:\n%s\nlog:\n%s", want, out, log)
		}
	}
	if log.String() != "" {
		t.Fatalf("a healthy export logs nothing:\n%s", log)
	}
}

// On a terminal the live lines are drawn while the nodes export and erased before the
// summary, which is printed on a clean screen (run with -race: the redraws run alongside the
// workers and the heartbeat).
func TestRunExport_LiveLinesOnATerminal(t *testing.T) {
	f := exportFake()
	opts, log := exportOptions(t, f)
	out := &syncBuffer{}
	opts.Out = out
	opts.Terminal = terminalOf(120, 40)
	shortHeartbeat(t, time.Millisecond)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s%s", err, log, out)
	}
	if !strings.Contains(out.String(), eraseLine) || !strings.Contains(out.String(), "  waiting for the next unit\n") {
		t.Fatalf("no live lines drawn:\n%q", out)
	}
	screen := replayTerminal(out.String())
	if strings.Contains(screen, "waiting for the next unit") || strings.Contains(screen, " rows  ") && strings.Contains(screen, "/s  ") {
		t.Fatalf("live lines left on the screen:\n%s", screen)
	}
	for _, want := range []string{"✓ " + nodeA + "  done: 3 unit(s)", "✓ " + nodeB + "  done: 1 unit(s)", "NODE ", "export written to"} {
		if !strings.Contains(screen, want) {
			t.Fatalf("screen lacks %q:\n%s", want, screen)
		}
	}
	if i := strings.Index(out.String(), "NODE "); strings.Contains(out.String()[i:], "\x1b") {
		t.Fatalf("the summary is followed by terminal control sequences:\n%q", out.String()[i:])
	}
}

// replayTerminal applies the carriage returns and the erase sequences of the progress to s
// and returns the lines left on the screen.
func replayTerminal(s string) string {
	lines := []string{""}
	for len(s) > 0 {
		switch {
		case strings.HasPrefix(s, eraseLine):
			lines = lines[:len(lines)-1]
			lines[len(lines)-1] = ""
			s = s[len(eraseLine):]
		case s[0] == '\r':
			lines[len(lines)-1] = ""
			s = s[1:]
		case s[0] == '\n':
			lines = append(lines, "")
			s = s[1:]
		default:
			lines[len(lines)-1] += s[:1]
			s = s[1:]
		}
	}
	return strings.Join(lines, "\n")
}

// Property units are keyed by (catalog, group): nodes of different stages holding the same
// property group make one unit with one source per node, staged as its first source.
func TestRunExport_PropertyUnitIgnoresTheStage(t *testing.T) {
	f := exportFake()
	f.planFrames = append(f.planFrames, &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{
		NodeId: nodeB, Stage: "warm", Units: []*transferv1.UnitInventory{{Kind: &transferv1.UnitInventory_Property{
			Property: &transferv1.PropertyInventory{Group: "ui", Shards: []*transferv1.PropertyShardStat{{ShardId: 0, DocCount: 3, EstimatedBytes: 10}}},
		}}},
	}}})
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	var props []manifest.Unit
	for _, u := range m.Units {
		if u.Catalog == "PROPERTY" {
			props = append(props, u)
		}
	}
	if len(props) != 1 {
		t.Fatalf("want one property unit, got %+v", props)
	}
	p := props[0]
	if p.Group != "ui" || p.Stage != DefaultStageName || len(p.Sources) != 2 || p.Sources[0].Node != nodeA || p.Sources[1].Node != nodeB ||
		len(p.Sources[0].Shards) != 2 || len(p.Sources[1].Shards) != 1 || p.Sources[1].Shards[0].DocCount != 3 {
		t.Fatalf("property unit = %+v", p)
	}
	for _, src := range p.Sources {
		for _, sh := range src.Shards {
			for _, file := range sh.Files {
				checkFile(t, opts.Output, file)
			}
		}
	}
}

// A stage records its own options, replicas 0 included, and its own ttl.
func TestBaseManifest_StagesRecordTheirOwnOptions(t *testing.T) {
	day := func(n uint32) *commonv1.IntervalRule {
		return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
	}
	g := &commonv1.Group{Metadata: &commonv1.Metadata{Name: "g"}, Catalog: commonv1.Catalog_CATALOG_MEASURE, ResourceOpts: &commonv1.ResourceOpts{
		ShardNum: 4, SegmentInterval: day(1), Ttl: day(7), Replicas: 2,
		Stages: []*commonv1.LifecycleStage{
			{Name: "warm", ShardNum: 2, SegmentInterval: day(3), Ttl: day(30)},
			{Name: "cold", ShardNum: 1, SegmentInterval: day(5), Ttl: day(365), Replicas: 1},
		},
	}}
	m := baseManifest(transferv1.Compress_COMPRESS_GZIP, []*commonv1.Group{g}, &schemasync.Result{})
	want := []manifest.Stage{
		{Name: "hot", ShardNum: 4, SegmentInterval: "1d", TTL: "7d", Replicas: 2},
		{Name: "warm", ShardNum: 2, SegmentInterval: "3d", TTL: "30d", Replicas: 0},
		{Name: "cold", ShardNum: 1, SegmentInterval: "5d", TTL: "365d", Replicas: 1},
	}
	if len(m.Groups) != 1 || len(m.Groups[0].Stages) != len(want) {
		t.Fatalf("groups = %+v", m.Groups)
	}
	for i, st := range m.Groups[0].Stages {
		if st != want[i] {
			t.Fatalf("stage %d = %+v, want %+v", i, st, want[i])
		}
	}
}

// The rows of segment-level files are counted on the source, those of shard files on their shard.
func TestUnitSource_CountsRowsWhereTheFilesBelong(t *testing.T) {
	u := &plannedUnit{segment: &transferv1.SegmentInventory{}, files: []receivedFile{
		{file: manifest.File{Path: "seg-1"}, segmentLevel: true, rowCounts: map[string]uint64{"m1": 2, "m2": 1}},
		{file: manifest.File{Path: "seg-2"}, segmentLevel: true, rowCounts: map[string]uint64{"m1": 3}},
		{file: manifest.File{Path: "shard-1"}, shardID: 1, rowCounts: map[string]uint64{"m1": 4}},
	}}
	src := unitSource("n", u)
	if !maps.Equal(src.RowCounts, map[string]uint64{"m1": 5, "m2": 1}) || src.TotalCount != 6 || len(src.SegmentFiles) != 2 {
		t.Fatalf("segment-level counts: rowCounts %v totalCount %d files %d", src.RowCounts, src.TotalCount, len(src.SegmentFiles))
	}
	if len(src.Shards) != 1 || !maps.Equal(src.Shards[0].RowCounts, map[string]uint64{"m1": 4}) || src.Shards[0].TotalCount != 4 {
		t.Fatalf("shard counts: %+v", src.Shards)
	}
}

// Two node ids that derive one directory name are refused rather than written into one subtree.
func TestBuildJobs_RefusesNodesSharingADirectory(t *testing.T) {
	plan := &PlanResult{Frames: []*transferv1.UnitFrame{{NodeId: "data-1:17912"}, {NodeId: "Data-1:17912"}}}
	_, err := buildJobs(plan)
	requireExit(t, err, ExitRuntime, "map to the same directory data-1-17912")
}

// A file of a shard the unit did not ask for is a node fault, not an extra manifest shard.
func TestStartFile_RefusesAnUnrequestedShard(t *testing.T) {
	n := &nodeExport{job: &nodeJob{nodeID: nodeA}, root: t.TempDir()}
	u := &plannedUnit{property: &transferv1.PropertyInventory{Group: "ui", Shards: []*transferv1.PropertyShardStat{{ShardId: 0}}}}
	_, err := n.startFile(u, &transferv1.FileStarted{RelativePath: "property/ui/shard-3-00001.csv", ShardId: 3})
	requireExit(t, err, ExitRuntime, "is of shard 3, which the unit did not ask for")
}

// The .tmp of a received file is created exclusively: an existing one is never truncated.
func TestStartFile_RefusesAnExistingTmp(t *testing.T) {
	root := t.TempDir()
	n := &nodeExport{job: &nodeJob{nodeID: nodeA}, root: root}
	u := &plannedUnit{property: &transferv1.PropertyInventory{Group: "ui", Shards: []*transferv1.PropertyShardStat{{ShardId: 0}}}}
	tmp := filepath.Join(root, "property", "ui", "shard-0-00001.csv.tmp")
	if err := os.MkdirAll(filepath.Dir(tmp), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := n.startFile(u, &transferv1.FileStarted{RelativePath: "property/ui/shard-0-00001.csv"}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("want an exists error, got %v", err)
	}
	if body, err := os.ReadFile(tmp); err != nil || string(body) != "keep" {
		t.Fatalf("the existing .tmp was touched: %q %v", body, err)
	}
	p, err := n.startFile(u, &transferv1.FileStarted{RelativePath: "property/ui/shard-1-00001.csv"})
	if err != nil {
		t.Fatal(err)
	}
	p.abandon()
	if info, statErr := os.Stat(p.tmp); statErr != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("a new .tmp is 0600: %v %v", info, statErr)
	}
}
