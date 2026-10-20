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
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

const (
	// measureUnitLabel is how messages name the measure unit of exportFake on data-a.
	measureUnitLabel = "measure/sw_metric/seg-20260930"
	// measureFilePath is the relative path of the shard-0 file of that unit.
	measureFilePath = "measure/sw_metric/seg-20260930/shard-0-00001.csv"
)

func wireUnitStarted() *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitStarted{UnitStarted: &transferv1.UnitStarted{}}}
}

func wireFileStarted(rel string, shard uint32, segmentLevel bool) *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{FileStarted: &transferv1.FileStarted{
		RelativePath: rel, ShardId: shard, SegmentLevel: segmentLevel,
	}}}
}

func wireChunk(content []byte) *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_Chunk{Chunk: &transferv1.DataChunk{Content: content}}}
}

// wireFileFinished announces content and the file's row counts and revisions as they are.
func wireFileFinished(file fakeFile) *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileFinished{FileFinished: &transferv1.FileFinished{
		Bytes: uint64(len(file.content)), Crc32: crc32.ChecksumIEEE(file.content), RowCounts: file.rows, SchemaModRevisions: file.revs,
	}}}
}

func wireUnitFinished() *transferv1.ExportResponse {
	return &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_UnitFinished{UnitFinished: &transferv1.UnitFinished{}}}
}

// wireFile is a whole, valid file: file_started, its content in one chunk and file_finished.
func wireFile(file fakeFile) []*transferv1.ExportResponse {
	return []*transferv1.ExportResponse{wireFileStarted(file.path, file.shard, file.segLevel), wireChunk(file.content), wireFileFinished(file)}
}

// rawFrames answers ready, receives the first unit and streams the frames build returns for
// its default file. With end the stream then ends; otherwise it waits until the client closes
// or abandons the stream.
func rawFrames(end bool, build func(file fakeFile) []*transferv1.ExportResponse) func(
	*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer,
) error {
	return func(f *fakeLiaison, init *transferv1.ExportRequest_Init, stream transferv1.ExportService_ExportServer) error {
		if err := sendReady(init, stream); err != nil {
			return err
		}
		req, err := f.recvUnit(init, stream)
		if err != nil {
			return err
		}
		for _, r := range build(f.filesFor(req)[0]) {
			if err = stream.Send(r); err != nil {
				return err
			}
		}
		if end {
			return nil
		}
		for {
			if _, err = stream.Recv(); err != nil {
				return nil
			}
		}
	}
}

// measureFileOf is the default file the fake streams for the measure unit of data-a.
func measureFileOf(f *fakeLiaison) fakeFile {
	unit := segmentOf(f.planFrames[0]).GetUnit()
	return f.filesFor(&transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Segment{Segment: unit}})[0]
}

// nodeAFile is where a relative path of data-a lands in the output.
func nodeAFile(opts testOptions, rel string) string {
	return filepath.Join(opts.Output, "nodes", manifest.NodeDir(nodeA), filepath.FromSlash(rel))
}

// requireNotRenamed checks the file never got its final name and its .tmp exists (with the
// content received) or not, as wantTmp says, and that no manifest was written.
func requireNotRenamed(t *testing.T, opts testOptions, rel string, wantTmp []byte) {
	t.Helper()
	final := nodeAFile(opts, rel)
	if _, err := os.Lstat(final); !os.IsNotExist(err) {
		t.Fatalf("%s must not get its final name: %v", rel, err)
	}
	body, err := os.ReadFile(final + ".tmp")
	switch {
	case wantTmp == nil && !os.IsNotExist(err):
		t.Fatalf("no .tmp of %s may be created: %v", rel, err)
	case wantTmp != nil && (err != nil || !bytes.Equal(body, wantTmp)):
		t.Fatalf("the .tmp of %s keeps what was received: %q %v, want %q", rel, body, err, wantTmp)
	}
	if _, statErr := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(statErr) {
		t.Fatal("a failed export writes no manifest")
	}
}

// finalNodeFiles lists the files of the nodes directory that are not .tmp files.
func finalNodeFiles(t *testing.T, opts testOptions) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(opts.Output, "nodes"), func(p string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if !d.IsDir() && !strings.HasSuffix(p, ".tmp") {
			out = append(out, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// A file whose announced size or CRC-32 differs from what arrived, or whose schema revisions
// differ from the exported schema, keeps only its .tmp and fails the export.
func TestRunExport_FileVerificationFailuresLeaveOnlyTheTmp(t *testing.T) {
	for _, tc := range []struct {
		change func(*transferv1.FileFinished)
		expect func(file fakeFile) string
		name   string
	}{
		{
			name:   "more bytes announced",
			change: func(ff *transferv1.FileFinished) { ff.Bytes++ },
			expect: func(file fakeFile) string {
				crc := manifest.FormatCRC32(crc32.ChecksumIEEE(file.content))
				return fmt.Sprintf("node %s: %s is corrupt: received %d bytes crc32 %s, the node announced %d bytes crc32 %s",
					nodeA, measureFilePath, len(file.content), crc, len(file.content)+1, crc)
			},
		},
		{
			name:   "fewer bytes announced",
			change: func(ff *transferv1.FileFinished) { ff.Bytes-- },
			expect: func(file fakeFile) string {
				return fmt.Sprintf("received %d bytes crc32 %s, the node announced %d bytes",
					len(file.content), manifest.FormatCRC32(crc32.ChecksumIEEE(file.content)), len(file.content)-1)
			},
		},
		{
			name:   "another crc32",
			change: func(ff *transferv1.FileFinished) { ff.Crc32 ^= 1 },
			expect: func(file fakeFile) string {
				crc := crc32.ChecksumIEEE(file.content)
				return fmt.Sprintf("received %d bytes crc32 %s, the node announced %d bytes crc32 %s",
					len(file.content), manifest.FormatCRC32(crc), len(file.content), manifest.FormatCRC32(crc^1))
			},
		},
		{
			name:   "a resource missing",
			change: func(ff *transferv1.FileFinished) { delete(ff.SchemaModRevisions, "service_idx") },
			expect: func(fakeFile) string {
				return "schema of measure sw_metric/service_idx changed during export: the node encoded " + measureFilePath +
					" with mod revision missing, the exported schema has 8; export again"
			},
		},
		{
			name:   "no revisions at all",
			change: func(ff *transferv1.FileFinished) { ff.SchemaModRevisions = nil },
			expect: func(fakeFile) string {
				return "schema of measure sw_metric/service_cpm changed during export: the node encoded " + measureFilePath +
					" with mod revision missing, the exported schema has 7"
			},
		},
		{
			name:   "an extra resource",
			change: func(ff *transferv1.FileFinished) { ff.SchemaModRevisions["a_new"] = 1 },
			expect: func(fakeFile) string {
				return "schema of measure sw_metric/a_new changed during export: the node encoded " + measureFilePath +
					" with mod revision 1, the exported schema has missing"
			},
		},
		{
			name:   "another revision",
			change: func(ff *transferv1.FileFinished) { ff.SchemaModRevisions["service_idx"] = 80 },
			expect: func(fakeFile) string {
				return "schema of measure sw_metric/service_idx changed during export: the node encoded " + measureFilePath +
					" with mod revision 80, the exported schema has 8"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := exportFake()
			onlyNodeA(f)
			sent := measureFileOf(f)
			f.exportScript = rawFrames(false, func(file fakeFile) []*transferv1.ExportResponse {
				finished := wireFileFinished(file)
				tc.change(finished.GetFileFinished())
				return []*transferv1.ExportResponse{wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content), finished}
			})
			opts, log := exportOptions(t, f)
			err := runExport(t, f, opts)
			requireExit(t, err, ExitRuntime, tc.expect(sent))
			requireNotRenamed(t, opts, measureFilePath, sent.content)
			requireKept(t, f, log)
		})
	}
}

// Without an exported schema there is nothing to compare the node's revisions with, so no
// set of revisions fails a file.
func TestRunExport_WithoutSchemaNoRevisionCheckFails(t *testing.T) {
	for name, revs := range map[string]map[string]int64{
		"no revisions":       nil,
		"a resource missing": {"service_cpm": 7},
		"an extra resource":  {"service_cpm": 7, "service_idx": 8, "a_new": 1},
		"other revisions":    {"service_cpm": 70, "service_idx": 80},
	} {
		t.Run(name, func(t *testing.T) {
			f := exportFake()
			onlyNodeA(f)
			f.exportScript = tamper(func(ff *fakeFile) { ff.revs = revs })
			opts, log := exportOptions(t, f)
			opts.IncludeSchema = false
			if err := runExport(t, f, opts); err != nil {
				t.Fatalf("%v\n%s", err, log)
			}
			if _, err := os.Stat(nodeAFile(opts, measureFilePath)); err != nil {
				t.Fatalf("the file is written: %v", err)
			}
		})
	}
}

// Paths that are absolute, climb with "..", use backslashes, leave <catalog>/<group>/ or name
// the prefix itself are refused before anything is created.
func TestRunExport_RejectsMoreUnsafePaths(t *testing.T) {
	for _, tc := range []struct{ path, expect string }{
		{"", "empty relative path"},
		{"//measure/sw_metric/seg-20260930/a.csv", "is not a relative slash path"},
		{"/measure/sw_metric/seg-20260930/a.csv", "is not a relative slash path"},
		{`measure\sw_metric\seg-20260930\a.csv`, "is not a relative slash path"},
		{`measure/sw_metric/seg-20260930/a\b.csv`, "is not a relative slash path"},
		{"measure/sw_metric/..", "leaves its directory"},
		{"measure/sw_metric/../sw_metric/seg-20260930/a.csv", "leaves its directory"},
		{"measure/sw_metric/seg-20260930/../../../../../evil.csv", "leaves its directory"},
		{"../out/nodes/evil.csv", "leaves its directory"},
		{"measure/sw_metric/", "is outside measure/sw_metric/"},
		{"measure/sw_metric/.", "is outside measure/sw_metric/"},
		{"measure/sw_metric//", "is outside measure/sw_metric/"},
		{"./measure/sw_metric", "is outside measure/sw_metric/"},
		{"measure/sw_metric_other/a.csv", "is outside measure/sw_metric/"},
		{"measure/sw_metricx", "is outside measure/sw_metric/"},
		{"measure", "is outside measure/sw_metric/"},
		{"property/ui/shard-0-00001.csv", "is outside measure/sw_metric/"},
		{"MEASURE/sw_metric/a.csv", "is outside measure/sw_metric/"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			f := exportFake()
			onlyNodeA(f)
			f.exportScript = tamper(func(ff *fakeFile) { ff.path = tc.path })
			opts, log := exportOptions(t, f)
			err := runExport(t, f, opts)
			requireExit(t, err, ExitRuntime, "node "+nodeA+" unit "+measureUnitLabel+": ")
			requireExit(t, err, ExitRuntime, tc.expect)
			requireKept(t, f, log)
			if files := finalNodeFiles(t, opts); len(files) != 0 {
				t.Fatalf("nothing may be written for %q: %v", tc.path, files)
			}
			var tmps []string
			_ = filepath.WalkDir(filepath.Dir(opts.Output), func(p string, _ fs.DirEntry, _ error) error {
				if strings.HasSuffix(p, ".tmp") || strings.HasSuffix(p, "evil.csv") {
					tmps = append(tmps, p)
				}
				return nil
			})
			if len(tmps) != 0 {
				t.Fatalf("no file may be created for %q: %v", tc.path, tmps)
			}
		})
	}
}

// A file of a shard the segment unit did not ask for fails the export before it is created.
func TestRunExport_RejectsAFileOfAnUnrequestedShard(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = tamper(func(ff *fakeFile) {
		ff.shard = 5
		ff.path = "measure/sw_metric/seg-20260930/shard-5-00001.csv"
	})
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime,
		"node "+nodeA+" unit "+measureUnitLabel+": file measure/sw_metric/seg-20260930/shard-5-00001.csv is of shard 5, which the unit did not ask for")
	requireNotRenamed(t, opts, "measure/sw_metric/seg-20260930/shard-5-00001.csv", nil)
	requireKept(t, f, log)
}

// A segment-level file belongs to the segment, not to a shard: its shard id is not checked and
// the manifest lists it with the source's segment files, its rows on the source. The unit's
// warnings reach the manifest.
func TestRunExport_SegmentLevelFile(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	segFile := fakeFile{
		path: "measure/sw_metric/seg-20260930/sidx-00001.csv", shard: 9, segLevel: true,
		content: []byte("_name,v\nservice_idx,1\n"), rows: map[string]uint64{"service_idx": 1},
		revs: map[string]int64{"service_cpm": 7, "service_idx": 8},
	}
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		return sendUnit(stream, append(f.filesFor(req), segFile), []string{"one sidx part skipped (injected)"})
	})
	opts, log := exportOptions(t, f)
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	m, err := manifest.Read(filepath.Join(opts.Output, manifest.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Units) != 1 || len(m.Units[0].Sources) != 1 {
		t.Fatalf("units = %+v", m.Units)
	}
	src := m.Units[0].Sources[0]
	if len(src.SegmentFiles) != 1 || !strings.HasSuffix(src.SegmentFiles[0].Path, "/"+segFile.path) || src.RowCounts["service_idx"] != 1 {
		t.Fatalf("segment files %+v, rowCounts %v", src.SegmentFiles, src.RowCounts)
	}
	if len(src.Shards) != 1 || src.Shards[0].Shard != 0 || len(src.Shards[0].Files) != 1 {
		t.Fatalf("a segment-level file adds no shard: %+v", src.Shards)
	}
	checkFile(t, opts.Output, src.SegmentFiles[0])
	if want := "node " + nodeA + " unit " + measureUnitLabel + ": one sidx part skipped (injected)"; !slices.Contains(m.Warnings, want) {
		t.Fatalf("the unit's warning is recorded: %v", m.Warnings)
	}
}

// Frames out of order fail the unit; a file open at that moment keeps only its .tmp.
func TestRunExport_FrameOrderViolations(t *testing.T) {
	prefix := "node " + nodeA + " unit " + measureUnitLabel + ": "
	for _, tc := range []struct {
		build   func(file fakeFile) []*transferv1.ExportResponse
		wantTmp func(file fakeFile) []byte
		name    string
		expect  string
		end     bool
	}{
		{
			name: "chunk before unit_started",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireChunk(file.content)}
			},
			expect: prefix + "unexpected *v1.ExportResponse_Chunk frame",
		},
		{
			name: "chunk before file_started",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), wireChunk(file.content)}
			},
			expect: prefix + "unexpected *v1.ExportResponse_Chunk frame",
		},
		{
			name: "chunk after file_finished",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return append(append([]*transferv1.ExportResponse{wireUnitStarted()}, wireFile(file)...), wireChunk(file.content))
			},
			expect: prefix + "unexpected *v1.ExportResponse_Chunk frame",
		},
		{
			name: "file_started before unit_started",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireFileStarted(file.path, file.shard, false)}
			},
			expect: prefix + "unexpected *v1.ExportResponse_FileStarted frame",
		},
		{
			name: "file_started inside an open file",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{
					wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content[:3]),
					wireFileStarted("measure/sw_metric/seg-20260930/shard-0-00002.csv", file.shard, false),
				}
			},
			expect:  prefix + "unexpected *v1.ExportResponse_FileStarted frame",
			wantTmp: func(file fakeFile) []byte { return file.content[:3] },
		},
		{
			name: "file_finished without a file",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), wireFileFinished(file)}
			},
			expect: prefix + "unexpected *v1.ExportResponse_FileFinished frame",
		},
		{
			name: "unit_finished inside an open file",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{
					wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content), wireUnitFinished(),
				}
			},
			expect:  prefix + "unexpected *v1.ExportResponse_UnitFinished frame",
			wantTmp: func(file fakeFile) []byte { return file.content },
		},
		{
			name:   "unit_finished before unit_started",
			build:  func(fakeFile) []*transferv1.ExportResponse { return []*transferv1.ExportResponse{wireUnitFinished()} },
			expect: prefix + "unexpected *v1.ExportResponse_UnitFinished frame",
		},
		{
			name: "a second unit_started",
			build: func(fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), wireUnitStarted()}
			},
			expect: prefix + "unexpected *v1.ExportResponse_UnitStarted frame",
		},
		{
			name: "a second unit_started inside an open file",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{
					wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content), wireUnitStarted(),
				}
			},
			expect:  prefix + "unexpected *v1.ExportResponse_UnitStarted frame",
			wantTmp: func(file fakeFile) []byte { return file.content },
		},
		{
			name: "a frame without a known kind",
			build: func(fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), {}}
			},
			expect: prefix + "unexpected <nil> frame",
		},
		{
			name: "a ready frame inside a unit",
			build: func(fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), {Frame: &transferv1.ExportResponse_Ready{Ready: &transferv1.Ready{}}}}
			},
			expect: prefix + "unexpected *v1.ExportResponse_Ready frame",
		},
		{
			name:   "the stream ends before unit_started",
			build:  func(fakeFile) []*transferv1.ExportResponse { return nil },
			expect: prefix + "the stream ended before unit_finished",
			end:    true,
		},
		{
			name: "the stream ends inside a file",
			build: func(file fakeFile) []*transferv1.ExportResponse {
				return []*transferv1.ExportResponse{wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content[:5])}
			},
			expect:  prefix + "the stream ended before unit_finished",
			wantTmp: func(file fakeFile) []byte { return file.content[:5] },
			end:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := exportFake()
			onlyNodeA(f)
			sent := measureFileOf(f)
			f.exportScript = rawFrames(tc.end, func(file fakeFile) []*transferv1.ExportResponse { return tc.build(file) })
			opts, log := exportOptions(t, f)
			requireExit(t, runExport(t, f, opts), ExitRuntime, tc.expect)
			requireKept(t, f, log)
			if tc.wantTmp != nil {
				requireNotRenamed(t, opts, measureFilePath, tc.wantTmp(sent))
			}
			if tc.name == "chunk after file_finished" {
				// The finished file was verified and kept; only the unit failed.
				if files := finalNodeFiles(t, opts); len(files) != 1 || files[0] != nodeAFile(opts, measureFilePath) {
					t.Fatalf("files = %v", files)
				}
				return
			}
			if files := finalNodeFiles(t, opts); len(files) != 0 {
				t.Fatalf("no file may get its final name: %v", files)
			}
		})
	}
}

// The stream ending after a finished file but before unit_finished fails the unit; the
// finished file was verified and stays.
func TestRunExport_StreamEndsAfterAFinishedFile(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = rawFrames(true, func(file fakeFile) []*transferv1.ExportResponse {
		return append([]*transferv1.ExportResponse{wireUnitStarted()}, wireFile(file)...)
	})
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "node "+nodeA+" unit "+measureUnitLabel+": the stream ended before unit_finished")
	requireKept(t, f, log)
	if _, err := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(err) {
		t.Fatal("a failed export writes no manifest")
	}
}

// A frame after the unit_finished of the last unit is a protocol violation, even though every
// unit completed.
func TestRunExport_FrameAfterTheLastUnit(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = rawFrames(false, func(file fakeFile) []*transferv1.ExportResponse {
		frames := append([]*transferv1.ExportResponse{wireUnitStarted()}, wireFile(file)...)
		return append(frames, wireUnitFinished(), wireUnitStarted())
	})
	opts, log := exportOptions(t, f)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "node "+nodeA+": unexpected *v1.ExportResponse_UnitStarted frame after the last unit")
	requireKept(t, f, log)
	if _, err := os.Stat(filepath.Join(opts.Output, manifest.FileName)); !os.IsNotExist(err) {
		t.Fatal("a failed export writes no manifest")
	}
}

// A node that stops sending in the middle of a file is abandoned after --node-idle-timeout;
// the file keeps only its .tmp.
func TestRunExport_IdleTimeoutFiresMidFile(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	sent := measureFileOf(f)
	f.exportScript = rawFrames(false, func(file fakeFile) []*transferv1.ExportResponse {
		return []*transferv1.ExportResponse{wireUnitStarted(), wireFileStarted(file.path, file.shard, false), wireChunk(file.content[:6])}
	})
	opts, log := exportOptions(t, f)
	opts.NodeIdleTimeout = 200 * time.Millisecond
	start := time.Now()
	requireExit(t, runExport(t, f, opts), ExitRuntime, "node "+nodeA+" sent nothing for 200ms (--node-idle-timeout)")
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("the idle timeout fired after %s", elapsed)
	}
	requireNotRenamed(t, opts, measureFilePath, sent.content[:6])
	requireKept(t, f, log)
}

// Every frame restarts the idle countdown: a file whose chunks arrive slowly, each well within
// the timeout but together far beyond it, is received.
func TestRunExport_IdleTimeoutSparesSlowChunks(t *testing.T) {
	const chunks = 8
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = scripted(func(f *fakeLiaison, _ *transferv1.ExportRequest_Init, req *transferv1.ExportRequest, stream transferv1.ExportService_ExportServer) error {
		file := f.filesFor(req)[0]
		file.content = []byte(strings.Repeat(string(file.content), chunks))
		step := len(file.content) / chunks
		if err := stream.Send(wireUnitStarted()); err != nil {
			return err
		}
		if err := stream.Send(wireFileStarted(file.path, file.shard, false)); err != nil {
			return err
		}
		for i := 0; i < chunks; i++ {
			time.Sleep(100 * time.Millisecond)
			if err := stream.Send(wireChunk(file.content[i*step : (i+1)*step])); err != nil {
				return err
			}
		}
		if err := stream.Send(wireFileFinished(file)); err != nil {
			return err
		}
		return stream.Send(wireUnitFinished())
	})
	opts, log := exportOptions(t, f)
	opts.NodeIdleTimeout = 400 * time.Millisecond
	if err := runExport(t, f, opts); err != nil {
		t.Fatalf("slow but steady chunks must not trip the idle timeout: %v\n%s", err, log)
	}
	if strings.Contains(log.String(), "--node-idle-timeout") {
		t.Fatalf("the idle timeout fired:\n%s", log)
	}
}

// A file whose final path already exists is refused without touching what is there: here
// the node sends a second file under the path of one already received, spelled differently.
func TestRunExport_ExistingFinalPathIsKept(t *testing.T) {
	f := exportFake()
	onlyNodeA(f)
	f.exportScript = rawFrames(false, func(file fakeFile) []*transferv1.ExportResponse {
		frames := append([]*transferv1.ExportResponse{wireUnitStarted()}, wireFile(file)...)
		return append(frames, wireFileStarted("measure/sw_metric/seg-20260930/./shard-0-00001.csv", file.shard, false), wireChunk([]byte("overwrite")))
	})
	opts, log := exportOptions(t, f)
	final := nodeAFile(opts, measureFilePath)
	requireExit(t, runExport(t, f, opts), ExitRuntime, "node "+nodeA+" unit "+measureUnitLabel+": "+final+" already exists")
	requireKept(t, f, log)
	body, err := os.ReadFile(final)
	if err != nil || strings.Contains(string(body), "overwrite") || !strings.HasPrefix(string(body), "_name,v\n") {
		t.Fatalf("the existing file must stay as it was: %q %v", body, err)
	}
	if _, statErr := os.Stat(final + ".tmp"); !os.IsNotExist(statErr) {
		t.Fatalf("no .tmp may be opened for a refused file: %v", statErr)
	}
}

// safeRelativePath accepts clean and redundant paths under the prefix and refuses the rest.
func TestSafeRelativePath(t *testing.T) {
	const prefix = "measure/sw_metric/"
	for raw, want := range map[string]string{
		"measure/sw_metric/a.csv":            "measure/sw_metric/a.csv",
		"measure/sw_metric/seg-1/a.csv":      "measure/sw_metric/seg-1/a.csv",
		"./measure/sw_metric/a.csv":          "measure/sw_metric/a.csv",
		"measure//sw_metric/seg-1/./a.csv":   "measure/sw_metric/seg-1/a.csv",
		"measure/sw_metric/seg-1/a.csv/":     "measure/sw_metric/seg-1/a.csv",
		"measure/sw_metric/.hidden/file.csv": "measure/sw_metric/.hidden/file.csv",
		"measure/sw_metric/a..b.csv":         "measure/sw_metric/a..b.csv",
	} {
		got, err := safeRelativePath(raw, prefix)
		if err != nil || got != want {
			t.Fatalf("%q: got %q %v, want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{
		"", "/measure/sw_metric/a.csv", `measure/sw_metric\a.csv`, "measure/sw_metric/../x/a.csv", "..",
		"measure/sw_metric", "measure/sw_metric/", "measure/sw_metric/.", "measure/other/a.csv", "measure/sw_metricx/a.csv",
	} {
		if got, err := safeRelativePath(raw, prefix); err == nil {
			t.Fatalf("%q must be refused, got %q", raw, got)
		}
	}
}
