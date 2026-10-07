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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// NIDX-03 §12 item 2 rollback proof, write side.
//
// TestGenerateNIDX03RollbackCorpus is a one-off generator, gated behind
// NIDX03_ROLLBACK_WRITE so it never runs in CI: it writes a representative
// sidx workload with the CURRENT (native-only) code -- normal-mode and
// index-mode series, an update (upsert of an already-live identity), an
// external segment receive, and enough volume to cross the native owner's
// default compaction threshold -- to NIDX03_ROLLBACK_DIR (default
// /mnt/d/tmp-gao-build/nidx03-rollback), then runs the same fixed set of
// queries (exact/prefix/wildcard lookup, index-order sort, projection,
// timestamps/versions) the design's rollback test names and dumps the
// results as JSON beside the data. docs/design/0.12.0/native-inverted-index/
// verification/nidx-03-series-cutover/README.md records the procedure and
// the comparison against the previous release's own reader, run from the
// 735e9ad2 export.
//
// A second run, after killing (not closing) a background writer mid-batch
// and copying the directory, produces the crash-cut variant's corpus.
func TestGenerateNIDX03RollbackCorpus(t *testing.T) {
	if os.Getenv("NIDX03_ROLLBACK_WRITE") == "" {
		t.Skip("one-off rollback-proof corpus generator; set NIDX03_ROLLBACK_WRITE=1 to (re)run it")
	}
	root := nidx03RollbackDir(t)
	require.NoError(t, os.RemoveAll(root))
	require.NoError(t, os.MkdirAll(root, 0o755))

	writeNIDX03NormalCorpus(t, filepath.Join(root, "normal"))
	writeNIDX03IndexModeCorpus(t, filepath.Join(root, "indexmode"))
	writeNIDX03CrashCutCorpus(t, filepath.Join(root, "crashcut"))
}

func nidx03RollbackDir(t *testing.T) string {
	t.Helper()
	if dir := os.Getenv("NIDX03_ROLLBACK_DIR"); dir != "" {
		return dir
	}
	return "/mnt/d/tmp-gao-build/nidx03-rollback"
}

// nidx03RollbackQueryResult is the comparable, JSON-serializable shape both
// the new-code writer (this file) and the previous-release reader (ported
// into the 735e9ad2 export) dump for the same fixed set of queries. Byte
// slices are hex-encoded by encoding/json's default []byte handling (base64,
// actually) -- irrelevant here since both sides use the same Go encoding/json
// package and only ever compare the resulting text for equality, never
// decode it independently.
//
// cross-version diff depends on (kept identical in the 735e9ad2-ported read
// side); it is not a hot allocation path, so pointer-byte packing does not
// matter here.
//
//nolint:govet // field order is the JSON key order this proof's byte-for-byte
type nidx03RollbackQueryResult struct {
	Name        string            `json:"name"`
	Identities  []string          `json:"identities"`
	Timestamps  []int64           `json:"timestamps,omitempty"`
	Versions    []int64           `json:"versions,omitempty"`
	Projection  []string          `json:"projection,omitempty"`
	SortedHex   []string          `json:"sortedHex,omitempty"`
	FieldValues map[string]string `json:"fieldValues,omitempty"`
}

func writeNIDX03QueryResults(t *testing.T, path string, results []nidx03RollbackQueryResult) {
	t.Helper()
	data, err := json.MarshalIndent(results, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

const (
	nidx03RollbackRegionKey = "region"
	nidx03RollbackScoreRule = uint32(55)
)

func nidx03RollbackSeries(service, instance string) *pbv1.Series {
	return &pbv1.Series{
		Subject: "nidx03_rollback",
		EntityValues: []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: service}}},
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: instance}}},
		},
	}
}

func nidx03RollbackDoc(t *testing.T, service, instance, region string, score, timestamp, version int64) index.Document {
	t.Helper()
	series := nidx03RollbackSeries(service, instance)
	require.NoError(t, series.Marshal())
	regionField := index.NewBytesField(index.FieldKey{TagName: nidx03RollbackRegionKey}, []byte(region))
	regionField.Store, regionField.Index, regionField.NoSort = true, true, true
	scoreField := index.NewBytesField(index.FieldKey{IndexRuleID: nidx03RollbackScoreRule}, fmt.Appendf(nil, "%010d", score))
	scoreField.Store, scoreField.Index = true, true
	return index.Document{
		EntityValues: append([]byte(nil), series.Buffer...),
		Fields:       []index.Field{regionField, scoreField},
		Timestamp:    timestamp,
		Version:      version,
	}
}

// writeNIDX03NormalCorpus writes 4 services x 5 instances (20 series) across
// 2 regions, an update that changes one series' region and score, enough
// additional batches to cross the owner's default compaction threshold (16
// segments), and an externally-received segment built by a second, separate
// series index -- then captures exact/prefix/wildcard/sorted query results.
func writeNIDX03NormalCorpus(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)

	services := []string{"svcA", "svcB", "svcC", "svcD"}
	var allDocs index.Documents
	n := int64(0)
	for _, svc := range services {
		for i := 0; i < 5; i++ {
			instance := fmt.Sprintf("inst-%02d", i)
			region := "us"
			if i%2 == 0 {
				region = "eu"
			}
			n++
			allDocs = append(allDocs, nidx03RollbackDoc(t, svc, instance, region, n*10, 1000+n, 1))
		}
	}
	require.NoError(t, si.Insert(allDocs))

	// Update: svcA/inst-00 moves region and gets a new score + version, same
	// identity (Update is upsert: "last writer wins, no version check").
	update := nidx03RollbackDoc(t, "svcA", "inst-00", "ap", 9999, 5000, 2)
	require.NoError(t, si.Update(index.Documents{update}))

	// Cross the native owner's default compaction threshold (16 segments):
	// each Insert of brand-new identities below admits as its own segment.
	for b := 0; b < 20; b++ {
		filler := index.Documents{nidx03RollbackDoc(t, "filler", fmt.Sprintf("b%03d", b), "us", int64(b), int64(b+1), 1)}
		require.NoError(t, si.Insert(filler))
	}
	// 20+ inserted batches cross the native owner's default compaction
	// threshold (16 segments; pkg/index/native/owner.go's runMaintenance).

	// External segment receive: a second, separate series index writes one
	// committed segment this one's EnableExternalSegments then ingests raw.
	externalDir := filepath.Join(dir, "..", "external-source")
	extSI, err := newSeriesIndex(ctx, externalDir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	require.NoError(t, extSI.Insert(index.Documents{nidx03RollbackDoc(t, "svcExternal", "inst-00", "us", 42, 6000, 1)}))
	require.NoError(t, extSI.Close())
	segBytes := nidx03SingleSegmentFile(t, filepath.Join(externalDir, "sidx"))
	streamer, err := si.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	require.NoError(t, streamer.WriteChunk(segBytes))
	require.NoError(t, streamer.CompleteSegment())

	results := nidx03CaptureNormalQueries(t, si)
	before, _ := si.Stats()
	require.NoError(t, si.owner.Compact(ctx))
	after, _ := si.Stats()
	require.Equal(t, before, after, "compaction must not change the live document count")
	// Re-run the same queries post-compaction: merging must not change any
	// result.
	require.Equal(t, results, nidx03CaptureNormalQueries(t, si))
	require.NoError(t, si.Close())
	writeNIDX03QueryResults(t, filepath.Join(dir, "..", "normal-results.json"), results)
}

// nidx03SingleSegmentFile returns the bytes of the one *.seg file under dir.
func nidx03SingleSegmentFile(t *testing.T, dir string) []byte {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	require.Len(t, matches, 1, "expected exactly one segment file in %s", dir)
	data, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	return data
}

// nidx03CaptureNormalQueries runs the fixed query set this proof compares
// across releases and returns the results in a stable (sorted) order.
func nidx03CaptureNormalQueries(t *testing.T, si *seriesIndex) []nidx03RollbackQueryResult {
	t.Helper()
	ctx := context.Background()
	var results []nidx03RollbackQueryResult

	// Exact: the updated series must show its new region/score/version, not
	// the pre-update one.
	exact, _, err := si.Search(ctx, []*pbv1.Series{nidx03RollbackSeries("svcA", "inst-00")}, IndexSearchOpts{
		Projection: []index.FieldKey{{TagName: nidx03RollbackRegionKey}},
	})
	require.NoError(t, err)
	results = append(results, nidx03RollbackResult(t, "exact-updated-svcA-inst-00", exact, nil))

	// Exact: the externally received series.
	ext, _, err := si.Search(ctx, []*pbv1.Series{nidx03RollbackSeries("svcExternal", "inst-00")}, IndexSearchOpts{})
	require.NoError(t, err)
	results = append(results, nidx03RollbackResult(t, "exact-external-svcExternal-inst-00", ext, nil))

	// Prefix: every svcB instance.
	prefixQuery := &pbv1.Series{Subject: "nidx03_rollback", EntityValues: []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svcB"}}}, pbv1.AnyTagValue,
	}}
	prefix, _, err := si.Search(ctx, []*pbv1.Series{prefixQuery}, IndexSearchOpts{})
	require.NoError(t, err)
	results = append(results, nidx03RollbackResult(t, "prefix-svcB", prefix, nil))

	// Wildcard: inst-02 across every service.
	wildcardQuery := &pbv1.Series{Subject: "nidx03_rollback", EntityValues: []*modelv1.TagValue{
		pbv1.AnyTagValue, {Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "inst-02"}}},
	}}
	wildcard, _, err := si.Search(ctx, []*pbv1.Series{wildcardQuery}, IndexSearchOpts{})
	require.NoError(t, err)
	results = append(results, nidx03RollbackResult(t, "wildcard-inst-02", wildcard, nil))

	// Index-order sort over svcC's instances by the score field.
	svcCQuery := &pbv1.Series{Subject: "nidx03_rollback", EntityValues: []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svcC"}}}, pbv1.AnyTagValue,
	}}
	order := &index.OrderBy{Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC, Index: &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: nidx03RollbackScoreRule},
	}}
	sorted, sortedValues, err := si.Search(ctx, []*pbv1.Series{svcCQuery}, IndexSearchOpts{Order: order, PreloadSize: 10})
	require.NoError(t, err)
	results = append(results, nidx03RollbackResult(t, "sorted-svcC-by-score", sorted, sortedValues))

	return results
}

func nidx03RollbackResult(t *testing.T, name string, sd SeriesData, sortedValues [][]byte) nidx03RollbackQueryResult {
	t.Helper()
	type row struct {
		identity  string
		sorted    string
		timestamp int64
		version   int64
	}
	rows := make([]row, 0, len(sd.SeriesList))
	for i, series := range sd.SeriesList {
		var sortedHex string
		if i < len(sortedValues) {
			sortedHex = fmt.Sprintf("%x", sortedValues[i])
		}
		rows = append(rows, row{
			identity:  nidx03SeriesText(t, series),
			timestamp: sd.Timestamps[i],
			version:   sd.Versions[i],
			sorted:    sortedHex,
		})
	}
	if len(sortedValues) == 0 {
		// Unordered results: sort rows by identity for a deterministic
		// comparison across two independent readers.
		sort.Slice(rows, func(i, j int) bool { return rows[i].identity < rows[j].identity })
	}
	result := nidx03RollbackQueryResult{Name: name}
	for _, r := range rows {
		result.Identities = append(result.Identities, r.identity)
		result.Timestamps = append(result.Timestamps, r.timestamp)
		result.Versions = append(result.Versions, r.version)
		if r.sorted != "" {
			result.SortedHex = append(result.SortedHex, r.sorted)
		}
	}
	return result
}

func nidx03SeriesText(t *testing.T, series *pbv1.Series) string {
	t.Helper()
	parts := make([]string, 0, len(series.EntityValues))
	for _, v := range series.EntityValues {
		parts = append(parts, v.GetStr().GetValue())
	}
	return fmt.Sprintf("%v", parts)
}

// writeNIDX03IndexModeCorpus writes an index-mode (SearchWithoutSeries)
// workload: two subjects, one of which receives an update.
func writeNIDX03IndexModeCorpus(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)

	buildIndexModeDoc := func(subject, service string, score, timestamp, version int64) index.Document {
		var series pbv1.Series
		series.Subject = subject
		series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: service}}}}
		require.NoError(t, series.Marshal())
		imName := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, subject)
		imName.Index, imName.NoSort = true, true
		scoreField := index.NewBytesField(index.FieldKey{IndexRuleID: nidx03RollbackScoreRule}, fmt.Appendf(nil, "%010d", score))
		scoreField.Store, scoreField.Index = true, true
		return index.Document{
			EntityValues: append([]byte(nil), series.Buffer...),
			Fields:       []index.Field{imName, scoreField},
			Timestamp:    timestamp,
			Version:      version,
		}
	}

	var docs index.Documents
	for i := 0; i < 5; i++ {
		docs = append(docs, buildIndexModeDoc("idx_measure", fmt.Sprintf("svc-%02d", i), int64(i*10), int64(2000+i), 1))
	}
	docs = append(docs, buildIndexModeDoc("other_measure", "svc-other", 999, 3000, 1))
	require.NoError(t, si.Insert(docs))
	// Update one row in place.
	require.NoError(t, si.Update(index.Documents{buildIndexModeDoc("idx_measure", "svc-00", 12345, 4000, 2)}))

	sd, _, err := si.SearchWithoutSeries(ctx, IndexSearchOpts{IndexModeSubject: "idx_measure"})
	require.NoError(t, err)
	results := []nidx03RollbackQueryResult{nidx03RollbackResult(t, "indexmode-idx_measure", sd, nil)}
	require.NoError(t, si.Close())
	writeNIDX03QueryResults(t, filepath.Join(dir, "..", "indexmode-results.json"), results)
}

// envNIDX03CrashCutSubprocess marks a `go test` process as the crash-cut
// writer subprocess (see TestNIDX03CrashCutWriterSubprocess) rather than the
// normal test binary entry point; envNIDX03CrashCutSubprocessDir names the
// directory it writes to.
const (
	envNIDX03CrashCutSubprocess    = "NIDX03_CRASHCUT_SUBPROCESS"
	envNIDX03CrashCutSubprocessDir = "NIDX03_CRASHCUT_SUBPROCESS_DIR"
	nidx03CrashCutReadyLine        = "NIDX03_CRASHCUT_MIDCUT_WRITTEN"
)

// TestNIDX03CrashCutWriterSubprocess is not a normal test: it only does
// anything when envNIDX03CrashCutSubprocess is set, which only
// writeNIDX03CrashCutCorpus's exec.Command sets. It opens a series index,
// writes a batch that becomes durable ("precut"), writes a second batch that
// may still be in-memory-only ("midcut"), prints nidx03CrashCutReadyLine to
// stdout once that second Insert call has returned, and then blocks forever
// -- it never calls Close, so a real SIGKILL (not a copy of a live
// directory) is the only way this process ends, exactly as a killed
// production writer would.
func TestNIDX03CrashCutWriterSubprocess(t *testing.T) {
	dir := os.Getenv(envNIDX03CrashCutSubprocessDir)
	if os.Getenv(envNIDX03CrashCutSubprocess) == "" || dir == "" {
		t.Skip("crash-cut writer subprocess entry point; not a normal test")
	}
	ctx := context.Background()
	// A positive flush timeout selects asynchronous persistence
	// (PersistInterval > 0), matching production's default -- Insert
	// returns once the batch is admitted in memory, before the background
	// persist cycle commits it to disk.
	si, err := newSeriesIndex(ctx, dir, 5, 0, nil, &testRootLease{})
	require.NoError(t, err)
	require.NoError(t, si.Insert(index.Documents{nidx03RollbackDoc(t, "precut", "inst-00", "us", 1, 100, 1)}))
	// Wait for "precut" to become durable (not a fixed sleep racing
	// PersistInterval): poll the owner's own durable-generation watermark,
	// the same mechanism nidx03SettleCompaction uses, until it reaches the
	// generation "precut"'s Insert published.
	view, acquireErr := si.owner.Acquire(ctx)
	require.NoError(t, acquireErr)
	precutGeneration := view.Generation()
	require.NoError(t, view.Close())
	require.Eventually(t, func() bool {
		return si.owner.DurableGeneration() >= precutGeneration
	}, 30*time.Second, 20*time.Millisecond, "'precut' never became durable")
	require.NoError(t, si.Insert(index.Documents{nidx03RollbackDoc(t, "midcut", "inst-00", "us", 2, 200, 1)}))
	fmt.Println(nidx03CrashCutReadyLine)
	select {} // wait to be SIGKILLed; never reached otherwise.
}

// writeNIDX03CrashCutCorpus spawns this same test binary as a real OS
// subprocess running TestNIDX03CrashCutWriterSubprocess, waits for it to
// report that its second ("midcut") batch has been admitted, and sends it
// SIGKILL -- a real crash, not a directory copied out from under a still-open
// owner. It then separately writes a reference copy of the same "precut"
// write path (closed gracefully) to pin what a from-the-start-correct reader
// shows, and explicitly asserts the crash-cut directory's own state: the
// batch that had time to persist ("precut") is present, and the directory a
// previous release opens is a structurally complete generation (it may or
// may not include "midcut", depending on exactly when the kill lands -- that
// race is the point of a crash-cut, not a bug in this harness).
func writeNIDX03CrashCutCorpus(t *testing.T, dir string) {
	t.Helper()
	ctx := context.Background()
	crashDir := filepath.Join(dir, "sidx-parent")
	require.NoError(t, os.MkdirAll(crashDir, 0o755))

	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestNIDX03CrashCutWriterSubprocess$", "-test.v")
	cmd.Env = append(os.Environ(), envNIDX03CrashCutSubprocess+"=1", envNIDX03CrashCutSubprocessDir+"="+crashDir)
	stdout, pipeErr := cmd.StdoutPipe()
	require.NoError(t, pipeErr)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), nidx03CrashCutReadyLine) {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("crash-cut writer subprocess exited before signaling %q: %w", nidx03CrashCutReadyLine, scanner.Err())
	}()
	select {
	case readyErr := <-ready:
		require.NoError(t, readyErr)
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("crash-cut writer subprocess never signaled readiness")
	}
	// The subprocess is blocked in `select {}`, past its own last write
	// call: SIGKILL now is a real crash mid-persist, not a race with its own
	// shutdown path (it has none -- it never calls Close).
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait() // exits non-zero (killed); only the directory it left behind matters.

	// The crash-cut directory itself, exactly as the kill left it, is what
	// writeNIDX03NormalCorpus's sibling comparison and the previous release
	// both open: no copy, no post-processing.
	// The subprocess's own root is crashDir, so newSeriesIndex created
	// crashDir/sidx (path.Join(root, seriesIndexDirName)); copy THAT, not
	// crashDir itself, into this corpus's conventional dir/sidx layout.
	require.NoError(t, nidx03CopyDir(filepath.Join(crashDir, "sidx"), filepath.Join(dir, "sidx")))

	// Open the crash-cut directory with the current code too (a second,
	// independent read, exactly like the previous release's own read-side
	// test performs) and assert explicitly what a real crash leaves:
	// "precut" (which had 200ms to persist) must be present; the directory
	// must open at all, proving it is a complete generation, not a torn one.
	reopened, reopenErr := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, reopenErr, "a crash-cut directory must still open as a complete generation")
	sd, _, searchErr := reopened.Search(ctx, []*pbv1.Series{nidx03RollbackSeries("precut", "inst-00")}, IndexSearchOpts{})
	require.NoError(t, searchErr)
	require.Len(t, sd.SeriesList, 1, "the pre-crash, already-persisted 'precut' write must survive the crash")
	results := []nidx03RollbackQueryResult{nidx03RollbackResult(t, "crashcut-precut-reference", sd, nil)}
	require.NoError(t, reopened.Close())
	writeNIDX03QueryResults(t, filepath.Join(dir, "crashcut-results.json"), results)
}

// nidx03CopyDir recursively copies src to dst. The crash-cut writer always
// SIGKILLed, never copied live; this moves ITS directory (now static, since
// the process that wrote to it is dead) into this corpus's conventional
// `dir/sidx` layout, which every other writeNIDX03*Corpus function and the
// previous-release read side also use.
func nidx03CopyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relative, relErr := filepath.Rel(src, path)
		if relErr != nil {
			return relErr
		}
		target := filepath.Join(dst, relative)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		in, openErr := os.Open(path)
		if openErr != nil {
			return openErr
		}
		defer func() { _ = in.Close() }()
		out, createErr := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if createErr != nil {
			return createErr
		}
		_, copyErr := io.Copy(out, in)
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}
