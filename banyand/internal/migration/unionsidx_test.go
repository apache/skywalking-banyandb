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

package migration

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// unionSidxTestLease is a minimal native.PathRootLease stub for writing
// throwaway source fixtures directly: these tests have no surrounding
// database lock to validate against.
type unionSidxTestLease struct{}

func (unionSidxTestLease) Validate() error           { return nil }
func (unionSidxTestLease) ValidatePath(string) error { return nil }

// Src-root resolution tests.

// TestCollectAllSrcGroupRoots_LiveDedup checks that the helper merges
// every entry's source dirs across the whole plan, drops duplicates,
// silently skips paths that don't host the group, and returns a sorted
// slice — these properties are what Phase A relies on to build a single
// union sidx per group.
func TestCollectAllSrcGroupRoots_LiveDedup(t *testing.T) {
	tmp := t.TempDir()
	hot0 := filepath.Join(tmp, "hot-0", "measure", "data")
	hot1 := filepath.Join(tmp, "hot-1", "measure", "data")
	warn0 := filepath.Join(tmp, "warn-0", "measure", "data")
	group := "sw_metricsMinute"

	// Only hot-0 and warn-0 actually carry the group; hot-1 doesn't.
	for _, root := range []string{hot0, warn0} {
		if err := os.MkdirAll(filepath.Join(root, group), 0o755); err != nil {
			t.Fatalf("seed %s: %v", root, err)
		}
	}
	if err := os.MkdirAll(hot1, 0o755); err != nil {
		t.Fatalf("seed hot-1: %v", err)
	}

	plan := &CopyPlan{
		Source: CopySource{Live: &LiveSource{Stages: map[string][]LiveStageNode{
			"hot":  {{Node: "hot-0", Root: hot0}, {Node: "hot-1", Root: hot1}},
			"warm": {{Node: "warn-0", Root: warn0}},
		}}},
		Entries: []CopyEntry{
			// Two entries both pointing at hot-0 — must dedup.
			{Stage: "hot", Target: filepath.Join(tmp, "out-a"), Nodes: []string{"hot-0"}},
			{Stage: "hot", Target: filepath.Join(tmp, "out-b"), Nodes: []string{"hot-0", "hot-1"}},
			{Stage: "warm", Target: filepath.Join(tmp, "out-c"), Nodes: []string{"warn-0"}},
		},
	}

	got := plan.CollectAllSrcGroupRoots(commonv1.Catalog_CATALOG_MEASURE, group)
	want := []string{
		filepath.Join(hot0, group),
		filepath.Join(warn0, group),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectAllSrcGroupRoots merged result mismatch:\n  got:  %v\n  want: %v", got, want)
	}
}

// TestCollectAllSrcGroupRoots_BackupNodes covers the backup-mode path
// (entry.Nodes drives root resolution via backupDir + date).
func TestCollectAllSrcGroupRoots_BackupNodes(t *testing.T) {
	tmp := t.TempDir()
	date := "2026-05-19"
	group := "sw_metricsMinute"

	for _, node := range []string{"hot-0", "warn-0"} {
		if err := os.MkdirAll(filepath.Join(tmp, node, date, "measure", group), 0o755); err != nil {
			t.Fatalf("seed %s: %v", node, err)
		}
	}

	plan := &CopyPlan{
		Source: CopySource{Backup: &BackupSource{Root: tmp, Date: date}},
		Entries: []CopyEntry{
			{Stage: "hot", Target: filepath.Join(tmp, "out-a"), Nodes: []string{"hot-0"}},
			// Entry-2 references both hot-0 (dup) and a non-existent node.
			{Stage: "hot", Target: filepath.Join(tmp, "out-b"), Nodes: []string{"hot-0", "ghost"}},
			{Stage: "warm", Target: filepath.Join(tmp, "out-c"), Nodes: []string{"warn-0"}},
		},
	}

	got := plan.CollectAllSrcGroupRoots(commonv1.Catalog_CATALOG_MEASURE, group)
	want := []string{
		filepath.Join(tmp, "hot-0", date, "measure", group),
		filepath.Join(tmp, "warn-0", date, "measure", group),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CollectAllSrcGroupRoots backup-mode mismatch:\n  got:  %v\n  want: %v", got, want)
	}
}

// Union sidx build tests.

// makeSidxSourceDoc builds one native series-index document whose layout
// mirrors what the measure write path emits into <segment>/sidx/: the
// identifier is the marshaled pbv1.Series buffer and the doc carries one
// stored+indexed tag field plus (optionally) a stored version field.
// Returns the SeriesID for assertions.
func makeSidxSourceDoc(t *testing.T, entity, tagValue string) (native.Document, common.SeriesID) {
	t.Helper()
	series := &pbv1.Series{
		Subject:      "m1",
		EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}}},
	}
	if err := series.Marshal(); err != nil {
		t.Fatalf("series.Marshal: %v", err)
	}
	doc := native.Document{
		Identifier: append([]byte(nil), series.Buffer...),
		Fields:     []native.Field{{Name: "service", Value: []byte(tagValue), Store: true, Index: true}},
	}
	return doc, series.ID
}

// writeSidxAt creates a native sidx at <seg>/sidx/ populated with docs. A nil
// or empty docs slice leaves the directory present but uncommitted (no
// generation ever published), the same shape
// TestBuildGroupUnionSidx_UncommittedSourceSidxIsEmpty exercises directly.
func writeSidxAt(t *testing.T, segDir string, docs []native.Document) {
	t.Helper()
	sidxPath := filepath.Join(segDir, sidxDirName)
	if err := os.MkdirAll(sidxPath, storage.DirPerm); err != nil {
		t.Fatalf("mkdir sidx: %v", err)
	}
	if len(docs) == 0 {
		return
	}
	owner, err := native.NewOwner(native.OwnerOptions{Lease: unionSidxTestLease{}, Path: sidxPath, IdentifierDocValues: true})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	done := make(chan error, 1)
	if err := owner.Batch(context.Background(), native.Batch{
		Documents:          docs,
		PersistentCallback: func(batchErr error) { done <- batchErr },
	}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("persist: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// countDocsInSidx opens a native sidx and returns the sorted list of
// SeriesIDs of docs whose _id unmarshals into a pbv1.Series.
func countDocsInSidx(t *testing.T, path string) []common.SeriesID {
	t.Helper()
	generation, err := native.OpenReadOnlyGeneration(path)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer func() { _ = generation.Close() }()
	var ids []common.SeriesID
	visitErr := generation.VisitLiveDocuments(context.Background(), func(doc native.StoredDocument) error {
		var entity []byte
		_ = doc.VisitStoredFields(func(field string, value []byte) bool {
			if field == sidxDocIDField {
				entity = append([]byte(nil), value...)
			}
			return true
		})
		if len(entity) == 0 {
			return nil
		}
		var s pbv1.Series
		if err := s.Unmarshal(entity); err != nil {
			return nil
		}
		ids = append(ids, s.ID)
		return nil
	})
	if visitErr != nil {
		t.Fatalf("iterate: %v", visitErr)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func TestBuildGroupUnionSidx_DeduplicatesAcrossSegments(t *testing.T) {
	srcGroupRoot := t.TempDir()
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")

	// Three source segments. Seg A carries entity {alpha, beta}, Seg B
	// carries {beta, gamma} (beta is the cross-segment overlap that the
	// dedup logic must collapse), Seg C carries {delta} only.
	segA := filepath.Join(srcGroupRoot, segPrefix+"20260101")
	segB := filepath.Join(srcGroupRoot, segPrefix+"20260102")
	segC := filepath.Join(srcGroupRoot, segPrefix+"20260103")

	docAlpha, sidAlpha := makeSidxSourceDoc(t, "alpha", "svc-a")
	docBetaA, sidBeta := makeSidxSourceDoc(t, "beta", "svc-b-from-A")
	docBetaB, sidBetaB := makeSidxSourceDoc(t, "beta", "svc-b-from-B")
	docGamma, sidGamma := makeSidxSourceDoc(t, "gamma", "svc-c")
	docDelta, sidDelta := makeSidxSourceDoc(t, "delta", "svc-d")

	if sidBeta != sidBetaB {
		t.Fatalf("setup mismatch: beta SeriesID should be stable across segments, got %d vs %d", sidBeta, sidBetaB)
	}

	writeSidxAt(t, segA, []native.Document{docAlpha, docBetaA})
	writeSidxAt(t, segB, []native.Document{docBetaB, docGamma})
	writeSidxAt(t, segC, []native.Document{docDelta})

	resultPath, err := BuildGroupUnionSidx(context.Background(), []string{srcGroupRoot}, stagingPath, nil)
	if err != nil {
		t.Fatalf("BuildGroupUnionSidx: %v", err)
	}
	if resultPath != stagingPath {
		t.Fatalf("expected resultPath==stagingPath, got %q vs %q", resultPath, stagingPath)
	}

	got := countDocsInSidx(t, resultPath)
	want := []common.SeriesID{sidAlpha, sidBeta, sidGamma, sidDelta}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("expected %d unique SeriesIDs, got %d (%v)", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("SeriesID mismatch at %d: got %d want %d", i, got[i], w)
		}
	}
}

// TestBuildGroupUnionSidx_PreservesVersionField is the L6 regression:
// buildNativeDocumentLocked built a native.Field{Name, Value} for
// sidxVersionField with no Store set, so the Index=false, Store=false
// zero value carried neither an index entry nor a stored value -- _version
// was silently dropped from the union sidx. It must survive the build,
// matching storage.EncodeSeriesDocument's own "_version" is stored-only"
// mapping.
func TestBuildGroupUnionSidx_PreservesVersionField(t *testing.T) {
	srcGroupRoot := t.TempDir()
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")

	seg := filepath.Join(srcGroupRoot, segPrefix+"20260101")
	doc, sid := makeSidxSourceDoc(t, "alpha", "svc-a")
	doc.Fields = append(doc.Fields, native.Field{Name: sidxVersionField, Value: convert.Int64ToBytes(7), Store: true})
	writeSidxAt(t, seg, []native.Document{doc})

	resultPath, err := BuildGroupUnionSidx(context.Background(), []string{srcGroupRoot}, stagingPath, nil)
	if err != nil {
		t.Fatalf("BuildGroupUnionSidx: %v", err)
	}

	generation, err := native.OpenReadOnlyGeneration(resultPath)
	if err != nil {
		t.Fatalf("OpenReadOnlyGeneration: %v", err)
	}
	defer func() { _ = generation.Close() }()

	var gotVersion []byte
	var found bool
	visitErr := generation.VisitLiveDocuments(context.Background(), func(doc native.StoredDocument) error {
		var entity []byte
		var version []byte
		_ = doc.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case sidxDocIDField:
				entity = append([]byte(nil), value...)
			case sidxVersionField:
				version = append([]byte(nil), value...)
			}
			return true
		})
		var s pbv1.Series
		if len(entity) > 0 {
			if err := s.Unmarshal(entity); err == nil && s.ID == sid {
				found = true
				gotVersion = version
			}
		}
		return nil
	})
	if visitErr != nil {
		t.Fatalf("VisitLiveDocuments: %v", visitErr)
	}
	if !found {
		t.Fatalf("expected to find series %d in the union sidx", sid)
	}
	if len(gotVersion) == 0 {
		t.Fatalf("expected a stored _version value, got none")
	}
	if got := convert.BytesToInt64(gotVersion); got != 7 {
		t.Fatalf("expected _version 7, got %d", got)
	}
}

func TestBuildGroupUnionSidx_EmptyGroupYieldsEmptyPath(t *testing.T) {
	srcGroupRoot := t.TempDir()
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")

	resultPath, err := BuildGroupUnionSidx(context.Background(), []string{srcGroupRoot}, stagingPath, nil)
	if err != nil {
		t.Fatalf("BuildGroupUnionSidx: %v", err)
	}
	if resultPath != "" {
		t.Fatalf("expected empty result path for empty group, got %q", resultPath)
	}
}

// TestBuildGroupUnionSidx_UncommittedSourceSidxIsEmpty verifies a discovered
// source sidx with no committed generation is tolerated as an empty source.
func TestBuildGroupUnionSidx_UncommittedSourceSidxIsEmpty(t *testing.T) {
	srcGroupRoot := t.TempDir()
	srcSidxPath := filepath.Join(srcGroupRoot, segPrefix+"20260101", sidxDirName)
	if err := os.MkdirAll(srcSidxPath, storage.DirPerm); err != nil {
		t.Fatalf("mkdir uncommitted source sidx: %v", err)
	}
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")

	resultPath, err := BuildGroupUnionSidx(context.Background(), []string{srcGroupRoot}, stagingPath, nil)
	if err != nil {
		t.Fatalf("BuildGroupUnionSidx: %v", err)
	}
	if resultPath != "" {
		t.Fatalf("expected empty result path for an uncommitted source sidx, got %q", resultPath)
	}
}

// TestBuildGroupUnionSidx_RecoveredWorkerPanicReturnsError verifies a panic
// recovered from a source worker fails the union rather than publishing it.
func TestBuildGroupUnionSidx_RecoveredWorkerPanicReturnsError(t *testing.T) {
	srcGroupRoot := t.TempDir()
	segPath := filepath.Join(srcGroupRoot, segPrefix+"20260101")
	writeSidxAt(t, segPath, nil)
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")

	resultPath, buildErr := BuildGroupUnionSidx(context.Background(), []string{srcGroupRoot}, stagingPath,
		func(format string, _ ...any) {
			if format == "union sidx: scanned %d/%d sidx dir(s) (%.1f%%): %s" {
				panic("source worker panic")
			}
		})
	if buildErr == nil {
		t.Fatal("expected recovered source-worker panic to fail the union")
	}
	if resultPath != "" {
		t.Fatalf("expected no published path after a recovered worker panic, got %q", resultPath)
	}
	if _, statErr := os.Stat(stagingPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected failed union staging path to be removed, stat error: %v", statErr)
	}
}

func TestBuildGroupUnionSidx_MissingSrcRootIsNoop(t *testing.T) {
	stagingPath := filepath.Join(t.TempDir(), "union", "sidx")
	resultPath, err := BuildGroupUnionSidx(context.Background(), []string{filepath.Join(t.TempDir(), "no-such-dir")}, stagingPath, nil)
	if err != nil {
		t.Fatalf("BuildGroupUnionSidx with missing src: %v", err)
	}
	if resultPath != "" {
		t.Fatalf("expected empty result path when src root missing, got %q", resultPath)
	}
}
