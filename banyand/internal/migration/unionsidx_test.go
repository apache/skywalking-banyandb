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

	"github.com/blugelabs/bluge"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

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

// makeSidxSourceDoc builds one bluge doc whose layout mirrors what the
// measure write path emits into <segment>/sidx/: bluge _id is the marshaled
// pbv1.Series buffer and the doc carries one stored+indexed tag field plus
// (optionally) a stored version field. Returns the SeriesID for assertions.
func makeSidxSourceDoc(t *testing.T, entity, tagValue string) (*bluge.Document, common.SeriesID) {
	t.Helper()
	series := &pbv1.Series{
		Subject:      "m1",
		EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}}},
	}
	if err := series.Marshal(); err != nil {
		t.Fatalf("series.Marshal: %v", err)
	}
	doc := bluge.NewDocument(string(series.Buffer))
	doc.AddField(bluge.NewKeywordFieldBytes("service", []byte(tagValue)).StoreValue())
	return doc, series.ID
}

// writeSidxAt creates a bluge sidx at <seg>/sidx/ populated with docs.
func writeSidxAt(t *testing.T, segDir string, docs []*bluge.Document) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(segDir, sidxDirName), storage.DirPerm); err != nil {
		t.Fatalf("mkdir sidx: %v", err)
	}
	w, err := bluge.OpenWriter(bluge.DefaultConfig(filepath.Join(segDir, sidxDirName)))
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	batch := bluge.NewBatch()
	for _, d := range docs {
		batch.Insert(d)
	}
	if err := w.Batch(batch); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// countDocsInSidx opens a bluge sidx and returns the count of docs whose
// _id unmarshals into a pbv1.Series, plus the sorted list of SeriesIDs.
func countDocsInSidx(t *testing.T, path string) []common.SeriesID {
	t.Helper()
	r, err := bluge.OpenReader(bluge.DefaultConfig(path))
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer r.Close()
	dmi, err := r.Search(context.Background(), bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	var ids []common.SeriesID
	for {
		next, e := dmi.Next()
		if e != nil {
			t.Fatalf("iterate: %v", e)
		}
		if next == nil {
			break
		}
		var entity []byte
		_ = next.VisitStoredFields(func(field string, value []byte) bool {
			if field == sidxDocIDField {
				entity = append([]byte(nil), value...)
			}
			return true
		})
		if len(entity) == 0 {
			continue
		}
		var s pbv1.Series
		if err := s.Unmarshal(entity); err != nil {
			continue
		}
		ids = append(ids, s.ID)
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

	writeSidxAt(t, segA, []*bluge.Document{docAlpha, docBetaA})
	writeSidxAt(t, segB, []*bluge.Document{docBetaB, docGamma})
	writeSidxAt(t, segC, []*bluge.Document{docDelta})

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
