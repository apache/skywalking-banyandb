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
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

// TestDirtyTargets_AllListedUpFront: every non-empty target the run would
// write into must be reported together, before any copy work, so the
// operator cleans them all in a single pass instead of hitting them one at
// a time mid-run.
func TestDirtyTargets_AllListedUpFront(t *testing.T) {
	root := t.TempDir()
	date := "2026-06-09"
	seedSrc := func(node, group string) {
		require.NoError(t, os.MkdirAll(filepath.Join(root, node, date, "stream", group), 0o755))
	}
	dirtyTarget := func(target, group string) string {
		dir := filepath.Join(target, group)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "seg-leftover"), []byte("x"), 0o600))
		return dir
	}
	seedSrc("hot-0", "g-a")
	seedSrc("hot-0", "g-b")
	seedSrc("warn-0", "g-a")

	hotTarget := filepath.Join(root, "out-hot")
	warmTarget := filepath.Join(root, "out-warm")
	plan := &CopyPlan{
		Source: CopySource{Backup: &BackupSource{Root: root, Date: date}},
		Groups: []string{"g-a", "g-b"},
		Entries: []CopyEntry{
			{Stage: "hot", Target: hotTarget, Nodes: []string{"hot-0"}},
			{Stage: "warm", Target: warmTarget, Nodes: []string{"warn-0"}},
		},
	}
	entries := plan.ResolvedEntries()
	groups := []string{"g-a", "g-b"}

	// Clean targets report nothing.
	clean, err := plan.dirtyTargets(commonv1.Catalog_CATALOG_STREAM, entries, groups)
	require.NoError(t, err)
	require.Empty(t, clean)

	// Dirty (hot, g-a) and (warm, g-a) must BOTH be reported.
	dirtyA := dirtyTarget(hotTarget, "g-a")
	dirtyWarmA := dirtyTarget(warmTarget, "g-a")
	// (warm, g-b) has no source dirs (warn-0 doesn't carry g-b), so a dirty
	// target there is ignored — Phase B would skip the pair anyway.
	skipped := dirtyTarget(warmTarget, "g-b")

	dirty, err := plan.dirtyTargets(commonv1.Catalog_CATALOG_STREAM, entries, groups)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{dirtyA, dirtyWarmA}, dirty)
	require.NotContains(t, dirty, skipped)
}

// skipUnionSidxExecutor is a fake CatalogExecutor that also implements the
// optional unionSidxSkipper capability: it reports skip for the groups in
// skipGroups and records the UnionSidxPath each CopyEntryGroup received.
type skipUnionSidxExecutor struct {
	gotUnionSidxPath map[string]string
	skipGroups       map[string]bool
}

func (e *skipUnionSidxExecutor) Catalog() commonv1.Catalog { return commonv1.Catalog_CATALOG_MEASURE }

func (e *skipUnionSidxExecutor) LogPrefix() string { return "[migration/measure-test]" }

func (e *skipUnionSidxExecutor) Prepare(_ context.Context, _ string, _ []string) error { return nil }

func (e *skipUnionSidxExecutor) CopyEntryGroup(_ context.Context, in EntryGroupInput) (EntryGroupResult, error) {
	if e.gotUnionSidxPath == nil {
		e.gotUnionSidxPath = map[string]string{}
	}
	e.gotUnionSidxPath[in.Group] = in.UnionSidxPath
	return EntryGroupResult{}, nil
}

func (e *skipUnionSidxExecutor) VerifyEntryGroup(_ context.Context, _ EntryGroupInput, _ func(report any)) error {
	return nil
}

func (e *skipUnionSidxExecutor) SkipUnionSidx(group string) bool { return e.skipGroups[group] }

// TestRunCopy_SkipsUnionSidxForIndexModeGroup verifies that a group an executor
// reports via SkipUnionSidx never goes through the Phase A union-sidx build and
// receives UnionSidxPath=="" in CopyEntryGroup, while a non-skipped group with
// real source sidx docs gets a non-empty UnionSidxPath broadcast.
func TestRunCopy_SkipsUnionSidxForIndexModeGroup(t *testing.T) {
	root := t.TempDir()
	stagingDir := t.TempDir()

	// Live-mode source so ResolvedEntries populates entry.Source; the per-group
	// source dirs are <root>/<group>. Seed both groups so neither is skipped for
	// "no source dirs". The index-mode group is left without any sidx so even if
	// a union build were (wrongly) attempted it would produce nothing.
	skipGroup := "sw_metadata"
	normalGroup := "sw_metrics"
	require.NoError(t, os.MkdirAll(filepath.Join(root, skipGroup), 0o755))
	// Seed a real source sidx under the normal group so Phase A's union build
	// yields a non-empty path it can broadcast.
	doc, _ := makeSidxSourceDoc(t, "alpha", "svc-a")
	writeSidxAt(t, filepath.Join(root, normalGroup, segPrefix+"20260101"), []*bluge.Document{doc})

	dayInterval := &commonv1.IntervalRule{
		Unit: commonv1.IntervalRule_UNIT_DAY,
		Num:  1,
	}
	groupProto := func(name string) *commonv1.Group {
		return &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name},
			Catalog:  commonv1.Catalog_CATALOG_MEASURE,
			ResourceOpts: &commonv1.ResourceOpts{
				SegmentInterval: dayInterval,
			},
		}
	}
	buckets := map[commonv1.Catalog][]string{}
	buckets[commonv1.Catalog_CATALOG_MEASURE] = []string{skipGroup, normalGroup}
	cls := &Classified{
		Buckets: buckets,
		Groups: map[string]*commonv1.Group{
			skipGroup:   groupProto(skipGroup),
			normalGroup: groupProto(normalGroup),
		},
		SchemaRoot: filepath.Join(root, "_schema"),
	}

	plan := &CopyPlan{
		Source: CopySource{Live: &LiveSource{
			SchemaPropertyPath: filepath.Join(root, "_schema"),
			Stages: map[string][]LiveStageNode{
				"hot": {{Node: "hot-0", Root: root}},
			},
		}},
		Groups: []string{skipGroup, normalGroup},
		Entries: []CopyEntry{
			{Stage: "hot", Target: filepath.Join(t.TempDir(), "out"), Nodes: []string{"hot-0"}},
		},
	}

	exec := &skipUnionSidxExecutor{skipGroups: map[string]bool{skipGroup: true}}
	_, err := plan.RunCopy(context.Background(), stagingDir, cls, []CatalogExecutor{exec})
	require.NoError(t, err)

	gotSkip, ok := exec.gotUnionSidxPath[skipGroup]
	require.True(t, ok, "index-mode group must still be copied")
	require.Empty(t, gotSkip, "index-mode group must receive empty UnionSidxPath")

	gotNormal, ok := exec.gotUnionSidxPath[normalGroup]
	require.True(t, ok, "normal group must be copied")
	require.NotEmpty(t, gotNormal, "normal group must receive a broadcast union sidx path")
}
