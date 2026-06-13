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
	"os"
	"path/filepath"
	"testing"

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
