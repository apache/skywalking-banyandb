// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

func TestControlledMergeSeedManifestFreezesSnapshotSelectionAndMatureClock(t *testing.T) {
	snapshotRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotRoot, "snapshot.snp"), []byte("snapshot"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(snapshotRoot, "0000000000000001"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(snapshotRoot, "0000000000000001", "meta.bin"), []byte("part"), 0o600))
	selection := storagetrace.BenchmarkMergeEvent{
		SelectionSHA256: "45aa16460d7ca36fc0ddaa1bc1d2e73f45109dbaab86328012798d5011e60b7b",
		InputPartIDs:    []uint64{3478, 3449, 3481}, InputRows: 147126, InputBytes: 36948453,
		MinTimestamp: 1767245676730035414, MaxTimestamp: 1767304511463187325, InputMinDepth: 2, InputMaxDepth: 4,
	}
	ledgers := map[string]string{"core": "core-sha", "latency": "latency-sha", "start_time": "start-sha"}
	depths := map[uint64]uint32{1: 2}

	manifest, buildErr := BuildControlledMergeSeedManifest(snapshotRoot, selection, 2*time.Hour, ledgers, depths)
	require.NoError(t, buildErr)
	require.Equal(t, uint32(1), manifest.Version)
	require.Equal(t, selection.SelectionSHA256, manifest.Selection.SHA256)
	require.Equal(t, selection.InputPartIDs, manifest.Selection.InputPartIDs)
	require.Equal(t, time.Unix(0, selection.MaxTimestamp).Add(2*time.Hour), manifest.MatureLogicalNow)
	require.NoError(t, ValidateControlledMergeSeedManifest(snapshotRoot, manifest, selection, ledgers, depths))

	require.NoError(t, os.WriteFile(filepath.Join(snapshotRoot, "snapshot.snp"), []byte("changed"), 0o600))
	require.ErrorContains(t, ValidateControlledMergeSeedManifest(snapshotRoot, manifest, selection, ledgers, depths), "snapshot manifest")
}

func TestControlledMergeRejectsUnknownPipelineMode(t *testing.T) {
	_, runErr := RunControlledMerge(context.Background(), ControlledMergeRunOptions{
		SeedManifestPath: filepath.Join(t.TempDir(), "missing-seed.json"),
		Mode:             "drop-everything",
	})
	require.ErrorContains(t, runErr, "unsupported controlled merge pipeline mode")
}

func TestControlledMergeRequiresNativeRetainAllPlugin(t *testing.T) {
	_, runErr := RunControlledMerge(context.Background(), ControlledMergeRunOptions{
		SeedManifestPath: filepath.Join(t.TempDir(), "missing-seed.json"),
		Mode:             string(ControlledMergePipelineRetainAll),
	})
	require.ErrorContains(t, runErr, "retain-all plugin path is required")
}

func TestControlledMergeSeedManifestRejectsSelectionDrift(t *testing.T) {
	snapshotRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(snapshotRoot, "snapshot.snp"), []byte("snapshot"), 0o600))
	selection := storagetrace.BenchmarkMergeEvent{
		SelectionSHA256: "45aa16460d7ca36fc0ddaa1bc1d2e73f45109dbaab86328012798d5011e60b7b",
		InputPartIDs:    []uint64{1, 2}, InputRows: 10, InputBytes: 20, MaxTimestamp: 100,
	}
	ledgers := map[string]string{"core": "a", "latency": "b", "start_time": "c"}
	depths := map[uint64]uint32{1: 0}
	manifest, buildErr := BuildControlledMergeSeedManifest(snapshotRoot, selection, 2*time.Hour, ledgers, depths)
	require.NoError(t, buildErr)
	selection.InputPartIDs = []uint64{2, 1}

	require.ErrorContains(t, ValidateControlledMergeSeedManifest(snapshotRoot, manifest, selection, ledgers, depths), "input part IDs")
}
