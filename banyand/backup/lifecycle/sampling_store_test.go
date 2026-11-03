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

package lifecycle

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

func TestSamplingDecisionTableIsAuthoritative(t *testing.T) {
	directory := t.TempDir()
	writer, err := newSamplingDecisionWriter(directory)
	require.NoError(t, err)
	require.NoError(t, writer.append("a", false))
	require.NoError(t, writer.append("b", true))
	require.NoError(t, writer.append("c", false))
	require.ErrorContains(t, writer.append("b", true), "not strictly ordered")
	require.NoError(t, writer.close())
	tableDigest, err := samplingFileDigest(filepath.Join(directory, "decisions"))
	require.NoError(t, err)
	indexDigest, err := samplingFileDigest(filepath.Join(directory, "offsets"))
	require.NoError(t, err)
	manifest := &samplingManifest{TableDigest: tableDigest, IndexDigest: indexDigest, Traces: 3}
	table, err := openSamplingDecisionTable(directory, manifest)
	require.NoError(t, err)
	for id, expected := range map[string]bool{"a": false, "b": true, "c": false} {
		keep, lookupErr := table.lookup(id)
		require.NoError(t, lookupErr)
		require.Equal(t, expected, keep)
	}
	_, err = table.lookup("missing")
	require.ErrorContains(t, err, "missing from authoritative")
	require.NoError(t, table.close())
	require.NoError(t, os.WriteFile(filepath.Join(directory, "decisions"), []byte("corruption"), 0o600))
	_, err = openSamplingDecisionTable(directory, manifest)
	require.ErrorContains(t, err, "checksum mismatch")
	require.NoError(t, os.Remove(filepath.Join(directory, "decisions")))
	_, err = openSamplingDecisionTable(directory, manifest)
	require.Error(t, err, "missing state must not create replacement verdicts")
}

func TestSamplingAtomicCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	initial := samplingCheckpoint{Sequence: 1, Cursor: samplingCursor{Part: "part-1", Block: 2, Row: 3}}
	require.NoError(t, atomicSamplingJSON(path, initial))
	advanced := samplingCheckpoint{Sequence: 2, Cursor: samplingCursor{Part: "part-2", Block: 0, Row: 1}, Delivered: true}
	require.NoError(t, atomicSamplingJSON(path, advanced))
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var restored samplingCheckpoint
	require.NoError(t, json.Unmarshal(payload, &restored))
	require.Equal(t, advanced, restored)
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	require.Len(t, entries, 1, "publication should leave no temporary commit artifacts")
	require.Error(t, atomicSamplingJSON(filepath.Join(path, "invalid-parent"), advanced), "durability failures must propagate")
	require.Error(t, atomicSamplingJSON(filepath.Dir(path), advanced), "rename over directory must fail")
	payload, err = os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &restored))
	require.Equal(t, advanced, restored, "failed publication must preserve last durable cursor")
}

func TestSamplingCheckpointsWaitForContiguousConfirmations(t *testing.T) {
	pipeline := newConfirmPipeline()
	first, second := make(chan error, 1), make(chan error, 1)
	checkpoints := make(chan int, 2)
	require.NoError(t, pipeline.addConfirmed(first, func() error { checkpoints <- 1; return nil }))
	require.NoError(t, pipeline.addConfirmed(second, func() error { checkpoints <- 2; return nil }))
	second <- nil
	finished := make(chan error, 1)
	go func() { finished <- pipeline.drain() }()
	select {
	case sequence := <-checkpoints:
		t.Fatalf("checkpoint %d passed an unconfirmed predecessor", sequence)
	case <-time.After(10 * time.Millisecond):
	}
	first <- nil
	require.NoError(t, <-finished)
	require.Equal(t, 1, <-checkpoints)
	require.Equal(t, 2, <-checkpoints)
}

func TestSamplingCheckpointFailureStopsAdvanceAndDrains(t *testing.T) {
	pipeline := newConfirmPipeline()
	first, second := make(chan error, 1), make(chan error, 1)
	advanced := 0
	failure := errors.New("manifest filesystem full")
	require.NoError(t, pipeline.addConfirmed(first, func() error { return failure }))
	require.NoError(t, pipeline.addConfirmed(second, func() error { advanced++; return nil }))
	first <- nil
	second <- nil
	require.ErrorIs(t, pipeline.drain(), failure)
	require.Zero(t, advanced)
	require.Empty(t, pipeline.inflight, "failure must still drain outstanding confirmations")
	require.True(t, pipeline.checkpointFailed)
	require.NoError(t, pipeline.drain())
}

func TestSamplingAuthoritativeStateRejectsTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint.json")
	checkpoint := samplingCheckpoint{Sequence: 7, Cursor: samplingCursor{Part: "part", Block: 2, Row: 4}}
	require.NoError(t, saveSamplingCheckpoint(path, checkpoint))
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	restored, err := readSamplingCheckpoint(payload)
	require.NoError(t, err)
	require.Equal(t, checkpoint, restored)
	var record samplingCheckpointRecord
	require.NoError(t, json.Unmarshal(payload, &record))
	record.Checkpoint.Sequence++
	tampered, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = readSamplingCheckpoint(tampered)
	require.ErrorContains(t, err, "checksum mismatch")
	manifest := &samplingManifest{Version: samplingArtifactVersion, Group: "group", PolicyDigest: "policy"}
	require.NoError(t, sealSamplingManifest(manifest))
	require.NoError(t, verifySamplingManifest(manifest))
	manifest.TargetShardNum++
	require.ErrorContains(t, verifySamplingManifest(manifest), "checksum mismatch")
}

func TestSamplingRecoveryRestoresPinnedGroupWithoutLegacyProgress(t *testing.T) {
	root := t.TempDir()
	incomplete := filepath.Join(root, ".preparing-interrupted")
	require.NoError(t, os.Mkdir(incomplete, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(incomplete, "manifest.json"), []byte("unfinished"), 0o600))
	progress := &Progress{CompletedGroups: map[string]bool{"group": true}, DeletedTraceGroups: map[string]bool{"group": true}}
	groups, err := recoverSamplingAttempts(root, progress)
	require.NoError(t, err)
	require.Empty(t, groups)
	group := &commonv1.Group{Metadata: &commonv1.Metadata{Name: "group"}, Catalog: commonv1.Catalog_CATALOG_TRACE, ResourceOpts: &commonv1.ResourceOpts{ShardNum: 3}}
	encoded, err := proto.Marshal(group)
	require.NoError(t, err)
	manifest := &samplingManifest{Version: samplingArtifactVersion, Group: "group", Snapshot: root, GroupConfig: encoded}
	require.NoError(t, sealSamplingManifest(manifest))
	prepared := filepath.Join(root, "published")
	require.NoError(t, os.Mkdir(prepared, 0o700))
	require.NoError(t, atomicSamplingJSON(filepath.Join(prepared, "manifest.json"), manifest))
	groups, err = recoverSamplingAttempts(root, progress)
	require.NoError(t, err)
	require.True(t, proto.Equal(group, groups["group"]))
	require.Equal(t, root, progress.SnapshotTraceDir)
	require.False(t, progress.CompletedGroups["group"])
	require.False(t, progress.DeletedTraceGroups["group"])
}

func TestSamplingModePinsLegacyAndSampledWork(t *testing.T) {
	root, snapshot := t.TempDir(), t.TempDir()
	selected, err := selectSamplingMode(root, "legacy", snapshot, true, true)
	require.NoError(t, err)
	require.False(t, selected, "new stage rules cannot retroactively sample legacy in-flight work")
	selected, err = selectSamplingMode(root, "legacy", snapshot, false, true)
	require.NoError(t, err)
	require.False(t, selected, "restart must preserve original transport mode")
	selected, err = selectSamplingMode(root, "sampled", snapshot, false, true)
	require.NoError(t, err)
	require.True(t, selected)
	selected, err = selectSamplingMode(root, "sampled", snapshot, false, false)
	require.NoError(t, err)
	require.True(t, selected, "disabled current policy must not remove prepared-mode ownership")
}

func TestSamplingRecoveryRejectsMissingRegisteredArtifact(t *testing.T) {
	root, snapshot := t.TempDir(), t.TempDir()
	selected, err := selectSamplingMode(root, "group", snapshot, false, true)
	require.NoError(t, err)
	require.True(t, selected)
	preparing := filepath.Join(root, "group", ".preparing-interrupted")
	require.NoError(t, os.Mkdir(preparing, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(preparing, "manifest.json"), []byte("incomplete"), 0o600))
	found, err := hasPreparedSampling(root, "group")
	require.NoError(t, err)
	require.False(t, found, "incomplete preparation cannot select prepared replay")
	group := &commonv1.Group{Metadata: &commonv1.Metadata{Name: "group"}, Catalog: commonv1.Catalog_CATALOG_TRACE}
	encoded, err := proto.Marshal(group)
	require.NoError(t, err)
	manifest := samplingManifest{Version: samplingArtifactVersion, Snapshot: snapshot, Group: "group", GroupConfig: encoded}
	require.NoError(t, sealSamplingManifest(&manifest))
	directory := filepath.Join(root, "group", "published")
	require.NoError(t, os.Mkdir(directory, 0o700))
	require.NoError(t, atomicSamplingJSON(filepath.Join(directory, "manifest.json"), &manifest))
	require.NoError(t, registerPreparedSampling(root, &preparedTraceMigration{directory: directory, manifest: manifest}))
	require.NoError(t, os.RemoveAll(directory))
	_, err = recoverSamplingAttempts(root, &Progress{})
	require.ErrorContains(t, err, "required prepared sampling artifact missing", "registered prepared work cannot silently become a new sampling attempt")
}

func TestSamplingModeOnlyRecoveryPreservesSnapshotWithoutPinningConfiguration(t *testing.T) {
	root, snapshot := t.TempDir(), t.TempDir()
	selected, err := selectSamplingMode(root, "group", snapshot, false, true)
	require.NoError(t, err)
	require.True(t, selected)
	progress := &Progress{}
	groups, err := recoverSamplingAttempts(root, progress)
	require.NoError(t, err)
	require.Empty(t, groups, "preparation failures must allow correction of invalid current policy")
	require.Equal(t, snapshot, progress.SnapshotTraceDir)
	require.True(t, progress.samplingOwnedGroups["group"])
}
