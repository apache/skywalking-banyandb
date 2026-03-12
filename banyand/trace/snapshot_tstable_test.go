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

package trace

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func traceSnapshotOption() option {
	return option{
		flushTimeout: 0,
		mergePolicy:  newDefaultMergePolicyForTesting(),
		protector:    protector.Nop{},
	}
}

func TestTraceMustWriteSnapshotUsesTempThenRename(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	const epoch = uint64(0x10)
	partNames := []string{partName(0x1)}
	tst.mustWriteSnapshot(epoch, partNames)
	snpPath := filepath.Join(tabDir, snapshotName(epoch))
	tmpPathFile := snpPath + ".tmp"
	require.False(t, fileSystem.IsExist(tmpPathFile), "temp file must be removed after rename")
	require.True(t, fileSystem.IsExist(snpPath), "final snapshot file must exist")
	data, readErr := fileSystem.Read(snpPath)
	require.NoError(t, readErr)
	var decoded []string
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, partNames, decoded)
}

func TestTraceMustWriteSnapshotWithMultipleParts(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	partNames := []string{partName(0x1), partName(0x2), partName(0xff)}
	tst.mustWriteSnapshot(1, partNames)
	parts, readErr := tst.readSnapshot(1)
	require.NoError(t, readErr)
	require.Equal(t, []uint64{0x1, 0x2, 0xff}, parts)
}

func TestTraceReadSnapshotReturnsErrorOnEmptyFile(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
	require.Contains(t, readErr.Error(), "cannot parse")
}

func TestTraceReadSnapshotReturnsErrorOnInvalidJSON(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte("{invalid}"), snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
	require.Contains(t, readErr.Error(), "cannot parse")
}

func TestTraceReadSnapshotReturnsErrorOnInvalidPartName(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	partNamesData, marshalErr := json.Marshal([]string{"not-hex"})
	require.NoError(t, marshalErr)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write(partNamesData, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	_, readErr := tst.readSnapshot(1)
	require.Error(t, readErr)
}

func TestTraceReadSnapshotSucceedsOnValidFile(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	tst.mustWriteSnapshot(1, []string{partName(0xa), partName(0xb)})
	parts, readErr := tst.readSnapshot(1)
	require.NoError(t, readErr)
	require.Equal(t, []uint64{0xa, 0xb}, parts)
}

func TestTraceMustReadSnapshotPanicsOnError(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst := &tsTable{fileSystem: fileSystem, root: tabDir}
	require.Panics(t, func() { _ = tst.mustReadSnapshot(1) })
}

func TestTraceInitTSTableEmptyDirectory(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, epoch := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	require.NotNil(t, tst)
	require.Greater(t, epoch, uint64(0))
	require.Nil(t, tst.snapshot)
}

func TestTraceInitTSTableEmptyTableWhenAllSnapshotsCorrupt(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	snpPath := filepath.Join(tabDir, snapshotName(1))
	_, writeErr := fileSystem.Write([]byte{}, snpPath, 0o600)
	require.NoError(t, writeErr)
	tst, epoch := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	require.NotNil(t, tst)
	require.Greater(t, epoch, uint64(0))
	require.Nil(t, tst.snapshot)
	require.False(t, fileSystem.IsExist(snpPath), "corrupt snapshot file must be deleted")
}

func TestTraceTolerantLoaderFallbackToOlderSnapshot(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()
	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)
	tst, err := newTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"),
		timestamp.TimeRange{}, traceSnapshotOption(), nil)
	require.NoError(t, err)
	tst.mustAddTraces(tsTS1, nil)
	time.Sleep(100 * time.Millisecond)
	require.Eventually(t, func() bool {
		dd := fileSystem.ReadDir(tabDir)
		for _, d := range dd {
			if d.IsDir() && d.Name() != sidxDirName && d.Name() != storage.FailedPartsDirName {
				return true
			}
		}
		return false
	}, flags.EventuallyTimeout, time.Millisecond, "wait for part")
	tst.Close()
	snapshots := make([]uint64, 0)
	for _, e := range fileSystem.ReadDir(tabDir) {
		if filepath.Ext(e.Name()) == snapshotSuffix {
			parsed, parseErr := parseSnapshot(e.Name())
			if parseErr == nil {
				snapshots = append(snapshots, parsed)
			}
		}
	}
	require.GreaterOrEqual(t, len(snapshots), 1)
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i] > snapshots[j] })
	validEpoch := snapshots[0]
	corruptEpoch := validEpoch + 1
	corruptPath := filepath.Join(tabDir, snapshotName(corruptEpoch))
	_, writeErr := fileSystem.Write([]byte{}, corruptPath, 0o600)
	require.NoError(t, writeErr)
	tst2, epoch2 := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	require.NotNil(t, tst2)
	require.Equal(t, validEpoch, epoch2, "should load older valid snapshot when newest is corrupt")
	require.NotNil(t, tst2.snapshot)
	require.Equal(t, validEpoch, tst2.snapshot.epoch)
}
