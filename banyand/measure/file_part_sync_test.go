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

package measure

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func buildAndFlushMeasureMemPart(t *testing.T, fileSystem fs.FileSystem, dps *dataPoints, destPath string) {
	t.Helper()
	mp := generateMemPart()
	mp.mustInitFromDataPoints(dps)
	mp.mustFlush(fileSystem, destPath)
	releaseMemPart(mp)
}

func makeTestDataPoints(now int64) *dataPoints {
	return &dataPoints{
		seriesIDs:  []common.SeriesID{1, 2},
		timestamps: []int64{now, now + 1000},
		versions:   []int64{1, 2},
		tagFamilies: [][]nameValues{
			{
				{
					name: "singleTag",
					values: []*nameValue{
						{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("val1"), valueArr: nil},
					},
				},
			},
			{
				{
					name: "singleTag",
					values: []*nameValue{
						{name: "strTag", valueType: pbv1.ValueTypeStr, value: []byte("val2"), valueArr: nil},
					},
				},
			},
		},
		fields: []nameValues{
			{
				name: "intField",
				values: []*nameValue{
					{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(100), valueArr: nil},
				},
			},
			{
				name: "intField",
				values: []*nameValue{
					{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(200), valueArr: nil},
				},
			},
		},
	}
}

// TestMustAddFilePart_FilesOnDisk verifies that mustAddFilePart introduces a file-backed part
// into the snapshot with mp == nil.
func TestMustAddFilePart_FilesOnDisk(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	now := int64(1_000_000_000)
	dps := makeTestDataPoints(now)

	partID := uint64(1)
	destPath := partPath(tabDir, partID)
	buildAndFlushMeasureMemPart(t, fileSystem, dps, destPath)

	tst.mustAddFilePart(partID)

	snp := tst.currentSnapshot()
	require.NotNil(t, snp, "snapshot must not be nil after mustAddFilePart")
	defer snp.decRef()

	require.Len(t, snp.parts, 1, "snapshot must contain exactly one part")
	require.Equal(t, partID, snp.parts[0].p.partMetadata.ID, "part ID must match")
	require.Nil(t, snp.parts[0].mp, "file-backed part must have nil memPart")
}

// TestIntroduceMemPart_NilMpGuard verifies that mustAddFilePart does not panic when
// introducing a file-backed part (mp == nil) through the introducer.
func TestIntroduceMemPart_NilMpGuard(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	now := int64(2_000_000_000)
	dps := makeTestDataPoints(now)

	partID := uint64(7)
	destPath := partPath(tabDir, partID)
	buildAndFlushMeasureMemPart(t, fileSystem, dps, destPath)

	require.NotPanics(t, func() {
		tst.mustAddFilePart(partID)
	})

	snp := tst.currentSnapshot()
	require.NotNil(t, snp)
	defer snp.decRef()
	require.Len(t, snp.parts, 1)
	require.Nil(t, snp.parts[0].mp, "file-backed part must have nil memPart")
}

// TestFilePart_ErrorCleanup verifies that when a syncPartContext is closed without calling
// FinishSync, the incomplete part directory is removed from disk.
func TestFilePart_ErrorCleanup(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), testSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	partID := uint64(300)
	destPath := partPath(tabDir, partID)
	fileSystem.MkdirPanicIfExist(destPath, storage.DirPerm)

	spc := &syncPartContext{
		tsTable:    tst,
		fileSystem: fileSystem,
		partPath:   destPath,
		partID:     partID,
	}
	w := generateWriters()
	w.metaWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, metaFilename), storage.FilePerm, false))
	w.primaryWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, primaryFilename), storage.FilePerm, false))
	w.timestampsWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, timestampsFilename), storage.FilePerm, false))
	w.fieldValuesWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, fieldValuesFilename), storage.FilePerm, false))
	spc.writers = w

	spc.writers.metaWriter.MustWrite([]byte("partial"))

	_, statErr := os.Stat(destPath)
	require.NoError(t, statErr, "part directory must exist before Close")

	require.NoError(t, spc.Close())

	_, statErr = os.Stat(destPath)
	require.True(t, os.IsNotExist(statErr), "incomplete part directory must be removed after Close without FinishSync")

	snp := tst.currentSnapshot()
	if snp != nil {
		defer snp.decRef()
		require.Empty(t, snp.parts, "no parts must be in snapshot after error cleanup")
	}
}
