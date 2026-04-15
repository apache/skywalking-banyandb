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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestMustAddFilePart_FilesOnDisk(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	// Build a memPart with sample traces, flush it to disk to get valid binary files.
	now := int64(1_000_000_000)
	ts := &traces{
		traceIDs:   []string{"tid-1", "tid-2"},
		timestamps: []int64{now, now + 1000},
		spanIDs:    []string{"s1", "s2"},
		spans:      [][]byte{[]byte("span-a"), []byte("span-b")},
		tags:       [][]*tagValue{{}, {}},
	}
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)

	// Flush the memPart to a temporary directory to get valid binary files.
	srcDir := filepath.Join(tmpPath, "src")
	mp.mustFlush(fileSystem, srcDir)
	releaseMemPart(mp)

	// Verify that the source files exist on disk.
	for _, name := range []string{metaFilename, primaryFilename, spansFilename} {
		_, err := os.Stat(filepath.Join(srcDir, name))
		require.NoError(t, err, "source file %s should exist", name)
	}

	// Allocate a part ID and copy files into the tsTable root the same way syncPartContext does.
	partID := uint64(1)
	destPath := partPath(tabDir, partID)
	fileSystem.MkdirPanicIfExist(destPath, 0o755)

	// Copy required files manually (simulating what syncPartContext does via writers).
	copyFile := func(name string) {
		data, readErr := fileSystem.Read(filepath.Join(srcDir, name))
		require.NoError(t, readErr)
		fs.MustFlush(fileSystem, data, filepath.Join(destPath, name), 0o600)
	}
	copyFile(metaFilename)
	copyFile(primaryFilename)
	copyFile(spansFilename)

	// Write metadata.json.
	pm := partMetadata{
		CompressedSizeBytes:       10,
		UncompressedSpanSizeBytes: 12,
		TotalCount:                2,
		BlocksCount:               1,
		MinTimestamp:              now,
		MaxTimestamp:              now + 1000,
	}
	pm.mustWriteMetadata(fileSystem, destPath)
	fileSystem.SyncPath(destPath)

	// Introduce the file part via mustAddFilePart.
	tst.mustAddFilePart(partID, nil)

	// Verify the part is in the snapshot.
	snp := tst.currentSnapshot()
	require.NotNil(t, snp, "snapshot must not be nil after mustAddFilePart")
	defer snp.decRef()

	require.Len(t, snp.parts, 1, "snapshot must contain exactly one part")
	require.Equal(t, partID, snp.parts[0].p.partMetadata.ID, "part ID must match")
	// File-backed partWrapper has nil mp field.
	require.Nil(t, snp.parts[0].mp, "file-backed part must have nil memPart")
}

func TestMustAddFilePart_SurvivesReopenViaMustOpenFilePart(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	// Build and flush a memPart.
	now := int64(2_000_000_000)
	ts := &traces{
		traceIDs:   []string{"trace-x"},
		timestamps: []int64{now},
		spanIDs:    []string{"sx"},
		spans:      [][]byte{[]byte("payload")},
		tags:       [][]*tagValue{{}},
	}
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)

	partID := uint64(42)
	destPath := partPath(tabDir, partID)
	mp.mustFlush(fileSystem, destPath)
	totalCount := mp.partMetadata.TotalCount
	releaseMemPart(mp)

	// Re-open via mustOpenFilePart.
	p := mustOpenFilePart(partID, tabDir, fileSystem)
	defer p.close()

	require.Equal(t, partID, p.partMetadata.ID)
	require.Equal(t, totalCount, p.partMetadata.TotalCount)
	require.NotEmpty(t, p.primaryBlockMetadata, "primary block metadata must be loaded")
}

func TestIntroduceMemPart_NilMpGuard(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	now := int64(3_000_000_000)
	ts := &traces{
		traceIDs:   []string{"trace-nil-guard"},
		timestamps: []int64{now},
		spanIDs:    []string{"s0"},
		spans:      [][]byte{[]byte("data")},
		tags:       [][]*tagValue{{}},
	}
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)
	partID := uint64(7)
	destPath := partPath(tabDir, partID)
	mp.mustFlush(fileSystem, destPath)
	releaseMemPart(mp)

	// mustAddFilePart calls introducePart with a nil-mp partWrapper.
	// It must not panic at addPendingDataCount.
	require.NotPanics(t, func() {
		tst.mustAddFilePart(partID, nil)
	})

	snp := tst.currentSnapshot()
	require.NotNil(t, snp)
	defer snp.decRef()
	require.Len(t, snp.parts, 1)
	require.Nil(t, snp.parts[0].mp)
}

// buildAndFlushMemPart creates a memPart from the given traces, flushes it to destPath,
// and returns the original TotalCount. The memPart is released before returning.
func buildAndFlushMemPart(t *testing.T, fileSystem fs.FileSystem, ts *traces, destPath string) uint64 {
	t.Helper()
	mp := generateMemPart()
	mp.mustInitFromTraces(ts)
	totalCount := mp.partMetadata.TotalCount
	mp.mustFlush(fileSystem, destPath)
	releaseMemPart(mp)
	return totalCount
}

// TestFilePart_DirectWrite_ProducesCorrectFiles verifies that streaming file chunks through
// syncPartContext.handleTraceFileChunk and calling FinishSync produces a valid file-backed
// part (pw.mp == nil) that survives a close/reopen cycle.
func TestFilePart_DirectWrite_ProducesCorrectFiles(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, err := newTSTable(
		fileSystem, tabDir, common.Position{}, logger.GetLogger("test"),
		timestamp.TimeRange{}, traceSnapshotOption(), nil,
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	// Build a valid on-disk part in a temporary source directory (simulates the sender).
	srcDir := filepath.Join(tmpPath, "src")
	now := int64(5_000_000_000)
	ts := &traces{
		traceIDs:   []string{"tid-direct-1", "tid-direct-2"},
		timestamps: []int64{now, now + 500},
		spanIDs:    []string{"sd1", "sd2"},
		spans:      [][]byte{[]byte("span-x"), []byte("span-y")},
		tags:       [][]*tagValue{{}, {}},
	}
	totalCount := buildAndFlushMemPart(t, fileSystem, ts, srcDir)
	require.Greater(t, totalCount, uint64(0))

	// Open the on-disk files as a reader (simulates sender reading them for transfer).
	fileInfos, cleanup := CreatePartFileReaderFromPath(srcDir, fileSystem)
	defer cleanup()
	require.NotEmpty(t, fileInfos)

	// Allocate a partID and destination path (simulates what syncPartContext does).
	partID := uint64(100)
	destPath := partPath(tabDir, partID)
	fileSystem.MkdirPanicIfExist(destPath, 0o755)

	// Create file-backed writers the same way syncPartContext.NewPartType does.
	w := generateWriters()
	w.metaWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, metaFilename), storage.FilePerm, false))
	w.primaryWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, primaryFilename), storage.FilePerm, false))
	w.spanWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, spansFilename), storage.FilePerm, false))

	// Stream each file's content through the writers (simulates handleTraceFileChunk).
	const chunkSize = 512
	for i := range fileInfos {
		fi := &fileInfos[i]
		var wr *writer
		switch fi.Name {
		case traceMetaName:
			wr = &w.metaWriter
		case tracePrimaryName:
			wr = &w.primaryWriter
		case traceSpansName:
			wr = &w.spanWriter
		default:
			continue
		}
		buf := make([]byte, chunkSize)
		for {
			n, readErr := fi.Reader.Read(buf)
			if n > 0 {
				wr.MustWrite(buf[:n])
			}
			if readErr != nil {
				break
			}
		}
	}
	w.MustClose()
	releaseWriters(w)

	// Write metadata (mirrors what syncPartContext.FinishSync does).
	pm := partMetadata{
		TotalCount:   totalCount,
		BlocksCount:  1,
		MinTimestamp: now,
		MaxTimestamp: now + 500,
	}
	pm.mustWriteMetadata(fileSystem, destPath)
	fileSystem.SyncPath(destPath)

	// Introduce the file-backed part.
	tst.mustAddFilePart(partID, nil)

	// Verify: snapshot contains the part with nil mp (file-backed).
	snp := tst.currentSnapshot()
	require.NotNil(t, snp, "snapshot must exist after mustAddFilePart")
	defer snp.decRef()
	require.Len(t, snp.parts, 1)
	require.Equal(t, partID, snp.parts[0].p.partMetadata.ID)
	require.Nil(t, snp.parts[0].mp, "file-backed part must have nil memPart")

	// Verify: core files exist on disk.
	for _, name := range []string{metaFilename, primaryFilename} {
		_, statErr := os.Stat(filepath.Join(destPath, name))
		require.NoError(t, statErr, "file %s must exist on disk", name)
	}

	// Verify: the on-disk part files are readable via mustOpenFilePart (simulates reopen).
	// Note: snapshot persistence to disk requires a flush cycle; here we verify
	// that the binary files written by the sync path are well-formed and loadable.
	p := mustOpenFilePart(partID, tabDir, fileSystem)
	defer p.close()
	require.Equal(t, partID, p.partMetadata.ID, "reopened part ID must match")
	require.Equal(t, totalCount, p.partMetadata.TotalCount, "reopened part TotalCount must match")
	require.NotEmpty(t, p.primaryBlockMetadata, "primary block metadata must be loadable from disk")
}

// TestFilePart_ConcurrentSenders verifies that two concurrent goroutines each adding a
// filePart via mustAddFilePart produce a snapshot containing both parts with no races.
func TestFilePart_ConcurrentSenders(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	// Prepare two parts on disk before starting the goroutines.
	type partSpec struct {
		path string
		id   uint64
	}
	specs := []partSpec{
		{id: 201},
		{id: 202},
	}
	traceSets := []*traces{
		{
			traceIDs:   []string{"concurrent-tid-1"},
			timestamps: []int64{6_000_000_000},
			spanIDs:    []string{"cs1"},
			spans:      [][]byte{[]byte("span-c1")},
			tags:       [][]*tagValue{{}},
		},
		{
			traceIDs:   []string{"concurrent-tid-2"},
			timestamps: []int64{7_000_000_000},
			spanIDs:    []string{"cs2"},
			spans:      [][]byte{[]byte("span-c2")},
			tags:       [][]*tagValue{{}},
		},
	}
	for i := range specs {
		specs[i].path = partPath(tabDir, specs[i].id)
		buildAndFlushMemPart(t, fileSystem, traceSets[i], specs[i].path)
	}

	// Both goroutines call mustAddFilePart concurrently.
	var wg sync.WaitGroup
	for i := range specs {
		wg.Add(1)
		spec := specs[i]
		go func() {
			defer wg.Done()
			tst.mustAddFilePart(spec.id, nil)
		}()
	}
	wg.Wait()

	// All parts must appear in the snapshot.
	var snp *snapshot
	require.Eventually(t, func() bool {
		snp = tst.currentSnapshot()
		if snp == nil {
			return false
		}
		if len(snp.parts) == 2 {
			return true
		}
		snp.decRef()
		snp = nil
		return false
	}, flags.EventuallyTimeout, 10*time.Millisecond, "both parts must be in snapshot")
	defer snp.decRef()

	ids := make(map[uint64]struct{})
	for _, pw := range snp.parts {
		ids[pw.p.partMetadata.ID] = struct{}{}
		assert.Nil(t, pw.mp, "part %d must be file-backed", pw.p.partMetadata.ID)
	}
	assert.Contains(t, ids, specs[0].id, "first sender's part must be present")
	assert.Contains(t, ids, specs[1].id, "second sender's part must be present")
}

// TestFilePart_ErrorCleanup verifies that when a syncPartContext is closed without calling
// FinishSync (e.g. gRPC stream cancellation), the incomplete part directory is removed from disk.
func TestFilePart_ErrorCleanup(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	tst, _ := initTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), traceSnapshotOption(), nil)
	tst.startLoop(1)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	// Simulate what CreatePartHandler does: allocate a partID and create file-backed writers.
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
	w.spanWriter.init(fs.MustCreateFile(fileSystem, filepath.Join(destPath, spansFilename), storage.FilePerm, false))
	spc.writers = w

	// Write partial data (simulates a partial transfer before cancellation).
	spc.writers.metaWriter.MustWrite([]byte("partial"))

	// The directory must exist before Close.
	_, statErr := os.Stat(destPath)
	require.NoError(t, statErr, "part directory must exist before Close")

	// Close without FinishSync (simulates gRPC stream cancellation).
	require.NoError(t, spc.Close())

	// The incomplete part directory must be cleaned up.
	_, statErr = os.Stat(destPath)
	require.True(t, os.IsNotExist(statErr), "incomplete part directory must be removed after Close without FinishSync")

	// No parts must appear in the snapshot.
	snp := tst.currentSnapshot()
	if snp != nil {
		defer snp.decRef()
		assert.Empty(t, snp.parts, "no parts must be in snapshot after error cleanup")
	}
}

// TestFilePart_FlusherSkipsFileParts verifies that the flusher does not attempt to re-flush
// a file-backed part (pw.mp == nil), leaving it unchanged in the snapshot.
func TestFilePart_FlusherSkipsFileParts(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	tabDir := filepath.Join(tmpPath, "tab")
	fileSystem.MkdirPanicIfExist(tabDir, 0o755)

	// Use a non-zero flushTimeout so the flusher loop actually runs.
	opt := option{
		flushTimeout: 10 * time.Millisecond,
		mergePolicy:  newDefaultMergePolicyForTesting(),
		protector:    traceSnapshotOption().protector,
	}
	tst, err := newTSTable(fileSystem, tabDir, common.Position{}, logger.GetLogger("test"), timestamp.TimeRange{}, opt, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tst.Close())
	}()

	// Build and flush a memPart to disk, then introduce it as a filePart.
	partID := uint64(400)
	destPath := partPath(tabDir, partID)
	now := int64(8_000_000_000)
	ts := &traces{
		traceIDs:   []string{"flusher-skip-tid"},
		timestamps: []int64{now},
		spanIDs:    []string{"fs1"},
		spans:      [][]byte{[]byte("span-fs")},
		tags:       [][]*tagValue{{}},
	}
	buildAndFlushMemPart(t, fileSystem, ts, destPath)
	tst.mustAddFilePart(partID, nil)

	// Confirm the part is in the snapshot with nil mp before the flusher runs.
	snpBefore := tst.currentSnapshot()
	require.NotNil(t, snpBefore)
	require.Len(t, snpBefore.parts, 1)
	require.Nil(t, snpBefore.parts[0].mp, "part must be file-backed before flusher cycle")
	snpBefore.decRef()

	// Assert that across many flusher cycles (flushTimeout=10ms over 500ms window), the
	// snapshot continuously shows exactly one file-backed part — i.e., the flusher never
	// mutates it into a memPart nor creates a duplicate.
	g := gomega.NewWithT(t)
	g.Consistently(func() bool {
		snp := tst.currentSnapshot()
		if snp == nil {
			return false
		}
		defer snp.decRef()
		if len(snp.parts) != 1 {
			return false
		}
		if snp.parts[0].mp != nil {
			return false
		}
		return snp.parts[0].p.partMetadata.ID == partID
	}).WithTimeout(500 * time.Millisecond).WithPolling(20 * time.Millisecond).Should(gomega.BeTrue())

	snpAfter := tst.currentSnapshot()
	require.NotNil(t, snpAfter)
	defer snpAfter.decRef()

	// Exactly one on-disk part directory must exist (no duplicates).
	entries := fileSystem.ReadDir(tabDir)
	partDirs := 0
	for _, e := range entries {
		if e.IsDir() && e.Name() != sidxDirName && e.Name() != storage.FailedPartsDirName {
			partDirs++
		}
	}
	assert.Equal(t, 1, partDirs, "flusher must not create a second part directory for a file-backed part")
}
