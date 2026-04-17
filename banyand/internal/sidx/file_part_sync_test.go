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

package sidx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// TestSyncPartContext_SetForFile_WritesFiles verifies that SetForFile creates the expected
// files on disk when data is written through the writers and then flushed.
func TestSyncPartContext_SetForFile_WritesFiles(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	destPath := filepath.Join(tmpPath, "sidx-part-1")

	spc := NewSyncPartContext()
	spc.SetForFile("test-part", fileSystem, destPath, &queue.ChunkedSyncPartContext{}, false)

	w := spc.GetWriters()
	require.NotNil(t, w, "writers must be initialized after SetForFile")

	// Write some minimal data to the core writers to produce non-empty files.
	w.SidxMetaWriter().MustWrite([]byte("meta-data"))
	w.SidxPrimaryWriter().MustWrite([]byte("primary-data"))
	w.SidxDataWriter().MustWrite([]byte("data-content"))
	w.SidxKeysWriter().MustWrite([]byte("keys-content"))

	finishedPath := spc.Finish()
	require.Equal(t, destPath, finishedPath, "Finish should return the part path")

	// All core files must exist on disk.
	for _, name := range []string{metaFilename, primaryFilename, dataFilename, keysFilename} {
		_, err := os.Stat(filepath.Join(destPath, name))
		require.NoError(t, err, "file %s must exist on disk after Finish", name)
	}
}

// TestPrepareFilePart_OpensWrittenPart verifies that PrepareFilePart successfully opens a
// previously flushed SIDX part and introduces it into the snapshot with mp == nil.
func TestPrepareFilePart_OpensWrittenPart(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel}))

	fileSystem := fs.NewLocalFileSystem()
	tmpPath, deferFn := test.Space(require.New(t))
	defer deferFn()

	opts := NewDefaultOptions()
	opts.Memory = protector.NewMemory(observability.NewBypassRegistry())
	opts.Path = tmpPath
	opts.AvailablePartIDs = nil

	sidxInstance, err := NewSIDX(fileSystem, opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, sidxInstance.Close())
	}()

	// Write data and flush it to disk to get a valid on-disk part.
	reqs := []WriteRequest{
		createTestWriteRequest(common.SeriesID(1), 100, "payload-1",
			Tag{Name: testTagStatus, Value: []byte("ok"), ValueType: pbv1.ValueTypeStr}),
		createTestWriteRequest(common.SeriesID(2), 200, "payload-2",
			Tag{Name: testTagStatus, Value: []byte("ok"), ValueType: pbv1.ValueTypeStr}),
	}
	mp, convErr := sidxInstance.ConvertToMemPart(reqs, 1, nil, nil)
	require.NoError(t, convErr)
	require.NotNil(t, mp)

	partID := uint64(1)
	destPath := partPath(tmpPath, partID)
	mp.MustFlush(fileSystem, destPath)
	ReleaseMemPart(mp)

	// Verify core files exist before calling PrepareFilePart.
	for _, name := range []string{primaryFilename, dataFilename, keysFilename, metaFilename} {
		_, statErr := os.Stat(filepath.Join(destPath, name))
		require.NoError(t, statErr, "core file %s must exist on disk", name)
	}

	// Apply PrepareFilePart by building and replacing the snapshot.
	transitionFn := sidxInstance.PrepareFilePart(partID, destPath)
	cur := sidxInstance.CurrentSnapshot()
	next := transitionFn(cur)
	if cur != nil {
		cur.DecRef()
	}
	sidxInstance.ReplaceSnapshot(next)

	// Verify: snapshot contains the part with nil mp (file-backed).
	snp := sidxInstance.CurrentSnapshot()
	require.NotNil(t, snp, "snapshot must not be nil after PrepareFilePart")
	defer snp.DecRef()

	require.Equal(t, 1, snp.getPartCount(), "snapshot must contain exactly one part")

	// The part must be file-backed (mp == nil).
	for _, pw := range snp.parts {
		require.Nil(t, pw.mp, "part introduced via PrepareFilePart must have nil memPart")
		require.Equal(t, partID, pw.p.partMetadata.ID, "part ID must match")
	}
}
