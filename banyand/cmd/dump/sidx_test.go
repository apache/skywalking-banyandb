// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

// TestDiscoverSeriesIDs_DedupesAcrossPhysicalSegments is the L7 regression:
// native.ReadOnlyGeneration.VisitIdentifiers walks term dictionaries per
// physical segment, and (its own doc comment) "global lexical ordering
// across segments is not promised" -- nothing collapses the same
// identifier term across two physical segments into one visit. A
// BatchInsertOnly admission (no existing-identifier check, unlike
// BatchUpsert/BatchInsertIfAbsent) for the same identifier in two separate
// batches reproduces this directly: both land as independent live
// documents in their own physical segment, so the same computed SeriesID
// surfaces twice. discoverSeriesIDs must report (and later query against)
// each series exactly once, not once per physical-segment occurrence.
func TestDiscoverSeriesIDs_DedupesAcrossPhysicalSegments(t *testing.T) {
	root := t.TempDir()
	segmentPath := filepath.Join(root, "seg-test")
	sidxPath := filepath.Join(segmentPath, "sidx")
	require.NoError(t, os.MkdirAll(segmentPath, 0o755))

	lfs := fs.NewLocalFileSystem()
	lockFile, err := lfs.CreateLockFile(filepath.Join(segmentPath, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lockFile, segmentPath)
	require.NoError(t, err)

	owner, err := native.NewOwner(native.OwnerOptions{
		Lease: lease, Path: sidxPath, IdentifierDocValues: true,
		// Disable compaction so the two batches below stay as two separate
		// physical segments instead of being merged away -- the scenario
		// that leaves a superseded term behind in the older segment.
		CompactionThreshold: -1,
	})
	require.NoError(t, err)

	identifier := []byte("dup-series-identifier")
	for i := range 2 {
		done := make(chan error, 1)
		batchErr := owner.Batch(context.Background(), native.Batch{
			Documents: []native.Document{{
				Identifier: identifier,
				Timestamp:  int64(i + 1),
			}},
			// BatchInsertOnly performs no existing-identifier check (unlike
			// BatchUpsert/BatchInsertIfAbsent), so both calls admit the
			// identifier as an independent live document in its own
			// physical segment.
			Mode:               native.BatchInsertOnly,
			PersistentCallback: func(err error) { done <- err },
		})
		require.NoError(t, batchErr)
		require.NoError(t, <-done)
	}
	require.NoError(t, owner.Close())

	seriesIDs, discoverErr := discoverSeriesIDs(segmentPath)
	require.NoError(t, discoverErr)
	require.Len(t, seriesIDs, 1, "the same identifier written twice across two physical segments must be reported once")
	require.Equal(t, common.SeriesID(convert.Hash(identifier)), seriesIDs[0])
}

func TestDebugPrintLen(t *testing.T) {
	root := t.TempDir()
	segmentPath := filepath.Join(root, "seg-test")
	sidxPath := filepath.Join(segmentPath, "sidx")
	os.MkdirAll(segmentPath, 0o755)
	lfs := fs.NewLocalFileSystem()
	lockFile, _ := lfs.CreateLockFile(filepath.Join(segmentPath, "lock"), 0o600)
	lease, _ := native.NewFileRootLease(lockFile, segmentPath)
	owner, _ := native.NewOwner(native.OwnerOptions{Lease: lease, Path: sidxPath, IdentifierDocValues: true, CompactionThreshold: -1})
	identifier := []byte("dup-series-identifier")
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		owner.Batch(context.Background(), native.Batch{
			Documents:          []native.Document{{Identifier: identifier, Timestamp: int64(i + 1)}},
			Mode:               native.BatchInsertOnly,
			PersistentCallback: func(err error) { done <- err },
		})
		<-done
	}
	owner.Close()
	ids, err := discoverSeriesIDs(segmentPath)
	println("LEN:", len(ids), "ERR:", err == nil)
}
