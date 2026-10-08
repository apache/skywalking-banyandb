// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package native

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func stagedMergeFiles(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)
	var staged []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), mergeStagingPrefix) {
			staged = append(staged, entry.Name())
		}
	}
	return staged
}

func admitSeries(t *testing.T, owner *Owner, identifiers ...string) {
	t.Helper()
	for _, identifier := range identifiers {
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
			Identifier: []byte(identifier), Fields: []Field{{Name: "tag", Value: []byte(identifier), Index: true, Store: true}},
		}}}))
	}
}

func requireSeries(t *testing.T, owner *Owner, identifiers ...string) {
	t.Helper()
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, identifier := range identifiers {
		_, found, lookupErr := view.Lookup(context.Background(), []byte(identifier))
		require.NoError(t, lookupErr)
		require.True(t, found, "identifier %q", identifier)
	}
}

func waitDurable(t *testing.T, owner *Owner, generation uint64) {
	t.Helper()
	require.Eventually(t, func() bool { return owner.DurableGeneration() >= generation }, 30*time.Second, time.Millisecond)
}

// TestOwnerCompactionStreamsMergeToLinkedStagedFile proves a directory-backed
// owner's merge output never enters the heap: it is staged on disk, served by
// a file-backed reader, published by hard link rather than copy, ignored by
// garbage collection, and removed once its segment is released.
func TestOwnerCompactionStreamsMergeToLinkedStagedFile(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a", "series-b")
	require.NoError(t, owner.Compact(context.Background()))

	owner.mu.Lock()
	require.Len(t, owner.root.segments, 1)
	handle := owner.root.segments[0].(*memorySegment).handle
	owner.mu.Unlock()
	require.Nil(t, handle.payload, "a merged segment must not hold its bytes in memory")
	require.True(t, handle.linkSource)
	require.Equal(t, []string{filepath.Base(handle.sourcePath)}, stagedMergeFiles(t, path))

	waitDurable(t, owner, 3)
	staged, err := os.Stat(handle.sourcePath)
	require.NoError(t, err)
	published, err := os.Stat(filepath.Join(path, fmt.Sprintf("%012x.seg", handle.id)))
	require.NoError(t, err)
	require.True(t, os.SameFile(staged, published), "publication must link the staged merge, not copy it")
	owner.mu.Lock()
	require.Same(t, handle, owner.root.segments[0].(*memorySegment).handle, "a linked merge output is not promoted to a second reader")
	owner.mu.Unlock()

	require.Eventually(t, func() bool {
		return !errors.Is(owner.CollectGarbage(context.Background()), ErrPersistenceBusy)
	}, 30*time.Second, 10*time.Millisecond)
	require.Len(t, stagedMergeFiles(t, path), 1, "garbage collection must never touch a staged merge")
	next, _, err := nativeice.NextPublicationIDs(path)
	require.NoError(t, err)
	require.Equal(t, handle.id+1, next, "staged merge names never take part in identifier allocation")
	requireSeries(t, owner, "series-a", "series-b")
	require.NoError(t, owner.Close())
	require.Empty(t, stagedMergeFiles(t, path), "releasing the merged segment removes its staged name")

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	requireSeries(t, reopened, "series-a", "series-b")
}

func TestOwnerStartupRemovesStagedMergesLeftByCrash(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a")
	require.NoError(t, owner.Close())
	nextSegment, nextSnapshot, err := nativeice.NextPublicationIDs(path)
	require.NoError(t, err)

	// A crash mid-merge leaves a partial output and its spill files behind; a
	// crash after publication leaves a staged name linked to a live segment.
	for _, name := range []string{".native-merge-99-1", ".native-merge-99-1.spill-3", ".native-merge-99-2"} {
		require.NoError(t, os.WriteFile(filepath.Join(path, name), []byte("partial"), 0o600))
	}
	published := filepath.Join(path, fmt.Sprintf("%012x.seg", nextSegment-1))
	require.NoError(t, os.Link(published, filepath.Join(path, ".native-merge-99-3")))
	ids, snapshots, err := nativeice.NextPublicationIDs(path)
	require.NoError(t, err)
	require.Equal(t, nextSegment, ids)
	require.Equal(t, nextSnapshot, snapshots)

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Empty(t, stagedMergeFiles(t, path))
	_, err = os.Stat(published)
	require.NoError(t, err, "removing a staged name must keep the published segment")
	requireSeries(t, reopened, "series-a")
}

func TestOwnerStartupKeepsStagedMergesWithoutLease(t *testing.T) {
	path := t.TempDir()
	staged := filepath.Join(path, ".native-merge-99-1")
	require.NoError(t, os.WriteFile(staged, []byte("partial"), 0o600))
	_, err := NewOwner(OwnerOptions{Lease: testLease{err: ErrLeaseUnavailable}, Path: path})
	require.Error(t, err)
	_, err = os.Stat(staged)
	require.NoError(t, err, "only the lease holder may clean the directory")
}

func TestOwnerStaleCompactionRemovesStagedMerge(t *testing.T) {
	path := t.TempDir()
	var owner *Owner
	var once sync.Once
	var deleteErr error
	callback := func(ctx context.Context, _ MergeDocument) (bool, error) {
		once.Do(func() {
			deleteErr = owner.Batch(ctx, Batch{Deletes: [][]byte{[]byte("series-a")}})
		})
		return false, nil
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1, PrepareMergeCallback: callback})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	admitSeries(t, owner, "series-a", "series-b")
	require.ErrorIs(t, owner.Compact(context.Background()), ErrStaleCompaction)
	require.NoError(t, deleteErr)
	require.Empty(t, stagedMergeFiles(t, path), "a discarded merge must remove its staged output")
}

func TestOwnerCanceledCompactionRemovesStagedMerge(t *testing.T) {
	path := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	callback := func(context.Context, MergeDocument) (bool, error) {
		cancel()
		return false, nil
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1, PrepareMergeCallback: callback})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	admitSeries(t, owner, "series-a", "series-b")
	require.ErrorIs(t, owner.Compact(ctx), context.Canceled)
	require.Empty(t, stagedMergeFiles(t, path))
	requireSeries(t, owner, "series-a", "series-b")
}

func TestOwnerCloseDuringCompactionLeavesNoStagedMerge(t *testing.T) {
	path := t.TempDir()
	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	callback := func(context.Context, MergeDocument) (bool, error) {
		once.Do(func() {
			close(reached)
			<-release
		})
		return false, nil
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1, PrepareMergeCallback: callback})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a", "series-b")
	compactErr := make(chan error, 1)
	go func() { compactErr <- owner.Compact(context.Background()) }()
	<-reached
	closeErr := make(chan error, 1)
	go func() { closeErr <- owner.Close() }()
	// Close waits for the in-flight compaction, which then finds the owner
	// closing and discards its merge.
	require.Eventually(t, func() bool {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.closing
	}, 10*time.Second, time.Millisecond)
	close(release)
	require.ErrorIs(t, <-compactErr, ErrOwnerClosed)
	require.NoError(t, <-closeErr)
	require.Empty(t, stagedMergeFiles(t, path))

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	requireSeries(t, reopened, "series-a", "series-b")
}
