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
	"strconv"
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

// TestOwnerCompactionStreamsMergeToStagedFileAndRenamesIt proves a
// directory-backed owner's merge output never enters the heap: it is staged
// on disk, served by a file-backed reader, ignored by garbage collection
// while staged, and published by renaming it to "<id>.seg", so no staged
// name outlives the publication.
func TestOwnerCompactionStreamsMergeToStagedFileAndRenamesIt(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a", "series-b")
	waitDurable(t, owner, 2)
	var staged string
	mergeStagedHook = func(stagedPath string) {
		staged = stagedPath
		require.Equal(t, []string{filepath.Base(stagedPath)}, stagedMergeFiles(t, path))
		require.Empty(t, spillFiles(t, path), "spill files never carry a name")
	}
	defer func() { mergeStagedHook = nil }()
	require.NoError(t, owner.Compact(context.Background()))
	require.NotEmpty(t, staged)

	owner.mu.Lock()
	require.Len(t, owner.root.segments, 1)
	handle := owner.root.segments[0].(*memorySegment).handle
	owner.mu.Unlock()
	require.Nil(t, handle.payload, "a merged segment must not hold its bytes in memory")
	require.True(t, handle.staged)

	waitDurable(t, owner, 3)
	require.Empty(t, stagedMergeFiles(t, path), "publication renames the staged merge")
	require.Empty(t, handle.currentSourcePath())
	_, err = os.Stat(filepath.Join(path, fmt.Sprintf("%012x.seg", handle.id)))
	require.NoError(t, err)
	owner.mu.Lock()
	require.Same(t, handle, owner.root.segments[0].(*memorySegment).handle, "a staged merge output is not reopened after publication")
	owner.mu.Unlock()
	requireSeries(t, owner, "series-a", "series-b")
	// Garbage collection never touches a staged name, even one it does not
	// know, such as another merge's output still being written.
	foreign := filepath.Join(path, mergeStagingPrefix+"in-flight")
	require.NoError(t, os.WriteFile(foreign, []byte("partial"), 0o600))
	require.Eventually(t, func() bool {
		return !errors.Is(owner.CollectGarbage(context.Background()), ErrPersistenceBusy)
	}, 30*time.Second, 10*time.Millisecond)
	_, err = os.Stat(foreign)
	require.NoError(t, err)
	require.NoError(t, os.Remove(foreign))
	snapshotPath := t.TempDir()
	require.NoError(t, owner.TakeFileSnapshot(snapshotPath))
	require.NoError(t, owner.Close())
	_, err = os.Stat(filepath.Join(path, fmt.Sprintf("%012x.seg", handle.id)))
	require.NoError(t, err, "releasing a published staged segment must keep its segment file")

	for _, directory := range []string{path, snapshotPath} {
		reopened, openErr := NewOwner(OwnerOptions{Lease: testLease{}, Path: directory, CompactionThreshold: -1})
		require.NoError(t, openErr)
		requireSeries(t, reopened, "series-a", "series-b")
		require.NoError(t, reopened.Close())
	}
}

func spillFiles(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	var spills []string
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".spill-") {
			spills = append(spills, entry.Name())
		}
	}
	return spills
}

// copyCrashImage copies every regular file of path, which is what a crash at
// this instant would leave on disk once the copied writes were durable.
func copyCrashImage(t *testing.T, path string) string {
	t.Helper()
	image := t.TempDir()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		data, readErr := os.ReadFile(filepath.Join(path, entry.Name()))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(image, entry.Name()), data, 0o600))
	}
	return image
}

// requirePreviousReleaseSafe checks that a directory holds only names the
// previous release's engine handles. That engine lists a directory by
// extension: every ".seg" and ".snp" name must parse as a hexadecimal
// identifier, or opening the index fails; it reads only the snapshots and the
// segments they reference, cleans only segments it has seen referenced, and
// ignores every other name. So each name must be a valid "<hex>.seg" or
// "<hex>.snp", or carry neither extension (and is then harmless: never read,
// never mistaken for a segment, never blocking identifier allocation).
func requirePreviousReleaseSafe(t *testing.T, path string) (ignored []string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		extension := filepath.Ext(name)
		if extension != ".seg" && extension != ".snp" {
			ignored = append(ignored, name)
			continue
		}
		_, parseErr := strconv.ParseUint(name[:len(name)-len(extension)], 16, 64)
		require.NoError(t, parseErr, "%q would stop the previous release from opening the index", name)
	}
	return ignored
}

func TestOwnerCrashMidMergeLeavesOnlyHarmlessNames(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	admitSeries(t, owner, "series-a", "series-b")
	waitDurable(t, owner, 2)
	var image string
	mergeStagedHook = func(string) { image = copyCrashImage(t, path) }
	defer func() { mergeStagedHook = nil }()
	require.NoError(t, owner.Compact(context.Background()))

	ignored := requirePreviousReleaseSafe(t, image)
	require.Len(t, ignored, 1, "only the staged merge output, which the previous release ignores")
	require.True(t, strings.HasPrefix(ignored[0], mergeStagingPrefix))
	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: image, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Empty(t, stagedMergeFiles(t, image), "the native owner removes the crashed merge's output")
	requireSeries(t, reopened, "series-a", "series-b")
}

func TestOwnerCrashAfterRenameBeforeManifest(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a", "series-b")
	waitDurable(t, owner, 2)
	var image string
	persistRenamedHook = func() {
		if image == "" {
			image = copyCrashImage(t, path)
		}
	}
	defer func() { persistRenamedHook = nil }()
	require.NoError(t, owner.Compact(context.Background()))
	waitDurable(t, owner, 3)
	owner.mu.Lock()
	orphanID := owner.root.segments[0].(*memorySegment).handle.id
	owner.mu.Unlock()
	require.NoError(t, owner.Close())
	require.NotEmpty(t, image)

	// The crash image holds the renamed merge output, referenced by no
	// manifest, and nothing else the previous release could misread.
	orphan := filepath.Join(image, fmt.Sprintf("%012x.seg", orphanID))
	_, err = os.Stat(orphan)
	require.NoError(t, err)
	require.Empty(t, requirePreviousReleaseSafe(t, image))
	nextSegment, _, err := nativeice.NextPublicationIDs(image)
	require.NoError(t, err)
	require.Greater(t, nextSegment, orphanID, "an unreferenced segment's identifier is never reused")

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: image, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	requireSeries(t, reopened, "series-a", "series-b")
	admitSeries(t, reopened, "series-c")
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	waitDurable(t, reopened, view.Generation())
	require.NoError(t, view.Close())
	require.Eventually(t, func() bool {
		collectErr := reopened.CollectGarbage(context.Background())
		_, statErr := os.Stat(orphan)
		return collectErr == nil && errors.Is(statErr, os.ErrNotExist)
	}, 30*time.Second, 10*time.Millisecond, "garbage collection removes the unreferenced segment once a newer one is kept")
	requireSeries(t, reopened, "series-a", "series-b", "series-c")
}

func TestOwnerStartupRemovesStagedMergesLeftByCrash(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admitSeries(t, owner, "series-a")
	require.NoError(t, owner.Close())
	nextSegment, nextSnapshot, err := nativeice.NextPublicationIDs(path)
	require.NoError(t, err)

	// A crash mid-merge leaves a partial output (and, only inside the
	// create-then-unlink step, a spill name) behind; external receives and
	// publication leave their own temporaries. A staged name hard-linked to a
	// live segment must lose only that name.
	for _, name := range []string{".native-merge-99-1", ".native-merge-99-1.spill-3", ".native-merge-99-2", ".native-external-99-1", ".nativeice-99-7"} {
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
	require.Empty(t, requirePreviousReleaseSafe(t, path))
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
