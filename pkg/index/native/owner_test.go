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

package native

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type testLease struct{ err error }

func (l testLease) Validate() error           { return l.err }
func (l testLease) ValidatePath(string) error { return l.err }

//nolint:govet // field layout is test-only synchronization state.
type gatedPathLease struct {
	pathCall    atomic.Int32
	reached     chan struct{}
	release     chan struct{}
	reachedOnce sync.Once
}

func (l *gatedPathLease) Validate() error { return nil }
func (l *gatedPathLease) ValidatePath(string) error {
	if l.pathCall.Add(1) == 3 {
		l.reachedOnce.Do(func() { close(l.reached) })
		<-l.release
	}
	return nil
}

func newTestOwner(t *testing.T, persist PersistFunc) *Owner {
	t.Helper()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Persist: persist})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return owner
}

func TestOwnerPinsImmutableRootsAcrossDeleteAndAppend(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("series-a"), Fields: []Field{{Name: "status", Value: []byte("old"), Index: true, Store: true}}},
	}}))
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, oldView.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes: [][]byte{[]byte("series-a")},
		Documents: []Document{{Identifier: []byte("series-b"), Fields: []Field{{
			Name: "status", Value: []byte("new"), Index: true, Store: true,
		}}}},
	}))
	newView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, newView.Close()) })
	oldDocument, found, err := oldView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), oldDocument.Fields[0].Value)
	_, found, err = newView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.False(t, found)
	newDocument, found, err := newView.Lookup(context.Background(), []byte("series-b"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), newDocument.Fields[0].Value)
}

func TestOwnerPrepareMergeCallbackAddsPrivateDrops(t *testing.T) {
	dropped := 0
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, CompactionThreshold: -1,
		PrepareMergeCallback: func(ctx context.Context, document MergeDocument) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			shouldDrop := false
			if err := document.StoredFields(func(name string, value []byte) bool {
				if name == "_deleted" && string(value) == "expired" {
					shouldDrop = true
				}
				return true
			}); err != nil {
				return false, err
			}
			if shouldDrop {
				dropped++
			}
			return shouldDrop, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("expired"), Fields: []Field{{Name: "_deleted", Value: []byte("expired"), Store: true}}},
		{Identifier: []byte("live"), Fields: []Field{{Name: "_deleted", Value: []byte("live"), Store: true}}},
	}}))
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, oldView.Close()) })
	require.NoError(t, owner.forceMergeAll(context.Background()))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	_, found, err := view.Lookup(context.Background(), []byte("expired"))
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = view.Lookup(context.Background(), []byte("live"))
	require.NoError(t, err)
	require.True(t, found)
	_, found, err = oldView.Lookup(context.Background(), []byte("expired"))
	require.NoError(t, err)
	require.True(t, found, "a pinned pre-compaction view must retain the expired document")
	require.Equal(t, 1, dropped)
}

func TestOwnerPrepareMergeFailureDoesNotPublish(t *testing.T) {
	callbackErr := errors.New("expiry callback failed")
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, CompactionThreshold: -1,
		PrepareMergeCallback: func(context.Context, MergeDocument) (bool, error) {
			return false, callbackErr
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("retained")}}}))
	before, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, before.Close()) })
	generation := before.Generation()
	require.ErrorIs(t, owner.forceMergeAll(context.Background()), callbackErr)
	after, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, after.Close()) })
	require.Equal(t, generation, after.Generation())
	_, found, err := after.Lookup(context.Background(), []byte("retained"))
	require.NoError(t, err)
	require.True(t, found)
}

func TestOwnerPrepareMergeCancellationDoesNotPublish(t *testing.T) {
	compactContext, cancel := context.WithCancel(context.Background())
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, CompactionThreshold: -1,
		PrepareMergeCallback: func(ctx context.Context, _ MergeDocument) (bool, error) {
			cancel()
			return false, ctx.Err()
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("retained")}}}))
	before, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, before.Close()) })
	generation := before.Generation()
	require.ErrorIs(t, owner.forceMergeAll(compactContext), context.Canceled)
	after, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, after.Close()) })
	require.Equal(t, generation, after.Generation())
	_, found, err := after.Lookup(context.Background(), []byte("retained"))
	require.NoError(t, err)
	require.True(t, found)
}

func TestOwnerPrepareMergePersistsExpiryDropForReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property")
	prepare := func(ctx context.Context, document MergeDocument) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		drop := false
		err := document.StoredFields(func(name string, value []byte) bool {
			drop = drop || name == "_deleted" && string(value) == "expired"
			return true
		})
		return drop, err
	}
	owner, err := NewOwner(OwnerOptions{
		Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: -1,
		PrepareMergeCallback: prepare,
	})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("expired"), Fields: []Field{{Name: "_deleted", Value: []byte("expired"), Store: true}}},
		{Identifier: []byte("live"), Fields: []Field{{Name: "_deleted", Value: []byte("live"), Store: true}}},
	}}))
	require.NoError(t, owner.forceMergeAll(context.Background()))
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	_, found, err := view.Lookup(context.Background(), []byte("expired"))
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = view.Lookup(context.Background(), []byte("live"))
	require.NoError(t, err)
	require.True(t, found)
}

func TestOwnerPersistenceFailureKeepsPublishedRoot(t *testing.T) {
	persistStarted := make(chan struct{})
	persistRelease := make(chan struct{})
	callback := make(chan error, 1)
	owner := newTestOwner(t, func(persistCtx context.Context, view *ReadView) error {
		close(persistStarted)
		<-persistRelease
		_, found, err := view.Lookup(persistCtx, []byte("series-a"))
		if err != nil {
			return err
		}
		if !found {
			return errors.New("published document missing")
		}
		return errors.New("durability failed")
	})
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a")}},
		PersistentCallback: func(err error) { callback <- err },
	}))
	<-persistStarted
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, view.Close())
	close(persistRelease)
	require.ErrorContains(t, <-callback, "durability failed")
}

func TestOwnerPersistenceFailureRejectsLaterAdmissions(t *testing.T) {
	firstCallback := make(chan error, 1)
	owner := newTestOwner(t, func(context.Context, *ReadView) error {
		return errors.New("manifest visibility uncertain")
	})
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a")}},
		PersistentCallback: func(err error) { firstCallback <- err },
	}))
	require.ErrorIs(t, <-firstCallback, ErrPersistenceFailed)
	secondCallback := make(chan error, 1)
	err := owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-b")}},
		PersistentCallback: func(callbackErr error) { secondCallback <- callbackErr },
	})
	require.ErrorIs(t, err, ErrPersistenceFailed)
	require.ErrorIs(t, <-secondCallback, ErrPersistenceFailed)
	view, acquireErr := owner.Acquire(context.Background())
	require.NoError(t, acquireErr)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, lookupErr := view.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, lookupErr)
	require.True(t, found)
}

func TestOwnerRejectsReservedIdentityAndTimestampFields(t *testing.T) {
	owner := newTestOwner(t, nil)
	for _, fieldName := range []string{identifierField, timestampField} {
		err := owner.Batch(context.Background(), Batch{Documents: []Document{{
			Identifier: []byte("series-a"),
			Fields:     []Field{{Name: fieldName, Value: []byte("alias"), Store: true}},
		}}})
		require.ErrorIs(t, err, ErrInvalidDocument)
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err := view.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.False(t, found)
}

func TestOwnerPersistenceCallbackMayCloseOwner(t *testing.T) {
	callbackDone := make(chan struct{})
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Persist: func(context.Context, *ReadView) error { return nil }})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-a")}},
		PersistentCallback: func(error) {
			require.NoError(t, owner.Close())
			close(callbackDone)
		},
	}))
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("persistence callback deadlocked while closing owner")
	}
}

func TestOwnerConcurrentCloseWaitsForPersistenceDrain(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Persist: func(context.Context, *ReadView) error {
		close(started)
		<-release
		return nil
	}})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	<-started
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.close-first", nil, func(context.Context) { firstDone <- owner.Close() })
	run.Go(context.Background(), "native.test.close-second", nil, func(context.Context) { secondDone <- owner.Close() })
	select {
	case <-secondDone:
		t.Fatal("concurrent Close returned before persistence drained")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
}

func TestOwnerCloseReportsUnobservedPersistenceFailure(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Persist: func(context.Context, *ReadView) error {
		return errors.New("durability failed")
	}})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	require.ErrorContains(t, owner.Close(), "durability failed")
}

func TestOwnerDurabilityUsesIndependentContextAfterAdmission(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	callback := make(chan error, 1)
	owner := newTestOwner(t, func(ctx context.Context, _ *ReadView) error {
		close(started)
		<-release
		return ctx.Err()
	})
	admissionCtx, cancel := context.WithCancel(context.Background())
	require.NoError(t, owner.Batch(admissionCtx, Batch{
		Documents:          []Document{{Identifier: []byte("series-a")}},
		PersistentCallback: func(err error) { callback <- err },
	}))
	<-started
	cancel()
	close(release)
	require.NoError(t, <-callback)
}

func TestOwnerPublishesNativeSnapshotWithoutDocumentRehydration(t *testing.T) {
	path := t.TempDir()
	callback := make(chan error, 1)
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-a"), Timestamp: 10, Fields: []Field{
			{Name: "analyzed", Terms: []Term{{Value: []byte("token"), Frequency: 1}}, Index: true},
			{Name: "sort", Value: []byte("rank"), Sort: true},
			{Name: "series", Value: []byte("series-a"), Index: true},
		}}},
		PersistentCallback: func(callbackErr error) { callback <- callbackErr },
	}))
	require.NoError(t, <-callback)
	require.NoError(t, owner.Close())

	reader, err := nativeice.OpenStrict(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	require.Equal(t, uint64(1), reader.SnapshotID())
	posting, found, err := reader.TermPosting("analyzed", []byte("token"))
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, posting.OneHit)
	values, err := reader.DocValues("sort")
	require.NoError(t, err)
	require.Equal(t, [][][]byte{{[]byte("rank")}}, values[0].Values)
	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	reopenedView, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	reopenedDocument, found, err := reopenedView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(10), reopenedDocument.Timestamp)
	require.NoError(t, reopenedView.Close())
	require.NoError(t, reopened.Close())
}

func TestOwnerDurableSnapshotOmitsFullyDeletedSegments(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	firstPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}}}},
		PersistentCallback: func(callbackErr error) { firstPersisted <- callbackErr },
	}))
	require.NoError(t, <-firstPersisted)
	deletedPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes:            [][]byte{[]byte("series-a")},
		PersistentCallback: func(callbackErr error) { deletedPersisted <- callbackErr },
	}))
	require.NoError(t, <-deletedPersisted)
	reader, err := nativeice.OpenStrict(path)
	require.NoError(t, err)
	require.Empty(t, reader.SnapshotMetadata().Segments)
	require.NoError(t, reader.Close())
}

func TestOwnerPromotionPreservesPinnedViewAcrossNewerMutation(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	persisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}}}},
		PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
	}))
	require.NoError(t, <-persisted)
	owner.mu.Lock()
	promotedSegment, ok := owner.root.segments[0].(*memorySegment)
	var promotedPayload []byte
	var promotedPersisted bool
	if ok {
		promotedPayload = promotedSegment.handle.payload
		promotedPersisted = promotedSegment.handle.persisted.Load()
	}
	owner.mu.Unlock()
	require.True(t, ok)
	require.Nil(t, promotedPayload, "a persisted segment is served from its file, not its admitted payload")
	require.True(t, promotedPersisted)
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, oldView.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes: [][]byte{[]byte("series-a")},
		Documents: []Document{{Identifier: []byte("series-a"), Fields: []Field{{
			Name: "status", Value: []byte("new"), Store: true, Index: true,
		}}}},
	}))
	newView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, newView.Close()) }()
	oldDocument, found, err := oldView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), oldDocument.Fields[0].Value)
	newDocument, found, err := newView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), newDocument.Fields[0].Value)
}

func TestOwnerPromotionPreservesMutationAdmittedWhilePersistenceRuns(t *testing.T) {
	path := t.TempDir()
	lease := &gatedPathLease{reached: make(chan struct{}), release: make(chan struct{})}
	owner, err := NewOwner(OwnerOptions{Lease: lease, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	firstPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}}}},
		PersistentCallback: func(callbackErr error) { firstPersisted <- callbackErr },
	}))
	<-lease.reached
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes: [][]byte{[]byte("series-a")},
		Documents: []Document{{Identifier: []byte("series-a"), Fields: []Field{{
			Name: "status", Value: []byte("new"), Store: true, Index: true,
		}}}},
	}))
	close(lease.release)
	require.NoError(t, <-firstPersisted)
	newView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, newView.Close()) }()
	defer func() { require.NoError(t, oldView.Close()) }()
	oldDocument, found, err := oldView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), oldDocument.Fields[0].Value)
	newDocument, found, err := newView.Lookup(context.Background(), []byte("series-a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), newDocument.Fields[0].Value)
}

func TestMemoryOwnerRejectsDurabilityCallback(t *testing.T) {
	owner := newTestOwner(t, nil)
	callback := make(chan error, 1)
	err := owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("series-a")}},
		PersistentCallback: func(callbackErr error) { callback <- callbackErr },
	})
	require.ErrorIs(t, err, ErrPersistenceConfiguration)
	require.ErrorIs(t, <-callback, ErrPersistenceConfiguration)
}

func TestOwnerExternalSegmentIntroducesAndDeduplicates(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, ExternalDedup: ExternalDedupPreferIncoming})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("doc-1"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}},
	}}}))
	payload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("doc-1"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("new"), Store: true, Index: true}}},
		{Identifier: []byte("doc-2"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("external"), Store: true, Index: true}}},
	}})
	require.NoError(t, err)
	streamer, err := owner.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	require.NoError(t, streamer.WriteChunk(payload[:len(payload)/2]))
	require.NoError(t, streamer.WriteChunk(payload[len(payload)/2:]))
	require.NoError(t, streamer.CompleteSegment())
	require.Equal(t, "complete", streamer.Status())
	require.NoError(t, owner.Close())
	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()); require.NoError(t, reopened.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("doc-1"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), document.Fields[0].Value)
	document, found, err = view.Lookup(context.Background(), []byte("doc-2"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("external"), document.Fields[0].Value)
}

// TestOwnerPersistRootSkipsRevalidatingAlreadyPersistedSegment reproduces a
// lifecycle-migration failure: a root snapshot built before an earlier
// generation's persistRoot call reached a given segment (exactly what a
// concurrently admitted introduceExternalSegment/Batch call legitimately
// produces, since promotePersistedHandles only patches the owner's *current*
// root, not roots already captured for persistence) must not re-trigger
// publication's "already exists" check for that segment. handle.persisted is
// the one signal that correctly reaches every such copy, since it is an
// atomic bool on the shared handle; sourcePath is a plain string that
// promotion never clears on the pre-promotion handle, so it must not gate
// whether a segment is treated as brand new.
func TestOwnerPersistRootSkipsRevalidatingAlreadyPersistedSegment(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })

	payload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("doc-1"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("a"), Store: true, Index: true}}},
	}})
	require.NoError(t, err)
	stagedPath := filepath.Join(t.TempDir(), "staged.seg")
	require.NoError(t, os.WriteFile(stagedPath, payload, 0o600))
	reader, err := nativeice.OpenSegmentFile(stagedPath)
	require.NoError(t, err)
	metadata := reader.SnapshotMetadata()
	require.NoError(t, reader.Close())
	require.Len(t, metadata.Segments, 1)

	segment, err := newSegmentFromFile(stagedPath, metadata.Segments[0], 0)
	require.NoError(t, err)
	segment.retain() // a second owning reference, one per root below
	t.Cleanup(func() { segment.release(); segment.release() })

	firstRoot := &publishedRoot{generation: 1, segments: []rootSegment{segment}, nextNumber: 1}
	firstRoot.refs.Store(1)
	// A root snapshot taken before firstRoot's persistRoot call (and its
	// promotion) runs still holds this same, pre-promotion segment handle --
	// exactly what introduceExternalSegment/Batch construct from o.root.segments
	// for a second admission racing the first's async persistence.
	concurrentlyAdmittedRoot := &publishedRoot{generation: 2, segments: []rootSegment{segment}, nextNumber: 1}
	concurrentlyAdmittedRoot.refs.Store(1)

	require.NoError(t, owner.persistRoot(firstRoot))
	// Before the fix this failed: "segment 0 already exists: nativeice:
	// publication conflict".
	require.NoError(t, owner.persistRoot(concurrentlyAdmittedRoot))
}

func TestOwnerExternalSegmentCreatesFreshIndexDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	payload, err := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("fresh"), Fields: []nativeice.EncodeField{{Name: "status", Value: []byte("ok"), Store: true, Index: true}}},
	}})
	require.NoError(t, err)
	streamer, err := owner.EnableExternalSegments()
	require.NoError(t, err)
	require.NoError(t, streamer.StartSegment())
	require.NoError(t, streamer.WriteChunk(payload))
	require.NoError(t, streamer.CompleteSegment())
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err := view.Lookup(context.Background(), []byte("fresh"))
	require.NoError(t, err)
	require.True(t, found)
}

func TestOwnerAutomaticCompactionPersistsAndReopens(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{
		Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: 2,
	})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("one"), Fields: []Field{{Name: "status", Value: []byte("ok"), Index: true, Store: true}},
	}}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("two"), Fields: []Field{{Name: "status", Value: []byte("ok"), Index: true, Store: true}},
	}}}))
	require.Eventually(t, func() bool {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return len(owner.root.segments) == 1
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	reopened.mu.Lock()
	require.Len(t, reopened.root.segments, 1)
	reopened.mu.Unlock()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	result, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte("ok")})
	require.NoError(t, err)
	require.Len(t, result.Identifiers, 2)
}

func TestOwnerAcquireAndBatchRemainAvailableDuringGarbageCollection(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	owner.mu.Lock()
	owner.collecting = true
	owner.mu.Unlock()
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("during-gc")}}}))
	owner.mu.Lock()
	owner.collecting = false
	owner.mu.Unlock()
}

func TestOwnerGarbageCollectionConcurrentAdmissions(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	admit := func(identifier string) {
		durable := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Documents:          []Document{{Identifier: []byte(identifier)}},
			PersistentCallback: func(callbackErr error) { durable <- callbackErr },
		}))
		require.NoError(t, <-durable)
	}
	admit("seed-0")
	admit("seed-1")
	require.NoError(t, owner.Compact(context.Background()))

	var workers sync.WaitGroup
	workers.Add(2)
	workersDone := make(chan struct{})
	run.Go(context.Background(), "native.test.admissions", nil, func(context.Context) {
		defer workers.Done()
		for index := 0; index < 12; index++ {
			admit(fmt.Sprintf("concurrent-%d", index))
		}
	})
	run.Go(context.Background(), "native.test.readers", nil, func(ctx context.Context) {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			view, acquireErr := owner.Acquire(ctx)
			if acquireErr == nil {
				_, _, _ = view.Lookup(ctx, []byte("seed-0"))
				_ = view.Close()
			} else {
				t.Errorf("acquire during collection: %v", acquireErr)
			}
		}
	})
	run.Go(context.Background(), "native.test.collection", nil, func(ctx context.Context) {
		defer close(workersDone)
		for index := 0; index < 100; index++ {
			collectErr := owner.CollectGarbage(ctx)
			if collectErr != nil && !errors.Is(collectErr, ErrPersistenceBusy) {
				t.Errorf("collect during admission: %v", collectErr)
			}
		}
	})
	<-workersDone
	workers.Wait()
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, identifier := range append([]string{"seed-0", "seed-1"}, []string{
		"concurrent-0", "concurrent-1", "concurrent-2", "concurrent-3", "concurrent-4", "concurrent-5",
		"concurrent-6", "concurrent-7", "concurrent-8", "concurrent-9", "concurrent-10", "concurrent-11",
	}...) {
		_, found, lookupErr := view.Lookup(context.Background(), []byte(identifier))
		require.NoError(t, lookupErr)
		require.True(t, found, identifier)
	}
}

func TestOwnerStatsResetAndFileSnapshot(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("doc-1"), Fields: []Field{{Name: "status", Value: []byte("ok"), Store: true, Index: true}},
	}}}))
	count, size := owner.Stats()
	require.Equal(t, int64(1), count)
	require.Positive(t, size)
	destination := t.TempDir()
	require.NoError(t, owner.TakeFileSnapshot(destination))
	snapshotReader, err := nativeice.OpenStrict(destination)
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapshotReader.SnapshotID())
	require.NoError(t, snapshotReader.Close())
	require.NoError(t, owner.Reset())
	count, size = owner.Stats()
	require.Zero(t, count)
	require.Zero(t, size)
	require.NoError(t, owner.Close())
	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	count, _ = reopened.Stats()
	require.Zero(t, count)
}

func TestOwnerActivityCountsAdmittedDocumentsAndAcquiredViews(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: t.TempDir()})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	admitted, acquired := owner.Activity()
	require.Zero(t, admitted)
	require.Zero(t, acquired)

	document := func(identifier string) Document {
		return Document{Identifier: []byte(identifier), Fields: []Field{{Name: "status", Value: []byte("ok"), Store: true, Index: true}}}
	}
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{document("doc-1"), document("doc-2")}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Mode: BatchInsertIfAbsent, Documents: []Document{document("doc-3")}}))
	// A rejected admission does not count.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, owner.Batch(canceled, Batch{Documents: []Document{document("doc-4")}}))

	for range 2 {
		view, acquireErr := owner.Acquire(context.Background())
		require.NoError(t, acquireErr)
		require.NoError(t, view.Close())
	}
	admitted, acquired = owner.Activity()
	require.Equal(t, uint64(3), admitted)
	require.Equal(t, uint64(2), acquired)
}

func TestOwnerFileSnapshotIncludesAdmittedNRTBeforePersistence(t *testing.T) {
	path := t.TempDir()
	lease := &gatedPathLease{reached: make(chan struct{}), release: make(chan struct{})}
	owner, err := NewOwner(OwnerOptions{Lease: lease, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("nrt"), Fields: []Field{{Name: "status", Value: []byte("visible"), Store: true, Index: true}},
	}}}))
	<-lease.reached
	destination := t.TempDir()
	require.NoError(t, owner.TakeFileSnapshot(destination))
	snapshot, err := nativeice.OpenStrict(destination)
	require.NoError(t, err)
	posting, found, err := snapshot.TermPosting("_id", []byte("nrt"))
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, posting.OneHit)
	require.NoError(t, snapshot.Close())
	close(lease.release)
}

func TestOwnerExactMatchUsesEncodedTermsAndMasksDeletes(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("doc-10"), Timestamp: 100, Fields: []Field{
			{Name: "status", Terms: []Term{{Value: []byte("ok"), Frequency: 1}}, Index: true},
			{Name: "series", Value: []byte("series-a"), Index: true},
		}},
		{Identifier: []byte("doc-11"), Timestamp: 300, Fields: []Field{
			{Name: "status", Value: []byte("bad"), Index: true}, {Name: "series", Value: []byte("series-a"), Index: true},
		}},
		{Identifier: []byte("doc-12"), Timestamp: 200, Fields: []Field{
			{Name: "status", Value: []byte("ok"), Index: true}, {Name: "series", Value: []byte("series-a"), Index: true},
		}},
		{Identifier: []byte("doc-13"), Timestamp: 400, Fields: []Field{
			{Name: "status", Value: []byte("ok"), Index: true}, {Name: "series", Value: []byte("series-a"), Index: true},
		}},
	}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("doc-13")}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	result, err := view.MatchTerms(context.Background(), MatchRequest{
		Field: "status", Term: []byte("ok"), SeriesField: "series", SeriesID: []byte("series-a"),
		TimeRange: &TimeRange{Lower: 100, Upper: 300, IncludesLower: false, IncludesUpper: true},
	})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("doc-12")}, result.Identifiers)
	require.Equal(t, []int64{200}, result.Timestamps)
}

func TestOwnerTimestampKeepsLegacyNumericTermsAndSortValues(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("early"), Timestamp: 100},
		{Identifier: []byte("late"), Timestamp: 300},
	}}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })

	// Legacy DateTime fields retain precision-shifted indexed terms in
	// addition to the shift-zero stored value. Querying a shifted range proves
	// that newly admitted native documents remain compatible with that format.
	hits, err := view.MatchRange(context.Background(), RangeRequest{
		Field:         timestampField,
		Lower:         nativeice.EncodePrefixCodedInt64Shift(100, 4),
		Upper:         nativeice.EncodePrefixCodedInt64Shift(300, 4),
		IncludesLower: true, IncludesUpper: true,
		MaxTerms: 2,
	})
	require.NoError(t, err)
	ordered, err := view.SortHits(context.Background(), hits, SortRequest{Field: timestampField})
	require.NoError(t, err)
	require.Equal(t, []string{"early", "late"}, queryIDs(ordered))
}

func TestOwnerInsertOnlyPreservesDuplicatePhysicalIdentifiers(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertOnly,
		Documents: []Document{
			{Identifier: []byte("duplicate"), Fields: []Field{{Name: "status", Terms: []Term{{Value: []byte("ok")}}, Index: true}}},
			{Identifier: []byte("duplicate"), Fields: []Field{{Name: "status", Terms: []Term{{Value: []byte("ok")}}, Index: true}}},
		},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	result, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte("ok")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("duplicate"), []byte("duplicate")}, result.Identifiers)
}

func TestOwnerRejectsMissingLeaseAndCloseIsIdempotent(t *testing.T) {
	_, err := NewOwner(OwnerOptions{})
	require.ErrorIs(t, err, ErrLeaseUnavailable)
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Close())
	require.NoError(t, owner.Close())
	_, err = owner.Acquire(context.Background())
	require.ErrorIs(t, err, ErrOwnerClosed)
}

func TestOwnerRequiresPathBoundLeaseForPersistence(t *testing.T) {
	path := t.TempDir()
	lease := pathBoundLease{expected: path}
	owner, err := NewOwner(OwnerOptions{Lease: lease, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Close())
	_, err = NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path + "/other"}, Path: path})
	require.Error(t, err)
}

type pathBoundLease struct{ expected string }

func (l pathBoundLease) Validate() error { return nil }
func (l pathBoundLease) ValidatePath(path string) error {
	if path != l.expected {
		return errors.New("lease path mismatch")
	}
	return nil
}

func TestOwnerSerializesConcurrentBatches(t *testing.T) {
	owner := newTestOwner(t, nil)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 16)
	for index := range 16 {
		wg.Add(1)
		run.Go(context.Background(), "native.test.concurrent-batch", nil, func(ctx context.Context) {
			defer wg.Done()
			if err := owner.Batch(ctx, Batch{Documents: []Document{{Identifier: []byte{byte(index + 1)}}}}); err != nil {
				errorsCh <- err
			}
		})
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	count := 0
	require.NoError(t, view.VisitIdentifiers(context.Background(), func([]byte) bool { count++; return true }))
	require.Equal(t, 16, count)
}

func TestOwnerCompactsWithoutInvalidatingPinnedView(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("series-a"), Fields: []Field{{Name: "status", Value: []byte("ok"), Index: true, Store: true}},
	}}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("series-b"), Fields: []Field{{Name: "status", Value: []byte("ok"), Index: true, Store: true}},
	}}}))
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, owner.Compact(context.Background()))
	newView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, oldView.Close())
	require.NoError(t, newView.Close())
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	for _, identifier := range [][]byte{[]byte("series-a"), []byte("series-b")} {
		_, found, lookupErr := view.Lookup(context.Background(), identifier)
		require.NoError(t, lookupErr)
		require.True(t, found)
	}
}

func TestOwnerCompactionRejectsLateAdmission(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, owner.Compact(ctx), context.Canceled)
}

func TestOwnerCompactionPersistsReopenableRoot(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("series-a"), Fields: []Field{{Name: "tag", Value: []byte("a"), Index: true, Store: true}},
	}}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("series-b"), Fields: []Field{{Name: "tag", Value: []byte("b"), Index: true, Store: true}},
	}}}))
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	require.NoError(t, owner.Compact(context.Background()))
	deadline := time.Now().Add(time.Second)
	for owner.DurableGeneration() < 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.GreaterOrEqual(t, owner.DurableGeneration(), uint64(3))
	require.ErrorIs(t, owner.CollectGarbage(context.Background()), ErrPersistenceBusy)
	require.NoError(t, oldView.Close())
	// Closing the view drops its reference, but it does not wait for a background
	// persist that is already holding an older root, and CollectGarbage refuses to
	// collect while any root other than the current one is still referenced. So
	// "busy" is a legitimate answer for a moment after the close, and asserting on
	// the first call made this test fail on roughly one run in 150 under CI's
	// narrower GOMAXPROCS. Wait the in-flight persist out instead: a reference that
	// never drops still fails here, and the deletion set below is only ever built
	// by a collection that actually ran.
	var collectErr error
	require.Eventually(t, func() bool {
		collectErr = owner.CollectGarbage(context.Background())
		return !errors.Is(collectErr, ErrPersistenceBusy)
	}, 30*time.Second, 10*time.Millisecond)
	require.NoError(t, collectErr)
	_, err = os.Stat(filepath.Join(path, fmt.Sprintf("%012x.seg", 0)))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, identifier := range [][]byte{[]byte("series-a"), []byte("series-b")} {
		_, found, lookupErr := view.Lookup(context.Background(), identifier)
		require.NoError(t, lookupErr)
		require.True(t, found)
	}
}

// TestOwnerCompactionRebasesOntoConcurrentlyAppendedSegment reproduces the
// OAP schema-registry preload pattern: many small single-document writes
// admitted back to back with no gap wide enough for a merge to finish
// uncontested. Before this fix, Compact discarded its entire merge the
// instant any write landed during the merge window (checking only root
// generation equality), so under continuous admission compaction could
// starve indefinitely -- root.segments grew without bound and every
// persistRoot manifest write slowed down with it, turning ~1500 sequential
// property writes into a multi-minute, still-growing stall instead of a
// roughly constant-time operation. The fix rebases the compacted result onto
// a purely-appended tail instead of discarding it.
func TestOwnerCompactionRebasesOntoConcurrentlyAppendedSegment(t *testing.T) {
	var owner *Owner
	var once sync.Once
	var admitErr error
	callback := func(ctx context.Context, _ MergeDocument) (bool, error) {
		once.Do(func() {
			admitErr = owner.Batch(ctx, Batch{Documents: []Document{
				{Identifier: []byte("series-c")},
			}})
		})
		return false, nil
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PrepareMergeCallback: callback})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-b")}}}))

	require.NoError(t, owner.Compact(context.Background()))
	require.NoError(t, admitErr)
	require.Len(t, owner.root.segments, 2, "compacted segment plus the concurrently appended tail, not a discarded merge")

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, view.Close()) })
	for _, identifier := range [][]byte{[]byte("series-a"), []byte("series-b"), []byte("series-c")} {
		_, found, lookupErr := view.Lookup(context.Background(), identifier)
		require.NoError(t, lookupErr)
		require.True(t, found, "identifier %q should survive the rebase", identifier)
	}
}

// TestOwnerCompactionStillRejectsConcurrentDeleteOnMergedSegment confirms the
// rebase fast path does not paper over a real conflict: when a concurrent
// write deletes a document out of a segment this merge is already
// processing, the merge's drop set is stale for that segment and the result
// must still be discarded.
func TestOwnerCompactionStillRejectsConcurrentDeleteOnMergedSegment(t *testing.T) {
	var owner *Owner
	var once sync.Once
	var deleteErr error
	callback := func(ctx context.Context, _ MergeDocument) (bool, error) {
		once.Do(func() {
			deleteErr = owner.Batch(ctx, Batch{Deletes: [][]byte{[]byte("series-a")}})
		})
		return false, nil
	}
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PrepareMergeCallback: callback})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-b")}}}))

	require.ErrorIs(t, owner.Compact(context.Background()), ErrStaleCompaction)
	require.NoError(t, deleteErr)
}

// TestOwnerBatchCoalescesConcurrentAdmissionsIntoOnePersist reproduces a bulk
// write burst (for example schema-registry preload): several Batch calls
// land while the single serial persistence worker is still busy with an
// earlier one. The worker always persists whatever o.root currently is, not
// a queued copy of each admission (see drainPersistence), so none of these
// calls ever block waiting for a slot, and the ones that land while the
// worker is busy share the single persist call it makes once free instead of
// each paying for their own.
func TestOwnerBatchCoalescesConcurrentAdmissionsIntoOnePersist(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{},
		Persist: func(context.Context, *ReadView) error {
			calls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	})
	require.NoError(t, err)
	// Registered before the Close cleanup below so it runs first (t.Cleanup is
	// LIFO): an early Fatalf must still unblock the gated Persist func, or
	// Close would hang waiting for a persistence worker that never drains.
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	t.Cleanup(closeRelease)

	done := make(chan error, 3)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-a")}}, PersistentCallback: func(callbackErr error) { done <- callbackErr },
	}))
	<-started // the worker has grabbed series-a's root and is now blocked in Persist.

	// Neither of these blocks even though the worker is still busy with
	// series-a; both must be admitted (and their callbacks eventually fired)
	// off whichever single persist call the worker next makes.
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-b")}}, PersistentCallback: func(callbackErr error) { done <- callbackErr },
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-c")}}, PersistentCallback: func(callbackErr error) { done <- callbackErr },
	}))

	closeRelease()
	for range 3 {
		require.NoError(t, <-done)
	}
	require.Equal(t, int32(2), calls.Load(), "series-b and series-c should have shared one persist call instead of getting one each")
}

// TestOwnerCloseFlushesPendingPersistenceBeforeReturning confirms Close gives
// admissions that landed just beforehand one final chance to become durable
// and fire their callback, rather than abandoning them. Unlike the old
// bounded-queue design, Batch never blocks waiting for a persistence slot, so
// there is no "Close must unblock a parked Batch call" case to cover here;
// losing a just-admitted callback on shutdown is the failure this design
// must avoid instead.
func TestOwnerCloseFlushesPendingPersistenceBeforeReturning(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{},
		Persist: func(context.Context, *ReadView) error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	})
	require.NoError(t, err)
	// Registered before closeRelease below so it runs last (t.Cleanup is
	// LIFO): Close is idempotent, so this is a no-op on the happy path and a
	// safety net if an assertion fails before the inline Close below runs.
	t.Cleanup(func() { _ = owner.Close() })
	t.Cleanup(closeRelease)

	callbackErr := make(chan error, 2)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-a")}}, PersistentCallback: func(e error) { callbackErr <- e },
	}))
	<-started // the worker is now blocked in Persist for series-a.

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("series-b")}}, PersistentCallback: func(e error) { callbackErr <- e },
	}))

	closeDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.close-owner", nil, func(context.Context) { closeDone <- owner.Close() })
	select {
	case <-closeDone:
		t.Fatal("Close returned before the gated Persist call completed")
	case <-time.After(20 * time.Millisecond):
	}

	closeRelease()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("owner Close did not finish within 1s after release")
	}
	require.NoError(t, <-callbackErr)
	require.NoError(t, <-callbackErr)
}

func TestOwnerBatchDuringCollectionInvokesPersistenceCallback(t *testing.T) {
	owner := newTestOwner(t, func(context.Context, *ReadView) error { return nil })
	owner.mu.Lock()
	owner.collecting = true
	owner.mu.Unlock()
	callback := make(chan error, 1)
	err := owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("blocked")}},
		PersistentCallback: func(callbackErr error) { callback <- callbackErr },
	})
	require.NoError(t, err)
	require.NoError(t, <-callback)
	owner.mu.Lock()
	owner.collecting = false
	owner.mu.Unlock()
}

func TestOwnerCompactionReportsStaleWhenConcurrentMergeShrankRoot(t *testing.T) {
	owner := newTestOwnerAtPath(t)
	for index := 0; index < 20; index++ {
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
			Identifier: []byte(fmt.Sprintf("doc-%02d", index)), Fields: []Field{{Name: "status", Value: []byte("ok"), Store: true, Index: true}},
		}}}))
	}
	shrunk := false
	plan := func(candidates []mergeCandidate) []mergeTask {
		// A concurrent merge folds every segment into one before this task
		// publishes, leaving the root smaller than the task itself. Background
		// maintenance may race it too, so retry until it lands.
		if !shrunk {
			var mergeErr error
			for attempt := 0; attempt < 100; attempt++ {
				if mergeErr = owner.forceMergeAll(context.Background()); !errors.Is(mergeErr, ErrStaleCompaction) &&
					!errors.Is(mergeErr, ErrPersistenceBusy) {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			require.NoError(t, mergeErr)
			shrunk = true
		}
		return []mergeTask{{candidates: candidates}}
	}
	var err error
	// Background garbage collection briefly refuses compaction; that is not
	// the case under test.
	for attempt := 0; attempt < 100; attempt++ {
		if err = owner.compact(context.Background(), plan); !errors.Is(err, ErrPersistenceBusy) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.ErrorIs(t, err, ErrStaleCompaction)

	done := make(chan error, 1)
	go func() {
		done <- owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("after")}}})
	}()
	select {
	case batchErr := <-done:
		require.NoError(t, batchErr)
	case <-time.After(10 * time.Second):
		t.Fatal("owner lock still held after the stale compaction")
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	for _, id := range []string{"doc-00", "doc-19", "after"} {
		_, found, lookupErr := view.Lookup(context.Background(), []byte(id))
		require.NoError(t, lookupErr)
		require.True(t, found, id)
	}
}

func newTestOwnerAtPath(t *testing.T) *Owner {
	t.Helper()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return owner
}

func TestOwnerUpsertKeepsOneLiveDocumentAcrossIndexedAndUnindexedSegments(t *testing.T) {
	path := t.TempDir()
	open := func() *Owner {
		owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, CompactionThreshold: -1})
		require.NoError(t, err)
		return owner
	}
	write := func(owner *Owner, identifier, value string) {
		persisted := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Documents:          []Document{{Identifier: []byte(identifier), Fields: []Field{{Name: "status", Value: []byte(value), Store: true, Index: true}}}},
			PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
		}))
		require.NoError(t, <-persisted)
	}
	expect := func(owner *Owner, identifier, value string, live int) {
		view, err := owner.Acquire(context.Background())
		require.NoError(t, err)
		defer func() { require.NoError(t, view.Close()) }()
		document, found, err := view.Lookup(context.Background(), []byte(identifier))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte(value), document.Fields[0].Value)
		result, err := view.MatchTerms(context.Background(), MatchRequest{Field: "status", Term: []byte(value)})
		require.NoError(t, err)
		require.Len(t, result.Identifiers, live, "identifier %s must have exactly one live document", identifier)
	}
	owner := open()
	write(owner, "a", "a1")
	write(owner, "b", "b1")
	write(owner, "a", "a2") // replaces a1 in an indexed admitted segment
	expect(owner, "a", "a2", 1)

	require.NoError(t, owner.forceMergeAll(context.Background()))
	owner.mu.Lock()
	require.Empty(t, owner.admittedSegments, "merged segments must leave the admitted index")
	owner.mu.Unlock()
	write(owner, "a", "a3") // a2 now lives only in the unindexed merge result
	expect(owner, "a", "a3", 1)
	require.NoError(t, owner.Close())

	reopened := open()
	defer func() { require.NoError(t, reopened.Close()) }()
	write(reopened, "a", "a4") // a3 lives only in a segment loaded at startup
	expect(reopened, "a", "a4", 1)
	expect(reopened, "b", "b1", 1)

	require.NoError(t, reopened.Reset())
	reopened.mu.Lock()
	require.Empty(t, reopened.admittedSegments)
	require.Empty(t, reopened.admittedIdentifiers)
	reopened.mu.Unlock()
}

func TestOwnerPersistIntervalSpacesBackgroundPersistsAndCloseFlushesTheRest(t *testing.T) {
	var persists atomic.Int32
	var persistedDocs atomic.Int64
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, PersistInterval: time.Hour,
		Persist: func(ctx context.Context, view *ReadView) error {
			persists.Add(1)
			var count int64
			visitErr := view.VisitIdentifiers(ctx, func([]byte) bool {
				count++
				return true
			})
			persistedDocs.Store(count)
			return visitErr
		},
	})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("first")}}}))
	require.Eventually(t, func() bool { return persists.Load() == 1 }, time.Second, time.Millisecond,
		"the first persist after an idle period runs immediately")
	for index := 0; index < 20; index++ {
		require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte(fmt.Sprintf("burst-%d", index))}}}))
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), persists.Load(), "a burst inside the interval must not persist again")
	require.NoError(t, owner.Close())
	require.Equal(t, int32(2), persists.Load(), "Close persists what the interval held back")
	require.Equal(t, int64(21), persistedDocs.Load())
}

func TestOwnerFileSnapshotStreamsReopenedDiskBackedSegments(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{
		{Identifier: []byte("kept"), Fields: []Field{{Name: "status", Value: []byte("ok"), Store: true, Index: true}}},
		{Identifier: []byte("removed"), Fields: []Field{{Name: "status", Value: []byte("ok"), Store: true, Index: true}}},
	}}))
	require.NoError(t, owner.Close())

	// A reopened owner holds disk-backed segments with neither a payload nor a
	// staged source path; the snapshot must stream them from their files.
	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.NoError(t, reopened.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("removed")}}))
	destination := t.TempDir()
	require.NoError(t, reopened.TakeFileSnapshot(destination))

	snapshot, err := nativeice.OpenStrict(destination)
	require.NoError(t, err)
	defer func() { require.NoError(t, snapshot.Close()) }()
	_, found, err := snapshot.TermPosting("_id", []byte("kept"))
	require.NoError(t, err)
	require.True(t, found)
	count, err := snapshot.VisibleDocCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "the snapshot keeps the reopened segment's deletion mask")
}
