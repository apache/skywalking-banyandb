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
	require.NoError(t, owner.Compact(context.Background()))
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
	require.ErrorIs(t, owner.Compact(context.Background()), callbackErr)
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
	require.ErrorIs(t, owner.Compact(compactContext), context.Canceled)
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
	require.NoError(t, owner.Compact(context.Background()))
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
	require.Nil(t, promotedPayload)
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
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path, DeduplicateExternal: true})
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
		InsertOnly: true,
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
	require.NoError(t, owner.CollectGarbage(context.Background()))
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

// TestOwnerBatchWaitsForPersistenceSlotInsteadOfFailing reproduces a bulk
// write burst (for example schema-registry preload) that admits documents
// faster than the single serial persistence worker drains them. Before this
// fix, the admission beyond QueueSize failed synchronously with
// ErrPersistenceBackpressure instead of waiting for a slot to free.
func TestOwnerBatchWaitsForPersistenceSlotInsteadOfFailing(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	owner, err := NewOwner(OwnerOptions{
		Lease:     testLease{},
		QueueSize: 1,
		Persist: func(context.Context, *ReadView) error {
			calls.Add(1)
			started <- struct{}{}
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

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	<-started // the worker dequeued series-a (releasing its slot) and is now blocked in Persist.

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-b")}}}))
	// series-b now holds the single queue slot while the worker is still busy
	// with series-a; a third admission must wait for a slot instead of
	// failing with ErrPersistenceBackpressure.
	thirdDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.third-batch", nil, func(ctx context.Context) {
		thirdDone <- owner.Batch(ctx, Batch{Documents: []Document{{Identifier: []byte("series-c")}}})
	})
	select {
	case err := <-thirdDone:
		t.Fatalf("third batch admitted before a persistence slot freed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	closeRelease()
	require.NoError(t, <-thirdDone)
	require.Eventually(t, func() bool { return calls.Load() == 3 }, time.Second, time.Millisecond)
}

// TestOwnerBatchAcquireRespectsContextCancellation confirms that a Batch call
// blocked waiting for a persistence slot still honors its own context
// cancellation rather than waiting indefinitely.
func TestOwnerBatchAcquireRespectsContextCancellation(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	owner, err := NewOwner(OwnerOptions{
		Lease:     testLease{},
		QueueSize: 1,
		Persist: func(context.Context, *ReadView) error {
			startedOnce.Do(func() { close(started) })
			<-release
			return nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { close(release); require.NoError(t, owner.Close()) })

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	<-started
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-b")}}}))

	ctx, cancel := context.WithCancel(context.Background())
	blockedDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.cancel-batch", nil, func(context.Context) {
		blockedDone <- owner.Batch(ctx, Batch{Documents: []Document{{Identifier: []byte("series-c")}}})
	})
	select {
	case <-blockedDone:
		t.Fatal("batch returned before a persistence slot freed or ctx was canceled")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-blockedDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("batch did not observe ctx cancellation within 1s")
	}
}

// TestOwnerCloseUnblocksPendingBatchAcquire confirms that Close wakes a Batch
// call parked waiting for a persistence slot instead of leaving it blocked
// forever once the owner stops draining its queue.
func TestOwnerCloseUnblocksPendingBatchAcquire(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	owner, err := NewOwner(OwnerOptions{
		Lease:     testLease{},
		QueueSize: 1,
		Persist: func(context.Context, *ReadView) error {
			startedOnce.Do(func() { close(started) })
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

	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-a")}}}))
	<-started
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{Identifier: []byte("series-b")}}}))

	blockedDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.close-unblocks-batch", nil, func(ctx context.Context) {
		blockedDone <- owner.Batch(ctx, Batch{Documents: []Document{{Identifier: []byte("series-c")}}})
	})
	select {
	case <-blockedDone:
		t.Fatal("batch returned before Close or a persistence slot freed")
	case <-time.After(20 * time.Millisecond):
	}

	closeDone := make(chan error, 1)
	run.Go(context.Background(), "native.test.close-owner", nil, func(context.Context) { closeDone <- owner.Close() })
	select {
	case err := <-blockedDone:
		require.ErrorIs(t, err, ErrOwnerClosed)
	case <-time.After(time.Second):
		t.Fatal("blocked batch did not observe owner closing within 1s")
	}
	closeRelease()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("owner Close did not finish within 1s after release")
	}
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
