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
	"testing"

	"github.com/stretchr/testify/require"
)

func statusField(value string) Field {
	return Field{Name: "status", Value: []byte(value), Index: true, Store: true}
}

//nolint:unparam // kept symmetric with statusField; every call site happens to use "us" today.
func regionField(value string) Field {
	return Field{Name: "region", Value: []byte(value), Index: true, Store: true}
}

func TestOwnerInsertIfAbsentAdmitsWhenIdentifierIsAbsent(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("new")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), document.Fields[0].Value)
}

func TestOwnerInsertIfAbsentSkipsWhenSameFieldsAlreadyLive(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("attempted-new")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), document.Fields[0].Value, "the existing document must survive untouched")
}

func TestOwnerInsertIfAbsentAdmitsWhenDocumentCarriesANewField(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{
			statusField("old"), regionField("us"),
		}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, document.Fields, 2, "the admitted document, carrying the new field, must replace the old one")
}

func TestOwnerInsertIfAbsentCollapsesWithinBatchDuplicates(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertIfAbsent,
		Documents: []Document{
			{Identifier: []byte("b"), Fields: []Field{statusField("first")}},
			{Identifier: []byte("b"), Fields: []Field{statusField("second"), regionField("us")}},
		},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("b"))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, document.Fields, 2)
	require.Equal(t, []byte("second"), fieldValue(t, document, "status"), "the last within-batch duplicate must win")
}

func TestOwnerInsertIfAbsentChecksPresenceAfterMerge(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	require.NoError(t, owner.forceMergeAll(context.Background()))

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("attempted-new")}}},
	}))
	skippedView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	skippedDocument, found, err := skippedView.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), skippedDocument.Fields[0].Value)
	require.NoError(t, skippedView.Close())

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{
			statusField("old"), regionField("us"),
		}}},
	}))
	admittedView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, admittedView.Close()) }()
	admittedDocument, found, err := admittedView.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, admittedDocument.Fields, 2)
}

func TestOwnerInsertIfAbsentChecksPresenceAfterPersistAndReopen(t *testing.T) {
	path := t.TempDir()
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	require.NoError(t, owner.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: testLease{}, Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.NoError(t, reopened.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("attempted-new")}}},
	}))
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), document.Fields[0].Value, "a reloaded, un-indexed segment must still be probed for presence")
}

func TestOwnerInsertIfAbsentPresenceCacheResetAndGenerationInvalidation(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PresenceCacheBytes: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	// First InsertIfAbsent populates the presence cache; the second reuses it.
	// Either way the outcome must be "skip", so this only guards against a
	// cache-path panic or crash, not the cached value itself.
	for i := 0; i < 2; i++ {
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Mode:      BatchInsertIfAbsent,
			Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("attempted-new")}}},
		}))
	}
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), document.Fields[0].Value)
	require.NoError(t, view.Close())

	// A delete advances the root generation and makes "a" absent again. A
	// presence cache entry still keyed to the pre-delete generation must not
	// be trusted, or this InsertIfAbsent would wrongly skip and "a" would stay
	// gone forever.
	require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("a")}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("resurrected")}}},
	}))
	resurrectedView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, resurrectedView.Close()) }()
	resurrected, found, err := resurrectedView.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("resurrected"), resurrected.Fields[0].Value)

	// ResetPresenceCache must not disturb correctness, and must be a no-op
	// when called on an owner without a configured cache.
	owner.ResetPresenceCache()
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("after-reset")}}},
	}))
	afterResetView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, afterResetView.Close()) }()
	afterReset, found, err := afterResetView.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("resurrected"), afterReset.Fields[0].Value, "reset only drops memoized entries, it never hides live data")

	noCacheOwner := newTestOwner(t, nil)
	noCacheOwner.ResetPresenceCache()
}

// fieldValue returns the stored value of name in document, failing the test
// if it is absent. Lookup returns fields in ascending name order rather than
// the order a caller listed them, so tests that care about one field's value
// look it up by name instead of by slice position.
func fieldValue(t *testing.T, document Document, name string) []byte {
	t.Helper()
	for _, field := range document.Fields {
		if field.Name == name {
			return field.Value
		}
	}
	t.Fatalf("document %q has no field %q", document.Identifier, name)
	return nil
}

// TestOwnerInsertIfAbsentAllPresentPublishesNothing is a regression test: an
// InsertIfAbsent batch whose documents are all already present with a
// satisfying field set must not publish a new root at all -- no generation
// bump, no segment, and the PersistentCallback (when supplied) must still
// fire exactly once with a nil error. Repeating it must not accumulate dead
// segments either.
func TestOwnerInsertIfAbsentAllPresentPublishesNothing(t *testing.T) {
	owner := newTestOwner(t, func(context.Context, *ReadView) error { return nil })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	owner.mu.Lock()
	initialGeneration := owner.root.generation
	initialSegments := len(owner.root.segments)
	owner.mu.Unlock()

	for i := 0; i < 5; i++ {
		callback := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Mode:               BatchInsertIfAbsent,
			Documents:          []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
			PersistentCallback: func(err error) { callback <- err },
		}))
		require.NoError(t, <-callback, "an all-present batch must still complete its callback with nil")
	}

	owner.mu.Lock()
	finalGeneration := owner.root.generation
	finalSegments := len(owner.root.segments)
	owner.mu.Unlock()
	require.Equal(t, initialGeneration, finalGeneration, "an all-present InsertIfAbsent batch must not publish a new generation")
	require.Equal(t, initialSegments, finalSegments, "an all-present InsertIfAbsent batch must not publish a dead segment")

	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("old"), document.Fields[0].Value)
}

// TestOwnerInsertIfAbsentWithOnlyExplicitDeletesStillApplies proves an
// InsertIfAbsent batch that carries explicit Deletes but whose Documents are
// all already present still applies those deletes: the "publish nothing"
// fast path for an all-present batch must not swallow an unrelated delete.
func TestOwnerInsertIfAbsentWithOnlyExplicitDeletesStillApplies(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{
			{Identifier: []byte("present"), Fields: []Field{statusField("old")}},
			{Identifier: []byte("doomed"), Fields: []Field{statusField("old")}},
		},
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("present"), Fields: []Field{statusField("old")}}},
		Deletes:   [][]byte{[]byte("doomed")},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err := view.Lookup(context.Background(), []byte("doomed"))
	require.NoError(t, err)
	require.False(t, found, "an explicit delete must apply even when every document in the same batch is already present")
	_, found, err = view.Lookup(context.Background(), []byte("present"))
	require.NoError(t, err)
	require.True(t, found)
}

// TestOwnerInsertIfAbsentAdmittedDocumentSegmentHasNoMaskedDocuments proves a
// batch mixing an absent and an already-present identifier publishes a
// segment containing only the admitted document: the published segment's
// physical document count matches exactly the one document that needed
// encoding, not the whole candidate set.
func TestOwnerInsertIfAbsentAdmittedDocumentSegmentHasNoMaskedDocuments(t *testing.T) {
	owner := newTestOwner(t, nil)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("present"), Fields: []Field{statusField("old")}}},
	}))
	owner.mu.Lock()
	segmentsBefore := len(owner.root.segments)
	owner.mu.Unlock()

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertIfAbsent,
		Documents: []Document{
			{Identifier: []byte("present"), Fields: []Field{statusField("old")}},
			{Identifier: []byte("absent"), Fields: []Field{statusField("new")}},
		},
	}))

	owner.mu.Lock()
	require.Equal(t, segmentsBefore+1, len(owner.root.segments), "exactly one new segment, carrying only the admitted document")
	newSegment, ok := owner.root.segments[len(owner.root.segments)-1].(*memorySegment)
	require.True(t, ok)
	require.Equal(t, uint64(1), newSegment.handle.count, "the published segment must contain only the admitted document")
	require.Empty(t, newSegment.deleted, "nothing in the published segment should need masking")
	owner.mu.Unlock()
}

// TestOwnerInsertIfAbsentPresenceCacheHitsAcrossBatches proves a repeat
// InsertIfAbsent admission for the same identifier and field set reuses the
// cached decision instead of rescanning segments: the cache's hit counter
// must advance on the second and later batches.
func TestOwnerInsertIfAbsentPresenceCacheHitsAcrossBatches(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PresenceCacheBytes: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	hitsBefore := owner.presenceCache.hits.Load()
	for i := 0; i < 3; i++ {
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Mode:      BatchInsertIfAbsent,
			Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
		}))
	}
	hitsAfter := owner.presenceCache.hits.Load()
	require.Greater(t, hitsAfter, hitsBefore, "a repeat InsertIfAbsent of the same identifier and field set must hit the cache")

	owner.mu.Lock()
	segments := len(owner.root.segments)
	owner.mu.Unlock()
	require.Equal(t, 1, segments, "none of the repeat batches should have published anything")
}

// TestOwnerInsertIfAbsentPresenceCacheInvalidatedByDelete proves an explicit
// delete drops the presence-cache entry for its identifier, so a later
// InsertIfAbsent cannot be hidden behind a stale "present" answer and fail to
// re-admit a document that is actually gone.
func TestOwnerInsertIfAbsentPresenceCacheInvalidatedByDelete(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PresenceCacheBytes: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{Deletes: [][]byte{[]byte("a")}}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("resurrected")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found, "a stale cache entry must not hide the delete and suppress re-admission")
	require.Equal(t, []byte("resurrected"), document.Fields[0].Value)
}

// TestOwnerInsertIfAbsentPresenceCacheInvalidatedByUpsertWithFewerFields
// proves a plain (non-InsertIfAbsent) upsert that replaces a document with
// one carrying fewer fields drops the stale cache entry, so a later
// InsertIfAbsent checking for the original, larger field set is not wrongly
// skipped.
func TestOwnerInsertIfAbsentPresenceCacheInvalidatedByUpsertWithFewerFields(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PresenceCacheBytes: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("v1"), regionField("us")}}},
	}))
	// A plain upsert (default Mode) replaces it with a document carrying only
	// "status": the live document now has fewer fields than InsertIfAbsent
	// last cached.
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("v2")}}},
	}))
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("v2"), regionField("us")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, document.Fields, 2, "a stale cache entry must not hide that the live document lost a field")
}

// TestOwnerInsertIfAbsentPresenceCacheResetByMergeDrop proves a compaction
// that drops a document via PrepareMergeCallback resets the presence cache,
// so a later InsertIfAbsent for the dropped identifier is not wrongly
// skipped.
func TestOwnerInsertIfAbsentPresenceCacheResetByMergeDrop(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{
		Lease: testLease{}, CompactionThreshold: -1, PresenceCacheBytes: 4096,
		PrepareMergeCallback: func(_ context.Context, document MergeDocument) (bool, error) {
			drop := false
			if visitErr := document.StoredFields(func(name string, value []byte) bool {
				if name == "mark" && string(value) == "drop" {
					drop = true
				}
				return true
			}); visitErr != nil {
				return false, visitErr
			}
			return drop, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode: BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{
			statusField("old"), {Name: "mark", Value: []byte("drop"), Store: true},
		}}},
	}))
	require.NoError(t, owner.forceMergeAll(context.Background()))

	require.NoError(t, owner.Batch(context.Background(), Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("resurrected")}}},
	}))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	document, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found, "the merge-dropped identifier must be re-admitted, not hidden by a stale cache entry")
	require.Equal(t, []byte("resurrected"), document.Fields[0].Value)
}

// TestOwnerInsertIfAbsentPresenceCacheClearedByReset proves Reset drops every
// cached presence entry, so a document inserted after a reset is admitted
// instead of being skipped behind a stale "present" answer.
func TestOwnerInsertIfAbsentPresenceCacheClearedByReset(t *testing.T) {
	owner, err := NewOwner(OwnerOptions{Lease: testLease{}, PresenceCacheBytes: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	insert := Batch{
		Mode:      BatchInsertIfAbsent,
		Documents: []Document{{Identifier: []byte("a"), Fields: []Field{statusField("old")}}},
	}
	require.NoError(t, owner.Batch(context.Background(), insert))
	require.NoError(t, owner.Reset())
	require.NoError(t, owner.Batch(context.Background(), insert))
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err := view.Lookup(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.True(t, found, "the insert after Reset must be admitted")
}
