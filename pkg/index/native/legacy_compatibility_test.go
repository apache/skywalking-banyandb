// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// TestOwnerPublicationOpensInLegacyReader proves the rollback reader against
// bytes emitted by the pure native owner, rather than only testing the native
// reader against its own files. This is intentionally a test-only oracle; the
// native production package does not depend on inverted.
func TestOwnerPublicationOpensInLegacyReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	require.NoError(t, owner.Batch(context.Background(), Batch{Documents: []Document{{
		Identifier: []byte("legacy-id"),
		Fields:     []Field{{Name: "status", Value: []byte("native"), Store: true, Index: true}},
	}}}))
	require.NoError(t, owner.Close())

	legacy, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	require.NoError(t, err)
	defer func() { require.NoError(t, legacy.Close()) }()
	fields, err := legacy.StoredFields(context.Background(), []byte("legacy-id"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("native")}, fields["status"])
}

func TestFullyDeletedNativeSnapshotSupportsLegacyInsertAndNativeReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	persisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          []Document{{Identifier: []byte("deleted"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}}}},
		PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
	}))
	require.NoError(t, <-persisted)
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	deletedPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes:            [][]byte{[]byte("deleted")},
		PersistentCallback: func(callbackErr error) { deletedPersisted <- callbackErr },
	}))
	require.NoError(t, <-deletedPersisted)
	_, found, err := oldView.Lookup(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.True(t, found, "a pinned view must retain the deleted document")
	require.NoError(t, oldView.Close())
	require.NoError(t, owner.Close())

	legacy, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	require.NoError(t, err)
	fields, err := legacy.StoredFields(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.Empty(t, fields)
	newField := legacyStoredField("status", "new")
	require.NoError(t, legacy.InsertSeriesBatch(index.Batch{Documents: []index.Document{{
		EntityValues: []byte("new"), Fields: []index.Field{newField},
	}}}))
	require.NoError(t, legacy.Close())

	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err = view.Lookup(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.False(t, found)
	document, found, err := view.Lookup(context.Background(), []byte("new"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), document.Fields[0].Value)
	require.NoError(t, reopened.Close())
}

func TestFileSnapshotFiltersFullyDeletedSegmentsForLegacyRestore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	persisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents: []Document{
			{Identifier: []byte("deleted"), Fields: []Field{{Name: "status", Value: []byte("old"), Store: true, Index: true}}},
			{Identifier: []byte("live"), Fields: []Field{{Name: "status", Value: []byte("live"), Store: true, Index: true}}},
		},
		PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
	}))
	require.NoError(t, <-persisted)
	oldView, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	deletedPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes:            [][]byte{[]byte("deleted")},
		PersistentCallback: func(callbackErr error) { deletedPersisted <- callbackErr },
	}))
	require.NoError(t, <-deletedPersisted)
	partialDestination := filepath.Join(t.TempDir(), "partial")
	require.NoError(t, owner.TakeFileSnapshot(partialDestination))
	partialLegacy, err := inverted.NewStore(inverted.StoreOpts{Path: partialDestination})
	require.NoError(t, err)
	deletedFields, err := partialLegacy.StoredFields(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.Empty(t, deletedFields)
	liveFields, err := partialLegacy.StoredFields(context.Background(), []byte("live"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("live")}, liveFields["status"])
	require.NoError(t, partialLegacy.Close())
	_, found, err := oldView.Lookup(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.True(t, found, "a pinned view must retain the deleted document")
	require.NoError(t, oldView.Close())

	emptyPersisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Deletes:            [][]byte{[]byte("live")},
		PersistentCallback: func(callbackErr error) { emptyPersisted <- callbackErr },
	}))
	require.NoError(t, <-emptyPersisted)
	emptyDestination := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, owner.TakeFileSnapshot(emptyDestination))
	require.NoError(t, owner.Close())
	emptyLegacy, err := inverted.NewStore(inverted.StoreOpts{Path: emptyDestination})
	require.NoError(t, err)
	newField := legacyStoredField("status", "new")
	require.NoError(t, emptyLegacy.InsertSeriesBatch(index.Batch{Documents: []index.Document{{
		EntityValues: []byte("new"), Fields: []index.Field{newField},
	}}}))
	require.NoError(t, emptyLegacy.Close())
	reopened, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: emptyDestination}, Path: emptyDestination})
	require.NoError(t, err)
	view, err := reopened.Acquire(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()
	_, found, err = view.Lookup(context.Background(), []byte("deleted"))
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = view.Lookup(context.Background(), []byte("live"))
	require.NoError(t, err)
	require.False(t, found)
	newDocument, found, err := view.Lookup(context.Background(), []byte("new"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("new"), newDocument.Fields[0].Value)
	require.NoError(t, reopened.Close())
}

func TestLegacyExactStoredChunkBoundaryOpensNatively(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-boundary")
	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	nativeDocuments := make([]Document, 640)
	for documentIndex := range nativeDocuments {
		nativeDocuments[documentIndex] = Document{
			Identifier: []byte(fmt.Sprintf("boundary-%03d", documentIndex)),
			Fields:     []Field{{Name: "boundary", Value: []byte(fmt.Sprintf("value-%03d", documentIndex)), Store: true, Index: true, Sort: true}},
		}
	}
	persisted := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), Batch{
		Documents:          nativeDocuments,
		PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
	}))
	require.NoError(t, <-persisted)
	require.NoError(t, owner.Close())

	legacy, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	require.NoError(t, err)
	legacyDocuments := make(index.Documents, 640)
	for documentIndex := range legacyDocuments {
		legacyDocuments[documentIndex] = index.Document{
			EntityValues: []byte(fmt.Sprintf("legacy-%03d", documentIndex)),
			Fields:       []index.Field{legacyStoredField("boundary", fmt.Sprintf("legacy-value-%03d", documentIndex))},
		}
	}
	require.NoError(t, legacy.InsertSeriesBatch(index.Batch{Documents: legacyDocuments}))
	require.NoError(t, legacy.Close())
	reader, err := OpenReadOnlyGeneration(path)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func legacyStoredField(name, value string) index.Field {
	field := index.NewBytesField(index.FieldKey{TagName: name}, []byte(value))
	field.Store, field.Index = true, true
	return field
}

// TestOwnerProcessRoundTripThroughLegacyWriter exercises the durable bytes
// across process boundaries. The first process writes with Owner, the second
// mutates and appends through the legacy compatibility writer, and this
// process reopens with native Owner. This is intentionally a test-only oracle;
// production native code does not import inverted.
func TestOwnerProcessRoundTripThroughLegacyWriter(t *testing.T) {
	if mode := os.Getenv("NATIVE_COMPAT_HELPER"); mode != "" {
		runCompatibilityHelper(t, mode, os.Getenv("NATIVE_COMPAT_PATH"))
		return
	}
	path := filepath.Join(t.TempDir(), "property")
	run := func(mode string) {
		// The subprocess is always this test binary; mode and path are test-only
		// values passed through the controlled helper environment.
		//nolint:gosec // os.Args[0] is the current test binary.
		command := exec.Command(os.Args[0], "-test.run", "^TestOwnerProcessRoundTripThroughLegacyWriter$", "-test.v")
		command.Env = append(os.Environ(), "NATIVE_COMPAT_HELPER="+mode, "NATIVE_COMPAT_PATH="+path)
		output, runErr := command.CombinedOutput()
		require.NoErrorf(t, runErr, "%s helper output: %s", mode, output)
	}
	run("owner")
	run("legacy")

	owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
	require.NoError(t, err)
	view, err := owner.Acquire(context.Background())
	require.NoError(t, err)
	hits, err := view.MatchField(context.Background(), FieldRequest{Field: "status", MaxTerms: 10})
	require.NoError(t, err)
	require.Len(t, hits, 3)
	expectedTimestamp := map[string]int64{"a": 300, "b": 200, "c": 400}
	expectedStatus := map[string][]byte{"a": []byte("updated"), "b": []byte("two"), "c": []byte("three")}
	expectedSort := map[string][]byte{"a": []byte("m"), "b": []byte("a"), "c": []byte("c")}
	seen := make(map[string]struct{}, len(hits))
	for _, hit := range hits {
		projected, projectErr := view.ProjectHit(context.Background(), hit, "status", "sort")
		require.NoError(t, projectErr)
		identifier := string(projected.Identifier)
		wantTimestamp, ok := expectedTimestamp[identifier]
		require.True(t, ok, "unexpected identifier %q", identifier)
		seen[identifier] = struct{}{}
		require.Equal(t, wantTimestamp, projected.Timestamp)
		require.Equal(t, [][]byte{expectedStatus[identifier]}, projected.Fields["status"])
		require.Equal(t, [][]byte{expectedSort[identifier]}, projected.Fields["sort"])
	}
	for identifier := range expectedTimestamp {
		_, found := seen[identifier]
		require.True(t, found, "missing identifier %q", identifier)
	}
	require.NoError(t, view.Close())
	require.NoError(t, owner.Close())
	legacy, err := inverted.NewStore(inverted.StoreOpts{Path: path})
	require.NoError(t, err)
	for identifier, want := range expectedStatus {
		fields, fieldErr := legacy.StoredFields(context.Background(), []byte(identifier), index.FieldKey{TagName: "status"})
		require.NoError(t, fieldErr)
		require.Equal(t, [][]byte{want}, fields["status"], identifier)
	}
	require.NoError(t, legacy.Close())
}

func runCompatibilityHelper(t *testing.T, mode, path string) {
	t.Helper()
	switch mode {
	case "owner":
		owner, err := NewOwner(OwnerOptions{Lease: pathBoundLease{expected: path}, Path: path})
		require.NoError(t, err)
		persisted := make(chan error, 1)
		require.NoError(t, owner.Batch(context.Background(), Batch{
			Documents: []Document{
				{Identifier: []byte("a"), Timestamp: 100, Fields: []Field{
					{Name: "status", Value: []byte("one"), Store: true, Index: true, Sort: true},
					{Name: "sort", Value: []byte("z"), Store: true, Index: true, Sort: true},
				}},
				{Identifier: []byte("b"), Timestamp: 200, Fields: []Field{
					{Name: "status", Value: []byte("two"), Store: true, Index: true, Sort: true},
					{Name: "sort", Value: []byte("a"), Store: true, Index: true, Sort: true},
				}},
			},
			PersistentCallback: func(callbackErr error) { persisted <- callbackErr },
		}))
		require.NoError(t, <-persisted)
		require.NoError(t, owner.Close())
	case "legacy":
		legacy, err := inverted.NewStore(inverted.StoreOpts{Path: path})
		require.NoError(t, err)
		updated := index.NewBytesField(index.FieldKey{TagName: "status"}, []byte("updated"))
		updated.Store, updated.Index = true, true
		sortValue := index.NewBytesField(index.FieldKey{TagName: "sort"}, []byte("m"))
		sortValue.Store, sortValue.Index = true, true
		require.NoError(t, legacy.UpdateSeriesBatch(index.Batch{Documents: []index.Document{{
			EntityValues: []byte("a"), Timestamp: 300, Fields: []index.Field{updated, sortValue},
		}}}))
		status := index.NewBytesField(index.FieldKey{TagName: "status"}, []byte("three"))
		status.Store, status.Index = true, true
		sortValue = index.NewBytesField(index.FieldKey{TagName: "sort"}, []byte("c"))
		sortValue.Store, sortValue.Index = true, true
		require.NoError(t, legacy.InsertSeriesBatch(index.Batch{Documents: []index.Document{{
			EntityValues: []byte("c"), Timestamp: 400, Fields: []index.Field{status, sortValue},
		}}}))
		require.NoError(t, legacy.Close())
	default:
		t.Fatalf("unknown compatibility helper mode %q", mode)
	}
}
