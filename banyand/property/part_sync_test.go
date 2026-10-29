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

package property

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
)

const (
	partSyncGroup = "part-sync-group"
	partSyncName  = "part-sync-name"
)

// revisionState is the newest stored revision of an entity and whether it is a tombstone.
type revisionState struct {
	revision int64
	deleted  bool
}

func openPartSyncDB(t *testing.T) db.Database {
	t.Helper()
	return openPartSyncDBAt(t, t.TempDir())
}

func openPartSyncDBAt(t *testing.T, location string) db.Database {
	t.Helper()
	database, err := db.OpenDB(context.Background(), db.Config{
		Location:               location,
		MetricsScopeName:       "property_part_sync_test",
		FlushInterval:          time.Hour,
		ExpireToDeleteDuration: time.Hour,
		Repair:                 db.RepairConfig{Location: t.TempDir(), BuildTreeCron: "@every 1h", TreeSlotCount: 32},
		Snapshot:               db.SnapshotConfig{Func: func(context.Context) (string, error) { return "", nil }},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func newPartSyncProperty(id string, revision int64) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: partSyncGroup, Name: partSyncName, ModRevision: revision, CreateRevision: 1},
		Id:       id,
		Tags: []*modelv1.Tag{{Key: "value", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{
			Int: &modelv1.Int{Value: revision},
		}}}},
	}
}

// put stores one revision of an entity in shard 0; a positive deleteTime stores it as a tombstone.
func put(t *testing.T, database db.Database, id string, revision, deleteTime int64) {
	t.Helper()
	putInShard(t, database, 0, id, revision, deleteTime)
}

func putInShard(t *testing.T, database db.Database, shardID uint32, id string, revision, deleteTime int64) {
	t.Helper()
	property := newPartSyncProperty(id, revision)
	require.NoError(t, database.Repair(context.Background(), db.GetPropertyID(property), uint64(shardID), property, deleteTime))
}

// snapshotShard copies the shards of the source into a fresh directory, as an export session
// does, and returns the directory of the given shard.
func snapshotShard(t *testing.T, database db.Database, shardID uint32) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "session")
	require.NoError(t, database.SnapshotShards(context.Background(), dst, nil))
	return filepath.Join(dst, partSyncGroup, fmt.Sprintf("shard-%d", shardID))
}

func newTestPartSyncHandler(t *testing.T, database db.Database) *partSyncHandler {
	t.Helper()
	return &partSyncHandler{
		db:        database,
		checkDisk: func() *common.Error { return nil },
		tempRoot:  filepath.Join(t.TempDir(), externalPartTempDirName),
	}
}

func shardFiles(t *testing.T, shardDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(shardDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	require.NotEmpty(t, names)
	return names
}

// pushShard delivers every file of the shard directory to the handler as one part.
func pushShard(t *testing.T, handler *partSyncHandler, shardDir string, shardID uint32) error {
	t.Helper()
	files, order := readShard(t, shardDir)
	return deliver(handler, partSyncGroup, shardID, files, order)
}

// newestRevisions returns the newest stored revision of every entity of the target.
func newestRevisions(t *testing.T, database db.Database) map[string]revisionState {
	t.Helper()
	states := make(map[string]revisionState)
	for _, hit := range queryAll(t, database) {
		var property propertyv1.Property
		require.NoError(t, protojson.Unmarshal(hit.Source(), &property))
		require.True(t, proto.Equal(newPartSyncProperty(property.GetId(), hit.Timestamp()), &property),
			"stored property %s differs from the one written: %v", property.GetId(), &property)
		if state, ok := states[property.GetId()]; ok && state.revision > hit.Timestamp() {
			continue
		}
		states[property.GetId()] = revisionState{revision: hit.Timestamp(), deleted: hit.DeleteTime() > 0}
	}
	return states
}

func storedDocumentCount(t *testing.T, database db.Database) int {
	t.Helper()
	return len(queryAll(t, database))
}

// queryAll returns every stored document of the test property, tombstones included.
func queryAll(t *testing.T, database db.Database) []db.QueriedProperty {
	t.Helper()
	result, err := database.Query(context.Background(), &propertyv1.QueryRequest{Groups: []string{partSyncGroup}, Name: partSyncName, Limit: 1000})
	require.NoError(t, err)
	return result
}

// newPart opens a part of the group on the handler, as the sync server does for a new part ID.
func newPart(t *testing.T, handler *partSyncHandler, group string) *queue.ChunkedSyncPartContext {
	t.Helper()
	partCtx := &queue.ChunkedSyncPartContext{ID: 1, Group: group, Context: context.Background()}
	partHandler, err := handler.CreatePartHandler(partCtx)
	require.NoError(t, err)
	partCtx.Handler = partHandler
	return partCtx
}

func requireNoStagedParts(t *testing.T, handler *partSyncHandler) {
	t.Helper()
	entries, err := os.ReadDir(handler.tempRoot)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	require.NoError(t, err)
	require.Empty(t, entries, "a staged part must not outlive its sync")
}

func TestPartSyncRestoresDocumentsAndTombstonesIntoEmptyTarget(t *testing.T) {
	source := openPartSyncDB(t)
	put(t, source, "live", 1, 0)
	put(t, source, "updated", 1, 0)
	put(t, source, "updated", 2, 0)
	put(t, source, "deleted", 1, 0)
	put(t, source, "deleted", 1, time.Now().UnixNano())
	shardDir := snapshotShard(t, source, 0)

	target := openPartSyncDB(t)
	handler := newTestPartSyncHandler(t, target)
	require.NoError(t, pushShard(t, handler, shardDir, 0))

	require.Equal(t, map[string]revisionState{
		"live":    {revision: 1},
		"updated": {revision: 2},
		"deleted": {revision: 1, deleted: true},
	}, newestRevisions(t, target))
	requireNoStagedParts(t, handler)
}

func TestPartSyncDeletesLiveTargetDocument(t *testing.T) {
	source := openPartSyncDB(t)
	put(t, source, "entity", 1, time.Now().UnixNano())
	shardDir := snapshotShard(t, source, 0)

	target := openPartSyncDB(t)
	put(t, target, "entity", 1, 0)
	handler := newTestPartSyncHandler(t, target)
	require.NoError(t, pushShard(t, handler, shardDir, 0))

	require.Equal(t, map[string]revisionState{"entity": {revision: 1, deleted: true}}, newestRevisions(t, target))
}

func TestPartSyncPushedTwiceIsIdempotent(t *testing.T) {
	source := openPartSyncDB(t)
	put(t, source, "live", 1, 0)
	put(t, source, "updated", 1, 0)
	put(t, source, "updated", 2, 0)
	put(t, source, "deleted", 3, time.Now().UnixNano())
	shardDir := snapshotShard(t, source, 0)

	target := openPartSyncDB(t)
	handler := newTestPartSyncHandler(t, target)
	require.NoError(t, pushShard(t, handler, shardDir, 0))
	states := newestRevisions(t, target)
	documents := storedDocumentCount(t, target)

	require.NoError(t, pushShard(t, handler, shardDir, 0))
	require.Equal(t, states, newestRevisions(t, target))
	require.Equal(t, documents, storedDocumentCount(t, target), "a second push must not add documents")
	requireNoStagedParts(t, handler)
}

func TestPartSyncRejectsFileNamesOutsideTheStagingDirectory(t *testing.T) {
	handler := newTestPartSyncHandler(t, openPartSyncDB(t))
	for _, name := range []string{"", "..", "../escape.seg", "nested/000000000001.seg", "/abs.seg"} {
		partCtx := newPart(t, handler, partSyncGroup)
		partCtx.FileName = name
		require.Error(t, handler.HandleFileChunk(partCtx, []byte("data")), "file name %q", name)
		require.NoError(t, partCtx.Close())
	}
	requireNoStagedParts(t, handler)
}

// deliveryChunkSize splits every file into many chunks, as the sync server hands them over.
const deliveryChunkSize = 64

// deliver hands the given files to the handler as one part in small chunks and then finishes
// it, the way the sync server does; the part is always closed, as the server's deferred Close does.
func deliver(handler *partSyncHandler, group string, shardID uint32, files map[string][]byte, order []string) (err error) {
	partCtx := &queue.ChunkedSyncPartContext{ID: 1, Group: group, ShardID: shardID, Context: context.Background()}
	partHandler, err := handler.CreatePartHandler(partCtx)
	if err != nil {
		return err
	}
	partCtx.Handler = partHandler
	defer func() {
		err = multierr.Append(err, partCtx.Close())
	}()
	for _, name := range order {
		partCtx.FileName = name
		for content := files[name]; len(content) > 0; content = content[min(deliveryChunkSize, len(content)):] {
			if err = handler.HandleFileChunk(partCtx, content[:min(deliveryChunkSize, len(content))]); err != nil {
				return err
			}
		}
	}
	return partCtx.Handler.FinishSync()
}

func readShard(t *testing.T, shardDir string) (map[string][]byte, []string) {
	t.Helper()
	names := shardFiles(t, shardDir)
	files := make(map[string][]byte, len(names))
	for _, name := range names {
		content, err := os.ReadFile(filepath.Join(shardDir, name))
		require.NoError(t, err)
		files[name] = content
	}
	return files, names
}

func TestPartSyncIncompletePartRepairsNothing(t *testing.T) {
	source := openPartSyncDB(t)
	for i := int64(1); i <= 20; i++ {
		put(t, source, fmt.Sprintf("entity-%d", i), i, 0)
	}
	files, names := readShard(t, snapshotShard(t, source, 0))
	var segments []string
	for _, name := range names {
		if filepath.Ext(name) == ".seg" {
			segments = append(segments, name)
		}
	}
	require.NotEmpty(t, segments)

	cases := map[string]func() (map[string][]byte, []string){
		"missing segment": func() (map[string][]byte, []string) {
			var order []string
			for _, name := range names {
				if name != segments[len(segments)-1] {
					order = append(order, name)
				}
			}
			return files, order
		},
		"truncated segment": func() (map[string][]byte, []string) {
			truncated := make(map[string][]byte, len(files))
			for name, content := range files {
				truncated[name] = content
			}
			last := segments[len(segments)-1]
			truncated[last] = files[last][:len(files[last])/2]
			return truncated, names
		},
		"duplicated delivery": func() (map[string][]byte, []string) {
			return files, append(append([]string{}, names...), names...)
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			target := openPartSyncDB(t)
			handler := newTestPartSyncHandler(t, target)
			partFiles, order := build()
			err := deliver(handler, partSyncGroup, 0, partFiles, order)
			require.Error(t, err)
			require.Zero(t, storedDocumentCount(t, target), "an incomplete part must not repair any document")
			requireNoStagedParts(t, handler)
		})
	}
}

func TestPartSyncInterruptedStreamDiscardsStagedPart(t *testing.T) {
	source := openPartSyncDB(t)
	put(t, source, "entity", 1, 0)
	files, names := readShard(t, snapshotShard(t, source, 0))

	target := openPartSyncDB(t)
	handler := newTestPartSyncHandler(t, target)
	partCtx := newPart(t, handler, partSyncGroup)
	partCtx.FileName = names[0]
	require.NoError(t, handler.HandleFileChunk(partCtx, files[names[0]]))
	staged, err := os.ReadDir(handler.tempRoot)
	require.NoError(t, err)
	require.Len(t, staged, 1)

	// A broken stream never reaches FinishSync; the server only closes the part.
	require.NoError(t, partCtx.Close())
	require.NoError(t, partCtx.Close(), "closing twice must be safe")
	require.Zero(t, storedDocumentCount(t, target))
	requireNoStagedParts(t, handler)
}

func TestPartSyncRejectsDocumentsOfAnotherGroup(t *testing.T) {
	source := openPartSyncDB(t)
	put(t, source, "entity", 1, 0)
	files, names := readShard(t, snapshotShard(t, source, 0))

	target := openPartSyncDB(t)
	handler := newTestPartSyncHandler(t, target)
	require.Error(t, deliver(handler, "another-group", 0, files, names))
	require.Zero(t, storedDocumentCount(t, target))
	requireNoStagedParts(t, handler)
}

func TestPartSyncRejectsPartWhenDiskIsFull(t *testing.T) {
	handler := newTestPartSyncHandler(t, openPartSyncDB(t))
	handler.checkDisk = func() *common.Error {
		return common.NewErrorWithStatus(modelv1.Status_STATUS_DISK_FULL, "disk usage is too high, stop writing")
	}
	_, err := handler.CreatePartHandler(&queue.ChunkedSyncPartContext{ID: 1, Group: partSyncGroup})
	require.Error(t, err)
	requireNoStagedParts(t, handler)
}

func TestDecodeStoredDocumentOfSnapshot(t *testing.T) {
	source := openPartSyncDB(t)
	deleteTime := time.Now().UnixNano()
	put(t, source, "entity", 1, deleteTime)
	var decoded []revisionState
	require.NoError(t, native.ReadOnlyWalkDocuments(context.Background(), snapshotShard(t, source, 0), func(doc native.StoredDocument) error {
		property, gotDeleteTime, err := db.DecodeStoredDocument(doc)
		if err != nil {
			return err
		}
		require.Equal(t, deleteTime, gotDeleteTime)
		decoded = append(decoded, revisionState{revision: property.GetMetadata().GetModRevision(), deleted: gotDeleteTime > 0})
		return nil
	}))
	require.Equal(t, []revisionState{{revision: 1, deleted: true}}, decoded)
}

func TestPartSyncRepairsIntoTheSyncedShard(t *testing.T) {
	source := openPartSyncDB(t)
	putInShard(t, source, 2, "entity", 1, 0)
	shardDir := snapshotShard(t, source, 2)

	location := t.TempDir()
	target := openPartSyncDBAt(t, location)
	handler := newTestPartSyncHandler(t, target)
	require.NoError(t, pushShard(t, handler, shardDir, 2))

	require.Equal(t, map[string]revisionState{"entity": {revision: 1}}, newestRevisions(t, target))
	count, err := native.ReadOnlyDocCount(filepath.Join(location, partSyncGroup, "shard-2"))
	require.NoError(t, err)
	require.EqualValues(t, 1, count, "the documents must land in the shard the sync names")
	_, err = os.Stat(filepath.Join(location, partSyncGroup, "shard-0"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// Two replicas exported different revisions of one entity and their parts arrive at once.
// Each repair must see the other's write, or both revisions end up live.
func TestPartSyncConcurrentPartsKeepOneLiveRevision(t *testing.T) {
	newer := openPartSyncDB(t)
	put(t, newer, "entity", 2, 0)
	older := openPartSyncDB(t)
	put(t, older, "entity", 1, 0)
	newerFiles, newerOrder := readShard(t, snapshotShard(t, newer, 0))
	olderFiles, olderOrder := readShard(t, snapshotShard(t, older, 0))

	for round := 0; round < 20; round++ {
		target := openPartSyncDB(t)
		handler := newTestPartSyncHandler(t, target)
		errs := make([]error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs[0] = deliver(handler, partSyncGroup, 0, newerFiles, newerOrder)
		}()
		go func() {
			defer wg.Done()
			errs[1] = deliver(handler, partSyncGroup, 0, olderFiles, olderOrder)
		}()
		wg.Wait()
		require.NoError(t, errors.Join(errs...))
		require.Equal(t, map[string]revisionState{"entity": {revision: 2}}, newestRevisions(t, target), "round %d", round)
		live := 0
		for _, hit := range queryAll(t, target) {
			if hit.DeleteTime() <= 0 {
				live++
			}
		}
		require.Equal(t, 1, live, "round %d: only the newest revision may stay live", round)
		requireNoStagedParts(t, handler)
	}
}

// fakeStoredDocument serves the given stored fields in a fixed order.
type fakeStoredDocument [][2]string

func (d fakeStoredDocument) VisitStoredFields(visit func(name string, value []byte) bool) error {
	for _, field := range d {
		if !visit(field[0], []byte(field[1])) {
			return nil
		}
	}
	return nil
}

func TestDecodeStoredDocumentRejectsCorruptDocuments(t *testing.T) {
	source := func(property *propertyv1.Property) string {
		encoded, err := protojson.Marshal(property)
		require.NoError(t, err)
		return string(encoded)
	}
	nameless := newPartSyncProperty("entity", 1)
	nameless.Metadata.Name = ""
	cases := map[string]fakeStoredDocument{
		"missing source":        {{"_deleted", string(convert.Int64ToBytes(1))}},
		"malformed source":      {{"_source", "{not json"}},
		"short deletion time":   {{"_source", source(newPartSyncProperty("entity", 1))}, {"_deleted", "1234"}},
		"source without a name": {{"_source", source(nameless)}},
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := db.DecodeStoredDocument(doc)
			require.ErrorIs(t, err, native.ErrCorrupt)
		})
	}

	property, deleteTime, err := db.DecodeStoredDocument(fakeStoredDocument{
		{"_source", source(newPartSyncProperty("entity", 3))}, {"_deleted", string(convert.Int64ToBytes(42))},
	})
	require.NoError(t, err)
	require.True(t, proto.Equal(newPartSyncProperty("entity", 3), property))
	require.EqualValues(t, 42, deleteTime)
}
