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

package export

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// propertyVersion is one stored document of a property: an id at one mod revision.
type propertyVersion struct {
	value      string
	id         string
	kind       string
	rev        int64
	deleteTime int64
	shard      uint32
}

func (v propertyVersion) key() string { return v.id + "@" + strconv.FormatInt(v.rev, 10) }

// writePropertyVersions writes, over shards 0 and 1, properties with a single live version,
// properties whose first version was superseded by a second one and then deleted the way
// the property service deletes an older version, and tombstones. It returns every document
// the store holds afterwards.
func writePropertyVersions(t *testing.T, dataDir string, valueSize int) map[string]propertyVersion {
	t.Helper()
	ctx := context.Background()
	database, err := db.OpenDB(ctx, db.Config{
		Location:         dataDir,
		MetricsScopeName: "export_property_versions_test",
		FlushInterval:    time.Second,
		Index:            db.IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, pkgfs.NewLocalFileSystem())
	require.NoError(t, err)
	newProp := func(v propertyVersion) *propertyv1.Property {
		return &propertyv1.Property{
			Metadata:  &commonv1.Metadata{Group: testPropGroup, Name: testPropName, ModRevision: v.rev, CreateRevision: v.rev - 1},
			Id:        v.id,
			UpdatedAt: timestamppb.New(time.Unix(0, v.rev)),
			Tags: []*modelv1.Tag{
				{Key: "name", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v.id}}}},
				{Key: "value", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v.value}}}},
			},
		}
	}
	docs := map[string]propertyVersion{}
	put := func(v propertyVersion) {
		require.NoError(t, database.Update(ctx, common.ShardID(v.shard), db.GetPropertyID(newProp(v)), newProp(v)))
		docs[v.key()] = v
	}
	remove := func(v propertyVersion, at time.Time) {
		require.NoError(t, database.Delete(ctx, [][]byte{db.GetPropertyID(newProp(v))}, at))
		v.deleteTime = at.UnixNano()
		docs[v.key()] = v
	}
	value := func(id string, rev int64) string {
		s := fmt.Sprintf("%s-%d-", id, rev)
		return s + strings.Repeat("x", max(valueSize-len(s), 0))
	}
	deletedAt := time.Unix(0, time.Now().UnixNano()).Truncate(time.Millisecond)
	for i := 0; i < 12; i++ {
		id := fmt.Sprintf("p-%02d", i)
		first := propertyVersion{id: id, rev: int64(1000 + i), shard: uint32(i % 2), kind: "live"}
		first.value = value(id, first.rev)
		put(first)
		switch i % 3 {
		case 1:
			second := propertyVersion{id: id, rev: int64(2000 + i), shard: first.shard, kind: "live"}
			second.value = value(id, second.rev)
			put(second)
			first.kind = "superseded"
			remove(first, deletedAt.Add(time.Duration(i)*time.Millisecond))
		case 2:
			first.kind = "tombstone"
			remove(first, deletedAt.Add(time.Duration(i)*time.Millisecond))
		default: // a single live version
		}
	}
	require.NoError(t, database.Close())
	return docs
}

// assertPropertyRows checks that the exported rows are exactly the stored documents, each
// once, with its revisions, delete time and tags.
func assertPropertyRows(t *testing.T, rows []map[string]string, docs map[string]propertyVersion) {
	t.Helper()
	seen := map[string]struct{}{}
	for _, row := range rows {
		rev, err := strconv.ParseInt(row["_mod_revision"], 10, 64)
		require.NoError(t, err)
		k := row["_id"] + "@" + strconv.FormatInt(rev, 10)
		doc, ok := docs[k]
		require.True(t, ok, "document %s was never written", k)
		_, dup := seen[k]
		require.False(t, dup, "document %s is exported twice", k)
		seen[k] = struct{}{}
		require.Equal(t, testPropName, row["_name"])
		require.Equal(t, strconv.FormatInt(doc.rev-1, 10), row["_create_revision"], k)
		require.Equal(t, csvfmt.EncodeTimestamp(doc.rev), row["_updated_at"], k)
		require.Equal(t, csvfmt.EncodeTimestamp(doc.deleteTime), row["_delete_time"], k)
		require.Equal(t, doc.id, row["name"], k)
		require.Equal(t, doc.value, row["value"], k)
	}
	require.Len(t, seen, len(docs), "every stored document is exported")
}

// The export of a property group holds one row per document of the store: every live
// property, every superseded version and every tombstone, so the row count equals the doc
// count the inventory reports.
func TestExport_PropertyRowsMatchTheStoredDocuments(t *testing.T) {
	backends, clock := newTestBackends(t)
	docs := writePropertyVersions(t, backends[commonv1.Catalog_CATALOG_PROPERTY].GetDataPath(), 16)
	s := newTestService(propertyRepo(t, propertySchema()), backends, clock)
	created, err := s.planner.sessions.create(context.Background(), testSessionID, catalogSet(commonv1.Catalog_CATALOG_PROPERTY), false)
	require.NoError(t, err)

	inv, err := statPropertyGroup(filepath.Join(created.roots[commonv1.Catalog_CATALOG_PROPERTY], testPropGroup), testPropGroup)
	require.NoError(t, err)
	var docCount uint64
	perShard := map[uint32]uint64{}
	for _, sh := range inv.GetProperty().GetShards() {
		docCount += sh.GetDocCount()
		perShard[sh.GetShardId()] = sh.GetDocCount()
	}
	require.Equal(t, uint64(len(docs)), docCount, "the store holds every version and tombstone")
	kinds := map[string]int{}
	shardOf := map[string]uint32{}
	for _, d := range docs {
		kinds[d.kind]++
		shardOf[d.id] = d.shard
	}
	require.Equal(t, map[string]int{"live": 8, "superseded": 4, "tombstone": 4}, kinds)

	// A max_file_size of a few rows rolls each shard over several files.
	for name, maxFileSize := range map[string]uint64{"one file per shard": 0, "rolled files": 256} {
		t.Run(name, func(t *testing.T) {
			rolled := maxFileSize > 0
			init := initFrame(testSessionID, transferv1.Format_FORMAT_CSV, transferv1.Compress_COMPRESS_NONE)
			init.GetInit().MaxFileSize = maxFileSize
			stream := newFakeExportStream(init, propertyFrame(testPropGroup, 0, 1))
			require.NoError(t, s.Export(stream))
			units := decodeFrames(t, stream.sent)
			require.Len(t, units, 1)
			require.Empty(t, units[0].warnings)
			var rows []map[string]string
			files := map[uint32]int{}
			for _, f := range units[0].files {
				fileRows := csvRows(t, f)
				require.Equal(t, map[string]uint64{testPropName: uint64(len(fileRows))}, f.finished.GetRowCounts(), f.started.GetRelativePath())
				shard := f.started.GetShardId()
				files[shard]++
				require.Equal(t, uint32(files[shard]), f.started.GetSeq())
				for _, row := range fileRows {
					require.Equal(t, shardOf[row["_id"]], shard, "property %s is in the file of its shard", row["_id"])
				}
				rows = append(rows, fileRows...)
			}
			for shard, n := range perShard {
				var exported uint64
				for _, row := range rows {
					if shardOf[row["_id"]] == shard {
						exported++
					}
				}
				require.Equal(t, n, exported, "shard %d exports its doc count", shard)
				if rolled {
					require.Greater(t, files[shard], 1, "shard %d rolls over several files", shard)
				} else {
					require.Equal(t, 1, files[shard])
				}
			}
			require.Equal(t, docCount, uint64(len(rows)), "the export holds as many rows as the store holds documents")
			assertPropertyRows(t, rows, docs)
		})
	}
}
