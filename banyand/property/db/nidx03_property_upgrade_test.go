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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package db

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestNIDX03PropertyUpgradeFromLegacyBlugeLayout is R5's "lost coverage"
// upgrade-direction companion to the rollback proof under test/rollback/
// nidx03 (which covers the opposite direction -- v0.11.1 opening data THIS
// code wrote). It always runs (no NIDX03_ROLLBACK gate): it never builds or
// runs a previous release, it only writes a shard directory in the exact
// shape the retired legacy writer (banyand/property/db/shard.go's removed
// NativeWriter=false path, which called index.SeriesStore.UpdateSeriesBatch
// with one bluge document keyed by EntityValues per property revision,
// before NIDX-03 §15 removed the switch) would have left on disk, using
// pkg/index/inverted.NewStore directly -- still in-tree for the NIDX-04
// element-index migration tool -- then opens that SAME directory through
// the current (native-only) OpenDB/Query path and asserts identical query
// results against hand-written expectations.
//
// Field shape mirrors shard.go's buildUpdateDocument exactly (sourceField,
// entityField, groupField, nameField, deletedField, plus one indexed field
// per property tag), with one simplification: no shaValueField. That field
// only feeds the anti-entropy repair/gossip protocol's content-hash
// comparison (repair.buildShaValue), never Query -- a missing shaValue
// changes what a repair round would detect as "differs," not what a query
// returns, so this still exercises the real read path identically.
func TestNIDX03PropertyUpgradeFromLegacyBlugeLayout(t *testing.T) {
	tmpPath, cleanup := test.Space(require.New(t))
	defer cleanup()

	const group = "nidx03-upgrade-group"
	const name = "nidx03-upgrade-name"
	shardPath := path.Join(tmpPath, group, "shard-0")

	alive := legacyTestProperty(group, name, "alive-id", 100, map[string]string{"status": "ok"})
	toDelete := legacyTestProperty(group, name, "deleted-id", 200, map[string]string{"status": "gone"})
	writeLegacyPropertyShard(t, shardPath, []legacyPropertyWrite{
		{property: alive},
		{property: toDelete},
		{property: toDelete, deleteTime: time.Now().UnixNano()},
	})

	database, err := OpenDB(context.Background(), Config{
		Location:         tmpPath,
		MetricsScopeName: "nidx03_property_upgrade_test",
		FlushInterval:    time.Second,
		Index:            IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err, "the native-only OpenDB path must open a directory the retired legacy writer left behind")
	defer func() { _ = database.Close() }()

	rows, queryErr := database.Query(context.Background(), &propertyv1.QueryRequest{Groups: []string{group}})
	require.NoError(t, queryErr)

	byID := make(map[string]QueriedProperty, len(rows))
	for _, row := range rows {
		byID[string(row.ID())] = row
	}

	require.Len(t, rows, 2, "both revisions (one later soft-deleted) must be present; delete is a tombstone, not a removal")

	aliveID := GetPropertyID(alive)
	aliveRow, found := byID[string(aliveID)]
	require.True(t, found, "the live property written by the legacy layout must be queryable through the native path")
	require.Equal(t, int64(0), aliveRow.DeleteTime(), "the live property must not be marked deleted")
	var decodedAlive propertyv1.Property
	require.NoError(t, protojson.Unmarshal(aliveRow.Source(), &decodedAlive))
	require.Equal(t, "alive-id", decodedAlive.GetId())
	require.Equal(t, "ok", decodedAlive.GetTags()[0].GetValue().GetStr().GetValue())

	deletedRowID := GetPropertyID(toDelete)
	deletedRow, found := byID[string(deletedRowID)]
	require.True(t, found, "the soft-deleted property must still be queryable (it is a tombstone, not absent)")
	require.Greater(t, deletedRow.DeleteTime(), int64(0), "the soft-deleted property's deleteTime must be set")
}

type legacyPropertyWrite struct {
	property   *propertyv1.Property
	deleteTime int64
}

func legacyTestProperty(group, name, id string, modRevision int64, tags map[string]string) *propertyv1.Property {
	property := &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: group, Name: name, ModRevision: modRevision},
		Id:       id,
	}
	for key, value := range tags {
		property.Tags = append(property.Tags, &modelv1.Tag{
			Key:   key,
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
		})
	}
	return property
}

// writeLegacyPropertyShard opens path with the retired bluge-backed store
// (inverted.NewStore, still in-tree for the NIDX-04 element migration tool)
// and writes each entry through UpdateSeriesBatch -- the same method and
// document shape shard.go's buildUpdateDocument + updateDocuments used
// before NIDX-03 removed the legacy writer switch -- then closes it so the
// directory left behind is exactly what an upgrade finds.
func writeLegacyPropertyShard(t *testing.T, shardPath string, writes []legacyPropertyWrite) {
	t.Helper()
	store, err := inverted.NewStore(inverted.StoreOpts{Path: shardPath, BatchWaitSec: 0})
	require.NoError(t, err)
	for _, w := range writes {
		id := GetPropertyID(w.property)
		doc, buildErr := legacyBuildUpdateDocument(id, w.property, w.deleteTime)
		require.NoError(t, buildErr)
		require.NoError(t, store.UpdateSeriesBatch(index.Batch{Documents: index.Documents{*doc}}))
	}
	require.NoError(t, store.Close())
}

// legacyBuildUpdateDocument mirrors shard.go's buildUpdateDocument (field
// names, index/store flags) without the shaValueField -- see this file's
// test doc comment for why that is a safe simplification for a query-path
// proof.
func legacyBuildUpdateDocument(id []byte, property *propertyv1.Property, deleteTime int64) (*index.Document, error) {
	pj, err := protojson.Marshal(property)
	if err != nil {
		return nil, err
	}
	sourceField := index.NewBytesField(sourceFieldKey, pj)
	sourceField.NoSort = true
	sourceField.Store = true
	entityField := index.NewBytesField(entityFieldKey, []byte(property.Id))
	entityField.Index = true
	groupField := index.NewBytesField(groupFieldKey, []byte(property.Metadata.Group))
	groupField.Index = true
	nameField := index.NewBytesField(nameFieldKey, []byte(property.Metadata.Name))
	nameField.Index = true

	doc := index.Document{
		EntityValues: id,
		Fields:       []index.Field{entityField, groupField, nameField, sourceField},
		Timestamp:    property.Metadata.ModRevision,
	}
	for i, tag := range property.Tags {
		tv, marshalErr := pbv1.MarshalTagValue(tag.Value)
		if marshalErr != nil {
			return nil, fmt.Errorf("tag %d: %w", i, marshalErr)
		}
		tagField := index.NewBytesField(index.FieldKey{IndexRuleID: uint32(convert.HashStr(tag.Key))}, tv)
		tagField.Index = tag.Key != unindexedSourceTag
		doc.Fields = append(doc.Fields, tagField)
	}

	if deleteTime > 0 {
		deleteField := index.NewBytesField(deletedFieldKey, convert.Int64ToBytes(deleteTime))
		deleteField.Store = true
		deleteField.NoSort = true
		doc.Fields = append(doc.Fields, deleteField)
	}
	return &doc, nil
}
