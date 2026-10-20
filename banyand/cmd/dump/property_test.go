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
	"encoding/csv"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDumpPropertyShardFormat tests that the dump tool's native read-only walk
// (walkPropertyRows / decodePropertyRow, banyand/cmd/dump/property.go) can
// parse property shard data written by the real native property store. This
// creates a real shard using the property module's own write path, then
// verifies the dump tool -- not a reimplementation of it -- decodes every row
// correctly.
func TestDumpPropertyShardFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	// Use property package to create a real shard using actual operations
	shardPath, cleanup := createTestPropertyShardForDump(tmpPath)
	defer cleanup()

	generation, err := native.OpenReadOnlyGeneration(shardPath)
	require.NoError(t, err, "should be able to open shard created by property module")
	defer func() { _ = generation.Close() }()

	var allResults []propertyRowData
	walkErr := walkPropertyRows(context.Background(), generation, func(row propertyRowData) error {
		allResults = append(allResults, row)
		return nil
	})
	require.NoError(t, walkErr, "should be able to walk the shard's live property rows")

	assert.Greater(t, len(allResults), 0, "should have at least one property")
	t.Logf("Found %d properties", len(allResults))

	// Verify we can parse all properties
	for i, result := range allResults {
		prop := result.property
		require.NotNil(t, prop, "property %d should have a decoded source", i)

		t.Logf("Property %d: Group=%s, Name=%s, EntityID=%s, ModRevision=%d, Tags=%d",
			i, prop.Metadata.Group, prop.Metadata.Name, prop.Id, prop.Metadata.ModRevision, len(prop.Tags))

		// Verify property has metadata
		assert.NotNil(t, prop.Metadata, "property %d should have metadata", i)
		assert.NotEmpty(t, prop.Metadata.Group, "property %d should have group", i)
		assert.NotEmpty(t, prop.Metadata.Name, "property %d should have name", i)
		assert.NotEmpty(t, prop.Id, "property %d should have entity ID", i)

		// Verify identity and timestamp were recovered from the native _id /
		// _timestamp fields.
		assert.NotEmpty(t, result.id, "property %d should have a decoded identifier", i)
		assert.Greater(t, result.timestamp, int64(0), "property %d should have valid timestamp", i)

		// Verify tags if present
		for _, tag := range prop.Tags {
			assert.NotEmpty(t, tag.Key, "property %d tag should have key", i)
			assert.NotNil(t, tag.Value, "property %d tag %s should have value", i, tag.Key)
		}
	}

	t.Logf("Successfully parsed %d properties from shard", len(allResults))
}

// createTestPropertyShardForDump creates a test property shard for testing the dump tool.
// It uses the property package's CreateTestShardForDump function.
func createTestPropertyShardForDump(tmpPath string) (string, func()) {
	fileSystem := fs.NewLocalFileSystem()
	return db.CreateTestShardForDump(tmpPath, fileSystem)
}

const dumpTestGroup = "dump-test-group"

// createDeletedPropertyShard opens a fresh property database, writes one
// live property and one property it then Deletes (a soft-delete tombstone:
// the document survives with _deleted set, per banyand/property/db/shard.go
// buildUpdateDocument/Delete), and returns the on-disk shard-0 path plus a
// cleanup. R5's own ask: decodePropertyRow's _deleted handling and the CSV/
// criteria paths had no test before this.
func createDeletedPropertyShard(t *testing.T) (shardPath string, aliveID, deletedID []byte) {
	t.Helper()
	tmpPath, cleanupSpace := test.Space(require.New(t))
	t.Cleanup(cleanupSpace)

	database, err := db.OpenDB(context.Background(), db.Config{
		Location:         tmpPath,
		MetricsScopeName: "dump_property_deleted_test",
		FlushInterval:    time.Second,
		Index:            db.IndexConfig{WaitForPersistence: true},
	}, observability.BypassRegistry, fs.NewLocalFileSystem())
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })

	alive := newDumpTestProperty("alive-id", 1, "status", "ok")
	require.NoError(t, database.Update(context.Background(), 0, db.GetPropertyID(alive), alive))
	toDelete := newDumpTestProperty("deleted-id", 1, "status", "gone")
	require.NoError(t, database.Update(context.Background(), 0, db.GetPropertyID(toDelete), toDelete))
	require.NoError(t, database.Delete(context.Background(), [][]byte{db.GetPropertyID(toDelete)}, time.Now()))

	return filepath.Join(tmpPath, dumpTestGroup, "shard-0"), db.GetPropertyID(alive), db.GetPropertyID(toDelete)
}

func newDumpTestProperty(id string, modRevision int64, tagKey, tagValue string) *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: dumpTestGroup, Name: "dump-test-name", ModRevision: modRevision},
		Id:       id,
		Tags: []*modelv1.Tag{
			{Key: tagKey, Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: tagValue}}}},
		},
	}
}

// TestDumpPropertyDecodesDeletedFlag is R5's _deleted coverage:
// decodePropertyRow must surface a soft-deleted property's deleteTime
// (non-zero), and a live property's must stay zero.
func TestDumpPropertyDecodesDeletedFlag(t *testing.T) {
	shardPath, aliveID, deletedID := createDeletedPropertyShard(t)

	generation, err := native.OpenReadOnlyGeneration(shardPath)
	require.NoError(t, err)
	defer func() { _ = generation.Close() }()

	rows := map[string]propertyRowData{}
	require.NoError(t, walkPropertyRows(context.Background(), generation, func(row propertyRowData) error {
		rows[string(row.id)] = row
		return nil
	}))

	alive, found := rows[string(aliveID)]
	require.True(t, found, "the live property must be present")
	assert.Equal(t, int64(0), alive.deleteTime, "a live property's deleteTime must be zero")

	deleted, found := rows[string(deletedID)]
	require.True(t, found, "the soft-deleted property must still be walked (it is a tombstone row, not absent)")
	assert.Greater(t, deleted.deleteTime, int64(0), "a soft-deleted property's deleteTime must be set")
}

// TestDumpPropertyCSVOutput is R5's CSV coverage: writePropertyRowAsCSV
// (the function newPropertyDumpContext's --csv path calls per row) must
// produce a row whose ID/Group/Name/EntityID/Deleted columns match the
// decoded propertyRowData, through the real csv.Writer encoding (quoting,
// field order) rather than a hand-built string comparison.
func TestDumpPropertyCSVOutput(t *testing.T) {
	shardPath, aliveID, deletedID := createDeletedPropertyShard(t)

	generation, err := native.OpenReadOnlyGeneration(shardPath)
	require.NoError(t, err)
	defer func() { _ = generation.Close() }()

	rows := map[string]propertyRowData{}
	require.NoError(t, walkPropertyRows(context.Background(), generation, func(row propertyRowData) error {
		rows[string(row.id)] = row
		return nil
	}))

	var buf strings.Builder
	writer := csv.NewWriter(&buf)
	require.NoError(t, writePropertyRowAsCSV(writer, rows[string(aliveID)], nil))
	require.NoError(t, writePropertyRowAsCSV(writer, rows[string(deletedID)], nil))
	writer.Flush()
	require.NoError(t, writer.Error())

	reader := csv.NewReader(strings.NewReader(buf.String()))
	records, readErr := reader.ReadAll()
	require.NoError(t, readErr)
	require.Len(t, records, 2)

	// Columns: ID, Timestamp, SeriesID, Series, Group, Name, EntityID, Deleted, ModRevision.
	assert.Equal(t, string(aliveID), records[0][0])
	assert.Equal(t, dumpTestGroup, records[0][4])
	assert.Equal(t, "false", records[0][7], "the live property's CSV row must report Deleted=false")

	assert.Equal(t, string(deletedID), records[1][0])
	assert.Equal(t, "true", records[1][7], "the soft-deleted property's CSV row must report Deleted=true")
}

// TestDumpPropertyCriteriaFilter is R5's criteria-filter coverage:
// propertyDumpContext.shouldSkip, built from the same --criteria JSON the
// CLI flag accepts (parsePropertyCriteriaJSON + logical.BuildSimpleTagFilter,
// newPropertyDumpContext's own construction path), must keep a property
// whose tag matches and skip one that does not.
func TestDumpPropertyCriteriaFilter(t *testing.T) {
	shardPath, aliveID, deletedID := createDeletedPropertyShard(t)

	ctx, err := newPropertyDumpContext(propertyDumpOptions{
		shardPath:    shardPath,
		criteriaJSON: `{"condition":{"name":"status","op":"BINARY_OP_EQ","value":{"str":{"value":"ok"}}}}`,
	})
	require.NoError(t, err)
	defer ctx.close()
	require.NotNil(t, ctx.tagFilter, "a --criteria JSON string must build a non-nil tag filter")

	var kept []string
	require.NoError(t, walkPropertyRows(context.Background(), ctx.generation, func(row propertyRowData) error {
		if !ctx.shouldSkip(row) {
			kept = append(kept, string(row.id))
		}
		return nil
	}))

	assert.Contains(t, kept, string(aliveID), "the property matching status=ok must not be skipped")
	assert.NotContains(t, kept, string(deletedID), "the property with status=gone must be skipped by the status=ok criteria")
}
