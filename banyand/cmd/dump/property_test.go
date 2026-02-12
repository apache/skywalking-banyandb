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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/property/db"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestDumpPropertyShardFormat tests that the dump tool can parse property shard data.
// This test creates a real shard using the property module's operations,
// then verifies the dump tool can correctly parse it.
func TestDumpPropertyShardFormat(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	// Use property package to create a real shard using actual operations
	shardPath, cleanup := createTestPropertyShardForDump(tmpPath)
	defer cleanup()

	// Open the shard using dump tool functions
	l := logger.GetLogger("dump-property")
	store, err := inverted.NewStore(inverted.StoreOpts{
		Path:   shardPath,
		Logger: l,
	})
	require.NoError(t, err, "should be able to open shard created by property module")
	defer store.Close()

	// Use SeriesIterator to iterate through all series and query properties
	searchCtx := context.Background()
	iter, err := store.SeriesIterator(searchCtx)
	require.NoError(t, err, "should be able to create series iterator")
	defer iter.Close()

	projection := []index.FieldKey{
		{TagName: "_id"},
		{TagName: "_timestamp"},
		{TagName: "_source"},
		{TagName: "_deleted"},
	}

	var allResults []index.SeriesDocument
	seriesCount := 0

	// Iterate through all series
	for iter.Next() {
		series := iter.Val()
		if len(series.EntityValues) == 0 {
			continue
		}

		seriesCount++
		// Build query for this specific series
		seriesMatchers := []index.SeriesMatcher{
			{
				Match: series.EntityValues,
				Type:  index.SeriesMatcherTypeExact,
			},
		}

		iq, err := store.BuildQuery(seriesMatchers, nil, nil)
		require.NoError(t, err, "should be able to build query for series")

		// Search properties for this series
		results, err := store.Search(searchCtx, projection, iq, 10000)
		require.NoError(t, err, "should be able to search properties for series")

		allResults = append(allResults, results...)
	}

	assert.Greater(t, len(allResults), 0, "should have at least one property")
	t.Logf("Found %d properties across %d series", len(allResults), seriesCount)

	// Verify we can parse all properties
	for i, result := range allResults {
		sourceBytes := result.Fields["_source"]
		require.NotNil(t, sourceBytes, "property %d should have source field", i)

		var prop propertyv1.Property
		err := protojson.Unmarshal(sourceBytes, &prop)
		require.NoError(t, err, "should be able to unmarshal property %d", i)

		t.Logf("Property %d: Group=%s, Name=%s, EntityID=%s, ModRevision=%d, Tags=%d",
			i, prop.Metadata.Group, prop.Metadata.Name, prop.Id, prop.Metadata.ModRevision, len(prop.Tags))

		// Verify property has metadata
		assert.NotNil(t, prop.Metadata, "property %d should have metadata", i)
		assert.NotEmpty(t, prop.Metadata.Group, "property %d should have group", i)
		assert.NotEmpty(t, prop.Metadata.Name, "property %d should have name", i)
		assert.NotEmpty(t, prop.Id, "property %d should have entity ID", i)

		// Verify timestamp
		assert.Greater(t, result.Timestamp, int64(0), "property %d should have valid timestamp", i)

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
