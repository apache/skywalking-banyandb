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
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// CreateTestShardForDump creates a test property shard for testing the dump tool.
// It takes a temporary path and a file system as input, generates test properties with various tag types,
// creates a shard, inserts properties, and returns the path to the created shard directory.
// Parameters:
//
//	tmpPath:    the base directory where the shard will be created.
//	fileSystem: the file system to use for writing the shard.
//
// Returns:
//
//	The path to the created shard directory and a cleanup function.
func CreateTestShardForDump(tmpPath string, fileSystem fs.FileSystem) (string, func()) {
	now := time.Now().UnixNano()

	// Create a database with a shard
	snapshotDir := tmpPath // Use same directory for snapshot
	db, err := openDB(context.Background(), tmpPath, 3*time.Second, time.Hour, 32, observability.BypassRegistry, fileSystem,
		true, snapshotDir, "@every 10m", time.Second*10, "* 2 * * *", nil, nil, nil)
	if err != nil {
		panic(err)
	}

	// Load shard 0
	shard, err := db.loadShard(context.Background(), 0)
	if err != nil {
		db.close()
		panic(err)
	}

	// Create test properties with various tag types
	properties := []*propertyv1.Property{
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group",
				Name:        "test-name",
				ModRevision: now,
			},
			Id: "test-id1",
			Tags: []*modelv1.Tag{
				{
					Key: "strTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "test-value"},
						},
					},
				},
				{
					Key: "intTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Int{
							Int: &modelv1.Int{Value: 100},
						},
					},
				},
			},
		},
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group",
				Name:        "test-name",
				ModRevision: now + 1000,
			},
			Id: "test-id2",
			Tags: []*modelv1.Tag{
				{
					Key: "strArrTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_StrArray{
							StrArray: &modelv1.StrArray{
								Value: []string{"value1", "value2"},
							},
						},
					},
				},
				{
					Key: "intArrTag",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_IntArray{
							IntArray: &modelv1.IntArray{
								Value: []int64{25, 30},
							},
						},
					},
				},
			},
		},
		{
			Metadata: &commonv1.Metadata{
				Group:       "test-group2",
				Name:        "test-name2",
				ModRevision: now + 2000,
			},
			Id: "test-id3",
			Tags: []*modelv1.Tag{
				{
					Key: "strTag1",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "tag1"},
						},
					},
				},
				{
					Key: "strTag2",
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{
							Str: &modelv1.Str{Value: "tag2"},
						},
					},
				},
			},
		},
	}

	// Insert properties
	for _, p := range properties {
		if err := shard.update(GetPropertyID(p), p); err != nil {
			db.close()
			panic(err)
		}
	}

	// Wait a bit for the data to be persisted
	time.Sleep(100 * time.Millisecond)

	// Get shard path before closing the database
	shardPath := shard.location

	// Close the database to release the lock on the directory
	// This allows the dump tool to open the same directory
	if err := db.close(); err != nil {
		panic(err)
	}

	// Wait a bit more to ensure all file handles are released
	time.Sleep(50 * time.Millisecond)

	cleanup := func() {
		// Cleanup is handled by the caller's test.Space cleanup
		// Database is already closed, so nothing to do here
	}

	return shardPath, cleanup
}
