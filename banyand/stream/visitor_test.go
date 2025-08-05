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

package stream

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestVisitor tracks visited components for testing.
type TestVisitor struct {
	visitedSeries []string
	visitedParts  []struct {
		path    string
		shardID common.ShardID
	}
	visitedElementIndex []struct {
		path    string
		shardID common.ShardID
	}
}

// VisitSeries records the visited series path.
func (tv *TestVisitor) VisitSeries(_ *timestamp.TimeRange, seriesIndexPath string, _ []common.ShardID) error {
	tv.visitedSeries = append(tv.visitedSeries, seriesIndexPath)
	return nil
}

// VisitPart records the visited part path and shard ID.
func (tv *TestVisitor) VisitPart(_ *timestamp.TimeRange, shardID common.ShardID, partPath string) error {
	tv.visitedParts = append(tv.visitedParts, struct {
		path    string
		shardID common.ShardID
	}{partPath, shardID})
	return nil
}

// VisitElementIndex records the visited element index path and shard ID.
func (tv *TestVisitor) VisitElementIndex(_ *timestamp.TimeRange, shardID common.ShardID, indexPath string) error {
	tv.visitedElementIndex = append(tv.visitedElementIndex, struct {
		path    string
		shardID common.ShardID
	}{indexPath, shardID})
	return nil
}

func TestVisitStreamsInTimeRange(t *testing.T) {
	// Create a temporary directory structure
	tmpDir, err := os.MkdirTemp("", "stream_visitor_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create segment directory structure
	now := time.Now()
	segmentTime := now.Format("2006010215") // hourly format
	segmentDir := filepath.Join(tmpDir, "seg-"+segmentTime)
	require.NoError(t, os.MkdirAll(segmentDir, 0o755))

	// Create series index directory
	seriesDir := filepath.Join(segmentDir, "sidx")
	require.NoError(t, os.MkdirAll(seriesDir, 0o755))

	// Create shard directory
	shardDir := filepath.Join(segmentDir, "shard-0")
	require.NoError(t, os.MkdirAll(shardDir, 0o755))

	// Create part directory within shard (16-character hex)
	partDir := filepath.Join(shardDir, "000000000000001a")
	require.NoError(t, os.MkdirAll(partDir, 0o755))

	// Create element index directory within shard
	elemIndexDir := filepath.Join(shardDir, elementIndexFilename)
	require.NoError(t, os.MkdirAll(elemIndexDir, 0o755))

	// Test the visitor
	visitor := &TestVisitor{}
	timeRange := timestamp.TimeRange{
		Start: now.Add(-time.Hour),
		End:   now.Add(time.Hour),
	}
	intervalRule := storage.IntervalRule{Unit: storage.HOUR, Num: 1}

	err = VisitStreamsInTimeRange(tmpDir, timeRange, visitor, intervalRule)
	require.NoError(t, err)

	// Verify visits occurred
	assert.Len(t, visitor.visitedSeries, 1)
	assert.Contains(t, visitor.visitedSeries[0], "sidx")

	assert.Len(t, visitor.visitedParts, 1)
	assert.Equal(t, common.ShardID(0), visitor.visitedParts[0].shardID)
	assert.Contains(t, visitor.visitedParts[0].path, "000000000000001a")

	assert.Len(t, visitor.visitedElementIndex, 1)
	assert.Equal(t, common.ShardID(0), visitor.visitedElementIndex[0].shardID)
	assert.Contains(t, visitor.visitedElementIndex[0].path, elementIndexFilename)
}
