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

package trace

import (
	"path/filepath"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Visitor defines the interface for visiting trace components.
type Visitor interface {
	// VisitSeries visits the series index directory for a segment.
	VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error
	// VisitPart visits a part directory for a shard.
	VisitPart(segmentTR *timestamp.TimeRange, shardID common.ShardID, partPath string) error
	// VisitElementIndex visits the element index(sidx) directory within a shard.
	VisitElementIndex(segmentTR *timestamp.TimeRange, shardID common.ShardID, indexPath string) error
}

// traceSegmentVisitor adapts Visitor to work with storage.SegmentVisitor.
type traceSegmentVisitor struct {
	visitor Visitor
}

// VisitSeries implements storage.SegmentVisitor.
func (tv *traceSegmentVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	return tv.visitor.VisitSeries(segmentTR, seriesIndexPath, shardIDs)
}

// VisitShard implements storage.SegmentVisitor.
func (tv *traceSegmentVisitor) VisitShard(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	// Visit parts within the shard
	if err := tv.visitShardParts(segmentTR, shardID, shardPath); err != nil {
		return err
	}
	lfs := fs.NewLocalFileSystem()
	entries := lfs.ReadDir(shardPath)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Check if this is a part directory (16-character hex string)
		if len(name) != 16 {
			continue // Skip non-part entries
		}

		// Validate it's a valid hex string (part ID)
		if _, err := strconv.ParseUint(name, 16, 64); err != nil {
			continue // Skip invalid part entries
		}

		partPath := filepath.Join(shardPath, name)
		if err := tv.visitor.VisitPart(segmentTR, shardID, partPath); err != nil {
			return err
		}
	}

	return nil
}

func (tv *traceSegmentVisitor) visitShardParts(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	lfs := fs.NewLocalFileSystem()
	entries := lfs.ReadDir(shardPath)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Check if this is a part directory (16-character hex string)
		if len(name) != 16 {
			continue // Skip non-part entries
		}

		// Validate it's a valid hex string (part ID)
		if _, err := strconv.ParseUint(name, 16, 64); err != nil {
			continue // Skip invalid part entries
		}

		partPath := filepath.Join(shardPath, name)
		if err := tv.visitor.VisitPart(segmentTR, shardID, partPath); err != nil {
			return err
		}
	}

	return tv.visitShardElementIndex(segmentTR, shardID, shardPath)
}

// visitShardElementIndex visits the element index directory within a shard.
func (tv *traceSegmentVisitor) visitShardElementIndex(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	indexPath := filepath.Join(shardPath, sidxDirName)
	return tv.visitor.VisitElementIndex(segmentTR, shardID, indexPath)
}

// VisitTracesInTimeRange traverses trace parts within the specified time range
// and calls the visitor methods for parts and sidx directories.
// This function works directly with the filesystem without requiring a database instance.
// Returns a list of segment suffixes that were visited.
func VisitTracesInTimeRange(tsdbRootPath string, timeRange timestamp.TimeRange, visitor Visitor, intervalRule storage.IntervalRule) ([]string, error) {
	adapter := &traceSegmentVisitor{visitor: visitor}
	return storage.VisitSegmentsInTimeRange(tsdbRootPath, timeRange, adapter, intervalRule)
}
