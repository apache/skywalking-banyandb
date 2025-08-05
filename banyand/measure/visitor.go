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

package measure

import (
	"path/filepath"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Visitor defines the interface for visiting measure components.
type Visitor interface {
	// VisitSeries visits the series index directory for a segment.
	VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error
	// VisitPart visits a part directory within a shard.
	VisitPart(segmentTR *timestamp.TimeRange, shardID common.ShardID, partPath string) error
}

// measureSegmentVisitor adapts Visitor to work with storage.SegmentVisitor.
type measureSegmentVisitor struct {
	visitor Visitor
}

// VisitSeries implements Visitor.
func (mv *measureSegmentVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	return mv.visitor.VisitSeries(segmentTR, seriesIndexPath, shardIDs)
}

// VisitShard implements storage.SegmentVisitor.
func (mv *measureSegmentVisitor) VisitShard(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	// Visit parts within the shard
	return mv.visitShardParts(segmentTR, shardID, shardPath)
}

// visitShardParts visits all part directories within a shard.
func (mv *measureSegmentVisitor) visitShardParts(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
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
		if err := mv.visitor.VisitPart(segmentTR, shardID, partPath); err != nil {
			return err
		}
	}

	return nil
}

// VisitMeasuresInTimeRange traverses measure segments within the specified time range
// and calls the visitor methods for parts within shards.
// This function works directly with the filesystem without requiring a database instance.
func VisitMeasuresInTimeRange(tsdbRootPath string, timeRange timestamp.TimeRange, visitor Visitor, intervalRule storage.IntervalRule) error {
	adapter := &measureSegmentVisitor{visitor: visitor}
	return storage.VisitSegmentsInTimeRange(tsdbRootPath, timeRange, adapter, intervalRule)
}
