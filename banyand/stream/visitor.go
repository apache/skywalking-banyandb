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
	"path/filepath"
	"strconv"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Visitor defines the interface for visiting stream components.
type Visitor interface {
	// VisitSeries visits the series index directory for a segment.
	VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error
	// VisitPart visits a part directory within a shard.
	VisitPart(segmentTR *timestamp.TimeRange, shardID common.ShardID, partPath string) error
	// VisitElementIndex visits the element index directory within a shard.
	VisitElementIndex(segmentTR *timestamp.TimeRange, shardID common.ShardID, indexPath string) error
}

// streamSegmentVisitor adapts Visitor to work with storage.SegmentVisitor.
type streamSegmentVisitor struct {
	visitor Visitor
}

// VisitSeries implements storage.SegmentVisitor.
func (sv *streamSegmentVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	return sv.visitor.VisitSeries(segmentTR, seriesIndexPath, shardIDs)
}

// VisitShard implements storage.SegmentVisitor.
func (sv *streamSegmentVisitor) VisitShard(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	// Visit parts within the shard
	if err := sv.visitShardParts(segmentTR, shardID, shardPath); err != nil {
		return err
	}

	// Visit element index within the shard
	return sv.visitShardElementIndex(segmentTR, shardID, shardPath)
}

// visitShardParts visits all part directories within a shard.
func (sv *streamSegmentVisitor) visitShardParts(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
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
		if err := sv.visitor.VisitPart(segmentTR, shardID, partPath); err != nil {
			return err
		}
	}

	return nil
}

// visitShardElementIndex visits the element index directory within a shard.
func (sv *streamSegmentVisitor) visitShardElementIndex(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error {
	indexPath := filepath.Join(shardPath, elementIndexFilename)
	return sv.visitor.VisitElementIndex(segmentTR, shardID, indexPath)
}

// VisitStreamsInTimeRange traverses stream segments within the specified time range
// and calls the visitor methods for series index, parts, and element indexes.
// This function works directly with the filesystem without requiring a database instance.
func VisitStreamsInTimeRange(tsdbRootPath string, timeRange timestamp.TimeRange, visitor Visitor, intervalRule storage.IntervalRule) error {
	adapter := &streamSegmentVisitor{visitor: visitor}
	return storage.VisitSegmentsInTimeRange(tsdbRootPath, timeRange, adapter, intervalRule)
}
