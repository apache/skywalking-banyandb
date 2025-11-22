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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// Visitor defines the interface for visiting trace components.
type Visitor interface {
	// VisitSeries visits the series index directory for a segment.
	VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error
	// VisitShard visits the shard directory for a segment.
	VisitShard(segmentTR *timestamp.TimeRange, shardID common.ShardID, segmentPath string) error
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
	return tv.visitor.VisitShard(segmentTR, shardID, shardPath)
}

// VisitTracesInTimeRange traverses trace parts within the specified time range
// and calls the visitor methods for parts and sidx directories.
// This function works directly with the filesystem without requiring a database instance.
// Returns a list of segment suffixes that were visited.
func VisitTracesInTimeRange(tsdbRootPath string, timeRange timestamp.TimeRange, visitor Visitor, intervalRule storage.IntervalRule) ([]string, error) {
	adapter := &traceSegmentVisitor{visitor: visitor}
	return storage.VisitSegmentsInTimeRange(tsdbRootPath, timeRange, adapter, intervalRule)
}
