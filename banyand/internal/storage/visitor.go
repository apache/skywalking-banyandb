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

package storage

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// SegmentVisitor defines the interface for visiting segment components.
type SegmentVisitor interface {
	// VisitSeries visits the series index directory for a segment.
	VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error
	// VisitShard visits a shard directory within a segment.
	VisitShard(segmentTR *timestamp.TimeRange, shardID common.ShardID, shardPath string) error
}

// VisitSegmentsInTimeRange traverses segments within the specified time range
// and calls the visitor methods for series index and shard directories.
// This function works directly with the filesystem without requiring a database instance.
func VisitSegmentsInTimeRange(tsdbRootPath string, timeRange timestamp.TimeRange, visitor SegmentVisitor, intervalRule IntervalRule) error {
	// Parse segment directories in the root path
	var segmentPaths []segmentInfo
	err := walkDir(tsdbRootPath, segPathPrefix, func(suffix string) error {
		startTime, err := parseSegmentTime(suffix, intervalRule.Unit)
		if err != nil {
			return err
		}

		// Calculate end time based on interval rule
		endTime := intervalRule.NextTime(startTime)
		segTR := timestamp.NewSectionTimeRange(startTime, endTime)

		// Check if segment overlaps with the requested time range
		if !segTR.Overlapping(timeRange) {
			return nil // Skip segments outside the time range
		}

		segmentPath := filepath.Join(tsdbRootPath, fmt.Sprintf(segTemplate, suffix))
		segmentPaths = append(segmentPaths, segmentInfo{
			path:      segmentPath,
			suffix:    suffix,
			timeRange: segTR,
		})
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to walk segment directories")
	}

	// Visit each matching segment
	for _, segInfo := range segmentPaths {
		// Collect shard IDs for this segment
		shardIDs, err := collectSegmentShardIDs(segInfo.path)
		if err != nil {
			return errors.Wrapf(err, "failed to collect shard IDs for segment %s", segInfo.suffix)
		}

		// Visit series index directory
		seriesIndexPath := filepath.Join(segInfo.path, seriesIndexDirName)
		if err := visitor.VisitSeries(&segInfo.timeRange, seriesIndexPath, shardIDs); err != nil {
			return errors.Wrapf(err, "failed to visit series index for segment (suffix: %s, path: %s, timeRange: %v)", segInfo.suffix, segInfo.path, segInfo.timeRange)
		}

		// Visit shard directories
		if err := visitSegmentShards(segInfo.path, &segInfo.timeRange, visitor); err != nil {
			return errors.Wrapf(err, "failed to visit shards for segment (suffix: %s, path: %s, timeRange: %v)", segInfo.suffix, segInfo.path, segInfo.timeRange)
		}
	}

	return nil
}

// segmentInfo holds information about a segment directory.
type segmentInfo struct {
	path      string
	suffix    string
	timeRange timestamp.TimeRange
}

// collectSegmentShardIDs collects all shard IDs within a segment.
func collectSegmentShardIDs(segmentPath string) ([]common.ShardID, error) {
	var shardIDs []common.ShardID
	err := walkDir(segmentPath, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return errors.Wrapf(err, "invalid shard suffix: %s", suffix)
		}
		shardIDs = append(shardIDs, common.ShardID(shardID))
		return nil
	})
	return shardIDs, err
}

// visitSegmentShards traverses shard directories within a segment.
func visitSegmentShards(segmentPath string, segmentTR *timestamp.TimeRange, visitor SegmentVisitor) error {
	return walkDir(segmentPath, shardPathPrefix, func(suffix string) error {
		shardID, err := strconv.Atoi(suffix)
		if err != nil {
			return errors.Wrapf(err, "invalid shard suffix: %s", suffix)
		}

		shardPath := filepath.Join(segmentPath, fmt.Sprintf(shardTemplate, shardID))
		return visitor.VisitShard(segmentTR, common.ShardID(shardID), shardPath)
	})
}
