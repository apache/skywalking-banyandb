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

package lifecycle

import (
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// migrateStreamWithFileBasedAndProgress performs file-based stream migration
// with progress tracking. The visitor's Mark*Completed/Error calls keep
// Progress.Stream*Counts in lock-step with progress, so no pre-walk for
// counting is needed; segment suffixes returned by the single visit feed
// the subsequent expired-segment deletion.
func migrateStreamWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int,
) ([]string, error) {
	// Convert segment Interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	visitor := newStreamMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval,
	)
	defer visitor.Close()

	segmentSuffixes, err := stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// migrateMeasureWithFileBasedAndProgress performs file-based measure migration
// with progress tracking.
func migrateMeasureWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int,
) ([]string, error) {
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)
	targetStageInterval := getTargetStageInterval(group)

	visitor := newMeasureMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval,
	)
	defer visitor.Close()

	segmentSuffixes, err := measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// migrateTraceWithFileBasedAndProgress performs file-based trace migration
// with progress tracking.
func migrateTraceWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int,
) ([]string, error) {
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)
	targetStageInterval := getTargetStageInterval(group)

	visitor := newTraceMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval,
	)
	defer visitor.Close()

	segmentSuffixes, err := trace.VisitTracesInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}
