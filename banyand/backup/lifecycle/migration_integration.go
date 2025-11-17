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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// migrateStreamWithFileBasedAndProgress performs file-based stream migration with progress tracking.
func migrateStreamWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int,
) ([]string, error) {
	// Convert segment Interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	// Count total parts before starting migration
	totalParts, segmentSuffixes, err := countStreamParts(tsdbRootPath, timeRange, segmentIntervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count stream parts, proceeding without part count")
	} else {
		logger.Info().Int("total_parts", totalParts).Strs("segment_suffixes", segmentSuffixes).
			Msg("counted stream parts for progres tracking")
	}

	// Create file-based migration visitor with progress tracking and target stage interval
	visitor := newStreamMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval,
	)
	defer visitor.Close()

	// Set the total part count for progress tracking
	if totalParts > 0 {
		visitor.SetStreamPartCount(totalParts)
	}

	// Use the existing VisitStreamsInTimeRange function with our file-based visitor
	_, err = stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// countStreamParts counts the total number of parts in the given time range.
func countStreamParts(tsdbRootPath string, timeRange timestamp.TimeRange, segmentInterval storage.IntervalRule) (int, []string, error) {
	// Create a simple visitor to count parts
	partCounter := &partCountVisitor{}

	// Use the existing VisitStreamsInTimeRange function to count parts
	segmentSuffixes, err := stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, partCounter, segmentInterval)
	if err != nil {
		return 0, nil, err
	}

	return partCounter.partCount, segmentSuffixes, nil
}

// partCountVisitor is a simple visitor that counts parts.
type partCountVisitor struct {
	partCount int
}

// VisitSeries implements stream.Visitor.
func (pcv *partCountVisitor) VisitSeries(_ *timestamp.TimeRange, _ string, _ []common.ShardID) error {
	return nil
}

// VisitPart implements stream.Visitor.
func (pcv *partCountVisitor) VisitPart(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.partCount++
	return nil
}

// VisitElementIndex implements stream.Visitor.
func (pcv *partCountVisitor) VisitElementIndex(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	return nil
}

// migrateMeasureWithFileBasedAndProgress performs file-based measure migration with progress tracking.
func migrateMeasureWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int,
) ([]string, error) {
	// Convert segment interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	// Count total parts before starting migration
	totalParts, segmentSuffixes, err := countMeasureParts(tsdbRootPath, timeRange, segmentIntervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count measure parts, proceeding without part count")
	} else {
		logger.Info().Int("total_parts", totalParts).Strs("segment_suffixes", segmentSuffixes).
			Msg("counted measure parts for progress tracking")
	}

	// Create file-based migration visitor with progress tracking and target stage interval
	visitor := newMeasureMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval,
	)
	defer visitor.Close()

	// Set the total part count for progress tracking
	if totalParts > 0 {
		visitor.SetMeasurePartCount(totalParts)
	}

	// Use the existing VisitMeasuresInTimeRange function with our file-based visitor
	_, err = measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// countMeasureParts counts the total number of parts in the given time range.
func countMeasureParts(tsdbRootPath string, timeRange timestamp.TimeRange, segmentInterval storage.IntervalRule) (int, []string, error) {
	// Create a simple visitor to count parts
	partCounter := &measurePartCountVisitor{}

	// Use the existing VisitMeasuresInTimeRange function to count parts
	segmentSuffixes, err := measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, partCounter, segmentInterval)
	if err != nil {
		return 0, nil, err
	}

	return partCounter.partCount, segmentSuffixes, nil
}

// measurePartCountVisitor is a simple visitor that counts measure parts.
type measurePartCountVisitor struct {
	partCount int
}

// VisitSeries implements measure.Visitor.
func (pcv *measurePartCountVisitor) VisitSeries(_ *timestamp.TimeRange, _ string, _ []common.ShardID) error {
	return nil
}

// VisitPart implements measure.Visitor.
func (pcv *measurePartCountVisitor) VisitPart(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.partCount++
	return nil
}
