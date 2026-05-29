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
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// migrateStreamWithFileBasedAndProgress performs file-based stream migration with progress tracking.
func migrateStreamWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int, md metadata.Repo,
) ([]string, error) {
	// Convert segment Interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	// Pre-walk source items so each resource has a fixed planned denominator.
	counter, segmentSuffixes, err := countStreamParts(tsdbRootPath, timeRange, segmentIntervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count stream source items, proceeding without planned counts")
	} else {
		logger.Info().Int("total_parts", counter.partCount).
			Int("total_series_files", counter.seriesFileCount).
			Int("total_element_index_visits", counter.elementIndexCount).
			Strs("segment_suffixes", segmentSuffixes).
			Msg("counted stream source items for progress tracking")
	}

	// Create file-based migration visitor with progress tracking and target stage interval
	visitor := newStreamMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval, md,
	)
	defer visitor.Close()

	// Set the planned source-item counts for progress tracking.
	if counter != nil {
		visitor.SetStreamPartCount(counter.partCount)
		visitor.SetStreamSeriesCount(counter.seriesFileCount)
		visitor.SetStreamElementIndexCount(counter.elementIndexCount)
	}

	// Use the existing VisitStreamsInTimeRange function with our file-based visitor
	_, err = stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// countStreamParts counts the total number of source items in the given time range.
func countStreamParts(tsdbRootPath string, timeRange timestamp.TimeRange, segmentInterval storage.IntervalRule) (*partCountVisitor, []string, error) {
	// Create a simple visitor to count source items
	partCounter := &partCountVisitor{lfs: fs.NewLocalFileSystem()}

	// Use the existing VisitStreamsInTimeRange function to count parts
	segmentSuffixes, err := stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, partCounter, segmentInterval)
	if err != nil {
		return nil, nil, err
	}

	return partCounter, segmentSuffixes, nil
}

// partCountVisitor is a simple visitor that counts source parts, series files and element-index visits.
type partCountVisitor struct {
	lfs               fs.FileSystem
	partCount         int
	seriesFileCount   int
	elementIndexCount int
}

// VisitSeries implements stream.Visitor; counts .seg files under the source series index directory.
func (pcv *partCountVisitor) VisitSeries(_ *timestamp.TimeRange, seriesIndexPath string, _ []common.ShardID) error {
	entries := pcv.lfs.ReadDir(seriesIndexPath)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			pcv.seriesFileCount++
		}
	}
	return nil
}

// VisitPart implements stream.Visitor.
func (pcv *partCountVisitor) VisitPart(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.partCount++
	return nil
}

// VisitElementIndex implements stream.Visitor.
func (pcv *partCountVisitor) VisitElementIndex(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.elementIndexCount++
	return nil
}

// migrateMeasureWithFileBasedAndProgress performs file-based measure migration with progress tracking.
func migrateMeasureWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int, md metadata.Repo,
) ([]string, error) {
	// Convert segment interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	// Pre-walk source items so each resource has a fixed planned denominator.
	counter, segmentSuffixes, err := countMeasureParts(tsdbRootPath, timeRange, segmentIntervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count measure source items, proceeding without planned counts")
	} else {
		logger.Info().Int("total_parts", counter.partCount).
			Int("total_series_files", counter.seriesFileCount).
			Strs("segment_suffixes", segmentSuffixes).
			Msg("counted measure source items for progress tracking")
	}

	// Create file-based migration visitor with progress tracking and target stage interval
	visitor := newMeasureMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval, md,
	)
	defer visitor.Close()

	// Set the planned source-item counts for progress tracking.
	if counter != nil {
		visitor.SetMeasurePartCount(counter.partCount)
		visitor.SetMeasureSeriesCount(counter.seriesFileCount)
	}

	// Use the existing VisitMeasuresInTimeRange function with our file-based visitor
	_, err = measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// countMeasureParts counts the total number of source items in the given time range.
func countMeasureParts(tsdbRootPath string, timeRange timestamp.TimeRange, segmentInterval storage.IntervalRule) (*measurePartCountVisitor, []string, error) {
	// Create a simple visitor to count source items
	partCounter := &measurePartCountVisitor{lfs: fs.NewLocalFileSystem()}

	// Use the existing VisitMeasuresInTimeRange function to count parts
	segmentSuffixes, err := measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, partCounter, segmentInterval)
	if err != nil {
		return nil, nil, err
	}

	return partCounter, segmentSuffixes, nil
}

// measurePartCountVisitor is a simple visitor that counts measure source parts and series files.
type measurePartCountVisitor struct {
	lfs             fs.FileSystem
	partCount       int
	seriesFileCount int
}

// VisitSeries implements measure.Visitor; counts .seg files under the source series index directory.
func (pcv *measurePartCountVisitor) VisitSeries(_ *timestamp.TimeRange, seriesIndexPath string, _ []common.ShardID) error {
	entries := pcv.lfs.ReadDir(seriesIndexPath)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			pcv.seriesFileCount++
		}
	}
	return nil
}

// VisitPart implements measure.Visitor.
func (pcv *measurePartCountVisitor) VisitPart(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.partCount++
	return nil
}

// migrateTraceWithFileBasedAndProgress performs file-based trace migration with progress tracking.
func migrateTraceWithFileBasedAndProgress(tsdbRootPath string, timeRange timestamp.TimeRange, group *GroupConfig,
	logger *logger.Logger, progress *Progress, chunkSize int, md metadata.Repo,
) ([]string, error) {
	// Convert segment interval to IntervalRule using storage.MustToIntervalRule
	segmentIntervalRule := storage.MustToIntervalRule(group.SegmentInterval)

	// Get target stage configuration
	targetStageInterval := getTargetStageInterval(group)

	// Pre-walk source items so each resource has a fixed planned denominator.
	counter, segmentSuffixes, err := countTraceShards(tsdbRootPath, timeRange, segmentIntervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count trace source items, proceeding without planned counts")
	} else {
		logger.Info().Int("total_shards", counter.shardCount).
			Int("total_series_files", counter.seriesFileCount).
			Strs("segment_suffixes", segmentSuffixes).
			Msg("counted trace source items for progress tracking")
	}

	// Create file-based migration visitor with progress tracking and target stage interval
	visitor := newTraceMigrationVisitor(
		group.Group, group.TargetShardNum, group.TargetReplicas, group.NodeSelector, group.QueueClient,
		logger, progress, chunkSize, targetStageInterval, md,
	)
	defer visitor.Close()

	// Set the planned source-item counts for progress tracking.
	if counter != nil {
		visitor.SetTraceShardCount(counter.shardCount)
		visitor.SetTraceSeriesCount(counter.seriesFileCount)
	}

	// Use the existing VisitTracesInTimeRange function with our file-based visitor
	_, err = trace.VisitTracesInTimeRange(tsdbRootPath, timeRange, visitor, segmentIntervalRule)
	if err != nil {
		return nil, err
	}
	return segmentSuffixes, nil
}

// countTraceShards counts the total number of source items in the given time range.
func countTraceShards(tsdbRootPath string, timeRange timestamp.TimeRange, segmentInterval storage.IntervalRule) (*traceShardsCountVisitor, []string, error) {
	// Create a simple visitor to count source items
	shardCounter := &traceShardsCountVisitor{lfs: fs.NewLocalFileSystem()}

	// Use the existing VisitTracesInTimeRange function to count parts
	segmentSuffixes, err := trace.VisitTracesInTimeRange(tsdbRootPath, timeRange, shardCounter, segmentInterval)
	if err != nil {
		return nil, nil, err
	}

	return shardCounter, segmentSuffixes, nil
}

// traceShardsCountVisitor is a simple visitor that counts trace source shards and series files.
type traceShardsCountVisitor struct {
	lfs             fs.FileSystem
	shardCount      int
	seriesFileCount int
}

// VisitSeries implements trace.Visitor; counts .seg files under the source series index directory.
func (pcv *traceShardsCountVisitor) VisitSeries(_ *timestamp.TimeRange, seriesIndexPath string, _ []common.ShardID) error {
	entries := pcv.lfs.ReadDir(seriesIndexPath)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			pcv.seriesFileCount++
		}
	}
	return nil
}

// VisitShard implements trace.Visitor.
func (pcv *traceShardsCountVisitor) VisitShard(_ *timestamp.TimeRange, _ common.ShardID, _ string) error {
	pcv.shardCount++
	return nil
}
