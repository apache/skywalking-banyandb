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
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// migrateStreamWithFileBasedAndProgress performs file-based stream migration with progress tracking.
func migrateStreamWithFileBasedAndProgress(
	tsdbRootPath string,
	timeRange timestamp.TimeRange,
	group *commonv1.Group,
	nodeLabels map[string]string,
	nodes []*databasev1.Node,
	metadata metadata.Repo,
	logger *logger.Logger,
	progress *Progress,
	chunkSize int,
) error {
	// Use parseGroup function to get sharding parameters and TTL
	shardNum, replicas, ttl, selector, client, err := parseGroup(group, nodeLabels, nodes, logger, metadata)
	if err != nil {
		return err
	}
	defer client.GracefulStop()

	// Convert TTL to IntervalRule using storage.MustToIntervalRule
	intervalRule := storage.MustToIntervalRule(ttl)

	// Count total parts before starting migration
	totalParts, err := countStreamParts(tsdbRootPath, timeRange, intervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count stream parts, proceeding without part count")
	} else {
		logger.Info().Int("total_parts", totalParts).Msg("counted stream parts for progress tracking")
	}

	// Create file-based migration visitor with progress tracking
	visitor := newStreamMigrationVisitor(group, shardNum, replicas, selector, client, logger, progress, chunkSize)
	defer visitor.Close()

	// Set the total part count for progress tracking
	if totalParts > 0 {
		visitor.SetStreamPartCount(totalParts)
	}

	// Use the existing VisitStreamsInTimeRange function with our file-based visitor
	return stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, visitor, intervalRule)
}

// countStreamParts counts the total number of parts in the given time range.
func countStreamParts(tsdbRootPath string, timeRange timestamp.TimeRange, intervalRule storage.IntervalRule) (int, error) {
	// Create a simple visitor to count parts
	partCounter := &partCountVisitor{}

	// Use the existing VisitStreamsInTimeRange function to count parts
	err := stream.VisitStreamsInTimeRange(tsdbRootPath, timeRange, partCounter, intervalRule)
	if err != nil {
		return 0, err
	}

	return partCounter.partCount, nil
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
func migrateMeasureWithFileBasedAndProgress(
	tsdbRootPath string,
	timeRange timestamp.TimeRange,
	group *commonv1.Group,
	nodeLabels map[string]string,
	nodes []*databasev1.Node,
	metadata metadata.Repo,
	logger *logger.Logger,
	progress *Progress,
	chunkSize int,
) error {
	// Use parseGroup function to get sharding parameters and TTL
	shardNum, replicas, ttl, selector, client, err := parseGroup(group, nodeLabels, nodes, logger, metadata)
	if err != nil {
		return err
	}
	defer client.GracefulStop()

	// Convert TTL to IntervalRule using storage.MustToIntervalRule
	intervalRule := storage.MustToIntervalRule(ttl)

	// Count total parts before starting migration
	totalParts, err := countMeasureParts(tsdbRootPath, timeRange, intervalRule)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to count measure parts, proceeding without part count")
	} else {
		logger.Info().Int("total_parts", totalParts).Msg("counted measure parts for progress tracking")
	}

	// Create file-based migration visitor with progress tracking
	visitor := newMeasureMigrationVisitor(group, shardNum, replicas, selector, client, logger, progress, chunkSize)
	defer visitor.Close()

	// Set the total part count for progress tracking
	if totalParts > 0 {
		visitor.SetMeasurePartCount(totalParts)
	}

	// Use the existing VisitMeasuresInTimeRange function with our file-based visitor
	return measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, visitor, intervalRule)
}

// countMeasureParts counts the total number of parts in the given time range.
func countMeasureParts(tsdbRootPath string, timeRange timestamp.TimeRange, intervalRule storage.IntervalRule) (int, error) {
	// Create a simple visitor to count parts
	partCounter := &measurePartCountVisitor{}

	// Use the existing VisitMeasuresInTimeRange function to count parts
	err := measure.VisitMeasuresInTimeRange(tsdbRootPath, timeRange, partCounter, intervalRule)
	if err != nil {
		return 0, err
	}

	return partCounter.partCount, nil
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
