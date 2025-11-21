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
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// traceMigrationVisitor implements the trace.Visitor interface for file-based migration.
type traceMigrationVisitor struct {
	selector            node.Selector                      // From parseGroup - node selector
	client              queue.Client                       // From parseGroup - queue client
	chunkedClients      map[string]queue.ChunkedSyncClient // Per-node chunked sync clients cache
	logger              *logger.Logger
	progress            *Progress // Progress tracker for migration states
	lfs                 fs.FileSystem
	group               string
	targetShardNum      uint32               // From parseGroup - target shard count
	replicas            uint32               // From parseGroup - replica count
	chunkSize           int                  // Chunk size for streaming data
	targetStageInterval storage.IntervalRule // NEW: target stage's segment interval
}

// newTraceMigrationVisitor creates a new file-based migration visitor.
func newTraceMigrationVisitor(group *commonv1.Group, shardNum, replicas uint32, selector node.Selector, client queue.Client,
	l *logger.Logger, progress *Progress, chunkSize int, targetStageInterval storage.IntervalRule,
) *traceMigrationVisitor {
	return &traceMigrationVisitor{
		group:               group.Metadata.Name,
		targetShardNum:      shardNum,
		replicas:            replicas,
		selector:            selector,
		client:              client,
		chunkedClients:      make(map[string]queue.ChunkedSyncClient),
		logger:              l,
		progress:            progress,
		chunkSize:           chunkSize,
		targetStageInterval: targetStageInterval,
		lfs:                 fs.NewLocalFileSystem(),
	}
}

// VisitSeries implements trace.Visitor.
func (mv *traceMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating trace series index")

	// Find all *.seg segment files in the seriesIndexPath
	entries := mv.lfs.ReadDir(seriesIndexPath)

	var segmentFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".seg") {
			segmentFiles = append(segmentFiles, entry.Name())
		}
	}

	if len(segmentFiles) == 0 {
		mv.logger.Debug().
			Str("path", seriesIndexPath).
			Msg("no .seg files found in trace series index path")
		return nil
	}

	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", seriesIndexPath).
		Msg("found trace segment files for migration")

	// Set the total number of series segments for progress tracking
	mv.SetTraceSeriesCount(len(segmentFiles))

	// Calculate ALL target segments this series index should go to
	targetSegments := calculateTargetSegments(
		segmentTR.Start.UnixNano(),
		segmentTR.End.UnixNano(),
		mv.targetStageInterval,
	)

	mv.logger.Info().
		Int("target_segments_count", len(targetSegments)).
		Int64("series_min_ts", segmentTR.Start.UnixNano()).
		Int64("series_max_ts", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating trace series index to multiple target segments")

	// Send series index to EACH target segment that overlaps with its time range
	for i, targetSegmentTime := range targetSegments {
		// Create StreamingPartData for this segment
		files := make([]fileInfo, 0, len(segmentFiles))
		// Process each segment file
		for _, segmentFileName := range segmentFiles {
			// Extract segment ID from filename (remove .seg extension)
			fileSegmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")

			// Parse hex segment ID
			segmentID, err := strconv.ParseUint(fileSegmentIDStr, 16, 64)
			if err != nil {
				mv.logger.Error().
					Str("filename", segmentFileName).
					Str("id_str", fileSegmentIDStr).
					Err(err).
					Msg("failed to parse segment ID from filename")
				continue
			}

			// Convert segmentID to ShardID for progress tracking
			shardID := common.ShardID(segmentID)

			// Check if this segment has already been completed for this target segment
			segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
			if mv.progress.IsTraceSeriesCompleted(mv.group, segmentIDStr, shardID) {
				mv.logger.Debug().
					Uint64("segment_id", segmentID).
					Str("filename", segmentFileName).
					Time("target_segment", targetSegmentTime).
					Str("group", mv.group).
					Msg("trace series segment already completed for this target segment, skipping")
				continue
			}

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Msg("migrating trace series segment file")

			// Create file reader for the segment file
			segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
			segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
			if err != nil {
				errorMsg := fmt.Sprintf("failed to open trace segment file %s: %v", segmentFilePath, err)
				mv.progress.MarkTraceSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
				mv.logger.Error().
					Str("path", segmentFilePath).
					Err(err).
					Msg("failed to open trace segment file")
				return fmt.Errorf("failed to open trace segment file %s: %w", segmentFilePath, err)
			}

			// Close the file reader
			defer func() {
				if i == len(targetSegments)-1 { // Only close on last iteration
					segmentFile.Close()
				}
			}()

			files = append(files, fileInfo{
				name: segmentFileName,
				file: segmentFile,
			})

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Int("completed_segments", mv.progress.GetTraceSeriesProgress(mv.group)).
				Int("total_segments", mv.progress.GetTraceSeriesCount(mv.group)).
				Msgf("trace series segment migration completed for target segment %d/%d", i+1, len(targetSegments))
		}

		// Send segment file to each shard in shardIDs for this target segment
		segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
		for _, shardID := range shardIDs {
			targetShardID := mv.calculateTargetShardID(uint32(shardID))
			ff := make([]queue.FileInfo, 0, len(files))
			for _, file := range files {
				ff = append(ff, queue.FileInfo{
					Name:   file.name,
					Reader: file.file.SequentialRead(),
				})
			}
			partData := mv.createStreamingSegmentFromFiles(targetShardID, ff, segmentTR, data.TopicTraceSeriesSync.String())

			// Stream segment to target shard replicas
			if err := mv.streamPartToTargetShard(targetShardID, []queue.StreamingPartData{partData}); err != nil {
				errorMsg := fmt.Sprintf("failed to stream trace segment to target shard %d: %v", targetShardID, err)
				mv.progress.MarkTraceSeriesError(mv.group, segmentIDStr, shardID, errorMsg)
				return fmt.Errorf("failed to stream trace segment to target shard %d: %w", targetShardID, err)
			}
			// Mark segment as completed for this specific target segment
			mv.progress.MarkTraceSeriesCompleted(mv.group, segmentIDStr, shardID)
		}
	}

	return nil
}

// VisitShard implements trace.Visitor - core and sidx migration logic.
func (mv *traceMigrationVisitor) VisitShard(timestampTR *timestamp.TimeRange, sourceShardID common.ShardID, shardPath string) error {
	segmentIDStr := timestampTR.String()
	if mv.progress.IsTraceShardCompleted(mv.group, shardPath, sourceShardID) {
		mv.logger.Debug().
			Str("shard_path", shardPath).
			Str("group", mv.group).
			Uint32("source_shard", uint32(sourceShardID)).
			Msg("trace shard already completed for this target segment, skipping")
		return nil
	}
	allParts := make([]queue.StreamingPartData, 0)

	sidxPartData, sidxReleases, err := mv.generateAllSidxPartData(timestampTR, sourceShardID, filepath.Join(shardPath, "sidx"))
	if err != nil {
		return fmt.Errorf("failed to generate sidx part data: %s: %w", shardPath, err)
	}
	defer func() {
		for _, release := range sidxReleases {
			release()
		}
	}()
	allParts = append(allParts, sidxPartData...)

	partDatas, partDataReleases, err := mv.generateAllPartData(sourceShardID, shardPath)
	if err != nil {
		return fmt.Errorf("failed to generate core par data: %s: %w", shardPath, err)
	}
	defer func() {
		for _, release := range partDataReleases {
			release()
		}
	}()
	allParts = append(allParts, partDatas...)

	targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

	sort.Slice(allParts, func(i, j int) bool {
		if allParts[i].ID == allParts[j].ID {
			return allParts[i].PartType < allParts[j].PartType
		}
		return allParts[i].ID < allParts[j].ID
	})

	// Stream part to target segment
	if err := mv.streamPartToTargetShard(targetShardID, allParts); err != nil {
		errorMsg := fmt.Errorf("failed to stream to target shard %d: %w", targetShardID, err)
		mv.progress.MarkTraceShardError(mv.group, segmentIDStr, sourceShardID, errorMsg.Error())
		return fmt.Errorf("failed to stream trace shard to target segment shard: %w", err)
	}

	// Mark shard as completed for this target segment
	mv.progress.MarkTraceShardCompleted(mv.group, segmentIDStr, sourceShardID)
	mv.logger.Info().
		Str("group", mv.group).
		Msgf("trace shard migration completed for target segment")

	return nil
}

func (mv *traceMigrationVisitor) generateAllSidxPartData(
	segmentTR *timestamp.TimeRange,
	sourceShardID common.ShardID,
	sidxPath string,
) ([]queue.StreamingPartData, []func(), error) {
	// Sidx structure: sidx/{index-name}/{part-id}/files
	// Find all index directories in the sidx directory
	entries := mv.lfs.ReadDir(sidxPath)

	parts := make([]queue.StreamingPartData, 0, len(entries))
	releases := make([]func(), 0, len(entries))

	var indexDirs []string
	for _, entry := range entries {
		if entry.IsDir() {
			indexDirs = append(indexDirs, entry.Name())
		}
	}

	if len(indexDirs) == 0 {
		mv.logger.Debug().
			Str("path", sidxPath).
			Msg("no index directories found in trace sidx directory")
		return nil, nil, nil
	}

	mv.logger.Info().
		Int("index_count", len(indexDirs)).
		Str("path", sidxPath).
		Uint32("source_shard", uint32(sourceShardID)).
		Msg("found trace sidx indexes for migration")

	// Calculate target shard ID
	targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

	// Process each index directory
	for _, indexName := range indexDirs {
		indexDirPath := filepath.Join(sidxPath, indexName)

		// Find all part directories in this index
		partEntries := mv.lfs.ReadDir(indexDirPath)
		var partDirs []string
		for _, entry := range partEntries {
			if entry.IsDir() {
				// Validate it's a valid hex string (part ID)
				if _, err := strconv.ParseUint(entry.Name(), 16, 64); err == nil {
					partDirs = append(partDirs, entry.Name())
				}
			}
		}

		if len(partDirs) == 0 {
			mv.logger.Debug().
				Str("index_name", indexName).
				Str("path", indexDirPath).
				Msg("no part directories found in trace sidx index")
			continue
		}

		mv.logger.Info().
			Str("index_name", indexName).
			Int("part_count", len(partDirs)).
			Str("path", indexDirPath).
			Msg("found trace sidx parts for index")

		// Process each part directory
		for _, partDirName := range partDirs {
			partID, _ := strconv.ParseUint(partDirName, 16, 64)
			partPath := filepath.Join(indexDirPath, partDirName)

			// Create file readers for this sidx part
			files, release := trace.CreatePartFileReaderFromPath(partPath, mv.lfs)

			// Create StreamingPartData with PartType = index name
			partData, err := sidx.ParsePartMetadata(mv.lfs, partPath)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to parse sidx part metadata: %s: %w", partPath, err)
			}
			partData.Group = mv.group
			partData.ShardID = targetShardID
			partData.Topic = data.TopicTracePartSync.String()
			partData.ID = partID
			partData.PartType = indexName // Use index name as PartType (not "core")
			partData.Files = files
			partData.MinTimestamp = segmentTR.Start.UnixNano()
			partData.MaxTimestamp = segmentTR.End.UnixNano()

			mv.logger.Info().
				Str("index_name", indexName).
				Uint64("part_id", partID).
				Uint32("source_shard", uint32(sourceShardID)).
				Uint32("target_shard", targetShardID).
				Str("group", mv.group).
				Msg("generated trace sidx part data")

			parts = append(parts, *partData)
			releases = append(releases, release)
		}
	}

	return parts, releases, nil
}

func (mv *traceMigrationVisitor) generateAllPartData(sourceShardID common.ShardID, shardPath string) ([]queue.StreamingPartData, []func(), error) {
	entries := mv.lfs.ReadDir(shardPath)

	allParts := make([]queue.StreamingPartData, 0)
	allReleases := make([]func(), 0)
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
		parts, releases, err := mv.generatePartData(sourceShardID, partPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate part data for path %s: %w", partPath, err)
		}
		allParts = append(allParts, parts...)
		allReleases = append(allReleases, releases...)
	}

	return allParts, allReleases, nil
}

func (mv *traceMigrationVisitor) generatePartData(sourceShardID common.ShardID, partPath string) ([]queue.StreamingPartData, []func(), error) {
	partData, err := trace.ParsePartMetadata(mv.lfs, partPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse trace part metadata: %w", err)
	}
	partID, err := parsePartIDFromPath(partPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse part ID from path: %w", err)
	}
	// Calculate ALL target segments this part should go to
	targetSegments := calculateTargetSegments(
		partData.MinTimestamp,
		partData.MaxTimestamp,
		mv.targetStageInterval,
	)

	mv.logger.Info().
		Uint64("part_id", partID).
		Uint32("source_shard", uint32(sourceShardID)).
		Int("target_segments_count", len(targetSegments)).
		Int64("part_min_ts", partData.MinTimestamp).
		Int64("part_max_ts", partData.MaxTimestamp).
		Str("group", mv.group).
		Msg("generate trace part files to multiple target segments")

	parts := make([]queue.StreamingPartData, 0)
	releases := make([]func(), 0)
	// Send part to EACH target segment that overlaps with its time range
	for i, targetSegmentTime := range targetSegments {
		targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

		// Create file readers for this part
		files, release := trace.CreatePartFileReaderFromPath(partPath, mv.lfs)

		// Clone part data for this target segment
		targetPartData := partData
		targetPartData.ID = partID
		targetPartData.Group = mv.group
		targetPartData.ShardID = targetShardID
		targetPartData.Topic = data.TopicTracePartSync.String()
		targetPartData.Files = files
		targetPartData.PartType = trace.PartTypeCore
		parts = append(parts, targetPartData)
		releases = append(releases, release)

		mv.logger.Info().
			Uint64("part_id", partID).
			Time("target_segment", targetSegmentTime).
			Str("group", mv.group).
			Msgf("generated trace part file migration completed for target segment %d/%d", i+1, len(targetSegments))
	}

	return parts, releases, nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *traceMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	return calculateTargetShardID(sourceShardID, mv.targetShardNum)
}

// streamPartToTargetShard sends part data to all replicas of the target shard.
func (mv *traceMigrationVisitor) streamPartToTargetShard(targetShardID uint32, partData []queue.StreamingPartData) error {
	copies := mv.replicas + 1

	// Send to all replicas using the exact pattern from steps.go:219-236
	for replicaID := uint32(0); replicaID < copies; replicaID++ {
		// Use selector.Pick exactly like steps.go:220
		nodeID, err := mv.selector.Pick(mv.group, "", targetShardID, replicaID)
		if err != nil {
			return fmt.Errorf("failed to pick node for shard %d replica %d: %w", targetShardID, replicaID, err)
		}

		// Stream part data to target node using chunked sync
		if err := mv.streamPartToNode(nodeID, targetShardID, partData); err != nil {
			return fmt.Errorf("failed to stream trace part to node %s: %w", nodeID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *traceMigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, partData []queue.StreamingPartData) error {
	// Get or create chunked client for this node (cache hit optimization)
	chunkedClient, exists := mv.chunkedClients[nodeID]
	if !exists {
		var err error
		// Create new chunked sync client via queue.Client
		chunkedClient, err = mv.client.NewChunkedSyncClient(nodeID, uint32(mv.chunkSize))
		if err != nil {
			return fmt.Errorf("failed to create chunked sync client for node %s: %w", nodeID, err)
		}
		mv.chunkedClients[nodeID] = chunkedClient // Cache for reuse
	}

	// Stream using chunked transfer (same as syncer.go:202)
	ctx := context.Background()
	result, err := chunkedClient.SyncStreamingParts(ctx, partData)
	if err != nil {
		return fmt.Errorf("failed to sync streaming parts to node %s: %w", nodeID, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.FailedParts)
	}

	mv.logger.Info().
		Str("node", nodeID).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Uint32("target_shard", targetShardID).
		Str("group", mv.group).
		Msg("file-based trace migration shard completed successfully")

	return nil
}

// Close cleans up all chunked sync clients.
func (mv *traceMigrationVisitor) Close() error {
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *traceMigrationVisitor) createStreamingSegmentFromFiles(
	targetShardID uint32,
	files []queue.FileInfo,
	segmentTR *timestamp.TimeRange,
	topic string,
) queue.StreamingPartData {
	segmentData := queue.StreamingPartData{
		Group:        mv.group,
		ShardID:      targetShardID, // Use calculated target shard
		Topic:        topic,         // Use the new topic
		Files:        files,
		MinTimestamp: segmentTR.Start.UnixNano(),
		MaxTimestamp: segmentTR.End.UnixNano(),
	}

	return segmentData
}

// SetTraceShardCount sets the total number of shards for the current trace.
func (mv *traceMigrationVisitor) SetTraceShardCount(totalShards int) {
	if mv.progress != nil {
		mv.progress.SetTraceShardCount(mv.group, totalShards)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_shards", totalShards).
			Msg("set trace part count for progress tracking")
	}
}

// SetTraceSeriesCount sets the total number of series segments for the current trace.
func (mv *traceMigrationVisitor) SetTraceSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetTraceSeriesCount(mv.group, totalSegments)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set trace series count for progress tracking")
	}
}
