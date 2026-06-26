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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// measureMigrationVisitor implements the measure.Visitor interface for file-based migration.
type measureMigrationVisitor struct {
	client         queue.Client
	lfs            fs.FileSystem
	metadata       metadata.Repo
	selector       node.Selector
	chunkedClients map[string]queue.ChunkedSyncClient
	logger         *logger.Logger
	progress       *Progress
	replayer       *measureRowReplayer
	skippedSourceTracker
	group                   string
	sourceStage             string
	targetStage             string
	orphanCfg               orphanConfig
	targetStageInterval     storage.IntervalRule
	sourceSegmentInterval   storage.IntervalRule
	chunkSize               int
	partsCopiedSingleTarget uint64
	partsReplayedRowLevel   uint64
	targetShardNum          uint32
	replicas                uint32
}

// newMeasureMigrationVisitor creates a new file-based migration visitor.
func newMeasureMigrationVisitor(group *commonv1.Group, shardNum, replicas uint32, selector node.Selector, client queue.Client,
	l *logger.Logger, progress *Progress, chunkSize int, targetStageInterval storage.IntervalRule, md metadata.Repo,
	sourceStage, targetStage string, sourceSegmentInterval storage.IntervalRule, orphanCfg orphanConfig,
) *measureMigrationVisitor {
	return &measureMigrationVisitor{
		group:                 group.Metadata.Name,
		sourceStage:           sourceStage,
		targetStage:           targetStage,
		sourceSegmentInterval: sourceSegmentInterval,
		targetShardNum:        shardNum,
		replicas:              replicas,
		selector:              selector,
		client:                client,
		chunkedClients:        make(map[string]queue.ChunkedSyncClient),
		logger:                l,
		progress:              progress,
		chunkSize:             chunkSize,
		targetStageInterval:   targetStageInterval,
		metadata:              md,
		lfs:                   fs.NewLocalFileSystem(),
		skippedSourceTracker:  newSkippedSourceTracker(),
		orphanCfg:             orphanCfg,
	}
}

// recordError appends a structured measure migration error to the report,
// keyed by the SOURCE segment (segmentTR) so segment/interval describe the data
// being migrated. partID is nil for series-scoped errors.
func (mv *measureMigrationVisitor) recordError(scope string, segmentTR *timestamp.TimeRange,
	shardID common.ShardID, partID *uint64, msg string,
) {
	seg, interval := segmentErrorLocation(segmentTR, mv.sourceSegmentInterval)
	s := uint32(shardID)
	mv.progress.AddMigrationError(MigrationError{
		SourceStage: mv.sourceStage, TargetStage: mv.targetStage, Group: mv.group,
		Catalog: catalogMeasure, Scope: scope, Segment: seg, Interval: interval,
		Shard: &s, Part: partID, Error: msg,
	})
}

// formatSkipExamples renders a bounded sample of skip locations for a report
// message.
func formatSkipExamples(detail []skipError) string {
	const maxExamples = 5
	if len(detail) == 0 {
		return "[]"
	}
	parts := make([]string, 0, maxExamples)
	for i := range detail {
		if i >= maxExamples {
			break
		}
		parts = append(parts, fmt.Sprintf("{seriesID=%d reason=%s}", detail[i].seriesID, detail[i].reason))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// CounterSnapshot returns the current (chunk-sync, row-replay) part counts.
func (mv *measureMigrationVisitor) CounterSnapshot() (chunkSync, rowReplay uint64) {
	return atomic.LoadUint64(&mv.partsCopiedSingleTarget), atomic.LoadUint64(&mv.partsReplayedRowLevel)
}

// ensureReplayer lazily constructs the measureRowReplayer on first use.
func (mv *measureMigrationVisitor) ensureReplayer(ctx context.Context) (*measureRowReplayer, error) {
	if mv.replayer != nil {
		return mv.replayer, nil
	}
	r, err := newMeasureRowReplayer(ctx, mv.group, mv.targetShardNum, mv.selector, mv.client, mv.metadata,
		mv.lfs, mv.logger, &mv.partsReplayedRowLevel, mv.orphanCfg, mv.sourceStage)
	if err != nil {
		return nil, fmt.Errorf("create measure row replayer: %w", err)
	}
	mv.replayer = r
	return r, nil
}

// VisitSeries implements measure.Visitor.
func (mv *measureMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error {
	mv.logger.Info().
		Str("path", seriesIndexPath).
		Int64("min_timestamp", segmentTR.Start.UnixNano()).
		Int64("max_timestamp", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating measure series index")

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
			Msg("no .seg files found in measure series index path")
		return nil
	}

	mv.logger.Info().
		Int("segment_count", len(segmentFiles)).
		Str("path", seriesIndexPath).
		Msg("found measure segment files for migration")

	// Calculate ALL target segments this series index should go to from the
	// source segment's own [start, end) boundaries.
	targetSegments := calculateTargetSegments(
		segmentTR.Start,
		segmentTR.End,
		mv.targetStageInterval,
	)

	mv.logger.Info().
		Int("target_segments_count", len(targetSegments)).
		Int64("series_min_ts", segmentTR.Start.UnixNano()).
		Int64("series_max_ts", segmentTR.End.UnixNano()).
		Str("group", mv.group).
		Msg("migrating measure series index")

	// When the source series-index spans multiple target segments, the row-replay
	// path (VisitPart -> visitPartRowReplay) will republish every row through the
	// real write API, and the receiver's writeCallback rebuilds the series index
	// on the fly. Shipping the source .seg files would land them all in one
	// target segment via Standard(MinTimestamp), so skip the file-based sync.
	if len(targetSegments) > 1 {
		mv.logger.Info().
			Str("path", seriesIndexPath).
			Int("target_segments_count", len(targetSegments)).
			Str("group", mv.group).
			Msg("source series index spans multiple target segments; skipping file-based sync (rebuilt by writeCallback)")
		for _, segmentFileName := range segmentFiles {
			fileSegmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")
			segmentID, parseErr := strconv.ParseUint(fileSegmentIDStr, 16, 64)
			if parseErr != nil {
				continue
			}
			mv.progress.MarkSourceMeasureSeriesCompleted(mv.group, seriesIndexPath, common.ShardID(segmentID))
		}
		return nil
	}

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
			if mv.progress.IsMeasureSeriesCompleted(mv.group, segmentIDStr, shardID) {
				mv.logger.Debug().
					Uint64("segment_id", segmentID).
					Str("filename", segmentFileName).
					Time("target_segment", targetSegmentTime).
					Str("group", mv.group).
					Msg("measure series segment already completed for this target segment, skipping")
				continue
			}

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Msg("migrating measure series segment file")

			// Create file reader for the segment file
			segmentFilePath := filepath.Join(seriesIndexPath, segmentFileName)
			segmentFile, err := mv.lfs.OpenFile(segmentFilePath)
			if err != nil {
				errorMsg := fmt.Sprintf("failed to open measure segment file %s: %v", segmentFilePath, err)
				mv.recordError(scopeSeries, segmentTR, shardID, nil, errorMsg)
				mv.logger.Error().
					Str("path", segmentFilePath).
					Err(err).
					Msg("failed to open measure segment file")
				return fmt.Errorf("failed to open measure segment file %s: %w", segmentFilePath, err)
			}

			defer segmentFile.Close()

			files = append(files, fileInfo{
				file: segmentFile,
				name: segmentFileName,
			})

			mv.logger.Info().
				Uint64("segment_id", segmentID).
				Str("filename", segmentFileName).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Int("completed_segments", mv.progress.GetMeasureSeriesProgress(mv.group)).
				Int("total_segments", mv.progress.GetMeasureSeriesCount(mv.group)).
				Msgf("measure series segment migration completed for target segment %d/%d", i+1, len(targetSegments))
		}

		// Send segment file to each shard in shardIDs for this target segment
		segmentIDStr := getSegmentTimeRange(targetSegmentTime, mv.targetStageInterval).String()
		for _, shardID := range shardIDs {
			targetShardID := mv.calculateTargetShardID(uint32(shardID))

			// Stream segment to target shard replicas. The factory rebuilds the
			// part on every retry so each attempt gets fresh offset-0 readers.
			if err := mv.streamPartToTargetShard(targetShardID, func() queue.StreamingPartData {
				return mv.createStreamingSegmentFromFiles(targetShardID, files, segmentTR, data.TopicMeasureSeriesSync.String())
			}); err != nil {
				errorMsg := fmt.Sprintf("failed to stream measure segment to target shard %d: %v", targetShardID, err)
				mv.recordError(scopeSeries, segmentTR, shardID, nil, errorMsg)
				return fmt.Errorf("failed to stream measure segment to target shard %d: %w", targetShardID, err)
			}
			// Mark segment as completed for this specific target segment
			mv.progress.MarkMeasureSeriesCompleted(mv.group, segmentIDStr, shardID)
		}
	}

	// Mark each source series file as fully migrated (idempotent — bumps Progress once per source).
	for _, segmentFileName := range segmentFiles {
		fileSegmentIDStr := strings.TrimSuffix(segmentFileName, ".seg")
		segmentID, parseErr := strconv.ParseUint(fileSegmentIDStr, 16, 64)
		if parseErr != nil {
			continue
		}
		mv.progress.MarkSourceMeasureSeriesCompleted(mv.group, seriesIndexPath, common.ShardID(segmentID))
	}
	return nil
}

// VisitPart implements measure.Visitor - core migration logic.
func (mv *measureMigrationVisitor) VisitPart(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
	partData, err := measure.ParsePartMetadata(mv.lfs, partPath)
	if err != nil {
		return fmt.Errorf("failed to parse measure part metadata: %w", err)
	}
	partID, err := parsePartIDFromPath(partPath)
	if err != nil {
		return fmt.Errorf("failed to parse part ID from path: %w", err)
	}
	// Decide the routing from the source segment's own [start, end) boundaries,
	// the same range VisitSeries uses, so the per-part chunk-sync/row-replay
	// decision stays consistent with whether the series index is file-synced.
	targetSegments := calculateTargetSegments(
		segmentTR.Start,
		segmentTR.End,
		mv.targetStageInterval,
	)

	// Cross-segment guard: when the source segment covers more than one target
	// segment, the chunk-sync path duplicates the same part bytes to every
	// target with identical Min/Max timestamps so the receiver's
	// Standard(MinTimestamp) routing lands every copy in a single target
	// segment. Row-replay sidesteps this by publishing rows through the
	// real write API, letting the receiver pick the target segment per row.
	if len(targetSegments) > 1 {
		return mv.visitPartRowReplay(context.Background(), segmentTR, sourceShardID, partID, partPath, targetSegments)
	}
	atomic.AddUint64(&mv.partsCopiedSingleTarget, 1)
	mv.progress.AddMeasureChunkSyncPart(mv.group)

	// Part completion is keyed by the SOURCE segment (segmentTR), not the target
	// segment: part IDs reset per source segment, so two distinct source segments
	// can both carry part_id=1 on the same shard. Keying by the target segment
	// would let the second source part collide with the first and be skipped,
	// dropping its data. Mirrors trace's source-keyed VisitShard.
	sourceSegmentIDStr := segmentTR.String()

	mv.logger.Info().
		Uint64("part_id", partID).
		Uint32("source_shard", uint32(sourceShardID)).
		Int64("part_min_ts", partData.MinTimestamp).
		Int64("part_max_ts", partData.MaxTimestamp).
		Str("group", mv.group).
		Msg("migrating measure part via chunk-sync to single target segment")

	// Send part to EACH target segment that overlaps with its time range
	for i, targetSegmentTime := range targetSegments {
		targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

		// Check if this part has already been completed (keyed by source segment).
		if mv.progress.IsMeasurePartCompleted(mv.group, sourceSegmentIDStr, sourceShardID, partID) {
			mv.logger.Debug().
				Uint64("part_id", partID).
				Uint32("source_shard", uint32(sourceShardID)).
				Time("target_segment", targetSegmentTime).
				Str("group", mv.group).
				Msg("measure part already completed for this target segment, skipping")
			continue
		}

		// Reopen the part each attempt for fresh offset-0 readers, releasing the
		// prior attempt's handles first; the deferred call frees the last set.
		var prevRelease func()
		defer func() {
			if prevRelease != nil {
				prevRelease()
			}
		}()
		mk := func() queue.StreamingPartData {
			if prevRelease != nil {
				prevRelease()
				prevRelease = nil
			}
			files, release := measure.CreatePartFileReaderFromPath(partPath, mv.lfs)
			prevRelease = release
			targetPartData := partData
			targetPartData.Group = mv.group
			targetPartData.ShardID = targetShardID
			targetPartData.Topic = data.TopicMeasurePartSync.String()
			targetPartData.Files = files
			return targetPartData
		}

		// Stream part to target segment
		if err := mv.streamPartToTargetShard(targetShardID, mk); err != nil {
			errorMsg := fmt.Sprintf("failed to stream measure part to target segment %s: %v", targetSegmentTime.Format(time.RFC3339), err)
			mv.recordError(scopePart, segmentTR, sourceShardID, &partID, errorMsg)
			return fmt.Errorf("failed to stream measure part to target segment: %w", err)
		}

		// Mark part as completed (source-segment keyed).
		mv.progress.MarkMeasurePartCompleted(mv.group, sourceSegmentIDStr, sourceShardID, partID)

		mv.logger.Info().
			Uint64("part_id", partID).
			Time("target_segment", targetSegmentTime).
			Str("group", mv.group).
			Msgf("measure part migration completed for target segment %d/%d", i+1, len(targetSegments))
	}

	mv.progress.MarkSourceMeasurePartCompleted(mv.group, partPath, sourceShardID, partID)
	return nil
}

// visitPartRowReplay handles the multi-target-segment case by re-publishing
// each row of the source part through the real write API. The receiver picks
// the destination segment per row, so the per-target loop is unnecessary
// here. Progress is marked once for the whole part (matching chunk-sync
// granularity).
func (mv *measureMigrationVisitor) visitPartRowReplay(ctx context.Context, segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partID uint64,
	partPath string, targetSegments []time.Time,
) error {
	// Resume guard: if a prior run already row-replayed this part, skip the
	// re-replay (it would republish every row again, wasting work). Keyed by the
	// SOURCE segment (segmentTR), not the target segments: part IDs reset per
	// source segment, so a target-keyed check could collide with a different
	// source segment's part of the same ID. Mirrors trace's VisitShard guard.
	sourceSegmentIDStr := segmentTR.String()
	if mv.progress.IsMeasurePartCompleted(mv.group, sourceSegmentIDStr, sourceShardID, partID) {
		mv.progress.MarkSourceMeasurePartCompleted(mv.group, partPath, sourceShardID, partID)
		mv.logger.Debug().
			Uint64("part_id", partID).
			Uint32("source_shard", uint32(sourceShardID)).
			Str("group", mv.group).
			Msg("measure part already row-replayed; skipping on resume")
		return nil
	}
	replayer, err := mv.ensureReplayer(ctx)
	if err != nil {
		return err
	}
	mv.logger.Info().
		Uint64("part_id", partID).
		Uint32("source_shard", uint32(sourceShardID)).
		Int("target_segments_count", len(targetSegments)).
		Str("group", mv.group).
		Msg("measure part spans multiple target segments; switching to row-replay")
	// replayPart sends and confirms the part's rows through the bounded pipeline,
	// draining all in-flight confirmations before returning. A non-nil error means
	// some rows were not durably delivered; marking the part errored (rather than
	// completed) ensures the resume guard re-replays the whole part.
	res, err := replayer.replayPart(ctx, partPath)
	if err != nil {
		// Row-replay is all-or-nothing per part; mark the source part errored so
		// resume retries the whole part (same source key the guard checks above).
		recordReplayNodeErrors(mv.progress, mv.group, mv.sourceStage, mv.targetStage, catalogMeasure, err)
		mv.recordError(scopePart, segmentTR, sourceShardID, &partID, err.Error())
		return fmt.Errorf("row-replay measure part %s: %w", partPath, err)
	}
	if res.skipped > 0 {
		// Some series could not be resolved from sidx nor rebuilt from the part's
		// columns: their rows remain only in the source part. Record a locatable
		// error and retain the source segment (excluded from the post-migration
		// delete set) so the data is not silently and permanently lost (S1).
		mv.recordSkippedSource(segmentTR)
		skipMsg := fmt.Sprintf("row-replay skipped %d unresolved rows (series-index gap, not rebuildable from columns); examples=%s",
			res.skipped, formatSkipExamples(filterSkipExamplesByKind(res.detail, skipKindSidxGap)))
		mv.recordError(scopePart, segmentTR, sourceShardID, &partID, skipMsg)
		mv.logger.Warn().
			Uint64("part_id", partID).
			Int("skipped_rows", res.skipped).
			Int("published_rows", res.rows).
			Str("group", mv.group).
			Msg("row-replay skipped unresolved series; retaining source segment to avoid data loss")
	}
	if res.orphanSkipped > 0 {
		// Orphan (deleted schema): archived or discarded; the source segment is NOT
		// retained — it is deleted normally. This is expected handling, not a
		// migration error: the per-subject counts are reported via orphans
		// (pushed at Close), not recorded in the errors buckets.
		mv.logger.Warn().
			Uint64("part_id", partID).
			Int("orphan_rows", res.orphanSkipped).
			Str("policy", orphanVerb(mv.orphanCfg.policy)).
			Str("group", mv.group).
			Msg("row-replay handled orphan-schema series; source segment will be deleted")
	}
	mv.progress.MarkMeasurePartCompleted(mv.group, sourceSegmentIDStr, sourceShardID, partID)
	mv.progress.MarkSourceMeasurePartCompleted(mv.group, partPath, sourceShardID, partID)
	mv.progress.AddMeasureRowReplay(mv.group, res.rows)
	mv.logger.Info().
		Uint64("part_id", partID).
		Int("rows_published", res.rows).
		Str("group", mv.group).
		Msg("measure row-replay completed")
	return nil
}

// calculateTargetShardID maps source shard ID to target shard ID.
func (mv *measureMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
	return calculateTargetShardID(sourceShardID, mv.targetShardNum)
}

// streamPartToTargetShard sends the part to every replica with bounded
// exponential-backoff retry (transient: target restarting, disconnect,
// receiver SERVER_BUSY). streamPartToNode closes the part's readers after each
// send, so mk() is called per attempt to rebuild fresh offset-0 readers.
func (mv *measureMigrationVisitor) streamPartToTargetShard(targetShardID uint32, mk func() queue.StreamingPartData) error {
	copies := mv.replicas + 1

	// Send to all replicas using the exact pattern from steps.go:219-236
	for replicaID := uint32(0); replicaID < copies; replicaID++ {
		err := pickAndRun(mv.logger, mv.selector, mv.group, "", targetShardID, replicaID, func(nodeID string) error {
			partData := mk()
			return mv.streamPartToNode(nodeID, partData.ShardID, partData)
		})
		if err != nil {
			return fmt.Errorf("failed to stream measure part to replica %d: %w", replicaID, err)
		}
	}

	return nil
}

// streamPartToNode streams part data to a specific target node.
func (mv *measureMigrationVisitor) streamPartToNode(nodeID string, targetShardID uint32, partData queue.StreamingPartData) error {
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
	result, err := chunkedClient.SyncStreamingParts(ctx, []queue.StreamingPartData{partData})
	if err != nil {
		return fmt.Errorf("failed to sync streaming parts to node %s: %w", nodeID, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.FailedParts)
	}

	// Log success metrics (same pattern as syncer.go:210-217)
	mv.logger.Info().
		Str("node", nodeID).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Uint32("target_shard", targetShardID).
		Uint64("part_id", partData.ID).
		Str("group", mv.group).
		Msg("file-based measure migration part completed successfully")

	return nil
}

// Close cleans up all chunked sync clients and the row-replayer.
func (mv *measureMigrationVisitor) Close() error {
	if mv.replayer != nil {
		mv.progress.AddOrphanRows(mv.group, catalogMeasure, mv.replayer.sender.orphanCounts)
		cee, replayCloseErr := mv.replayer.Close()
		recordNodeErrors(mv.progress, mv.group, mv.sourceStage, mv.targetStage, catalogMeasure, cee)
		if replayCloseErr != nil {
			mv.logger.Warn().Err(replayCloseErr).Interface("node_errors", cee).Msg("failed to close measure row replayer")
		} else if len(cee) > 0 {
			mv.logger.Warn().Interface("node_errors", cee).Msg("measure row replayer reported per-node errors")
		}
		mv.replayer = nil
	}
	for nodeID, client := range mv.chunkedClients {
		if err := client.Close(); err != nil {
			mv.logger.Warn().Err(err).Str("node", nodeID).Msg("failed to close chunked sync client")
		}
	}
	mv.chunkedClients = make(map[string]queue.ChunkedSyncClient)
	chunkSync, rowReplay := mv.CounterSnapshot()
	mv.logger.Info().
		Uint64("parts_chunk_sync", chunkSync).
		Uint64("parts_row_replay", rowReplay).
		Str("group", mv.group).
		Msg("measure migration visitor closed")
	return nil
}

// createStreamingSegmentFromFiles creates StreamingPartData from segment files.
func (mv *measureMigrationVisitor) createStreamingSegmentFromFiles(
	targetShardID uint32,
	files []fileInfo,
	segmentTR *timestamp.TimeRange,
	topic string,
) queue.StreamingPartData {
	filesInfo := make([]queue.FileInfo, 0, len(files))
	for _, file := range files {
		filesInfo = append(filesInfo, queue.FileInfo{
			Name:   file.name,
			Reader: file.file.SequentialRead(),
		})
	}
	segmentData := queue.StreamingPartData{
		Group:        mv.group,
		ShardID:      targetShardID, // Use calculated target shard
		Topic:        topic,         // Use the new topic
		Files:        filesInfo,
		MinTimestamp: segmentTR.Start.UnixNano(),
		MaxTimestamp: segmentTR.End.UnixNano(),
	}

	return segmentData
}

// SetMeasurePartCount sets the total number of parts for the current measure.
func (mv *measureMigrationVisitor) SetMeasurePartCount(totalParts int) {
	if mv.progress != nil {
		mv.progress.SetMeasurePartCount(mv.group, totalParts)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_parts", totalParts).
			Msg("set measure part count for progress tracking")
	}
}

// SetMeasureSeriesCount sets the total number of series segments for the current measure.
func (mv *measureMigrationVisitor) SetMeasureSeriesCount(totalSegments int) {
	if mv.progress != nil {
		mv.progress.SetMeasureSeriesCount(mv.group, totalSegments)
		mv.logger.Info().
			Str("group", mv.group).
			Int("total_segments", totalSegments).
			Msg("set measure series count for progress tracking")
	}
}
