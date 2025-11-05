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
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

// PartTypeCore is the type of the core part.
const PartTypeCore = "core"

func (tst *tsTable) syncLoop(syncCh chan *syncIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()

	var epoch uint64
	var lastTriggerTime time.Time
	firstSync := true

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	syncTimer := time.NewTimer(tst.option.syncInterval)
	defer syncTimer.Stop()

	trySync := func(triggerTime time.Time) bool {
		defer syncTimer.Reset(tst.option.syncInterval)
		curSnapshot := tst.currentSnapshot()
		if curSnapshot == nil {
			return false
		}
		defer curSnapshot.decRef()
		if firstSync || curSnapshot.epoch != epoch {
			tst.incTotalSyncLoopStarted(1)
			defer tst.incTotalSyncLoopFinished(1)
			var err error
			if err = tst.syncSnapshot(curSnapshot, syncCh); err != nil {
				if tst.loopCloser.Closed() {
					return true
				}
				tst.l.Logger.Warn().Err(err).Msgf("cannot sync snapshot: %d", curSnapshot.epoch)
				tst.incTotalSyncLoopErr(1)
				time.Sleep(2 * time.Second)
				return false
			}
			epoch = curSnapshot.epoch
			firstSync = false
			lastTriggerTime = triggerTime
			if tst.currentEpoch() != epoch {
				return false
			}
		}
		ew = flusherNotifier.Add(epoch, tst.loopCloser.CloseNotify())
		return ew == nil
	}

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if trySync(time.Now()) {
				return
			}
		case <-syncTimer.C:
			now := time.Now()
			if now.Sub(lastTriggerTime) < tst.option.syncInterval {
				syncTimer.Reset(tst.option.syncInterval - now.Sub(lastTriggerTime))
				continue
			}
			if trySync(now) {
				return
			}
		}
	}
}

func createPartFileReaders(part *part) ([]queue.FileInfo, func()) {
	var files []queue.FileInfo

	buf := bigValuePool.Generate()
	// Stream metadata
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	bigValuePool.Release(buf)
	files = append(files,
		queue.FileInfo{
			Name:   streamMetaName,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   streamPrimaryName,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   streamTimestampsName,
			Reader: part.timestamps.SequentialRead(),
		},
	)

	// Stream tag families data
	if part.tagFamilies != nil {
		for name, reader := range part.tagFamilies {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", streamTagFamiliesPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}

		for name, reader := range part.tagFamilyMetadata {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", streamTagMetadataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}

		for name, reader := range part.tagFamilyFilter {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", streamTagFilterPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
	}

	return files, func() {
		bigValuePool.Release(bb)
	}
}

// syncPartsToNodesHelper syncs given parts to all nodes and returns failed parts.
// This helper is used for both initial sync and retry attempts.
func (tst *tsTable) syncPartsToNodesHelper(ctx context.Context, parts []*part, nodes []string, chunkSize uint32, releaseFuncs *[]func()) ([]queue.FailedPart, error) {
	var allFailedParts []queue.FailedPart

	for _, node := range nodes {
		// Get chunked sync client for this node
		chunkedClient, err := tst.option.tire2Client.NewChunkedSyncClient(node, chunkSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunked sync client for node %s: %w", node, err)
		}
		defer chunkedClient.Close()

		// Prepare streaming parts data
		var streamingParts []queue.StreamingPartData
		for _, part := range parts {
			files, release := createPartFileReaders(part)
			*releaseFuncs = append(*releaseFuncs, release)
			streamingParts = append(streamingParts, queue.StreamingPartData{
				ID:                    part.partMetadata.ID,
				Group:                 tst.group,
				ShardID:               uint32(tst.shardID),
				Topic:                 data.TopicStreamPartSync.String(),
				Files:                 files,
				CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
				UncompressedSizeBytes: part.partMetadata.UncompressedSizeBytes,
				TotalCount:            part.partMetadata.TotalCount,
				BlocksCount:           part.partMetadata.BlocksCount,
				MinTimestamp:          part.partMetadata.MinTimestamp,
				MaxTimestamp:          part.partMetadata.MaxTimestamp,
				PartType:              PartTypeCore,
			})
		}

		result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
		if err != nil {
			return nil, fmt.Errorf("failed to sync streaming parts to node %s: %w", node, err)
		}

		tst.incTotalSyncLoopBytes(result.TotalBytes)
		if dl := tst.l.Debug(); dl.Enabled() {
			dl.
				Str("node", node).
				Str("session", result.SessionID).
				Uint64("bytes", result.TotalBytes).
				Int64("duration_ms", result.DurationMs).
				Uint32("chunks", result.ChunksCount).
				Uint32("parts", result.PartsCount).
				Int("failed_parts", len(result.FailedParts)).
				Msg("chunked sync completed")
		}

		allFailedParts = append(allFailedParts, result.FailedParts...)
	}

	return allFailedParts, nil
}

func (tst *tsTable) syncSnapshot(curSnapshot *snapshot, syncCh chan *syncIntroduction) error {
	startTime := time.Now()
	defer func() {
		tst.incTotalSyncLoopLatency(time.Since(startTime).Seconds())
	}()

	// Get all parts from the current snapshot
	var partsToSync []*part
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}

	if len(partsToSync) == 0 {
		return nil
	}
	nodes := tst.getNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to sync parts")
	}

	// Sort parts from old to new (by part ID)
	// Parts with lower IDs are older
	for i := 0; i < len(partsToSync); i++ {
		for j := i + 1; j < len(partsToSync); j++ {
			if partsToSync[i].partMetadata.ID > partsToSync[j].partMetadata.ID {
				partsToSync[i], partsToSync[j] = partsToSync[j], partsToSync[i]
			}
		}
	}

	// Initialize failed parts handler
	failedPartsHandler := storage.NewFailedPartsHandler(tst.fileSystem, tst.root, tst.l)

	// Build part info map for failed parts handler
	partsInfo := make(map[uint64]*storage.PartInfo)
	for _, part := range partsToSync {
		partsInfo[part.partMetadata.ID] = &storage.PartInfo{
			PartID:     part.partMetadata.ID,
			SourcePath: part.path,
			PartType:   PartTypeCore,
		}
	}

	// Use chunked sync with streaming for better memory efficiency
	ctx := context.Background()
	releaseFuncs := make([]func(), 0, len(partsToSync))
	defer func() {
		for _, release := range releaseFuncs {
			release()
		}
	}()

	// Initial sync attempt - track failures per node
	perNodeFailures := make(map[string][]queue.FailedPart) // node -> failed parts
	for _, node := range nodes {
		failedParts, err := tst.syncPartsToNodesHelper(ctx, partsToSync, []string{node}, 1024*1024, &releaseFuncs)
		if err != nil {
			return err
		}
		if len(failedParts) > 0 {
			perNodeFailures[node] = failedParts
		}
	}

	// If there are failed parts, use the retry handler
	if len(perNodeFailures) > 0 {
		// Collect all unique failed parts for retry handler
		allFailedParts := make([]queue.FailedPart, 0)
		for _, failedParts := range perNodeFailures {
			allFailedParts = append(allFailedParts, failedParts...)
		}

		// Create a sync function for retries - only retry on nodes where parts failed
		syncFunc := func(partIDs []uint64) ([]queue.FailedPart, error) {
			// Build map of partIDs to retry
			partIDsSet := make(map[uint64]struct{})
			for _, partID := range partIDs {
				partIDsSet[partID] = struct{}{}
			}

			// Filter parts to retry
			partsToRetry := make([]*part, 0)
			for _, partID := range partIDs {
				for _, part := range partsToSync {
					if part.partMetadata.ID == partID {
						partsToRetry = append(partsToRetry, part)
						break
					}
				}
			}

			if len(partsToRetry) == 0 {
				return nil, nil
			}

			retryReleaseFuncs := make([]func(), 0)
			defer func() {
				for _, release := range retryReleaseFuncs {
					release()
				}
			}()

			// Only retry on nodes where these specific parts failed
			var retryFailedParts []queue.FailedPart
			for node, nodeFailedParts := range perNodeFailures {
				// Check if any of the parts to retry failed on this node
				shouldRetryOnNode := false
				for _, failedPart := range nodeFailedParts {
					failedPartID, _ := strconv.ParseUint(failedPart.PartID, 10, 64)
					if _, exists := partIDsSet[failedPartID]; exists {
						shouldRetryOnNode = true
						break
					}
				}

				if !shouldRetryOnNode {
					continue
				}

				// Retry only on this specific node
				failedParts, err := tst.syncPartsToNodesHelper(ctx, partsToRetry, []string{node}, 1024*1024, &retryReleaseFuncs)
				if err != nil {
					// On error, mark all parts as failed for this node
					for _, partID := range partIDs {
						retryFailedParts = append(retryFailedParts, queue.FailedPart{
							PartID: strconv.FormatUint(partID, 10),
							Error:  fmt.Sprintf("node %s: %v", node, err),
						})
					}
					continue
				}
				retryFailedParts = append(retryFailedParts, failedParts...)
			}

			return retryFailedParts, nil
		}

		_, err := failedPartsHandler.RetryFailedParts(ctx, allFailedParts, partsInfo, syncFunc)
		if err != nil {
			tst.l.Warn().Err(err).Msg("error during retry process")
		}
	}

	// Construct syncIntroduction to remove synced parts from snapshot
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})

	// Add all parts (both successful and permanently failed)
	for _, part := range partsToSync {
		si.synced[part.partMetadata.ID] = struct{}{}
	}
	// Permanently failed parts are already included since they're in partsToSync

	select {
	case syncCh <- si:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}
	select {
	case <-si.applied:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}

	return nil
}
