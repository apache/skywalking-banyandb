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
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
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
		if curSnapshot.epoch != epoch {
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
	// Trace metadata
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	bigValuePool.Release(buf)
	files = append(files,
		queue.FileInfo{
			Name:   traceMetaName,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   tracePrimaryName,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   traceSpansName,
			Reader: part.spans.SequentialRead(),
		},
	)

	// Trace tags data
	if part.tags != nil {
		for name, reader := range part.tags {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", traceTagsPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
		for name, reader := range part.tagMetadata {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", traceTagMetadataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
	}

	// TraceID filter data
	if part.fileSystem != nil {
		traceIDFilterPath := filepath.Join(part.path, traceIDFilterFilename)
		if filterData, err := part.fileSystem.Read(traceIDFilterPath); err == nil && len(filterData) > 0 {
			filterBuf := bigValuePool.Generate()
			filterBuf.Buf = append(filterBuf.Buf[:0], filterData...)
			files = append(files, queue.FileInfo{
				Name:   traceIDFilterFilename,
				Reader: filterBuf.SequentialRead(),
			})
		}
	}

	// Tag type data
	if part.fileSystem != nil {
		tagTypePath := filepath.Join(part.path, tagTypeFilename)
		if tagTypeData, err := part.fileSystem.Read(tagTypePath); err == nil && len(tagTypeData) > 0 {
			tagTypeBuf := bigValuePool.Generate()
			tagTypeBuf.Buf = append(tagTypeBuf.Buf[:0], tagTypeData...)
			files = append(files, queue.FileInfo{
				Name:   tagTypeFilename,
				Reader: tagTypeBuf.SequentialRead(),
			})
		}
	}

	return files, func() {
		bigValuePool.Release(bb)
	}
}

func (tst *tsTable) syncSnapshot(curSnapshot *snapshot, syncCh chan *syncIntroduction) error {
	// Get all parts from the current snapshot
	var partsToSync []*part
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}
	sidxPartsToSync := sidx.NewPartsToSync()
	for name, sidx := range tst.sidxMap {
		sidxPartsToSync[name] = sidx.PartsToSync()
	}

	hasCoreParts := len(partsToSync) > 0
	hasSidxParts := false
	for _, parts := range sidxPartsToSync {
		if len(parts) > 0 {
			hasSidxParts = true
			break
		}
	}
	if !hasCoreParts && !hasSidxParts {
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

	// Use chunked sync with streaming for better memory efficiency
	ctx := context.Background()
	releaseFuncs := make([]func(), 0, len(partsToSync))
	defer func() {
		for _, release := range releaseFuncs {
			release()
		}
	}()
	if err := tst.syncStreamingPartsToNodes(ctx, nodes, partsToSync, sidxPartsToSync, &releaseFuncs); err != nil {
		return err
	}

	// Construct syncIntroduction to remove synced parts from snapshot
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})
	for _, part := range partsToSync {
		ph := partHandle{
			partID:   part.partMetadata.ID,
			partType: PartTypeCore,
		}
		si.synced[ph] = struct{}{}
	}
	// Construct sidx sync introductions for each sidx instance
	sidxSyncIntroductions := make(map[string]*sidx.SyncIntroduction)
	for name, sidxParts := range sidxPartsToSync {
		if len(sidxParts) > 0 {
			ssi := sidx.GenerateSyncIntroduction()
			ssi.Applied = make(chan struct{})
			for _, part := range sidxParts {
				ssi.Synced[part.ID()] = struct{}{}
			}
			sidxSyncIntroductions[name] = ssi
		}
	}
	// Send sync introductions
	select {
	case syncCh <- si:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}
	for name, ssi := range sidxSyncIntroductions {
		select {
		case tst.sidxMap[name].SyncCh() <- ssi:
		case <-tst.loopCloser.CloseNotify():
			return errClosed
		}
	}
	// Wait for sync introductions to be applied
	select {
	case <-si.applied:
	case <-tst.loopCloser.CloseNotify():
		return errClosed
	}
	for _, ssi := range sidxSyncIntroductions {
		select {
		case <-ssi.Applied:
		case <-tst.loopCloser.CloseNotify():
			return errClosed
		}
		sidx.ReleaseSyncIntroduction(ssi)
	}
	return nil
}

// syncStreamingPartsToNodes synchronizes streamingparts to multiple nodes.
func (tst *tsTable) syncStreamingPartsToNodes(ctx context.Context, nodes []string,
	partsToSync []*part, sidxPartsToSync map[string][]*sidx.Part, releaseFuncs *[]func(),
) error {
	for _, node := range nodes {
		// Prepare all streaming parts data
		streamingParts := make([]queue.StreamingPartData, 0)
		snapshot := tst.currentSnapshot()
		// Add sidx streaming parts
		for name, sidxParts := range sidxPartsToSync {
			// TODO: minTimestmaps should be read only once
			minTimestamps := make([]int64, 0)
			for _, part := range sidxParts {
				partID := part.ID()
				for _, pw := range snapshot.parts {
					if pw.p.partMetadata.ID == partID {
						minTimestamps = append(minTimestamps, pw.p.partMetadata.MinTimestamp)
						break
					}
				}
			}
			sidxStreamingParts, sidxReleaseFuncs := tst.sidxMap[name].StreamingParts(sidxParts, tst.group, uint32(tst.shardID), name, minTimestamps)
			streamingParts = append(streamingParts, sidxStreamingParts...)
			*releaseFuncs = append(*releaseFuncs, sidxReleaseFuncs...)
		}
		// Create streaming parts from core trace parts.
		for _, part := range partsToSync {
			files, release := createPartFileReaders(part)
			*releaseFuncs = append(*releaseFuncs, release)
			streamingParts = append(streamingParts, queue.StreamingPartData{
				ID:                    part.partMetadata.ID,
				Group:                 tst.group,
				ShardID:               uint32(tst.shardID),
				Topic:                 data.TopicTracePartSync.String(),
				Files:                 files,
				CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
				UncompressedSizeBytes: part.partMetadata.UncompressedSpanSizeBytes,
				TotalCount:            part.partMetadata.TotalCount,
				BlocksCount:           part.partMetadata.BlocksCount,
				MinTimestamp:          part.partMetadata.MinTimestamp,
				MaxTimestamp:          part.partMetadata.MaxTimestamp,
				PartType:              PartTypeCore,
			})
		}
		if err := tst.syncStreamingPartsToNode(ctx, node, streamingParts); err != nil {
			return err
		}
	}
	return nil
}

// syncStreamingPartsToNode synchronizes streaming parts to a node.
func (tst *tsTable) syncStreamingPartsToNode(ctx context.Context, node string, streamingParts []queue.StreamingPartData) error {
	// Get chunked sync client for this node
	chunkedClient, err := tst.option.tire2Client.NewChunkedSyncClient(node, 1024*1024)
	if err != nil {
		return fmt.Errorf("failed to create chunked sync client for node %s: %w", node, err)
	}
	defer chunkedClient.Close()

	// Sync parts using chunked transfer with streaming
	result, err := chunkedClient.SyncStreamingParts(ctx, streamingParts)
	if err != nil {
		return fmt.Errorf("failed to sync streaming parts to node %s: %w", node, err)
	}

	if !result.Success {
		return fmt.Errorf("chunked sync partially failed: %v", result.ErrorMessage)
	}
	tst.l.Info().
		Str("node", node).
		Str("session", result.SessionID).
		Uint64("bytes", result.TotalBytes).
		Int64("duration_ms", result.DurationMs).
		Uint32("chunks", result.ChunksCount).
		Uint32("parts", result.PartsCount).
		Msg("chunked sync completed successfully")
	return nil
}
