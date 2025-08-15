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

package measure

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

type syncIntroduction struct {
	synced  map[uint64]struct{}
	applied chan struct{}
}

func (i *syncIntroduction) reset() {
	for k := range i.synced {
		delete(i.synced, k)
	}
	i.applied = nil
}

var syncIntroductionPool = pool.Register[*syncIntroduction]("measure-sync-introduction")

func generateSyncIntroduction() *syncIntroduction {
	v := syncIntroductionPool.Get()
	if v == nil {
		return &syncIntroduction{
			synced: make(map[uint64]struct{}),
		}
	}
	i := v
	i.reset()
	return i
}

func releaseSyncIntroduction(i *syncIntroduction) {
	syncIntroductionPool.Put(i)
}

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

	// Common sync logic extracted into a closure
	trySync := func(triggerTime time.Time) bool {
		defer syncTimer.Reset(tst.option.syncInterval)
		curSnapshot := tst.currentSnapshot()
		if curSnapshot == nil {
			return false
		}
		defer curSnapshot.decRef()
		if curSnapshot.epoch != epoch {
			// Use existing metric methods for measure
			tst.incTotalFlushLoopStarted(1)
			defer tst.incTotalFlushLoopFinished(1)
			var err error
			if err = tst.syncSnapshot(curSnapshot, syncCh); err != nil {
				if tst.loopCloser.Closed() {
					return true
				}
				tst.l.Error().Err(err).Msgf("cannot sync snapshot: %d", curSnapshot.epoch)
				tst.incTotalFlushLoopErr(1)
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
	// Stream metadata.
	for i := range part.primaryBlockMetadata {
		buf.Buf = part.primaryBlockMetadata[i].marshal(buf.Buf)
	}
	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], buf.Buf, 1)
	bigValuePool.Release(buf)
	files = append(files,
		queue.FileInfo{
			Name:   measureMetaName,
			Reader: bb.SequentialRead(),
		},
		queue.FileInfo{
			Name:   measurePrimaryName,
			Reader: part.primary.SequentialRead(),
		},
		queue.FileInfo{
			Name:   measureTimestampsName,
			Reader: part.timestamps.SequentialRead(),
		},
		queue.FileInfo{
			Name:   measureFieldValuesName,
			Reader: part.fieldValues.SequentialRead(),
		},
	)

	// Stream tag families data.
	if part.tagFamilies != nil {
		for name, reader := range part.tagFamilies {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", measureTagFamiliesPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}

		for name, reader := range part.tagFamilyMetadata {
			files = append(files, queue.FileInfo{
				Name:   fmt.Sprintf("%s%s", measureTagMetadataPrefix, name),
				Reader: reader.SequentialRead(),
			})
		}
	}

	return files, func() {
		bigValuePool.Release(bb)
	}
}

func (tst *tsTable) syncSnapshot(curSnapshot *snapshot, syncCh chan *syncIntroduction) error {
	var partsToSync []*part
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}

	if len(partsToSync) == 0 {
		return nil
	}

	// Sort parts from old to new (by part ID).
	for i := 0; i < len(partsToSync); i++ {
		for j := i + 1; j < len(partsToSync); j++ {
			if partsToSync[i].partMetadata.ID > partsToSync[j].partMetadata.ID {
				partsToSync[i], partsToSync[j] = partsToSync[j], partsToSync[i]
			}
		}
	}

	nodes := tst.getNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to sync parts")
	}

	// Use chunked sync with streaming for better memory efficiency.
	ctx := context.Background()
	releaseFuncs := make([]func(), 0, len(partsToSync))
	defer func() {
		for _, release := range releaseFuncs {
			release()
		}
	}()

	for _, node := range nodes {
		// Get chunked sync client for this node.
		chunkedClient, err := tst.option.tire2Client.NewChunkedSyncClient(node, 512*1024)
		if err != nil {
			return fmt.Errorf("failed to create chunked sync client for node %s: %w", node, err)
		}
		defer chunkedClient.Close()

		// Prepare streaming parts data for chunked sync.
		var streamingParts []queue.StreamingPartData
		for _, part := range partsToSync {
			// Create streaming reader for the part.
			files, release := createPartFileReaders(part)
			releaseFuncs = append(releaseFuncs, release)

			// Create streaming part sync data.
			streamingParts = append(streamingParts, queue.StreamingPartData{
				ID:                    part.partMetadata.ID,
				Group:                 tst.group,
				ShardID:               uint32(tst.shardID),
				Topic:                 data.TopicMeasurePartSync.String(),
				Files:                 files,
				CompressedSizeBytes:   part.partMetadata.CompressedSizeBytes,
				UncompressedSizeBytes: part.partMetadata.UncompressedSizeBytes,
				TotalCount:            part.partMetadata.TotalCount,
				BlocksCount:           part.partMetadata.BlocksCount,
				MinTimestamp:          part.partMetadata.MinTimestamp,
				MaxTimestamp:          part.partMetadata.MaxTimestamp,
			})
		}

		// Sync parts using chunked transfer with streaming.
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
	}

	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})

	for _, part := range partsToSync {
		si.synced[part.partMetadata.ID] = struct{}{}
	}

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
