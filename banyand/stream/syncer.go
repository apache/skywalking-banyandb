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
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func (tst *tsTable) syncLoop(syncCh chan *syncIntroduction, flusherNotifier watcher.Channel) {
	defer tst.loopCloser.Done()

	var epoch uint64

	ew := flusherNotifier.Add(0, tst.loopCloser.CloseNotify())
	if ew == nil {
		return
	}

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-ew.Watch():
			if func() bool {
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
						return false
					}
					epoch = curSnapshot.epoch
					if tst.currentEpoch() != epoch {
						return false
					}
				}
				ew = flusherNotifier.Add(epoch, tst.loopCloser.CloseNotify())
				return ew == nil
			}() {
				return
			}
		}
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

	// Send parts through pipeline
	ctx := context.Background()

	for _, node := range nodes {
		for _, part := range partsToSync {
			// Convert part to memPart
			memPart := generateMemPart()
			memPart.mustInitFromPart(part)

			// Marshal memPart to []byte
			partData, err := memPart.Marshal()
			releaseMemPart(memPart)
			if err != nil {
				return fmt.Errorf("failed to marshal part %d: %w", part.partMetadata.ID, err)
			}

			// Encode group, shardID and prepend to partData
			combinedData := make([]byte, 0, len(partData)+len(tst.group)+4)
			combinedData = encoding.EncodeBytes(combinedData, convert.StringToBytes(tst.group))
			combinedData = encoding.Uint32ToBytes(combinedData, uint32(tst.shardID))
			combinedData = append(combinedData, partData...)

			// Create message with combined data (group + shardID + partData)
			message := bus.NewMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, combinedData)
			f, err := tst.option.tire2Client.Publish(ctx, data.TopicStreamPartSync, message)
			if err != nil {
				return fmt.Errorf("failed to publish part %d to pipeline: %w", part.partMetadata.ID, err)
			}
			if f != nil {
				_, err = f.GetAll()
				if err != nil {
					tst.l.Warn().Err(err).Msgf("failed to get all sync results: %v", err)
				}
			}
		}
	}

	// Construct syncIntroduction to remove synced parts from snapshot
	si := generateSyncIntroduction()
	defer releaseSyncIntroduction(si)
	si.applied = make(chan struct{})

	// Mark all synced parts for removal
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
