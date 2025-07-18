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
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
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
	var partsToSync []*part
	for _, pw := range curSnapshot.parts {
		if pw.mp == nil && pw.p.partMetadata.TotalCount > 0 {
			partsToSync = append(partsToSync, pw.p)
		}
	}

	if len(partsToSync) == 0 {
		return nil
	}

	// Sort parts from old to new (by part ID)
	for i := 0; i < len(partsToSync); i++ {
		for j := i + 1; j < len(partsToSync); j++ {
			if partsToSync[i].partMetadata.ID > partsToSync[j].partMetadata.ID {
				partsToSync[i], partsToSync[j] = partsToSync[j], partsToSync[i]
			}
		}
	}

	nodes := tst.getNodes
	if nodes == nil {
		nodes = func() []string { return nil }
	}
	allNodes := nodes()
	if len(allNodes) == 0 {
		return fmt.Errorf("no nodes to sync parts")
	}

	ctx := context.Background()

	for _, node := range allNodes {
		for _, part := range partsToSync {
			memPart := generateMemPart()
			memPart.mustInitFromPart(part)

			partData, err := memPart.Marshal()
			releaseMemPart(memPart)
			if err != nil {
				return fmt.Errorf("failed to marshal part %d: %w", part.partMetadata.ID, err)
			}

			combinedData := make([]byte, 0, len(partData)+len(tst.group)+4)
			combinedData = encoding.EncodeBytes(combinedData, convert.StringToBytes(tst.group))
			combinedData = encoding.Uint32ToBytes(combinedData, uint32(tst.shardID))
			combinedData = append(combinedData, partData...)

			message := bus.NewMessageWithNode(bus.MessageID(time.Now().UnixNano()), node, combinedData)
			f, publishErr := tst.option.tire2Client.Publish(ctx, data.TopicMeasurePartSync, message)
			if publishErr != nil {
				return fmt.Errorf("failed to publish part %d: %w", part.partMetadata.ID, publishErr)
			}
			if f != nil {
				_, publishErr = f.GetAll()
				if publishErr != nil {
					tst.l.Warn().Err(publishErr).Msgf("failed to get all sync results: %v", publishErr)
				}
			}
		}
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
