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
	"errors"
	"math"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func (tst *tsTable) flusherLoop(flushCh chan *flusherIntroduction, mergeCh chan *mergerIntroduction, introducerWatcher, flusherWatcher watcher.Channel, epoch uint64) {
	defer tst.loopCloser.Done()
	epochWatcher := introducerWatcher.Add(epoch, tst.loopCloser.CloseNotify())
	if epochWatcher == nil {
		return
	}
	var flusherWatchers watcher.Epochs

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case e := <-flusherWatcher:
			flusherWatchers.Add(e)
		case <-epochWatcher.Watch():
			if func() bool {
				tst.incTotalFlushLoopStarted(1)
				start := time.Now()
				defer func() {
					tst.incTotalFlushLoopFinished(1)
					tst.incTotalFlushLatency(time.Since(start).Seconds())
				}()
				curSnapshot := tst.currentSnapshot()
				var ok bool
				var merged bool
				curSnapshot, flusherWatchers, ok, merged = tst.pauseFlusherToPileupMemPartsWithMerge(curSnapshot, flusherWatcher, flusherWatchers, epoch, mergeCh)
				if curSnapshot != nil {
					defer curSnapshot.decRef()
					if !ok {
						return false
					}
					if !merged {
						tst.flush(curSnapshot, flushCh)
					}
					flusherWatchers.Notify(math.MaxUint64)
					flusherWatchers = nil
					epoch = curSnapshot.epoch
					if tst.currentEpoch() != epoch {
						tst.incTotalFlushLoopProgress(1)
						return false
					}
				}
				epochWatcher = introducerWatcher.Add(epoch, tst.loopCloser.CloseNotify())
				return epochWatcher == nil
			}() {
				return
			}
		}
	}
}

func (tst *tsTable) pauseFlusherToPileupMemPartsWithMerge(
	curSnapshot *snapshot, flusherWatcher watcher.Channel, flusherWatchers watcher.Epochs,
	epoch uint64, mergeCh chan *mergerIntroduction,
) (*snapshot, watcher.Epochs, bool, bool) {
	if tst.option.flushTimeout < 1 {
		return curSnapshot, flusherWatchers, true, false
	}
	if curSnapshot != nil {
		flusherWatchers = tst.pauseFlusherToPileupMemParts(epoch, flusherWatcher, flusherWatchers)
		curSnapshot.decRef()
		curSnapshot = nil
	}
	tst.RLock()
	if tst.snapshot != nil && tst.snapshot.epoch > epoch {
		curSnapshot = tst.snapshot
		curSnapshot.incRef()
	}
	tst.RUnlock()
	if curSnapshot == nil {
		return nil, flusherWatchers, false, false
	}
	merged, err := tst.mergeMemParts(curSnapshot, mergeCh)
	if err != nil {
		tst.l.Logger.Warn().Err(err).Msgf("cannot merge snapshot: %d", curSnapshot.epoch)
		tst.incTotalFlushLoopErr(1)
		return curSnapshot, flusherWatchers, false, false
	}
	return curSnapshot, flusherWatchers, true, merged
}

// pauseFlusherToPileupMemParts takes a pause to wait for in-memory parts to pile up.
// If there is no in-memory part, we can skip the pause.
// When a merging is finished, we can skip the pause.
func (tst *tsTable) pauseFlusherToPileupMemParts(epoch uint64, flushWatcher watcher.Channel, flusherWatchers watcher.Epochs) watcher.Epochs {
	curSnapshot := tst.currentSnapshot()
	if curSnapshot == nil {
		return flusherWatchers
	}
	curSnapshot.decRef()
	flusherWatchers.Notify(epoch)
	select {
	case <-tst.loopCloser.CloseNotify():
	case <-time.After(tst.option.flushTimeout):
		tst.incTotalFlushPauseCompleted(1)
	case e := <-flushWatcher:
		flusherWatchers.Add(e)
		flusherWatchers.Notify(epoch)
		tst.incTotalFlushPauseBreak(1)
	}
	return flusherWatchers
}

func (tst *tsTable) mergeMemParts(snp *snapshot, mergeCh chan *mergerIntroduction) (bool, error) {
	var memParts []*partWrapper
	mergedIDs := make(map[uint64]struct{})
	for i := range snp.parts {
		if snp.parts[i].mp != nil {
			memParts = append(memParts, snp.parts[i])
			mergedIDs[snp.parts[i].ID()] = struct{}{}
			continue
		}
	}
	if len(memParts) < 2 {
		return false, nil
	}
	// merge memory must not be closed by the tsTable.close
	closeCh := make(chan struct{})
	newPart, err := tst.mergePartsThenSendIntroduction(snapshotCreatorMergedFlusher, memParts,
		mergedIDs, mergeCh, closeCh, "mem")
	close(closeCh)
	if err != nil {
		if errors.Is(err, errClosed) {
			return true, nil
		}
		return false, err
	}
	if newPart == nil {
		return false, nil
	}
	return true, nil
}

func (tst *tsTable) flush(snapshot *snapshot, flushCh chan *flusherIntroduction) {
	ind := generateFlusherIntroduction()
	defer releaseFlusherIntroduction(ind)
	start := time.Now()
	partsCount := 0
	for _, pw := range snapshot.parts {
		if pw.mp == nil || pw.mp.partMetadata.TotalCount < 1 {
			continue
		}
		partsCount++
		partPath := partPath(tst.root, pw.ID())
		pw.mp.mustFlush(tst.fileSystem, partPath)
		newPW := newPartWrapper(nil, mustOpenFilePart(pw.ID(), tst.root, tst.fileSystem))
		newPW.p.partMetadata.ID = pw.ID()
		ind.flushed[newPW.ID()] = newPW
	}
	if len(ind.flushed) < 1 {
		return
	}
	end := time.Now()
	tst.incTotalFlushed(1)
	tst.incTotalFlushedMemParts(partsCount)
	tst.incTotalFlushLatency(end.Sub(start).Seconds())
	ind.applied = make(chan struct{})
	select {
	case flushCh <- ind:
	case <-tst.loopCloser.CloseNotify():
		return
	}
	select {
	case <-ind.applied:
	case <-tst.loopCloser.CloseNotify():
	}
	tst.incTotalFlushIntroLatency(time.Since(end).Seconds())
}

func (tst *tsTable) persistSnapshot(snapshot *snapshot) {
	var partNames []string
	for i := range snapshot.parts {
		partNames = append(partNames, partName(snapshot.parts[i].ID()))
	}
	tst.mustWriteSnapshot(snapshot.epoch, partNames)
	tst.gc.registerSnapshot(snapshot)
}
