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
	"errors"
	"math"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

const flusherWaitTimeout = 5 * time.Second

func (tst *tsTable) flusherLoop(flushCh chan *flusherIntroduction, mergeCh chan *mergerIntroduction, introducerWatcher, flusherWatcher watcher.Channel, epoch uint64) {
	defer tst.loopCloser.Done()
	epochWatcher := introducerWatcher.Add(0, tst.loopCloser.CloseNotify())
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
			curSnapshot := tst.currentSnapshot()
			if curSnapshot != nil {
				epoch, flusherWatchers = tst.pauseFlusherToPileupMemParts(epoch, flusherWatcher, introducerWatcher, flusherWatchers)
				curSnapshot.decRef()
				curSnapshot = nil
			}
			tst.RLock()
			if tst.snapshot != nil && tst.snapshot.epoch > epoch {
				curSnapshot = tst.snapshot
				curSnapshot.incRef()
			}
			tst.RUnlock()
			if curSnapshot != nil {
				newSnapshot, err := tst.mergeMemParts(curSnapshot, mergeCh)
				if err != nil {
					tst.l.Logger.Warn().Err(err).Msgf("cannot merge snapshot: %d", curSnapshot.epoch)
					curSnapshot.decRef()
					continue
				}
				if newSnapshot != nil {
					epoch = tst.persistSnapshot(newSnapshot)
				} else {
					epoch = tst.flush(curSnapshot, flushCh)
					if epoch == 0 {
						curSnapshot.decRef()
						return
					}
					tst.persistSnapshot(curSnapshot)
				}
				// Notify merger to start a new round of merge.
				// This round might have be triggered in pauseFlusherToPileupMemParts.
				flusherWatchers.Notify(math.MaxUint64)
				flusherWatcher = nil
				curSnapshot.decRef()
				if tst.currentEpoch() != epoch {
					continue
				}
			}
			epochWatcher = introducerWatcher.Add(epoch, tst.loopCloser.CloseNotify())
			if epochWatcher == nil {
				return
			}
			tst.gc.clean()
		}
	}
}

// pauseFlusherToPileupMemParts takes a pause to wait for in-memory parts to pile up.
// If there is no in-memory part, we can skip the pause.
// When a merging is finished, we can skip the pause.
func (tst *tsTable) pauseFlusherToPileupMemParts(epoch uint64, flushWatcher, introducerWatcher watcher.Channel, flusherWatchers watcher.Epochs) (uint64, watcher.Epochs) {
	curSnapshot := tst.currentSnapshot()
	if curSnapshot == nil {
		return epoch, flusherWatchers
	}
	if curSnapshot.creator != snapshotCreatorMemPart {
		curSnapshot.decRef()
		return epoch, flusherWatchers
	}
	curSnapshot.decRef()
	select {
	case <-tst.loopCloser.CloseNotify():
	case <-time.After(flusherWaitTimeout):
	case e := <-flushWatcher:
		flusherWatchers.Add(e)
		epoch = e.Val()
		flusherWatchers.Notify(epoch)
	}
	return epoch, flusherWatchers
}

func (tst *tsTable) mergeMemParts(snp *snapshot, mergeCh chan *mergerIntroduction) (*snapshot, error) {
	var memParts []*partWrapper
	var remainingParts []*partWrapper
	remainingIDs := make(map[uint64]struct{})
	for i := range snp.parts {
		if snp.parts[i].mp != nil {
			memParts = append(memParts, snp.parts[i])
			continue
		}
		remainingParts = append(remainingParts, snp.parts[i])
		remainingIDs[snp.parts[i].ID()] = struct{}{}
	}
	if len(memParts) < 2 {
		return nil, nil
	}
	// merge memory must not be closed by the tsTable.close
	closeCh := make(chan struct{})
	newPart, err := tst.mergePartsThenSendIntroduction(memParts, remainingIDs, mergeCh, closeCh)
	close(closeCh)
	if err != nil {
		if errors.Is(err, errClosed) {
			return &snapshot{
				epoch:   snp.epoch,
				parts:   append(remainingParts, newPart),
				creator: snapshotCreatorMergedFlusher,
				ref:     1,
			}, nil
		}
		return nil, err
	}
	if newPart == nil {
		return nil, nil
	}
	return &snapshot{
		epoch:   snp.epoch,
		parts:   append(remainingParts, newPart),
		creator: snapshotCreatorMergedFlusher,
		ref:     1,
	}, nil
}

func (tst *tsTable) flush(snapshot *snapshot, flushCh chan *flusherIntroduction) uint64 {
	ind := generateFlusherIntroduction()
	defer releaseFlusherIntroduction(ind)
	for _, pw := range snapshot.parts {
		if pw.mp == nil || pw.mp.partMetadata.TotalCount < 1 {
			continue
		}
		partPath := partPath(tst.root, pw.ID())
		pw.mp.mustFlush(tst.fileSystem, partPath)
		newPW := newPartWrapper(nil, mustOpenFilePart(pw.ID(), tst.root, tst.fileSystem))
		newPW.p.partMetadata.ID = pw.ID()
		ind.flushed[newPW.ID()] = newPW
	}
	if len(ind.flushed) < 1 {
		return 0
	}
	ind.applied = make(chan struct{})
	select {
	case flushCh <- ind:
	case <-tst.loopCloser.CloseNotify():
		return snapshot.epoch
	}
	select {
	case <-ind.applied:
		return snapshot.epoch
	case <-tst.loopCloser.CloseNotify():
		return snapshot.epoch
	}
}

func (tst *tsTable) persistSnapshot(snapshot *snapshot) uint64 {
	var partNames []string
	for i := range snapshot.parts {
		partNames = append(partNames, partName(snapshot.parts[i].ID()))
	}
	tst.mustWriteSnapshot(snapshot.epoch, partNames)
	tst.gc.registerSnapshot(snapshot)
	return snapshot.epoch
}
