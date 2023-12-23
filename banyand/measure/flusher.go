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
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

func (tst *tsTable) flusherLoop(flushCh chan *flusherIntroduction, introducerWatcher watcher.Channel, epoch uint64) {
	defer tst.loopCloser.Done()
	epochWatcher := introducerWatcher.Add(0, tst.loopCloser.CloseNotify())

	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case <-epochWatcher.Watch():
			var curSnapshot *snapshot
			tst.Lock()
			if tst.snapshot != nil && tst.snapshot.epoch > epoch {
				curSnapshot = tst.snapshot
				curSnapshot.incRef()
			}
			tst.Unlock()
			if curSnapshot != nil {
				epoch = tst.flush(curSnapshot, flushCh)
				if epoch == 0 {
					return
				}
				tst.persistSnapshot(curSnapshot)
				if tst.currentEpoch() != epoch {
					continue
				}
			}
			epochWatcher = introducerWatcher.Add(epoch, tst.loopCloser.CloseNotify())
			tst.gc.clean()
		}
	}
}

func (tst *tsTable) flush(snapshot *snapshot, flushCh chan *flusherIntroduction) uint64 {
	ind := generateFlusherIntroduction()
	for _, pw := range snapshot.parts {
		if pw.mp == nil || pw.mp.partMetadata.TotalCount < 1 {
			continue
		}
		partPath := partPath(tst.root, pw.ID())
		pw.mp.mustFlush(tst.fileSystem, partPath)
		newPW := newPartWrapper(nil, mustOpenFilePart(partPath, tst.fileSystem), tst.fileSystem)
		ind.flushed[newPW.ID()] = newPW
	}
	ind.applied = make(chan struct{})
	select {
	case flushCh <- ind:
	case <-tst.loopCloser.CloseNotify():
		return 0
	}
	select {
	case <-ind.applied:
		return snapshot.epoch
	case <-tst.loopCloser.CloseNotify():
		return 0
	}
}

func (tst *tsTable) persistSnapshot(snapshot *snapshot) {
	var partNames []string
	for i := range snapshot.parts {
		partNames = append(partNames, snapshotName(snapshot.parts[i].ID()))
	}
	tst.mustWriteSnapshot(snapshot.epoch, partNames)
	tst.gc.registerSnapshot(snapshot.epoch)
}
