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
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

type introduction struct {
	memPart *partWrapper
	applied chan struct{}
}

func (i *introduction) reset() {
	i.memPart = nil
	i.applied = nil
}

var introductionPool = pool.Register[*introduction]("trace-introduction")

func generateIntroduction() *introduction {
	v := introductionPool.Get()
	if v == nil {
		return &introduction{}
	}
	intro := v
	intro.reset()
	return intro
}

func releaseIntroduction(i *introduction) {
	introductionPool.Put(i)
}

type flusherIntroduction struct {
	flushed map[uint64]*partWrapper
	applied chan struct{}
}

func (i *flusherIntroduction) reset() {
	for k := range i.flushed {
		delete(i.flushed, k)
	}
	i.applied = nil
}

var flusherIntroductionPool = pool.Register[*flusherIntroduction]("trace-flusher-introduction")

func generateFlusherIntroduction() *flusherIntroduction {
	v := flusherIntroductionPool.Get()
	if v == nil {
		return &flusherIntroduction{
			flushed: make(map[uint64]*partWrapper),
		}
	}
	fi := v
	fi.reset()
	return fi
}

func releaseFlusherIntroduction(i *flusherIntroduction) {
	flusherIntroductionPool.Put(i)
}

type mergerIntroduction struct {
	merged  map[uint64]struct{}
	newPart *partWrapper
	applied chan struct{}
	creator snapshotCreator
}

func (i *mergerIntroduction) reset() {
	for k := range i.merged {
		delete(i.merged, k)
	}
	i.newPart = nil
	i.applied = nil
	i.creator = 0
}

var mergerIntroductionPool = pool.Register[*mergerIntroduction]("trace-merger-introduction")

func generateMergerIntroduction() *mergerIntroduction {
	v := mergerIntroductionPool.Get()
	if v == nil {
		return &mergerIntroduction{}
	}
	mi := v
	mi.reset()
	return mi
}

func releaseMergerIntroduction(i *mergerIntroduction) {
	mergerIntroductionPool.Put(i)
}

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

var syncIntroductionPool = pool.Register[*syncIntroduction]("trace-sync-introduction")

func generateSyncIntroduction() *syncIntroduction {
	v := syncIntroductionPool.Get()
	if v == nil {
		return &syncIntroduction{
			synced: make(map[uint64]struct{}),
		}
	}
	si := v
	si.reset()
	return si
}

func releaseSyncIntroduction(i *syncIntroduction) {
	syncIntroductionPool.Put(i)
}

func (tst *tsTable) introducerLoop(flushCh chan *flusherIntroduction, mergeCh chan *mergerIntroduction, watcherCh watcher.Channel, epoch uint64) {
	var introducerWatchers watcher.Epochs
	defer tst.loopCloser.Done()
	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case next := <-tst.introductions:
			tst.incTotalIntroduceLoopStarted("mem")
			tst.introduceMemPart(next, epoch)
			tst.incTotalIntroduceLoopFinished("mem")
			epoch++
		case next := <-flushCh:
			tst.incTotalIntroduceLoopStarted("flush")
			tst.introduceFlushed(next, epoch)
			tst.incTotalIntroduceLoopFinished("flush")
			tst.gc.clean()
			epoch++
		case next := <-mergeCh:
			tst.incTotalIntroduceLoopStarted("merge")
			tst.introduceMerged(next, epoch)
			tst.incTotalIntroduceLoopFinished("merge")
			tst.gc.clean()
			epoch++
		case epochWatcher := <-watcherCh:
			introducerWatchers.Add(epochWatcher)
		}
		curEpoch := tst.currentEpoch()
		introducerWatchers.Notify(curEpoch)
	}
}

func (tst *tsTable) introducerLoopWithSync(flushCh chan *flusherIntroduction, syncCh chan *syncIntroduction, watcherCh watcher.Channel, epoch uint64) {
	var introducerWatchers watcher.Epochs
	defer tst.loopCloser.Done()
	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case next := <-tst.introductions:
			tst.incTotalIntroduceLoopStarted("mem")
			tst.introduceMemPart(next, epoch)
			tst.incTotalIntroduceLoopFinished("mem")
			epoch++
		case next := <-flushCh:
			tst.incTotalIntroduceLoopStarted("flush")
			tst.introduceFlushed(next, epoch)
			tst.incTotalIntroduceLoopFinished("flush")
			tst.gc.clean()
			epoch++
		case next := <-syncCh:
			tst.incTotalIntroduceLoopStarted("sync")
			tst.introduceSync(next, epoch)
			tst.incTotalIntroduceLoopFinished("sync")
			tst.gc.clean()
			epoch++
		case epochWatcher := <-watcherCh:
			introducerWatchers.Add(epochWatcher)
		}
		curEpoch := tst.currentEpoch()
		introducerWatchers.Notify(curEpoch)
	}
}

func (tst *tsTable) introduceMemPart(nextIntroduction *introduction, epoch uint64) {
	cur := tst.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
	} else {
		cur = new(snapshot)
	}

	next := nextIntroduction.memPart
	nextSnp := cur.copyAllTo(epoch)
	nextSnp.parts = append(nextSnp.parts, next)
	nextSnp.creator = snapshotCreatorMemPart
	tst.replaceSnapshot(&nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceFlushed(nextIntroduction *flusherIntroduction, epoch uint64) {
	cur := tst.currentSnapshot()
	if cur == nil {
		tst.l.Panic().Msg("current snapshot is nil")
	}
	defer cur.decRef()
	nextSnp := cur.merge(epoch, nextIntroduction.flushed)
	nextSnp.creator = snapshotCreatorFlusher
	tst.replaceSnapshot(&nextSnp)
	tst.persistSnapshot(&nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceMerged(nextIntroduction *mergerIntroduction, epoch uint64) {
	cur := tst.currentSnapshot()
	if cur == nil {
		tst.l.Panic().Msg("current snapshot is nil")
		return
	}
	defer cur.decRef()
	nextSnp := cur.remove(epoch, nextIntroduction.merged)
	nextSnp.parts = append(nextSnp.parts, nextIntroduction.newPart)
	nextSnp.creator = nextIntroduction.creator
	tst.replaceSnapshot(&nextSnp)
	tst.persistSnapshot(&nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceSync(nextIntroduction *syncIntroduction, epoch uint64) {
	cur := tst.currentSnapshot()
	if cur == nil {
		tst.l.Panic().Msg("current snapshot is nil")
		return
	}
	defer cur.decRef()
	nextSnp := cur.remove(epoch, nextIntroduction.synced)
	nextSnp.creator = snapshotCreatorSyncer
	tst.replaceSnapshot(&nextSnp)
	tst.persistSnapshot(&nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) replaceSnapshot(next *snapshot) {
	tst.Lock()
	defer tst.Unlock()
	if tst.snapshot != nil {
		tst.snapshot.decRef()
	}
	tst.snapshot = next
}

func (tst *tsTable) currentEpoch() uint64 {
	s := tst.currentSnapshot()
	if s == nil {
		return 0
	}
	defer s.decRef()
	return s.epoch
}
