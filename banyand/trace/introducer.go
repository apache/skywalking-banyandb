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
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	snapshotpkg "github.com/apache/skywalking-banyandb/banyand/internal/snapshot"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

type introduction struct {
	memPart     *partWrapper
	applied     chan struct{}
	sidxReqsMap map[string]*sidx.MemPart
}

func (i *introduction) reset() {
	i.memPart = nil
	i.applied = nil
	i.sidxReqsMap = nil
}

var introductionPool = pool.Register[*introduction]("trace-introduction")

func generateIntroduction() *introduction {
	v := introductionPool.Get()
	if v == nil {
		return &introduction{
			sidxReqsMap: make(map[string]*sidx.MemPart),
		}
	}
	intro := v
	intro.reset()
	return intro
}

func releaseIntroduction(i *introduction) {
	introductionPool.Put(i)
}

type flusherIntroduction struct {
	flushed               map[uint64]*partWrapper
	sidxFlusherIntroduced map[string]*sidx.FlusherIntroduction
	applied               chan struct{}
}

func (i *flusherIntroduction) reset() {
	for k := range i.flushed {
		delete(i.flushed, k)
	}
	for k := range i.sidxFlusherIntroduced {
		delete(i.sidxFlusherIntroduced, k)
	}
	i.applied = nil
}

var flusherIntroductionPool = pool.Register[*flusherIntroduction]("trace-flusher-introduction")

func generateFlusherIntroduction() *flusherIntroduction {
	v := flusherIntroductionPool.Get()
	if v == nil {
		return &flusherIntroduction{
			flushed:               make(map[uint64]*partWrapper),
			sidxFlusherIntroduced: make(map[string]*sidx.FlusherIntroduction),
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
	merged               map[uint64]struct{}
	newPart              *partWrapper
	sidxMergerIntroduced map[string]*sidx.MergerIntroduction
	applied              chan struct{}
	creator              snapshotCreator
}

func (i *mergerIntroduction) reset() {
	i.merged = nil
	i.sidxMergerIntroduced = nil
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

func (tst *tsTable) introducerLoopWithSync(flushCh chan *flusherIntroduction, mergeCh chan *mergerIntroduction,
	syncCh chan *syncIntroduction, watcherCh watcher.Channel, epoch uint64,
) {
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
			tst.introduceFlushedForSync(next, epoch)
			tst.incTotalIntroduceLoopFinished("flush")
			tst.gc.clean()
			epoch++
		case next := <-mergeCh:
			tst.incTotalIntroduceLoopStarted("merge")
			tst.introduceMerged(next, epoch)
			tst.incTotalIntroduceLoopFinished("merge")
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
	// Create generic transaction
	txn := snapshotpkg.NewTransaction()
	defer txn.Release()

	// Prepare trace snapshot transition
	next := nextIntroduction.memPart
	tst.addPendingDataCount(-int64(next.mp.partMetadata.TotalCount))
	partID := next.p.partMetadata.ID

	traceTransition := snapshotpkg.NewTransition(tst, func(cur *snapshot) *snapshot {
		if cur == nil {
			cur = new(snapshot)
		}
		nextSnp := cur.copyAllTo(epoch)
		nextSnp.parts = append(nextSnp.parts, next)
		nextSnp.creator = snapshotCreatorMemPart
		return &nextSnp
	})
	defer traceTransition.Release()
	snapshotpkg.AddTransition(txn, traceTransition)

	// Prepare sidx snapshot transitions
	var sidxTransitions []*snapshotpkg.Transition[*sidx.Snapshot]
	for name, memPart := range nextIntroduction.sidxReqsMap {
		sidxInstance := tst.mustGetOrCreateSidx(name)
		prepareFunc := sidxInstance.PrepareMemPart(partID, memPart)
		sidxTransition := snapshotpkg.NewTransition(sidxInstance, prepareFunc)
		sidxTransitions = append(sidxTransitions, sidxTransition)
		snapshotpkg.AddTransition(txn, sidxTransition)
	}
	defer func() {
		for _, t := range sidxTransitions {
			t.Release()
		}
	}()

	// Commit all atomically under single transaction lock
	txn.Commit()

	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceFlushed(nextIntroduction *flusherIntroduction, epoch uint64) {
	// Create generic transaction
	txn := snapshotpkg.NewTransaction()
	defer txn.Release()

	// Prepare trace snapshot transition
	traceTransition := snapshotpkg.NewTransition(tst, func(cur *snapshot) *snapshot {
		if cur == nil {
			tst.l.Panic().Msg("current snapshot is nil")
		}
		nextSnp := cur.merge(epoch, nextIntroduction.flushed)
		nextSnp.creator = snapshotCreatorFlusher
		return &nextSnp
	})
	defer traceTransition.Release()
	snapshotpkg.AddTransition(txn, traceTransition)

	// Prepare sidx snapshot transitions
	var sidxTransitions []*snapshotpkg.Transition[*sidx.Snapshot]
	for name, sidxFlusherIntroduced := range nextIntroduction.sidxFlusherIntroduced {
		sidxInstance := tst.mustGetSidx(name)
		prepareFunc := sidxInstance.PrepareFlushed(sidxFlusherIntroduced)
		sidxTransition := snapshotpkg.NewTransition(sidxInstance, prepareFunc)
		sidxTransitions = append(sidxTransitions, sidxTransition)
		snapshotpkg.AddTransition(txn, sidxTransition)
	}
	defer func() {
		for _, t := range sidxTransitions {
			t.Release()
		}
	}()

	// Commit all atomically under single transaction lock
	txn.Commit()

	// Persist snapshot after commit
	cur := tst.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
		tst.persistSnapshot(cur)
	}

	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

// introduceFlushedForSync introduces the flushed trace parts for syncing.
// The snapshots are updated atomically so the syncer can always find
// the corresponding index once a flushed trace part becomes visible.
func (tst *tsTable) introduceFlushedForSync(nextIntroduction *flusherIntroduction, epoch uint64) {
	// Create generic transaction
	txn := snapshotpkg.NewTransaction()
	defer txn.Release()

	// Prepare sidx snapshot transitions first for visibility ordering
	var sidxTransitions []*snapshotpkg.Transition[*sidx.Snapshot]
	for name, sidxFlusherIntroduced := range nextIntroduction.sidxFlusherIntroduced {
		sidxInstance := tst.mustGetSidx(name)
		prepareFunc := sidxInstance.PrepareFlushed(sidxFlusherIntroduced)
		sidxTransition := snapshotpkg.NewTransition(sidxInstance, prepareFunc)
		sidxTransitions = append(sidxTransitions, sidxTransition)
		snapshotpkg.AddTransition(txn, sidxTransition)
	}
	defer func() {
		for _, t := range sidxTransitions {
			t.Release()
		}
	}()

	// Prepare trace snapshot transition
	traceTransition := snapshotpkg.NewTransition(tst, func(cur *snapshot) *snapshot {
		if cur == nil {
			tst.l.Panic().Msg("current snapshot is nil")
		}
		nextSnp := cur.merge(epoch, nextIntroduction.flushed)
		nextSnp.creator = snapshotCreatorFlusher
		return &nextSnp
	})
	defer traceTransition.Release()
	snapshotpkg.AddTransition(txn, traceTransition)

	// Commit all atomically under single transaction lock
	txn.Commit()

	// Persist snapshot after commit
	cur := tst.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
		tst.persistSnapshot(cur)
	}

	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceMerged(nextIntroduction *mergerIntroduction, epoch uint64) {
	// Create generic transaction
	txn := snapshotpkg.NewTransaction()
	defer txn.Release()

	// Prepare trace snapshot transition
	traceTransition := snapshotpkg.NewTransition(tst, func(cur *snapshot) *snapshot {
		if cur == nil {
			tst.l.Panic().Msg("current snapshot is nil")
		}
		nextSnp := cur.remove(epoch, nextIntroduction.merged)
		nextSnp.parts = append(nextSnp.parts, nextIntroduction.newPart)
		nextSnp.creator = nextIntroduction.creator
		return &nextSnp
	})
	defer traceTransition.Release()
	snapshotpkg.AddTransition(txn, traceTransition)

	// Prepare sidx snapshot transitions
	var sidxTransitions []*snapshotpkg.Transition[*sidx.Snapshot]
	for name, sidxMergerIntroduced := range nextIntroduction.sidxMergerIntroduced {
		sidxInstance := tst.mustGetSidx(name)
		prepareFunc := sidxInstance.PrepareMerged(sidxMergerIntroduced)
		sidxTransition := snapshotpkg.NewTransition(sidxInstance, prepareFunc)
		sidxTransitions = append(sidxTransitions, sidxTransition)
		snapshotpkg.AddTransition(txn, sidxTransition)
	}
	defer func() {
		for _, t := range sidxTransitions {
			t.Release()
		}
	}()

	// Commit all atomically under single transaction lock
	txn.Commit()

	// Persist snapshot after commit
	cur := tst.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
		tst.persistSnapshot(cur)
	}

	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (tst *tsTable) introduceSync(nextIntroduction *syncIntroduction, epoch uint64) {
	synced := nextIntroduction.synced

	// Create generic transaction
	txn := snapshotpkg.NewTransaction()
	defer txn.Release()

	// Prepare sidx snapshot transitions
	var sidxTransitions []*snapshotpkg.Transition[*sidx.Snapshot]
	allSidx := tst.getAllSidx()
	for _, sidxInstance := range allSidx {
		prepareFunc := sidxInstance.PrepareSynced(synced)
		sidxTransition := snapshotpkg.NewTransition(sidxInstance, prepareFunc)
		sidxTransitions = append(sidxTransitions, sidxTransition)
		snapshotpkg.AddTransition(txn, sidxTransition)
	}
	defer func() {
		for _, t := range sidxTransitions {
			t.Release()
		}
	}()

	// Prepare trace snapshot transition
	traceTransition := snapshotpkg.NewTransition(tst, func(cur *snapshot) *snapshot {
		if cur == nil {
			tst.l.Panic().Msg("current snapshot is nil")
		}
		nextSnp := cur.remove(epoch, synced)
		nextSnp.creator = snapshotCreatorSyncer
		return &nextSnp
	})
	defer traceTransition.Release()
	snapshotpkg.AddTransition(txn, traceTransition)

	// Commit all atomically under single transaction lock
	txn.Commit()

	// Persist snapshot after commit
	cur := tst.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
		tst.persistSnapshot(cur)
	}

	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

// CurrentSnapshot returns the current snapshot with incremented reference count.
// Implements snapshot.Manager[*snapshot] interface.
func (tst *tsTable) CurrentSnapshot() *snapshot {
	tst.RLock()
	defer tst.RUnlock()
	if tst.snapshot == nil {
		return nil
	}
	tst.snapshot.IncRef()
	return tst.snapshot
}

// ReplaceSnapshot atomically replaces the current snapshot with next.
// Implements snapshot.Manager[*snapshot] interface.
// The old snapshot's DecRef is called automatically per the Manager contract.
func (tst *tsTable) ReplaceSnapshot(next *snapshot) {
	tst.Lock()
	defer tst.Unlock()
	if tst.snapshot != nil {
		tst.snapshot.DecRef()
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
