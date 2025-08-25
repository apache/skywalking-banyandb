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

package sidx

import (
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

type introduction struct {
	memPart *memPart
	applied chan struct{}
}

func (i *introduction) reset() {
	i.memPart = nil
	i.applied = nil
}

var introductionPool = pool.Register[*introduction]("sidx-introduction")

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
	flushed map[uint64]*part
	applied chan struct{}
}

func (i *flusherIntroduction) reset() {
	for k := range i.flushed {
		delete(i.flushed, k)
	}
	i.applied = nil
}

var flusherIntroductionPool = pool.Register[*flusherIntroduction]("sidx-flusher-introduction")

func generateFlusherIntroduction() *flusherIntroduction {
	v := flusherIntroductionPool.Get()
	if v == nil {
		return &flusherIntroduction{
			flushed: make(map[uint64]*part),
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
	newPart *part
	applied chan struct{}
}

func (i *mergerIntroduction) reset() {
	for k := range i.merged {
		delete(i.merged, k)
	}
	i.newPart = nil
	i.applied = nil
}

var mergerIntroductionPool = pool.Register[*mergerIntroduction]("sidx-merger-introduction")

func generateMergerIntroduction() *mergerIntroduction {
	v := mergerIntroductionPool.Get()
	if v == nil {
		return &mergerIntroduction{
			merged: make(map[uint64]struct{}),
		}
	}
	mi := v
	mi.reset()
	return mi
}

func releaseMergerIntroduction(i *mergerIntroduction) {
	mergerIntroductionPool.Put(i)
}

func (s *sidx) introducerLoop(flushCh chan *flusherIntroduction, mergeCh chan *mergerIntroduction, watcherCh watcher.Channel, epoch uint64) {
	var introducerWatchers watcher.Epochs
	defer s.loopCloser.Done()
	for {
		select {
		case <-s.loopCloser.CloseNotify():
			return
		case next := <-s.introductions:
			s.incTotalIntroduceLoopStarted("mem")
			s.introduceMemPart(next, epoch)
			s.incTotalIntroduceLoopFinished("mem")
			epoch++
		case next := <-flushCh:
			s.incTotalIntroduceLoopStarted("flush")
			s.introduceFlushed(next, epoch)
			s.incTotalIntroduceLoopFinished("flush")
			s.gc.clean()
			epoch++
		case next := <-mergeCh:
			s.incTotalIntroduceLoopStarted("merge")
			s.introduceMerged(next, epoch)
			s.incTotalIntroduceLoopFinished("merge")
			s.gc.clean()
			epoch++
		case epochWatcher := <-watcherCh:
			introducerWatchers.Add(epochWatcher)
		}
		curEpoch := s.currentEpoch()
		introducerWatchers.Notify(curEpoch)
	}
}

func (s *sidx) introduceMemPart(nextIntroduction *introduction, epoch uint64) {
	cur := s.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
	} else {
		cur = generateSnapshot()
	}

	next := nextIntroduction.memPart
	nextSnp := cur.copyAllTo(epoch)

	// Convert memPart to part and wrap it
	part := openMemPart(next)
	pw := newPartWrapper(part)
	nextSnp.parts = append(nextSnp.parts, pw)

	s.replaceSnapshot(nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (s *sidx) introduceFlushed(nextIntroduction *flusherIntroduction, epoch uint64) {
	cur := s.currentSnapshot()
	if cur == nil {
		s.l.Panic().Msg("current snapshot is nil")
	}
	defer cur.decRef()
	nextSnp := cur.merge(epoch, nextIntroduction.flushed)
	s.replaceSnapshot(nextSnp)
	s.persistSnapshot(nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (s *sidx) introduceMerged(nextIntroduction *mergerIntroduction, epoch uint64) {
	cur := s.currentSnapshot()
	if cur == nil {
		s.l.Panic().Msg("current snapshot is nil")
		return
	}
	defer cur.decRef()
	nextSnp := cur.remove(epoch, nextIntroduction.merged)

	// Wrap the new part
	pw := newPartWrapper(nextIntroduction.newPart)
	nextSnp.parts = append(nextSnp.parts, pw)

	s.replaceSnapshot(nextSnp)
	s.persistSnapshot(nextSnp)
	if nextIntroduction.applied != nil {
		close(nextIntroduction.applied)
	}
}

func (s *sidx) replaceSnapshot(next *snapshot) {
	s.Lock()
	defer s.Unlock()
	if s.snapshot != nil {
		s.snapshot.decRef()
	}
	s.snapshot = next
}

func (s *sidx) currentEpoch() uint64 {
	snap := s.currentSnapshot()
	if snap == nil {
		return 0
	}
	defer snap.decRef()
	return snap.epoch
}
