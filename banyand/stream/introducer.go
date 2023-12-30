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
	"sync"

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

var introductionPool = sync.Pool{}

func generateIntroduction() *introduction {
	v := introductionPool.Get()
	if v == nil {
		return &introduction{}
	}
	return v.(*introduction)
}

func releaseIntroduction(i *introduction) {
	i.reset()
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

var flusherIntroductionPool = sync.Pool{}

func generateFlusherIntroduction() *flusherIntroduction {
	v := flusherIntroductionPool.Get()
	if v == nil {
		return &flusherIntroduction{
			flushed: make(map[uint64]*partWrapper),
		}
	}
	return v.(*flusherIntroduction)
}

func releaseFlusherIntroduction(i *flusherIntroduction) {
	i.reset()
	flusherIntroductionPool.Put(i)
}

func (tst *tsTable) introducerLoop(flushCh chan *flusherIntroduction, watcherCh watcher.Channel, epoch uint64) {
	var introducerWatchers watcher.Epochs
	defer tst.loopCloser.Done()
	for {
		select {
		case <-tst.loopCloser.CloseNotify():
			return
		case next := <-tst.introductions:
			tst.introduceMemPart(next, epoch)
			epoch++
		case next := <-flushCh:
			tst.introduceFlushed(next, epoch)
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
	next.p.partMetadata.ID = epoch
	nextSnp.parts = append(nextSnp.parts, next)
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
	tst.replaceSnapshot(&nextSnp)
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