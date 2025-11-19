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
)

// FlusherIntroduction represents the introduction of a flusher operation.
type FlusherIntroduction struct {
	flushed map[uint64]*partWrapper
}

// Release releases the FlusherIntroduction back to the pool.
func (i *FlusherIntroduction) Release() {
	releaseFlusherIntroduction(i)
}

func (i *FlusherIntroduction) reset() {
	for k := range i.flushed {
		delete(i.flushed, k)
	}
}

var flusherIntroductionPool = pool.Register[*FlusherIntroduction]("sidx-flusher-introduction")

func generateFlusherIntroduction() *FlusherIntroduction {
	v := flusherIntroductionPool.Get()
	if v == nil {
		return &FlusherIntroduction{
			flushed: make(map[uint64]*partWrapper),
		}
	}
	fi := v
	fi.reset()
	return fi
}

func releaseFlusherIntroduction(i *FlusherIntroduction) {
	flusherIntroductionPool.Put(i)
}

// MergerIntroduction represents the introduction of a merger operation.
type MergerIntroduction struct {
	merged  map[uint64]struct{}
	newPart *partWrapper
}

// Release releases the MergerIntroduction back to the pool.
func (i *MergerIntroduction) Release() {
	releaseMergerIntroduction(i)
}

func (i *MergerIntroduction) reset() {
	for k := range i.merged {
		delete(i.merged, k)
	}
	i.newPart = nil
}

var mergerIntroductionPool = pool.Register[*MergerIntroduction]("sidx-merger-introduction")

func generateMergerIntroduction() *MergerIntroduction {
	v := mergerIntroductionPool.Get()
	if v == nil {
		return &MergerIntroduction{
			merged: make(map[uint64]struct{}),
		}
	}
	mi := v
	mi.reset()
	return mi
}

func releaseMergerIntroduction(i *MergerIntroduction) {
	mergerIntroductionPool.Put(i)
}

func (s *sidx) IntroduceMemPart(partID uint64, memPart *memPart) {
	memPart.partMetadata.ID = partID
	cur := s.currentSnapshot()
	if cur != nil {
		defer cur.decRef()
	} else {
		cur = &snapshot{}
	}

	nextSnp := cur.copyAllTo()

	// Convert memPart to part and wrap it
	part := openMemPart(memPart)
	pw := newPartWrapper(memPart, part)
	nextSnp.parts = append(nextSnp.parts, pw)

	s.replaceSnapshot(nextSnp)
}

func (s *sidx) IntroduceFlushed(nextIntroduction *FlusherIntroduction) {
	cur := s.currentSnapshot()
	if cur == nil {
		s.l.Panic().Msg("current snapshot is nil")
	}
	defer cur.decRef()
	nextSnp := cur.merge(nextIntroduction.flushed)
	s.replaceSnapshot(nextSnp)
}

func (s *sidx) IntroduceMerged(nextIntroduction *MergerIntroduction) func() {
	cur := s.currentSnapshot()
	if cur == nil {
		s.l.Panic().Msg("current snapshot is nil")
		return nil
	}
	nextSnp := cur.remove(nextIntroduction.merged)

	// Wrap the new part
	nextSnp.parts = append(nextSnp.parts, nextIntroduction.newPart)

	s.replaceSnapshot(nextSnp)
	return cur.decRef
}

func (s *sidx) IntroduceSynced(partIDsToSync map[uint64]struct{}) func() {
	cur := s.currentSnapshot()
	if cur == nil {
		s.l.Panic().Msg("current snapshot is nil")
		return nil
	}
	nextSnp := cur.remove(partIDsToSync)
	s.replaceSnapshot(nextSnp)
	return cur.decRef
}

func (s *sidx) replaceSnapshot(next *snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot != nil {
		s.snapshot.decRef()
	}
	s.snapshot = next
}
