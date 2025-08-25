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
	"context"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/watcher"
)

// sidx implements the SIDX interface with introduction channels for async operations.
type sidx struct {
	snapshot                   *snapshot
	introductions              chan *introduction
	flushCh                    chan *flusherIntroduction
	mergeCh                    chan *mergerIntroduction
	loopCloser                 *run.Closer
	gc                         *gc
	l                          *logger.Logger
	totalIntroduceLoopStarted  atomic.Int64
	totalIntroduceLoopFinished atomic.Int64
	mu                         sync.RWMutex
}

// NewSIDX creates a new SIDX instance with introduction channels.
func NewSIDX(opts *Options) (SIDX, error) {
	if opts == nil {
		opts = NewDefaultOptions()
	}

	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &sidx{
		introductions: make(chan *introduction),
		flushCh:       make(chan *flusherIntroduction),
		mergeCh:       make(chan *mergerIntroduction),
		loopCloser:    run.NewCloser(1),
		l:             logger.GetLogger().Named("sidx"),
	}

	// Initialize garbage collector
	s.gc = newGC(opts)

	// Start introducer loop
	watcherCh := make(watcher.Channel, 10)
	go s.introducerLoop(s.flushCh, s.mergeCh, watcherCh, 0)

	return s, nil
}

// Write implements SIDX interface.
func (s *sidx) Write(ctx context.Context, reqs []WriteRequest) error {
	// Validate requests
	for _, req := range reqs {
		if err := req.Validate(); err != nil {
			return err
		}
	}

	// Create elements from requests
	es := generateElements()
	defer releaseElements(es)

	for _, req := range reqs {
		es.mustAppend(req.SeriesID, req.Key, req.Data, req.Tags)
	}

	// Create memory part from elements
	mp := generateMemPart()
	mp.mustInitFromElements(es)

	// Create introduction
	intro := generateIntroduction()
	intro.memPart = mp
	intro.applied = make(chan struct{})

	// Send to introducer loop
	select {
	case s.introductions <- intro:
		// Wait for introduction to be applied
		<-intro.applied
		releaseIntroduction(intro)
		return nil
	case <-ctx.Done():
		releaseIntroduction(intro)
		releaseMemPart(mp)
		return ctx.Err()
	}
}

// Query implements SIDX interface.
func (s *sidx) Query(ctx context.Context, req QueryRequest) (QueryResult, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return &emptyQueryResult{}, nil
	}

	// Create query result
	qr := &queryResult{
		snapshot: snap,
		request:  req,
		ctx:      ctx,
	}

	return qr, nil
}

// Stats implements SIDX interface.
func (s *sidx) Stats(_ context.Context) (*Stats, error) {
	snap := s.currentSnapshot()
	if snap == nil {
		return &Stats{}, nil
	}
	defer snap.decRef()

	stats := &Stats{
		PartCount: int64(snap.getPartCount()),
	}

	// Load atomic counters
	stats.QueryCount.Store(s.totalIntroduceLoopStarted.Load())
	stats.WriteCount.Store(s.totalIntroduceLoopFinished.Load())

	return stats, nil
}

// Flush implements Flusher interface.
func (s *sidx) Flush() error {
	// Get current memory parts that need flushing
	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	// Create flush introduction
	flushIntro := generateFlusherIntroduction()
	flushIntro.applied = make(chan struct{})

	// Select memory parts to flush (simplified logic)
	for _, pw := range snap.parts {
		if pw.isMemPart() && pw.isActive() {
			flushIntro.flushed[pw.ID()] = pw.p
		}
	}

	if len(flushIntro.flushed) == 0 {
		releaseFlusherIntroduction(flushIntro)
		return nil
	}

	// Send to introducer loop
	s.flushCh <- flushIntro

	// Wait for flush to complete
	<-flushIntro.applied
	releaseFlusherIntroduction(flushIntro)

	return nil
}

// Merge implements Merger interface.
func (s *sidx) Merge() error {
	// Get current snapshot
	snap := s.currentSnapshot()
	if snap == nil {
		return nil
	}
	defer snap.decRef()

	// Create merge introduction
	mergeIntro := generateMergerIntroduction()
	mergeIntro.applied = make(chan struct{})

	// Select parts to merge (simplified logic - merge first 2 parts)
	var partsToMerge []*partWrapper
	for _, pw := range snap.parts {
		if pw.isActive() && !pw.isMemPart() {
			partsToMerge = append(partsToMerge, pw)
			if len(partsToMerge) >= 2 {
				break
			}
		}
	}

	if len(partsToMerge) < 2 {
		releaseMergerIntroduction(mergeIntro)
		return nil
	}

	// Mark parts for merging
	for _, pw := range partsToMerge {
		mergeIntro.merged[pw.ID()] = struct{}{}
	}

	// Create new merged part (simplified - would actually merge the parts)
	mergeIntro.newPart = partsToMerge[0].p

	// Send to introducer loop
	s.mergeCh <- mergeIntro

	// Wait for merge to complete
	<-mergeIntro.applied
	releaseMergerIntroduction(mergeIntro)

	return nil
}

// Close implements SIDX interface.
func (s *sidx) Close() error {
	s.loopCloser.CloseThenWait()

	// Close current snapshot
	s.mu.Lock()
	if s.snapshot != nil {
		s.snapshot.decRef()
		s.snapshot = nil
	}
	s.mu.Unlock()

	return nil
}

// currentSnapshot returns the current snapshot with incremented reference count.
func (s *sidx) currentSnapshot() *snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapshot == nil {
		return nil
	}

	if s.snapshot.acquire() {
		return s.snapshot
	}

	return nil
}

// decRef decrements the snapshot reference count (helper for snapshot interface).
func (s *snapshot) decRef() {
	s.release()
}

// Helper methods for metrics.
func (s *sidx) incTotalIntroduceLoopStarted(_ string) {
	s.totalIntroduceLoopStarted.Add(1)
}

func (s *sidx) incTotalIntroduceLoopFinished(_ string) {
	s.totalIntroduceLoopFinished.Add(1)
}

// persistSnapshot persists the snapshot to disk (placeholder).
func (s *sidx) persistSnapshot(_ *snapshot) {
	// TODO: Implement snapshot persistence
}

// Lock/Unlock methods for introducer loop.
func (s *sidx) Lock() {
	s.mu.Lock()
}

func (s *sidx) Unlock() {
	s.mu.Unlock()
}

// gc represents garbage collector (placeholder).
type gc struct{}

func newGC(_ *Options) *gc {
	return &gc{}
}

func (g *gc) clean() {
	// TODO: Implement garbage collection
}

// emptyQueryResult represents an empty query result.
type emptyQueryResult struct{}

func (e *emptyQueryResult) Pull() *QueryResponse {
	return nil
}

func (e *emptyQueryResult) Release() {
	// Nothing to release
}

// queryResult implements QueryResult interface.
type queryResult struct {
	ctx      context.Context
	snapshot *snapshot
	request  QueryRequest
	released bool
}

func (qr *queryResult) Pull() *QueryResponse {
	if qr.released {
		return nil
	}

	// Simple implementation - return empty response
	// TODO: Implement actual query logic
	return &QueryResponse{
		Keys: []int64{},
		Data: [][]byte{},
		Tags: [][]Tag{},
		SIDs: []common.SeriesID{},
		Metadata: ResponseMetadata{
			ExecutionTimeMs: 0,
		},
	}
}

func (qr *queryResult) Release() {
	if qr.released {
		return
	}
	qr.released = true
	if qr.snapshot != nil {
		qr.snapshot.decRef()
		qr.snapshot = nil
	}
}
