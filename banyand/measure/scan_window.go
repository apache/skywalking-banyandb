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
	"container/heap"
	"context"
	"math"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Series-window scan (measure scan path v2, step 4a).
//
// The matching blocks of a query are grouped into series windows: all the
// in-range blocks of one series, from every part. Windows are visited one after
// another in the order the legacy heap merge would have produced their rows, and
// a window is cut into segments from block metadata alone: blocks ordered by
// their first timestamp, a block that starts at or before the previous segment's
// last timestamp (an equal boundary may be a duplicate version) joining that
// segment. A segment is decoded only when the scan reaches it, so decoded memory
// is bounded by one segment instead of by the whole result.
//
//   - A segment of one block is emitted as a batch that lends the block's decoded
//     columns (blockCursor.lendBatch); nothing is copied.
//   - A segment of several overlapping blocks goes through the existing row
//     merge (mergeBatch: the newest version of a timestamp wins), restricted to
//     that segment. Step 4c replaces it.
//
// Decoding a segment is the one place the node-wide decode pool of step 4b plugs
// in: decodeSegment is called when a segment is reached and may become "take the
// segment the pool already decoded"; the plan (windows and their segments) is
// known up front, which is what lets the pool run ahead.

// coalesceTargetRows is how many rows the scan gathers into one owned batch from
// segments smaller than model.SmallBatchRows before emitting it.
const coalesceTargetRows = 1024

// maxSelectionRows is the largest batch a uint16 Selection can address.
const maxSelectionRows = math.MaxUint16 + 1

// legacyBatchScan routes PullBatch to the pre-window implementation (decode every
// block, then merge row by row). It exists so tests can compare the two.
var legacyBatchScan atomic.Bool

// setLegacyBatchScan switches the legacy scan and returns the previous setting.
func setLegacyBatchScan(on bool) bool { return legacyBatchScan.Swap(on) }

// scanStats counts what a window scan did. It is read by tests and mirrored to
// the query metrics.
type scanStats struct {
	// windows is the number of series windows started.
	windows int64
	// windowsOverlapped is the number of windows with at least one segment of two
	// or more blocks, decided from metadata.
	windowsOverlapped int64
	// segmentsFast is the number of segments emitted by lending one block.
	segmentsFast int64
	// segmentsOverlap is the number of segments of two or more decoded blocks
	// that took the row merge.
	segmentsOverlap int64
	// coalesced is the number of batches emitted by copying segments smaller
	// than model.SmallBatchRows together.
	coalesced int64
}

// scanSegment is a run of blocks of one series whose time ranges chain together.
type scanSegment struct {
	cursors []*blockCursor
}

// seriesWindow is every in-range block of one series, cut into segments in
// emission order.
type seriesWindow struct {
	// cursors is every cursor of the series in arrival order; planning cuts it
	// into segments and the segments take over the cursors.
	cursors  []*blockCursor
	segments []scanSegment
	sid      common.SeriesID
	minTS    int64
	maxTS    int64
}

// windowScan streams the windows of one queryResult.
type windowScan struct {
	qr       *queryResult
	cur      *seriesWindow
	merge    *queryResult
	builder  *model.MeasureBatch
	ready    *model.MeasureBatch
	err      error
	windows  []seriesWindow
	boundary []int
	stats    scanStats
	winIdx   int
	segIdx   int
	desc     bool
}

// newWindowScan plans the scan over qr.data and takes ownership of its cursors.
// It returns nil, leaving qr.data untouched, when the windows cannot be emitted
// in the order the legacy merge would produce.
func newWindowScan(qr *queryResult) *windowScan {
	windows := groupWindows(qr.data)
	if !orderWindows(qr, windows) {
		return nil
	}
	for i := range qr.data {
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
	s := &windowScan{qr: qr, windows: windows, desc: qr.orderByTimestampDesc()}
	for i := range s.windows {
		w := &s.windows[i]
		w.segments = planSegments(w.cursors, s.desc)
		w.cursors = nil
	}
	return s
}

// groupWindows groups cursors by series, keeping first-seen series order and the
// order the cursors arrived in within a series. The windows hold the cursors
// uncut; planSegments cuts them.
func groupWindows(data []*blockCursor) []seriesWindow {
	index := make(map[common.SeriesID]int)
	var windows []seriesWindow
	for _, bc := range data {
		i, ok := index[bc.bm.seriesID]
		if !ok {
			i = len(windows)
			index[bc.bm.seriesID] = i
			windows = append(windows, seriesWindow{
				sid:   bc.bm.seriesID,
				minTS: bc.bm.timestamps.min,
				maxTS: bc.bm.timestamps.max,
			})
		}
		w := &windows[i]
		w.cursors = append(w.cursors, bc)
		w.minTS = min(w.minTS, bc.bm.timestamps.min)
		w.maxTS = max(w.maxTS, bc.bm.timestamps.max)
	}
	return windows
}

// orderWindows sorts windows into the order the legacy heap yields their rows and
// reports whether that order is a concatenation of windows at all.
//
// Without a time order the heap sorts by the series' position in the query, then
// by time, so windows follow sidToIndex. With a time order it interleaves series
// by timestamp: the legacy output is a concatenation of whole series only when
// the series' time ranges are strictly disjoint, in which case windows follow
// time (ascending, or descending for a descending order). Otherwise the rows of
// different series interleave and the caller keeps the legacy merge.
func orderWindows(qr *queryResult, windows []seriesWindow) bool {
	if len(windows) < 2 {
		return true
	}
	if !qr.orderByTS {
		sort.SliceStable(windows, func(i, j int) bool {
			return qr.sidToIndex[windows[i].sid] < qr.sidToIndex[windows[j].sid]
		})
		return true
	}
	if qr.ascTS {
		sort.SliceStable(windows, func(i, j int) bool { return windows[i].minTS < windows[j].minTS })
		for i := 1; i < len(windows); i++ {
			if windows[i-1].maxTS >= windows[i].minTS {
				return false
			}
		}
		return true
	}
	sort.SliceStable(windows, func(i, j int) bool { return windows[i].maxTS > windows[j].maxTS })
	for i := 1; i < len(windows); i++ {
		if windows[i-1].minTS <= windows[i].maxTS {
			return false
		}
	}
	return true
}

// planSegments cuts one series' cursors into segments from block metadata. The
// cursors are ordered by their first timestamp (arrival order breaks ties); a
// cursor whose first timestamp is at or before the running last timestamp of the
// segment before it joins that segment. For a descending scan the segments come
// out latest first.
func planSegments(cursors []*blockCursor, desc bool) []scanSegment {
	if len(cursors) == 1 {
		return []scanSegment{{cursors: cursors}}
	}
	sorted := make([]*blockCursor, len(cursors))
	copy(sorted, cursors)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].bm.timestamps.min < sorted[j].bm.timestamps.min })
	var segments []scanSegment
	var segMax int64
	for _, bc := range sorted {
		if len(segments) > 0 && bc.bm.timestamps.min <= segMax {
			last := &segments[len(segments)-1]
			last.cursors = append(last.cursors, bc)
			segMax = max(segMax, bc.bm.timestamps.max)
			continue
		}
		segments = append(segments, scanSegment{cursors: []*blockCursor{bc}})
		segMax = bc.bm.timestamps.max
	}
	if desc {
		for i, j := 0, len(segments)-1; i < j; i, j = i+1, j-1 {
			segments[i], segments[j] = segments[j], segments[i]
		}
	}
	return segments
}

// next returns the next batch, or nil at the end of the scan. The error is
// sticky.
func (s *windowScan) next(ctx context.Context) (*model.MeasureBatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	for {
		if s.ready != nil {
			b := s.ready
			s.ready = nil
			return b, nil
		}
		piece, err := s.nextPiece(ctx)
		if err != nil {
			s.err = err
			return nil, err
		}
		if piece == nil {
			if s.builder != nil {
				return s.takeBuilder(), nil
			}
			return nil, nil
		}
		if piece.ActiveRows() < model.SmallBatchRows {
			s.appendToBuilder(piece)
			if s.builder.RowCount() >= coalesceTargetRows {
				return s.takeBuilder(), nil
			}
			continue
		}
		if s.builder != nil {
			s.ready = piece
			return s.takeBuilder(), nil
		}
		return piece, nil
	}
}

// nextPiece returns the next batch of one segment: a lent block, or one page of
// an overlap merge. nil means the scan is exhausted.
func (s *windowScan) nextPiece(ctx context.Context) (*model.MeasureBatch, error) {
	qr := s.qr
	for {
		if s.merge != nil {
			b, err := s.merge.mergeBatch(qr.storedIndexValue, qr.batchSchema)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			s.merge = nil
			continue
		}
		if s.cur == nil || s.segIdx >= len(s.cur.segments) {
			if s.winIdx >= len(s.windows) {
				s.cur = nil
				return nil, nil
			}
			s.startWindow(&s.windows[s.winIdx])
			s.winIdx++
			continue
		}
		seg := &s.cur.segments[s.segIdx]
		s.segIdx++
		live, err := s.decodeSegment(ctx, seg)
		if err != nil {
			return nil, err
		}
		switch len(live) {
		case 0:
		case 1:
			s.stats.segmentsFast++
			s.countSegment(true)
			return s.lendCursor(live[0]), nil
		default:
			s.stats.segmentsOverlap++
			s.countSegment(false)
			s.merge = &queryResult{
				ctx:        qr.ctx,
				sidToIndex: qr.sidToIndex,
				data:       live,
				orderByTS:  qr.orderByTS,
				ascTS:      qr.ascTS,
			}
			heap.Init(s.merge)
		}
	}
}

func (s *windowScan) startWindow(w *seriesWindow) {
	s.cur = w
	s.segIdx = 0
	s.stats.windows++
	overlapped := false
	for i := range w.segments {
		if len(w.segments[i].cursors) > 1 {
			overlapped = true
			break
		}
	}
	if overlapped {
		s.stats.windowsOverlapped++
	}
	if qm := s.qr.qm; qm != nil {
		qm.scanWindows.Inc(1)
		if overlapped {
			qm.scanWindowsOverlapped.Inc(1)
		}
	}
}

func (s *windowScan) countSegment(fast bool) {
	qm := s.qr.qm
	if qm == nil {
		return
	}
	if fast {
		qm.scanSegmentsFast.Inc(1)
		return
	}
	qm.scanSegmentsOverlap.Inc(1)
}

// decodeSegment decodes the cursors of seg and returns those that have rows in
// the query time range, in the order they were planned. The scan owns the
// returned cursors; seg gives them up. A cursor with no row in range is
// released here.
func (s *windowScan) decodeSegment(ctx context.Context, seg *scanSegment) ([]*blockCursor, error) {
	cursors := seg.cursors
	seg.cursors = nil
	select {
	case <-ctx.Done():
		releaseCursors(cursors)
		return nil, errors.WithMessagef(ctx.Err(), "interrupt: hit %d", s.qr.hit)
	default:
	}
	// live reuses cursors' array: a write at position k never passes the read at i >= k.
	live := cursors[:0]
	for _, bc := range cursors {
		tmpBlock := generateBlock()
		ok := bc.loadData(tmpBlock, true)
		releaseBlock(tmpBlock)
		if !ok {
			releaseBlockCursor(bc)
			continue
		}
		if s.desc {
			bc.idx = len(bc.timestamps) - 1
		}
		live = append(live, bc)
	}
	return live, nil
}

// lendCursor turns one decoded cursor into a batch and drops the scan's
// reference; the batch's owners keep the cursor alive for as long as it lends
// memory.
func (s *windowScan) lendCursor(bc *blockCursor) *model.MeasureBatch {
	qr := s.qr
	var b *model.MeasureBatch
	if len(bc.timestamps) > maxSelectionRows && s.desc {
		// A descending block this large cannot be addressed by a uint16 selection.
		b = newMeasureBatchForSchema(qr.batchSchema, len(bc.timestamps))
		bc.copyAllToBatch(b, qr.batchSchema, qr.storedIndexValue, true)
	} else {
		b = bc.lendBatch(qr.batchSchema, qr.storedIndexValue, s.desc)
	}
	releaseBlockCursor(bc)
	return b
}

// appendToBuilder copies every active row of piece onto the coalescing builder
// and releases piece. The builder pins whatever owners piece pinned, since its
// cells may still borrow from them.
func (s *windowScan) appendToBuilder(piece *model.MeasureBatch) {
	if s.builder == nil {
		s.builder = newMeasureBatchForSchema(s.qr.batchSchema, coalesceTargetRows)
		s.boundary = s.boundary[:0]
	}
	b := s.builder
	rows := piece.ActiveRows()
	if rows > 0 && b.RowCount() > 0 && b.SeriesIDs[len(b.SeriesIDs)-1] != piece.SeriesIDs[0] {
		s.boundary = append(s.boundary, b.RowCount())
	}
	b.Owners.PinAll(&piece.Owners)
	copyRow := func(r int) {
		b.Timestamps = append(b.Timestamps, piece.Timestamps[r])
		b.Versions = append(b.Versions, piece.Versions[r])
		b.ShardIDs = append(b.ShardIDs, piece.ShardIDs[r])
		b.SeriesIDs = append(b.SeriesIDs, piece.SeriesIDs[r])
		for i := range piece.Tags {
			// Columns of one schema always share a type, so the copy cannot fail.
			_ = vectorized.AppendColumnRange(b.Tags[i], piece.Tags[i], r, 1)
		}
		for i := range piece.Fields {
			_ = vectorized.AppendColumnRange(b.Fields[i], piece.Fields[i], r, 1)
		}
	}
	if piece.Selection != nil {
		for _, r := range piece.Selection {
			copyRow(int(r))
		}
	} else {
		for r := range piece.RowCount() {
			copyRow(r)
		}
	}
	piece.Release()
}

// takeBuilder hands the coalesced batch to the caller.
func (s *windowScan) takeBuilder() *model.MeasureBatch {
	b := s.builder
	s.builder = nil
	if len(s.boundary) > 0 {
		b.SeriesBoundaries = append(b.SeriesBoundaries[:0], s.boundary...)
		b.SeriesBoundaries = append(b.SeriesBoundaries, b.RowCount())
	}
	s.boundary = s.boundary[:0]
	s.stats.coalesced++
	return b
}

// close releases every cursor and batch the scan still holds. It is idempotent.
func (s *windowScan) close() {
	if s.merge != nil {
		releaseCursors(s.merge.data)
		s.merge.data = nil
		s.merge = nil
	}
	for i := range s.windows {
		segments := s.windows[i].segments
		for j := range segments {
			releaseCursors(segments[j].cursors)
			segments[j].cursors = nil
		}
	}
	s.windows = nil
	s.cur = nil
	s.builder.Release()
	s.builder = nil
	s.ready.Release()
	s.ready = nil
}

func releaseCursors(cursors []*blockCursor) {
	for i, bc := range cursors {
		if bc != nil {
			releaseBlockCursor(bc)
			cursors[i] = nil
		}
	}
}
