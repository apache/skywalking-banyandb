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
	"context"
	"math"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Series-window scan (measure scan path v2, steps 4a and 4c).
//
// The matching blocks of a query are grouped into series windows: all the
// in-range blocks of one series, from every part. The windows are cut into units
// that are visited one after another in the order the legacy heap merge would have
// produced their rows:
//
//   - Without a time order, a unit is one window, and windows follow the series
//     order of the query.
//   - With a time order, windows whose time ranges are disjoint from every other
//     window are units of their own, in time order. Windows whose ranges overlap
//     or touch interleave their rows, so they form one merge unit (scan_merge.go).
//
// A window is cut into segments from block metadata alone: blocks ordered by their
// first timestamp, a block that starts at or before the previous segment's last
// timestamp (an equal boundary may be a duplicate version) joining that segment. A
// segment is decoded only when the scan reaches it, so decoded memory is bounded
// by one segment instead of by the whole result.
//
//   - A segment of one block is emitted as a batch that lends the block's decoded
//     columns (blockCursor.lendBatch); nothing is copied.
//   - A segment of several overlapping blocks, and a merge unit, go through
//     timeMerge: keys first, then one gather per column, decoding each block only
//     when the merge reaches it.
//
// Decoding goes through the scan's prefetcher (scan_prefetch.go): the plan (units,
// windows and their segments) is known up front, so the node-wide decode pool of step
// 4b decodes the next cursors while the current one is emitted, in the order the scan
// will take them, each under a reservation released with the cursor.

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
	// segmentsOverlap is the number of segments of two or more blocks, decided from
	// metadata, that took the merge.
	segmentsOverlap int64
	// coalesced is the number of batches emitted by copying segments smaller
	// than model.SmallBatchRows together.
	coalesced int64
	// mergeUnits is the number of merges started: one per overlapping segment and
	// one per group of series whose time ranges overlap.
	mergeUnits int64
	// mergePages is the number of batches the merges emitted.
	mergePages int64
	// mergeRows is the number of rows the merges emitted.
	mergeRows int64
	// mergeCursors is the number of cursors the merges decoded, each when the merge
	// frontier reached it.
	mergeCursors int64
	// mergePeakLive is the most cursors any one merge held decoded at once.
	mergePeakLive int64
	// prefetchPeak is the most decodes the scan had requested ahead of the one it
	// was consuming.
	prefetchPeak int64
	// reservationWaits is the number of times prefetch found no room in the budget
	// and waited; reservationFails the number of times the cursor the scan needed
	// could not be reserved.
	reservationWaits int64
	reservationFails int64
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

// scanUnit is what the scan visits as one step: a single window, or a group of
// windows whose rows interleave in time and are merged together.
type scanUnit struct {
	// windows holds one window for a window unit and two or more for a merge unit.
	windows []seriesWindow
}

// windowScan streams the units of one queryResult.
type windowScan struct {
	qr       *queryResult
	cur      *seriesWindow
	merge    *timeMerge
	load     cursorLoader
	pf       *prefetcher
	spare    *timeMerge
	plan     []colPlan
	builder  *model.MeasureBatch
	ready    *model.MeasureBatch
	err      error
	units    []scanUnit
	boundary []int
	stats    scanStats
	unitIdx  int
	segIdx   int
	desc     bool
	started  bool
}

// newWindowScan plans the scan over qr.data and takes ownership of its cursors.
func newWindowScan(qr *queryResult) *windowScan {
	s := &windowScan{qr: qr, desc: qr.orderByTimestampDesc(), load: decodeCursor}
	s.units = planUnits(qr, groupWindows(qr.data), s.desc)
	for i := range qr.data {
		qr.data[i] = nil
	}
	qr.data = qr.data[:0]
	for i := range s.units {
		if len(s.units[i].windows) != 1 {
			continue
		}
		w := &s.units[i].windows[0]
		w.segments = planSegments(w.cursors, s.desc)
		w.cursors = nil
	}
	s.pf = newPrefetcher(qr.ctx, qr.dp, qr.budget, qr.qm, &s.stats, &s.load, s.desc, s.sequence())
	return s
}

// sequence lists every cursor of the plan in the order the scan decodes them.
func (s *windowScan) sequence() []*blockCursor {
	var seq []*blockCursor
	for i := range s.units {
		unit := &s.units[i]
		if len(unit.windows) == 1 {
			for _, seg := range unit.windows[0].segments {
				if len(seg.cursors) == 1 {
					seq = append(seq, seg.cursors[0])
					continue
				}
				seq = append(seq, orderPending(seg.cursors, s.desc)...)
			}
			continue
		}
		var all []*blockCursor
		for j := range unit.windows {
			all = append(all, unit.windows[j].cursors...)
		}
		seq = append(seq, orderPending(all, s.desc)...)
	}
	return seq
}

// loadCursor is the cursorLoader of the scan's merges: it takes the next decode of
// the sequence.
func (s *windowScan) loadCursor(ctx context.Context, bc *blockCursor, _ bool) (bool, error) {
	return s.pf.take(ctx, bc)
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

// planUnits orders windows into the units the scan visits, in the order the legacy
// heap yields their rows.
//
// Without a time order the heap sorts by the series' position in the query, then
// by time, so every window is a unit and units follow sidToIndex. With a time order
// the heap interleaves series by timestamp, so windows are swept in time (latest
// first for a descending order) and a window joins the unit before it when its
// range reaches the unit's range (an equal timestamp counts: there the series are
// ordered by id). A unit of one window is emitted whole; a unit of several is
// merged.
func planUnits(qr *queryResult, windows []seriesWindow, desc bool) []scanUnit {
	if !qr.orderByTS {
		sort.SliceStable(windows, func(i, j int) bool {
			return qr.sidToIndex[windows[i].sid] < qr.sidToIndex[windows[j].sid]
		})
		units := make([]scanUnit, len(windows))
		for i := range windows {
			units[i].windows = windows[i : i+1 : i+1]
		}
		return units
	}
	if desc {
		sort.SliceStable(windows, func(i, j int) bool { return windows[i].maxTS > windows[j].maxTS })
	} else {
		sort.SliceStable(windows, func(i, j int) bool { return windows[i].minTS < windows[j].minTS })
	}
	var units []scanUnit
	start := 0
	var edge int64 // the unit's last timestamp (first, descending) reached so far
	for i := range windows {
		w := &windows[i]
		if i > start {
			if (!desc && w.minTS <= edge) || (desc && w.maxTS >= edge) {
				if desc {
					edge = min(edge, w.minTS)
				} else {
					edge = max(edge, w.maxTS)
				}
				continue
			}
			units = append(units, scanUnit{windows: windows[start:i:i]})
			start = i
		}
		if desc {
			edge = w.minTS
		} else {
			edge = w.maxTS
		}
	}
	if len(windows) > 0 {
		units = append(units, scanUnit{windows: windows[start:]})
	}
	return units
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
// sticky. Every batch is marked with what the scan knows about its rows: where each
// series ends, and that time runs in the scan's direction within a series (a series'
// segments are visited in time order, and a merge emits its rows in time order), so
// an aggregation can split the batch into runs without reading its rows.
func (s *windowScan) next(ctx context.Context) (*model.MeasureBatch, error) {
	b, err := s.nextUnmarked(ctx)
	if b != nil {
		b.SeriesKnown = true
		b.TimeOrder = vectorized.TimeOrderAsc
		if s.desc {
			b.TimeOrder = vectorized.TimeOrderDesc
		}
	}
	return b, err
}

func (s *windowScan) nextUnmarked(ctx context.Context) (*model.MeasureBatch, error) {
	if s.err != nil {
		return nil, s.err
	}
	if !s.started {
		s.started = true
		s.pf.pump()
	}
	for {
		if s.ready != nil {
			b := s.ready
			s.ready = nil
			return b, nil
		}
		if s.builder != nil && s.pf.waiting() {
			// The batch being coalesced pins the cursors of the pieces it holds, and the
			// budget has no room for more: hand it over now rather than wait to fill it.
			return s.takeBuilder(), nil
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

// nextPiece returns the next batch of one unit: a lent block, or one page of a
// merge. nil means the scan is exhausted.
func (s *windowScan) nextPiece(ctx context.Context) (*model.MeasureBatch, error) {
	for {
		if s.merge != nil {
			b, err := s.merge.nextPage(ctx)
			if err != nil {
				return nil, err
			}
			if b != nil {
				return b, nil
			}
			s.finishMerge()
			continue
		}
		if s.cur == nil || s.segIdx >= len(s.cur.segments) {
			if s.unitIdx >= len(s.units) {
				s.cur = nil
				return nil, nil
			}
			unit := &s.units[s.unitIdx]
			s.unitIdx++
			if len(unit.windows) == 1 {
				s.startWindow(&unit.windows[0])
				continue
			}
			s.startMergeUnit(unit)
			continue
		}
		seg := &s.cur.segments[s.segIdx]
		s.segIdx++
		if len(seg.cursors) > 1 {
			s.stats.segmentsOverlap++
			s.countSegment(false)
			cursors := seg.cursors
			seg.cursors = nil
			s.merge = s.newMerge(cursors)
			continue
		}
		bc, err := s.decodeSegment(ctx, seg)
		if err != nil {
			return nil, err
		}
		if bc == nil {
			continue
		}
		s.stats.segmentsFast++
		s.countSegment(true)
		return s.lendCursor(bc), nil
	}
}

// newMerge starts a merge over cursors, reusing the buffers of the last one.
func (s *windowScan) newMerge(cursors []*blockCursor) *timeMerge {
	if s.plan == nil {
		s.plan = newColPlan(s.qr.batchSchema)
	}
	m := newTimeMerge(s.qr, &s.stats, s.loadCursor, s.plan, cursors, s.desc, s.spare)
	m.pf = s.pf
	s.spare = nil
	return m
}

// finishMerge closes the running merge and keeps it as the spare for the next.
func (s *windowScan) finishMerge() {
	s.merge.close()
	s.pf.setFrontier(0)
	s.spare, s.merge = s.merge, nil
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

// startMergeUnit starts the merge of windows whose rows interleave in time. The
// windows are counted as overlapped: their blocks overlap by definition.
func (s *windowScan) startMergeUnit(unit *scanUnit) {
	var cursors []*blockCursor
	for i := range unit.windows {
		cursors = append(cursors, unit.windows[i].cursors...)
		unit.windows[i].cursors = nil
	}
	n := int64(len(unit.windows))
	s.stats.windows += n
	s.stats.windowsOverlapped += n
	s.stats.segmentsOverlap++
	if qm := s.qr.qm; qm != nil {
		qm.scanWindows.Inc(float64(n))
		qm.scanWindowsOverlapped.Inc(float64(n))
	}
	s.countSegment(false)
	s.cur = nil
	s.merge = s.newMerge(cursors)
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

// decodeSegment decodes the one cursor of seg and returns it, or nil when the block
// has no row in the query time range. The scan owns the returned cursor; seg gives
// it up.
func (s *windowScan) decodeSegment(ctx context.Context, seg *scanSegment) (*blockCursor, error) {
	bc := seg.cursors[0]
	seg.cursors = nil
	select {
	case <-ctx.Done():
		// A decode of bc may be running on a pool worker: withdraw or wait for it
		// before the cursor is reset and pooled.
		s.pf.discard(bc)
		releaseBlockCursor(bc)
		return nil, errors.WithMessagef(ctx.Err(), "interrupt: hit %d", s.qr.hit)
	default:
	}
	ok, err := s.pf.take(ctx, bc)
	if err != nil {
		releaseBlockCursor(bc)
		return nil, errors.WithMessagef(err, "interrupt: hit %d", s.qr.hit)
	}
	if !ok {
		releaseBlockCursor(bc)
		return nil, nil
	}
	return bc, nil
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
// cells may still borrow from them. A row of another series than the row before it
// starts a new series run.
func (s *windowScan) appendToBuilder(piece *model.MeasureBatch) {
	if s.builder == nil {
		s.builder = newMeasureBatchForSchema(s.qr.batchSchema, coalesceTargetRows)
		s.boundary = s.boundary[:0]
	}
	b := s.builder
	b.Owners.PinAll(&piece.Owners)
	copyRow := func(r int) {
		if n := len(b.SeriesIDs); n > 0 && b.SeriesIDs[n-1] != piece.SeriesIDs[r] {
			s.boundary = append(s.boundary, n)
		}
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
	} else {
		// One series throughout: its entity tags are constant for the whole batch.
		constifyEntityTags(b, s.qr.batchSchema)
	}
	s.boundary = s.boundary[:0]
	s.stats.coalesced++
	return b
}

// close releases every cursor and batch the scan still holds. It is idempotent.
func (s *windowScan) close() {
	// Stop the decodes first: no worker may read a cursor that is about to be released.
	s.pf.close()
	if s.merge != nil {
		s.finishMerge()
	}
	s.spare = nil
	for i := range s.units {
		for j := range s.units[i].windows {
			w := &s.units[i].windows[j]
			releaseCursors(w.cursors)
			w.cursors = nil
			for k := range w.segments {
				releaseCursors(w.segments[k].cursors)
				w.segments[k].cursors = nil
			}
		}
	}
	s.units = nil
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
