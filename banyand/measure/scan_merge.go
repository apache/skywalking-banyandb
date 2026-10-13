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
	"cmp"
	"context"
	"slices"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Time-ordered merge (measure scan path v2, step 4c).
//
// timeMerge replaces the legacy "decode every block, then merge row by row" path
// for the blocks whose rows interleave in time: the series of a time-ordered query
// whose time ranges overlap, and the overlapping blocks of one series. It works in
// two steps per output batch.
//
//  1. Keys first. A heap over the head row of every live cursor yields rows in the
//     legacy order (queryResult.Less: timestamp, then series id, then the higher
//     version first) and applies the legacy duplicate rule (mergeBatch: of the rows
//     of one series sharing a timestamp, the highest version wins, the first one
//     popped on a tie). Only timestamp, version and series id are looked at; the
//     result is the batch's metadata columns and a winners list of (cursor, row).
//  2. Gather. Each output column is built once from the winners, with a loop typed
//     by the column: numerics are an index copy, string, bytes and string-array
//     cells are views over the winning cursor's decoded bytes (the batch pins that
//     cursor), and only passthrough cells, index-substituted tags, missing columns
//     and cells whose stored type is not the projected one are decoded or filled
//     one cell at a time, exactly as the legacy copy does.
//
// Decoding is lazy. Cursors are ordered by the first timestamp the merge can read
// from them (the last, going down, for a descending scan) and a cursor is decoded
// only when the merge frontier reaches that timestamp, so the decoded memory is
// that of the blocks overlapping the current instant, not of the whole result. A
// cursor is released as soon as the merge has consumed its last row; batches that
// borrow from it keep it alive through their own references.
//
// cursorLoader is where step 4b's node-wide decode pool plugs in.

// mergeFirstPageRows and mergeMaxPageRows bound the rows of one merged batch. The
// first page is small so a query with a limit does not gather rows it never reads;
// the page then doubles up to a block's worth of rows (8,192), the size the
// zero-copy path already hands the pipeline, which amortizes the per-page work
// (the column plan, owner pins) without holding more than a block of rows in
// flight. The legacy merge cut pages at 4,096 rows.
const (
	minFinishedLimit   = 8
	mergeFirstPageRows = 1024
	mergeMaxPageRows   = 8192
)

// cursorLoader decodes bc, a cursor planned from block metadata only, for the
// scan: it reports whether the block has rows in the query time range. On true the
// cursor is positioned at its first row for the scan direction (the last row for
// a descending scan). The caller releases the cursor when it reports false or an
// error.
type cursorLoader func(ctx context.Context, bc *blockCursor, desc bool) (bool, error)

// decodeCursor is the default cursorLoader: it decodes the block on the calling
// goroutine.
func decodeCursor(ctx context.Context, bc *blockCursor, desc bool) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	tmpBlock := generateBlock()
	ok := bc.loadData(tmpBlock, true)
	releaseBlock(tmpBlock)
	if !ok {
		return false, nil
	}
	if desc {
		bc.idx = len(bc.timestamps) - 1
	}
	return true, nil
}

// srcKind says how one projected column is read from one cursor.
type srcKind uint8

const (
	// srcOther is decoded cell by cell through the legacy helpers.
	srcOther srcKind = iota
	// srcNull is a column the cursor lacks, or whose stored type is not the schema's.
	srcNull
	// srcHidden is a tag substituted from the index.
	srcHidden
	// srcInt64 and srcFloat64 are typed numeric columns read by index.
	srcInt64
	srcFloat64
	// srcStr, srcBin and srcStrArr are bytes-form columns lent as views.
	srcStr
	srcBin
	srcStrArr
)

// colSource is how one cursor serves one projected column; it is resolved once,
// when the cursor is decoded.
type colSource struct {
	col    *column
	hidden *modelv1.TagValue
	kind   srcKind
}

// colPlan is one projected tag or field column of the output.
type colPlan struct {
	def   vectorized.ColumnDef
	out   int // index into MeasureBatch.Tags or Fields
	ord   int // index into mergeEntry.src
	isTag bool
}

func newColPlan(schema *vectorized.BatchSchema) []colPlan {
	var plan []colPlan
	tagIdx, fieldIdx := 0, 0
	for _, def := range schema.Columns {
		switch def.Role {
		case vectorized.RoleTag:
			plan = append(plan, colPlan{def: def, out: tagIdx, ord: len(plan), isTag: true})
			tagIdx++
		case vectorized.RoleField:
			plan = append(plan, colPlan{def: def, out: fieldIdx, ord: len(plan)})
			fieldIdx++
		case vectorized.RoleTimestamp, vectorized.RoleVersion,
			vectorized.RoleSeriesID, vectorized.RoleShardID,
			vectorized.RoleElementID, vectorized.RoleOrderKey:
		}
	}
	return plan
}

// resolveSources decides, for every planned column, how bc serves it. The order
// of the checks is the legacy order (fillTagColumnAllRows, fillFieldColumnAllRows).
func resolveSources(bc *blockCursor, plan []colPlan, indexValue map[string]*modelv1.TagValue, dst []colSource) []colSource {
	dst = dst[:0]
	for i := range plan {
		cp := &plan[i]
		if cp.isTag {
			dst = append(dst, resolveTagSource(bc, cp.def, indexValue))
		} else {
			dst = append(dst, resolveFieldSource(bc, cp.def))
		}
	}
	return dst
}

func resolveTagSource(bc *blockCursor, def vectorized.ColumnDef, indexValue map[string]*modelv1.TagValue) colSource {
	if indexValue != nil && indexValue[def.Name] != nil {
		return colSource{kind: srcHidden, hidden: indexValue[def.Name]}
	}
	cf := bc.findTagFamily(def.TagFamily)
	if cf == nil {
		return colSource{kind: srcNull}
	}
	column := cf.findColumn(def.Name)
	if column == nil {
		return colSource{kind: srcNull}
	}
	schemaType, hasSchemaType := bc.schemaTagTypes[def.Name]
	if !hasSchemaType || column.valueType != schemaType {
		return colSource{kind: srcNull}
	}
	src := colSource{kind: srcOther, col: column}
	switch column.form {
	case numericFormInt64:
		if def.Type == vectorized.ColumnTypeInt64 {
			src.kind = srcInt64
		}
	case numericFormBytes:
		switch {
		case def.Type == vectorized.ColumnTypeString && column.valueType == pbv1.ValueTypeStr:
			src.kind = srcStr
		case def.Type == vectorized.ColumnTypeBytes && column.valueType == pbv1.ValueTypeBinaryData:
			src.kind = srcBin
		case def.Type == vectorized.ColumnTypeStrArray && column.valueType == pbv1.ValueTypeStrArr:
			src.kind = srcStrArr
		}
	case numericFormFloat64:
	}
	return src
}

func resolveFieldSource(bc *blockCursor, def vectorized.ColumnDef) colSource {
	column := bc.findFieldColumn(def.Name)
	if column == nil {
		return colSource{kind: srcNull}
	}
	src := colSource{kind: srcOther, col: column}
	switch column.form {
	case numericFormInt64:
		if def.Type == vectorized.ColumnTypeInt64 {
			src.kind = srcInt64
		}
	case numericFormFloat64:
		if def.Type == vectorized.ColumnTypeFloat64 {
			src.kind = srcFloat64
		}
	case numericFormBytes:
	}
	return src
}

// mergeEntry is a decoded cursor in the merge, with the key of its head row cached
// so the heap never reads the cursor's slices.
type mergeEntry struct {
	bc  *blockCursor
	src []colSource
	// ts, ver and sid are the key of the row at idx.
	ts  int64
	ver int64
	sid common.SeriesID
	idx int
	// seq is the cursor's position in the planned order; it breaks ties between
	// rows whose keys are equal.
	seq int
	// pageStamp and pageSlot say whether, and where, the entry is in the current
	// page's table.
	pageStamp uint32
	pageSlot  int
}

// mergeWinner is a row that survives into the output: the entry's slot in the
// page table and its row in the cursor.
type mergeWinner struct {
	slot uint32
	row  uint32
}

// pageEntry is an entry as the page uses it.
type pageEntry struct {
	e      *mergeEntry
	pinned bool
}

// pendingCursor is a cursor not yet decoded and the first timestamp the merge can
// read from it.
type pendingCursor struct {
	bc    *blockCursor
	start int64
}

// timeMerge merges the rows of a set of cursors into batches in time order. A
// merge is recycled for the next one through newTimeMerge: the scan runs one merge at a
// time, so the buffers it grew are reused instead of reallocated per series.
type timeMerge struct {
	ctx         context.Context
	qr          *queryResult
	stats       *scanStats
	load        cursorLoader
	schema      *vectorized.BatchSchema
	indexValues map[common.SeriesID]map[string]*modelv1.TagValue
	// pending are the cursors not yet decoded, in the order the frontier reaches
	// them.
	pending []pendingCursor
	heap    []*mergeEntry
	// free holds entries of consumed cursors, ready for the next decode.
	free        []*mergeEntry
	finished    []*mergeEntry
	pageEntries []pageEntry
	winners     []mergeWinner
	plan        []colPlan
	i64Scratch  [][]int64
	f64Scratch  [][]float64
	boundaries  []int
	rows        int64
	cursors     int64
	pi          int
	live        int
	peakLive    int
	frontier    int
	pageRows    int
	page        uint32
	desc        bool
	closed      bool
}

// newTimeMerge plans a merge over cursors, which it takes ownership of. The
// cursors are undecoded; their metadata orders them. plan is the output column
// plan of the query's schema (newColPlan), shared by every merge of a scan; spare,
// when not nil, is a finished merge whose buffers the new one reuses.
func newTimeMerge(qr *queryResult, stats *scanStats, load cursorLoader, plan []colPlan, cursors []*blockCursor,
	desc bool, spare *timeMerge,
) *timeMerge {
	m := spare
	if m == nil {
		m = &timeMerge{}
	}
	m.ctx, m.qr, m.stats, m.load = qr.ctx, qr, stats, load
	m.schema, m.indexValues, m.plan, m.desc = qr.batchSchema, qr.storedIndexValue, plan, desc
	m.pageRows, m.pi, m.live, m.peakLive, m.frontier = mergeFirstPageRows, 0, 0, 0, 0
	m.rows, m.cursors, m.page, m.closed = 0, 0, 0, false
	m.pending = m.pending[:0]
	for _, bc := range cursors {
		m.pending = append(m.pending, pendingCursor{bc: bc, start: mergeStart(bc, desc)})
	}
	// Equal starts keep the order the cursors arrived in.
	slices.SortStableFunc(m.pending, func(a, b pendingCursor) int {
		if desc {
			return cmp.Compare(b.start, a.start)
		}
		return cmp.Compare(a.start, b.start)
	})
	m.stats.mergeUnits++
	return m
}

// mergeStart is the first timestamp the merge can read from bc: its first (last,
// descending) timestamp clamped to the query time range.
func mergeStart(bc *blockCursor, desc bool) int64 {
	if desc {
		return min(bc.bm.timestamps.max, bc.maxTimestamp)
	}
	return max(bc.bm.timestamps.min, bc.minTimestamp)
}

// less is the legacy queryResult.Less for a time-ordered query, with the position
// in the planned order as the last tie-break.
func (m *timeMerge) less(a, b *mergeEntry) bool {
	if a.ts != b.ts {
		if m.desc {
			return a.ts > b.ts
		}
		return a.ts < b.ts
	}
	if a.sid != b.sid {
		return a.sid < b.sid
	}
	if a.ver != b.ver {
		return a.ver > b.ver
	}
	return a.seq < b.seq
}

func (m *timeMerge) siftUp(i int) {
	h := m.heap
	for i > 0 {
		parent := (i - 1) / 2
		if !m.less(h[i], h[parent]) {
			return
		}
		h[i], h[parent] = h[parent], h[i]
		i = parent
	}
}

func (m *timeMerge) siftDown(i int) {
	h := m.heap
	n := len(h)
	for {
		child := 2*i + 1
		if child >= n {
			return
		}
		if right := child + 1; right < n && m.less(h[right], h[child]) {
			child = right
		}
		if !m.less(h[child], h[i]) {
			return
		}
		h[i], h[child] = h[child], h[i]
		i = child
	}
}

// fixTop restores the heap after the key of its root grew. A cursor that keeps
// winning (one series, or a run inside a block) stays at the root after two
// comparisons. Otherwise the new key is usually larger than most of the heap (the
// series that just emitted a timestamp has the largest key among those sharing
// it), so instead of comparing against both children on the way down, the hole is
// walked down the smaller children to a leaf, one comparison per level, and the
// entry climbs back up from there.
func (m *timeMerge) fixTop() {
	h := m.heap
	n := len(h)
	e := h[0]
	if n < 3 {
		m.siftDown(0)
		return
	}
	if !m.less(h[1], e) && !m.less(h[2], e) {
		return
	}
	i := 0
	for {
		child := 2*i + 1
		if child >= n {
			break
		}
		if right := child + 1; right < n && m.less(h[right], h[child]) {
			child = right
		}
		h[i] = h[child]
		i = child
	}
	for i > 0 {
		parent := (i - 1) / 2
		if !m.less(e, h[parent]) {
			break
		}
		h[i] = h[parent]
		i = parent
	}
	h[i] = e
}

// reaches reports whether the frontier, the head row of the heap, has reached the
// first timestamp start can be read from, so the cursor must join the merge. An
// equal timestamp counts: the cursor may hold a row that sorts before the head's.
func (m *timeMerge) reaches(start, head int64) bool {
	if m.desc {
		return start >= head
	}
	return start <= head
}

// fill decodes every pending cursor the frontier has reached. With an empty heap
// it decodes the next pending cursor, whatever its start.
func (m *timeMerge) fill() error {
	for m.pi < len(m.pending) {
		if len(m.heap) > 0 && !m.reaches(m.pending[m.pi].start, m.heap[0].ts) {
			return nil
		}
		bc := m.pending[m.pi].bc
		seq := m.pi
		m.pending[m.pi].bc = nil
		m.pi++
		ok, err := m.load(m.ctx, bc, m.desc)
		if err != nil {
			releaseBlockCursor(bc)
			return errors.WithMessagef(err, "interrupt: hit %d", m.qr.hit)
		}
		if !ok {
			releaseBlockCursor(bc)
			continue
		}
		m.stats.mergeCursors++
		m.cursors++
		m.live++
		m.peakLive = max(m.peakLive, m.live)
		var e *mergeEntry
		if n := len(m.free); n > 0 {
			e = m.free[n-1]
			m.free[n-1] = nil
			m.free = m.free[:n-1]
		} else {
			e = &mergeEntry{}
		}
		e.bc, e.seq, e.idx, e.sid, e.pageStamp = bc, seq, bc.idx, bc.bm.seriesID, 0
		var indexValue map[string]*modelv1.TagValue
		if m.indexValues != nil {
			indexValue = m.indexValues[e.sid]
		}
		e.src = resolveSources(bc, m.plan, indexValue, e.src)
		e.ts, e.ver = bc.timestamps[e.idx], bc.versions[e.idx]
		m.heap = append(m.heap, e)
		m.siftUp(len(m.heap) - 1)
		m.frontier = max(m.frontier, len(m.heap))
	}
	return nil
}

// nextPage returns the next merged batch, or nil when every cursor is consumed.
func (m *timeMerge) nextPage(ctx context.Context) (*model.MeasureBatch, error) {
	select {
	case <-ctx.Done():
		return nil, errors.WithMessagef(ctx.Err(), "interrupt: hit %d", m.qr.hit)
	default:
	}
	m.page++
	m.winners = m.winners[:0]
	m.pageEntries = m.pageEntries[:0]
	b := newMeasureBatchForSchema(m.schema, m.pageRows)
	if err := m.mergeKeys(b); err != nil {
		b.Release()
		m.releaseFinished()
		return nil, err
	}
	if len(m.winners) == 0 {
		b.Release()
		return nil, nil
	}
	m.gather(b)
	m.markSeriesRuns(b)
	m.releaseFinished()
	m.stats.mergePages++
	m.stats.mergeRows += int64(len(m.winners))
	m.rows += int64(len(m.winners))
	m.pageRows = min(m.pageRows*2, mergeMaxPageRows)
	return b, nil
}

// finishedLimit is how many consumed-but-unreleased cursors a page may accumulate:
// they stay decoded until the page is gathered, and the cursors that replace them
// are decoded in the meantime, so an unbounded page that crosses a block boundary
// of every series would hold twice the frontier. A quarter of the widest frontier
// seen keeps the peak within a quarter of one decoded block per series; the floor
// keeps small merges (a series' two or three overlapping blocks) in one page.
func (m *timeMerge) finishedLimit() int {
	return max(m.frontier/4, minFinishedLimit)
}

// mergeKeys pops rows in order into b's metadata columns and the winners list.
//
// The duplicate rule is mergeBatch's: a row with the same timestamp and series as
// the last winner replaces it when its version is higher, and is dropped
// otherwise. Unlike mergeBatch, a full page is not closed in the middle of such a
// group (mergeBatch closed at 4,096 rows wherever it stood, so a duplicate that
// straddled the cut was emitted as a second row), which keeps the dedup exact
// whatever the page size.
func (m *timeMerge) mergeKeys(b *model.MeasureBatch) error {
	step := 1
	if m.desc {
		step = -1
	}
	var lastTS, lastVer int64
	var lastSID common.SeriesID
	for {
		if err := m.fill(); err != nil {
			return err
		}
		if len(m.heap) == 0 {
			return nil
		}
		top := m.heap[0]
		n := len(m.winners)
		duplicate := n > 0 && top.ts == lastTS && top.sid == lastSID
		if (n >= m.pageRows || len(m.finished) >= m.finishedLimit()) && !duplicate {
			return nil
		}
		switch {
		case !duplicate:
			m.appendWinner(b, top)
			lastTS, lastVer, lastSID = top.ts, top.ver, top.sid
		case top.ver > lastVer:
			m.replaceWinner(b, top)
			lastVer = top.ver
		}
		top.idx += step
		if top.idx < 0 || top.idx >= len(top.bc.timestamps) {
			last := len(m.heap) - 1
			m.heap[0] = m.heap[last]
			m.heap[last] = nil
			m.heap = m.heap[:last]
			if last > 0 {
				m.siftDown(0)
			}
			m.finished = append(m.finished, top)
			continue
		}
		top.ts, top.ver = top.bc.timestamps[top.idx], top.bc.versions[top.idx]
		m.fixTop()
	}
}

func (m *timeMerge) slotOf(e *mergeEntry) uint32 {
	if e.pageStamp != m.page {
		e.pageStamp = m.page
		e.pageSlot = len(m.pageEntries)
		m.pageEntries = append(m.pageEntries, pageEntry{e: e})
	}
	return uint32(e.pageSlot)
}

func (m *timeMerge) appendWinner(b *model.MeasureBatch, e *mergeEntry) {
	b.Timestamps = append(b.Timestamps, e.ts)
	b.Versions = append(b.Versions, e.ver)
	b.ShardIDs = append(b.ShardIDs, e.bc.shardID)
	b.SeriesIDs = append(b.SeriesIDs, e.sid)
	m.winners = append(m.winners, mergeWinner{slot: m.slotOf(e), row: uint32(e.idx)})
}

func (m *timeMerge) replaceWinner(b *model.MeasureBatch, e *mergeEntry) {
	last := len(m.winners) - 1
	b.Versions[last] = e.ver
	b.ShardIDs[last] = e.bc.shardID
	m.winners[last] = mergeWinner{slot: m.slotOf(e), row: uint32(e.idx)}
}

// releaseFinished drops the merge's reference on every cursor whose rows are all
// consumed. It runs after the gather, which reads the cursors of the page's
// winners; a batch that borrows from a cursor holds its own reference.
func (m *timeMerge) releaseFinished() {
	for i, e := range m.finished {
		releaseBlockCursor(e.bc)
		e.bc = nil
		clear(e.src)
		m.free = append(m.free, e)
		m.finished[i] = nil
		m.live--
	}
	m.finished = m.finished[:0]
}

// markSeriesRuns records, when the batch interleaves several series, where each run
// of one series ends (MeasureBatch.SeriesBoundaries).
func (m *timeMerge) markSeriesRuns(b *model.MeasureBatch) {
	ids := b.SeriesIDs
	m.boundaries = m.boundaries[:0]
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1] {
			m.boundaries = append(m.boundaries, i)
		}
	}
	if len(m.boundaries) == 0 {
		return
	}
	b.SeriesBoundaries = append(b.SeriesBoundaries[:0], m.boundaries...)
	b.SeriesBoundaries = append(b.SeriesBoundaries, len(ids))
}

// gather builds every projected column of b from the winners.
func (m *timeMerge) gather(b *model.MeasureBatch) {
	for i := range m.plan {
		cp := &m.plan[i]
		var col vectorized.Column
		if cp.isTag {
			col = b.Tags[cp.out]
		} else {
			col = b.Fields[cp.out]
		}
		switch cp.def.Type {
		case vectorized.ColumnTypeInt64:
			if c, ok := col.(*vectorized.TypedColumn[int64]); ok {
				gatherNumeric(m, c, cp, srcInt64, func(s *colSource) []int64 { return s.col.int64s }, &m.i64Scratch)
				continue
			}
		case vectorized.ColumnTypeFloat64:
			if c, ok := col.(*vectorized.TypedColumn[float64]); ok {
				gatherNumeric(m, c, cp, srcFloat64, func(s *colSource) []float64 { return s.col.float64s }, &m.f64Scratch)
				continue
			}
		case vectorized.ColumnTypeString:
			if c, ok := col.(*vectorized.TypedColumn[string]); ok {
				m.gatherStrings(b, c, cp)
				continue
			}
		case vectorized.ColumnTypeBytes:
			if c, ok := col.(*vectorized.TypedColumn[[]byte]); ok {
				m.gatherBytes(b, c, cp)
				continue
			}
		case vectorized.ColumnTypeStrArray:
			if c, ok := col.(*vectorized.TypedColumn[[]string]); ok {
				m.gatherStrArrays(b, c, cp)
				continue
			}
		case vectorized.ColumnTypeInt64Array, vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
		}
		m.gatherCells(col, cp)
	}
}

// cell appends the row of winner w when its source is not served by a typed loop:
// a null, an index-substituted tag, or a cell decoded the legacy way.
func (m *timeMerge) cell(col vectorized.Column, cp *colPlan, s *colSource, row int) {
	switch s.kind {
	case srcNull:
		if cp.isTag {
			appendNullTagN(col, cp.def, 1)
		} else {
			appendNullFieldN(col, cp.def, 1)
		}
	case srcHidden:
		appendTagValueAt(col, cp.def, s.hidden)
	case srcOther, srcInt64, srcFloat64, srcStr, srcBin, srcStrArr:
		if cp.isTag {
			fillTagRows(col, cp.def, s.col, row, row+1, false)
		} else {
			fillFieldRows(col, cp.def, s.col, row, row+1, false)
		}
	}
}

// gatherCells builds a column whose cells all go through the legacy per-cell decode:
// passthrough columns and array types that do not borrow.
func (m *timeMerge) gatherCells(col vectorized.Column, cp *colPlan) {
	for _, w := range m.winners {
		e := m.pageEntries[w.slot].e
		m.cell(col, cp, &e.src[cp.ord], int(w.row))
	}
}

// gatherNumeric builds a typed numeric column. When every cursor of the page
// serves it as a typed slice the loop is a plain index copy.
func gatherNumeric[T int64 | float64](m *timeMerge, c *vectorized.TypedColumn[T], cp *colPlan, kind srcKind,
	data func(*colSource) []T, scratch *[][]T,
) {
	slices := (*scratch)[:0]
	allTyped := true
	for i := range m.pageEntries {
		s := &m.pageEntries[i].e.src[cp.ord]
		if s.kind != kind {
			allTyped = false
			break
		}
		slices = append(slices, data(s))
	}
	*scratch = slices
	if allTyped {
		for _, w := range m.winners {
			c.Append(slices[w.slot][w.row])
		}
		return
	}
	for _, w := range m.winners {
		s := &m.pageEntries[w.slot].e.src[cp.ord]
		if s.kind == kind {
			c.Append(data(s)[w.row])
			continue
		}
		m.cell(c, cp, s, int(w.row))
	}
}

// pin makes the batch hold the cursor of page slot, once per page.
func (m *timeMerge) pin(b *model.MeasureBatch, slot uint32) {
	pe := &m.pageEntries[slot]
	if !pe.pinned {
		pe.pinned = true
		b.Owners.Pin(pe.e.bc)
	}
}

func (m *timeMerge) gatherStrings(b *model.MeasureBatch, c *vectorized.TypedColumn[string], cp *colPlan) {
	for _, w := range m.winners {
		s := &m.pageEntries[w.slot].e.src[cp.ord]
		if s.kind != srcStr {
			m.cell(c, cp, s, int(w.row))
			continue
		}
		raw := s.col.values[w.row]
		if raw == nil {
			c.AppendNull()
			continue
		}
		if len(raw) > 0 {
			m.pin(b, w.slot)
		}
		c.Append(convert.BytesToString(raw))
	}
}

func (m *timeMerge) gatherBytes(b *model.MeasureBatch, c *vectorized.TypedColumn[[]byte], cp *colPlan) {
	for _, w := range m.winners {
		s := &m.pageEntries[w.slot].e.src[cp.ord]
		if s.kind != srcBin {
			m.cell(c, cp, s, int(w.row))
			continue
		}
		raw := s.col.values[w.row]
		if raw == nil {
			c.AppendNull()
			continue
		}
		if len(raw) > 0 {
			m.pin(b, w.slot)
		}
		c.Append(raw[:len(raw):len(raw)])
	}
}

func (m *timeMerge) gatherStrArrays(b *model.MeasureBatch, c *vectorized.TypedColumn[[]string], cp *colPlan) {
	var bb *bytes.Buffer
	var slab []string
	defer func() {
		if bb != nil {
			bigValuePool.Release(bb)
		}
	}()
	for _, w := range m.winners {
		s := &m.pageEntries[w.slot].e.src[cp.ord]
		if s.kind != srcStrArr {
			m.cell(c, cp, s, int(w.row))
			continue
		}
		if bb == nil {
			bb = bigValuePool.Generate()
		}
		if appendBorrowedStrArr(c, s.col.values[w.row], &slab, bb) {
			m.pin(b, w.slot)
		}
	}
}

// close releases every cursor the merge still holds. It is idempotent.
func (m *timeMerge) close() {
	if m.closed {
		return
	}
	m.closed = true
	for i, e := range m.heap {
		releaseBlockCursor(e.bc)
		e.bc = nil
		m.heap[i] = nil
	}
	m.heap = m.heap[:0]
	m.releaseFinished()
	for i := m.pi; i < len(m.pending); i++ {
		if bc := m.pending[i].bc; bc != nil {
			releaseBlockCursor(bc)
			m.pending[i].bc = nil
		}
	}
	m.pending = m.pending[:0]
	m.live = 0
	m.stats.mergePeakLive = max(m.stats.mergePeakLive, int64(m.peakLive))
	if qm := m.qr.qm; qm != nil {
		qm.scanMergeRows.Inc(float64(m.rows))
		qm.scanMergeCursors.Inc(float64(m.cursors))
		qm.scanMergePeakLive.Observe(float64(m.peakLive))
	}
}
