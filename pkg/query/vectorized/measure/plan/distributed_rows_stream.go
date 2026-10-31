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

package plan

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	itersort "github.com/apache/skywalking-banyandb/pkg/iter/sort"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// seenSIDEntryBytes is what one (group, series ID) entry of the index-mode seen-set is charged.
const seenSIDEntryBytes uint64 = 64

// rowBatchSource yields the decoded batches of one data node in order.
type rowBatchSource interface {
	// next returns the next non-empty batch, which the caller releases, or nil at the end.
	next(ctx context.Context) (*vectorized.RecordBatch, error)
}

// rowStreamInput is one source of the merge, with the batch already pulled from it, if any.
type rowStreamInput struct {
	src    rowBatchSource
	first  *vectorized.RecordBatch
	source int
	group  int
}

// retiredBatches collects the source batches whose rows have all been handed to the merge. A retired batch's last
// row was popped by the call that retired it, so the row waits in the open sort group or in the one the call
// opened, and the batch must outlive both: it is released at the second flush after its retirement.
type retiredBatches struct {
	now  []*vectorized.RecordBatch
	prev []*vectorized.RecordBatch
}

func (r *retiredBatches) retire(b *vectorized.RecordBatch) { r.now = append(r.now, b) }

// flushed is called after a sort group has been flushed.
func (r *retiredBatches) flushed() {
	releaseBatches(r.prev)
	r.prev, r.now = r.now, r.prev[:0]
}

// releaseAll releases every retired batch, for the end of the merge and for closing it.
func (r *retiredBatches) releaseAll() {
	releaseBatches(r.prev)
	releaseBatches(r.now)
	r.prev, r.now = r.prev[:0], r.now[:0]
}

// lazyRowIter is a source of the k-way merge that pulls its next batch when the current one is used up. A batch is
// sorted on its own, which keeps the merge correct when a data node emits several shards one after another.
type lazyRowIter struct {
	ctx        context.Context
	src        rowBatchSource
	reshape    func(*vectorized.RecordBatch) (*vectorized.RecordBatch, error)
	schema     *vectorized.BatchSchema
	retired    *retiredBatches
	err        error
	cur        *distributedRowSourceIter
	curBatch   *vectorized.RecordBatch
	first      *vectorized.RecordBatch
	val        *distributedRowItem
	seqBase    int
	source     int
	group      int
	sortColIdx int
	desc       bool
	done       bool
}

// Next implements itersort.Iterator. A failure ends the iteration; the merge reads it from err.
func (l *lazyRowIter) Next() bool {
	for {
		if l.cur != nil {
			if l.cur.Next() {
				l.val = l.cur.Val()
				l.val.seq += l.seqBase
				return true
			}
			l.seqBase += l.cur.seq
			l.cur = nil
			l.retired.retire(l.curBatch)
			l.curBatch = nil
		}
		if l.done {
			return false
		}
		batch := l.first
		l.first = nil
		if batch == nil {
			var err error
			batch, err = l.src.next(l.ctx)
			if err != nil {
				l.err, l.done = err, true
				return false
			}
		}
		if batch == nil {
			l.done = true
			return false
		}
		if l.reshape != nil {
			reshaped, err := l.reshape(batch)
			if err != nil {
				batch.ReleaseOwners()
				l.err, l.done = err, true
				return false
			}
			batch = reshaped
		}
		cur, err := newDistributedRowSourceIter(batch, l.schema, l.source, l.group, l.desc, l.sortColIdx)
		if err != nil {
			batch.ReleaseOwners()
			l.err, l.done = err, true
			return false
		}
		l.cur, l.curBatch = cur, batch
	}
}

// Val implements itersort.Iterator.
func (l *lazyRowIter) Val() *distributedRowItem { return l.val }

// Close implements itersort.Iterator. It releases the batch in use and the one that was pulled but not started.
func (l *lazyRowIter) Close() error {
	if l.curBatch != nil {
		l.curBatch.ReleaseOwners()
		l.curBatch, l.cur = nil, nil
	}
	if l.first != nil {
		l.first.ReleaseOwners()
		l.first = nil
	}
	l.done = true
	return nil
}

// rowMerger is the liaison-side non-aggregation row merge as a pull source. It pulls from the data nodes only as
// far as the next output batch needs, keeping the two dedup rules of the row merge: within a sort group one row
// per (series ID, timestamp), the one with the highest version, and in index mode a (group, series ID) is
// suppressed once an earlier sort group has emitted it.
type rowMerger struct {
	ctx     context.Context
	merger  itersort.Iterator[*distributedRowItem]
	iters   []*lazyRowIter
	emitter *distributedRowEmitter
	retired *retiredBatches
	spec    distributedRowsSpec
	schema  *vectorized.BatchSchema
	rows    int
	done    bool
	closed  bool
	err     error
}

// newRowMerger builds the merge over inputs, whose batches have the layout of schema once reshape has run. It
// takes ownership of every first batch.
func newRowMerger(ctx context.Context, inputs []rowStreamInput, schema *vectorized.BatchSchema,
	reshape func(*vectorized.RecordBatch) (*vectorized.RecordBatch, error), spec distributedRowsSpec,
) (*rowMerger, error) {
	releaseInputs := func() {
		for _, in := range inputs {
			if in.first != nil {
				in.first.ReleaseOwners()
			}
		}
	}
	if schema.TimestampIndex() < 0 {
		releaseInputs()
		return nil, fmt.Errorf("row merge requires a timestamp column")
	}
	batchSize := spec.BatchSize
	if batchSize <= 0 {
		batchSize = vectorized.DefaultBatchSize
	}
	spec.BatchSize = batchSize
	sortColIdx, sortErr := resolveDistributedSortColumn(schema, spec)
	if sortErr != nil {
		releaseInputs()
		return nil, sortErr
	}
	retired := &retiredBatches{}
	iters := make([]*lazyRowIter, 0, len(inputs))
	merged := make([]itersort.Iterator[*distributedRowItem], 0, len(inputs))
	for _, in := range inputs {
		iter := &lazyRowIter{
			ctx: ctx, src: in.src, reshape: reshape, schema: schema, retired: retired, first: in.first,
			source: in.source, group: in.group, sortColIdx: sortColIdx, desc: spec.Desc,
		}
		iters = append(iters, iter)
		merged = append(merged, iter)
	}
	merger := &rowMerger{
		ctx: ctx, iters: iters, retired: retired, spec: spec, schema: schema,
		emitter: &distributedRowEmitter{
			pool:       vectorized.NewBatchPool(schema, batchSize),
			schema:     schema,
			batchSize:  batchSize,
			tracker:    spec.Tracker,
			rowWidth:   estimateRowWidth(schema),
			seenSID:    make(map[seenSIDKey]struct{}),
			indexMode:  spec.IndexMode,
			chargeSeen: spec.ChargeSeen,
			window:     make(map[distributedRowKey]*distributedRowItem),
			noDedup:    schema.SeriesIDIndex() < 0 || schema.VersionIndex() < 0,
		},
	}
	// Building the merge pulls the first row of every source.
	merger.merger = itersort.NewItemIter(merged, spec.Desc)
	if err := merger.sourceError(); err != nil {
		_ = merger.Close()
		return nil, err
	}
	return merger, nil
}

// sourceError reports the first failure of a source.
func (m *rowMerger) sourceError() error {
	for _, iter := range m.iters {
		if iter.err != nil {
			return iter.err
		}
	}
	return nil
}

// Init implements vectorized.BatchOperator.
func (m *rowMerger) Init(context.Context) error { return nil }

// OutputSchema implements vectorized.BatchOperator.
func (m *rowMerger) OutputSchema() *vectorized.BatchSchema { return m.schema }

// NextBatch returns the next merged batch, or nil once every source is exhausted.
func (m *rowMerger) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	for {
		if len(m.emitter.output) > 0 {
			out := m.emitter.output[0]
			m.emitter.output[0] = nil
			m.emitter.output = m.emitter.output[1:]
			return out, nil
		}
		if m.err != nil {
			return nil, m.err
		}
		if m.done || m.closed {
			return nil, nil
		}
		if err := m.advance(ctx); err != nil {
			m.err = err
			return nil, err
		}
	}
}

// advance merges rows until the emitter has an output batch or the sources end.
func (m *rowMerger) advance(ctx context.Context) error {
	e := m.emitter
	for len(e.output) == 0 {
		if m.rows%interruptCheckRows == 0 {
			if ctxErr := errors.Join(ctx.Err(), m.ctx.Err()); ctxErr != nil {
				return ctxErr
			}
			if m.spec.Interrupt != nil {
				if interruptErr := m.spec.Interrupt(); interruptErr != nil {
					return interruptErr
				}
			}
		}
		if !m.merger.Next() {
			if err := m.sourceError(); err != nil {
				return err
			}
			if err := e.flushWindow(); err != nil {
				return err
			}
			if err := e.finalizeBatch(); err != nil {
				return err
			}
			m.retired.releaseAll()
			m.done = true
			return nil
		}
		m.rows++
		item := m.merger.Val()
		// A null sort cell encodes as a nil key, so "no window open" is tracked explicitly: a
		// nil windowKey alone would merge a null row's window into the next non-null row's.
		if e.windowOpen && !bytes.Equal(item.sortField, e.windowKey) {
			if err := e.flushWindow(); err != nil {
				return err
			}
			m.retired.flushed()
		}
		if !e.windowOpen {
			e.windowKey = item.sortField
			e.windowOpen = true
		}
		e.accept(item)
	}
	return nil
}

// Close releases every batch the merge still holds. It does not close the node streams, which the scan set owns.
func (m *rowMerger) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	err := m.merger.Close()
	m.retired.releaseAll()
	if m.emitter.current != nil {
		m.emitter.current.ReleaseOwners()
		m.emitter.current = nil
	}
	releaseBatches(m.emitter.output)
	m.emitter.output = nil
	return err
}

// layoutReshape returns the function that brings a source batch onto schema: nothing when the layouts are the
// same, the coercion of the cells otherwise. A batch whose layout differs from the schema's in more than a
// promoted column type is a producer bug.
func layoutReshape(schema *vectorized.BatchSchema, always bool) func(*vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
	return func(b *vectorized.RecordBatch) (*vectorized.RecordBatch, error) {
		if !always && slices.Equal(b.Schema.Columns, schema.Columns) {
			return b, nil
		}
		return reshapeToMergedSchema(b, schema)
	}
}

// sliceBatchSource serves already decoded batches, for the callers that hold their frames.
type sliceBatchSource struct {
	batches []*vectorized.RecordBatch
}

func (s *sliceBatchSource) next(context.Context) (*vectorized.RecordBatch, error) {
	for len(s.batches) > 0 {
		b := s.batches[0]
		s.batches = s.batches[1:]
		if b != nil && b.ActiveLen() > 0 {
			return b, nil
		}
		if b != nil {
			b.ReleaseOwners()
		}
	}
	return nil, nil
}
