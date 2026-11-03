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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// SortKey defines one typed ordering expression and its resolved null placement.
// Callers resolve SQL defaults to NullsFirst=false for ASC and true for DESC.
type SortKey struct {
	Expr       *Expression
	Descending bool
	NullsFirst bool
}

// SortPlan orders completed rows, selects visible outputs, and then paginates.
// Outputs must be explicit; an empty non-nil slice selects only row metadata.
// FullSort selects the reference full-sort path even when Limit is finite.
type SortPlan struct {
	Limit    *uint32
	Keys     []SortKey
	Outputs  []Slot
	Offset   uint32
	FullSort bool
}

type sortBoundKey struct {
	root    *expressionNode
	read    func(*expressionVector, int) sortValue
	compare func(sortValue, sortValue) int
}

type sortCandidate struct {
	keys        []sortValue
	values      []sortValue
	payload     int64
	livePayload int64
	descriptors int64
}

type sortOperator struct {
	source              vectorized.PullOperator
	tracker             *vectorized.MemoryTracker
	schema              *vectorized.BatchSchema
	kernel              *expressionOperator
	output              *vectorized.RecordBatch
	keys                []sortBoundKey
	indices             []int
	timestampAliases    []int
	readers             []func(int) sortValue
	vectors             []*expressionVector
	rows                []*sortCandidate
	scratch             sortCandidate
	stats               SortStats
	reserved            int64
	descriptor          int64
	rowDescriptor       int64
	outputBytes         int64
	comparisonsRecorded uint64
	position            uint64
	end                 uint64
	k                   uint64
	limit               uint64
	offset              uint64
	limited             bool
	loaded              bool
	empty               bool
	sourceClosed        bool
	closed              bool
}

// WrapSort takes ownership of source on every return path and resolves typed keys once.
// Returned batches borrow only the operator's owned candidates until the next pull or Close.
func WrapSort(ctx context.Context, source vectorized.PullOperator, plan *SortPlan) (_ vectorized.PullOperator, err error) {
	defer func() {
		if err != nil && source != nil {
			_ = source.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || source.OutputSchema() == nil || plan == nil || plan.Outputs == nil || len(plan.Keys) == 0 {
		return nil, fmt.Errorf("sort requires a source schema, keys and explicit outputs")
	}
	o := &sortOperator{source: source, tracker: vectorized.QueryMemoryTracker(ctx, 256<<20), offset: uint64(plan.Offset)}
	o.stats.FullSort = plan.FullSort || sortFullSort(ctx) || plan.Limit == nil
	if plan.Limit != nil {
		o.limited, o.limit = true, uint64(*plan.Limit)
		// Widen both operands before addition; the largest supported k is 2^33-2.
		o.k = o.offset + o.limit
		o.stats.K = o.k
		o.empty = o.limit == 0
	}
	defer func() {
		if err != nil {
			o.release()
		}
	}()
	schema := source.OutputSchema()
	bytes, boundErr := rawDescriptorBound(len(plan.Outputs)+len(plan.Keys), len(schema.Columns), 0)
	if boundErr != nil {
		return nil, boundErr
	}
	if err = o.reserve(bytes); err != nil {
		return nil, err
	}
	o.descriptor = bytes
	defs := make([]vectorized.ColumnDef, 0, len(plan.Outputs)+len(schema.Columns))
	for _, slot := range plan.Outputs {
		index := sourceIndex(schema, slot.Source)
		if index < 0 {
			return nil, fmt.Errorf("sort output column %q in family %q is absent", slot.Source.Name, slot.Source.TagFamily)
		}
		definition := slot.Output
		definition.Type = schema.Columns[index].Type
		if slot.Timestamp || slot.Source.Role == vectorized.RoleTimestamp && slot.Output.Role == vectorized.RoleTag {
			o.timestampAliases = append(o.timestampAliases, len(defs))
		}
		o.indices = append(o.indices, index)
		defs = append(defs, definition)
	}
	for index, definition := range schema.Columns {
		if definition.Role != vectorized.RoleField && definition.Role != vectorized.RoleTag {
			o.indices = append(o.indices, index)
			defs = append(defs, definition)
		}
	}
	o.schema = vectorized.NewBatchSchema(defs)
	o.readers = make([]func(int) sortValue, len(o.indices))
	o.vectors = make([]*expressionVector, len(plan.Keys))
	o.scratch.keys = make([]sortValue, len(plan.Keys))
	o.scratch.values = make([]sortValue, len(o.indices))
	// Each live candidate owns two exact-length cell slices and its descriptor.
	// 128 bytes per cell conservatively includes allocator and uncommon boxed-array headers.
	o.rowDescriptor = 128 + int64(len(plan.Keys)+len(o.indices))*128
	o.kernel = &expressionOperator{source: source, tracker: o.tracker}
	o.kernel.metrics = &operatorMetrics{expressionWork: o.recordKeyWork, reserve: o.recordReserve, release: o.recordRelease}
	for _, key := range plan.Keys {
		cloneBytes, cloneErr := expressionCloneBound(key.Expr, 0)
		if cloneErr != nil {
			return nil, cloneErr
		}
		if err = o.kernel.reserve(cloneBytes); err != nil {
			return nil, err
		}
		resolved, resolveErr := ResolveExpression(key.Expr)
		if resolveErr != nil {
			return nil, resolveErr
		}
		bound, keyErr := o.bindKey(resolved, key)
		if keyErr != nil {
			return nil, keyErr
		}
		o.keys = append(o.keys, bound)
	}
	return o, nil
}

func (o *sortOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if o.empty || o.closed {
		return nil
	}
	if err := o.source.Init(ctx); err != nil {
		_ = o.Close()
		return err
	}
	return nil
}

func (o *sortOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }

func (o *sortOperator) closeSource() error {
	if o.sourceClosed {
		return nil
	}
	o.sourceClosed = true
	return o.source.Close()
}

func (o *sortOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.release()
	return o.closeSource()
}

func (o *sortOperator) release() {
	o.flushComparisons()
	o.clearInput()
	if o.kernel != nil {
		o.kernel.release()
	}
	o.rows = nil
	o.scratch = sortCandidate{}
	o.keys = nil
	o.indices = nil
	o.timestampAliases = nil
	o.readers = nil
	o.vectors = nil
	o.output = nil
	o.schema = nil
	if o.reserved > 0 {
		o.unreserve(o.reserved)
	}
	o.stats.Candidates, o.stats.PayloadBytes, o.stats.PayloadCapacity = 0, 0, 0
}

func (o *sortOperator) clearInput() {
	if o.kernel == nil {
		return
	}
	o.kernel.clearBatch()
	for _, node := range o.kernel.nodes {
		typ := node.value.typ
		node.value = node.bufferValue
		if node.capacity == 0 {
			node.value = expressionVector{typ: typ}
		}
	}
	clear(o.readers)
	clear(o.vectors)
	clear(o.scratch.keys)
	clear(o.scratch.values)
}

func (o *sortOperator) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return nil, err
	}
	if o.closed || o.empty {
		return nil, nil
	}
	if !o.loaded {
		if err := o.load(ctx); err != nil {
			_ = o.Close()
			return nil, err
		}
	}
	if o.position >= o.end {
		return nil, nil
	}
	batch, err := o.emit(ctx)
	if err != nil {
		_ = o.Close()
		return nil, err
	}
	return batch, nil
}

func (o *sortOperator) load(ctx context.Context) error {
	for {
		o.clearInput()
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := o.source.NextBatch(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		if err = o.consume(ctx, batch); err != nil {
			return err
		}
	}
	o.clearInput()
	if err := o.closeSource(); err != nil {
		return err
	}
	if err := o.sortRows(ctx); err != nil {
		return err
	}
	o.flushComparisons()
	o.loaded = true
	o.position, o.end = min(o.offset, uint64(len(o.rows))), uint64(len(o.rows))
	if o.limited {
		o.end = min(o.end, o.k)
	}
	if o.position < o.end {
		if err := o.prepareOutput(int(min(uint64(vectorized.DefaultBatchSize), o.end-o.position))); err != nil {
			return err
		}
	}
	o.stats.Executions++
	recordSortCompletion(o.stats)
	return nil
}

func (o *sortOperator) consume(ctx context.Context, batch *vectorized.RecordBatch) error {
	if batch.Len < 0 || batch.Len > 65536 {
		return expressionError("sort batch size exceeds selection range")
	}
	for _, selected := range batch.Selection {
		if int(selected) >= batch.Len {
			return expressionError("sort selection index exceeds batch rows")
		}
	}
	if batch.ActiveLen() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.stats.Batches++
	o.stats.InputRows += uint64(batch.ActiveLen())
	recordSortWork(SortStats{Batches: 1, InputRows: uint64(batch.ActiveLen())})
	for index, key := range o.keys {
		value, err := key.root.eval(ctx, batch, batch.Selection)
		if err != nil {
			return err
		}
		o.vectors[index] = value
	}
	for pos, index := range o.indices {
		if index >= len(batch.Columns) || batch.Columns[index] == nil || batch.Columns[index].Len() < batch.Len {
			return expressionError("sort output column does not cover batch")
		}
		if batch.Columns[index].Type() != o.schema.Columns[pos].Type {
			return expressionError("sort output column type changed")
		}
		reader, err := sortReadColumn(batch.Columns[index])
		if err != nil {
			return err
		}
		o.readers[pos] = reader
	}
	for pos := 0; pos < batch.ActiveLen(); pos++ {
		if pos%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		row := expressionRow(batch.Selection, pos)
		for index, key := range o.keys {
			o.scratch.keys[index] = key.read(o.vectors[index], row)
		}
		if !o.stats.FullSort && uint64(len(o.rows)) == o.k && o.compare(&o.scratch, o.rows[0]) >= 0 {
			continue
		}
		for index, read := range o.readers {
			o.scratch.values[index] = read(row)
		}
		if err := o.acceptCandidate(ctx); err != nil {
			return err
		}
	}
	o.clearInput()
	return sortExecutionHook(ctx, o.stats)
}

func (o *sortOperator) acceptCandidate(ctx context.Context) error {
	if o.stats.FullSort || uint64(len(o.rows)) < o.k {
		if err := o.appendCandidate(); err != nil {
			return err
		}
		if !o.stats.FullSort {
			for child := len(o.rows) - 1; child > 0; {
				parent := (child - 1) / 2
				if o.compare(o.rows[parent], o.rows[child]) >= 0 {
					break
				}
				o.rows[parent], o.rows[child] = o.rows[child], o.rows[parent]
				child = parent
			}
		}
		return nil
	}
	candidate := o.rows[0]
	clear(candidate.keys)
	clear(candidate.values)
	o.unreserve(candidate.payload)
	o.descriptor -= candidate.descriptors
	o.stats.PayloadBytes -= uint64(candidate.livePayload)
	o.stats.PayloadCapacity -= uint64(candidate.livePayload * 2)
	candidate.payload, candidate.livePayload, candidate.descriptors = 0, 0, 0
	if err := o.copyCandidate(candidate); err != nil {
		return err
	}
	o.stats.AcceptedRows++
	o.stats.EvictedRows++
	o.stats.DescriptorBytes = uint64(o.descriptor)
	recordSortWork(SortStats{AcceptedRows: 1, EvictedRows: 1})
	return o.siftDown(ctx, 0, len(o.rows))
}

func (o *sortOperator) appendCandidate() error {
	if len(o.rows) == cap(o.rows) {
		if cap(o.rows) > math.MaxInt/16 {
			return fmt.Errorf("%w: sort candidate capacity overflows", vectorized.ErrMemoryBudgetExceeded)
		}
		capacity := max(16, cap(o.rows)*2)
		if !o.stats.FullSort {
			capacity = int(min(uint64(capacity), o.k))
		}
		if capacity > math.MaxInt/8 {
			return fmt.Errorf("%w: sort candidate capacity overflows", vectorized.ErrMemoryBudgetExceeded)
		}
		bytes := int64(capacity) * 8
		if err := o.reserve(bytes); err != nil {
			return err
		}
		rows := make([]*sortCandidate, len(o.rows), capacity)
		copy(rows, o.rows)
		old := int64(cap(o.rows)) * 8
		o.rows = rows
		if old != 0 {
			o.unreserve(old)
		}
		o.descriptor += bytes - old
	}
	if err := o.reserve(o.rowDescriptor); err != nil {
		return err
	}
	candidate := &sortCandidate{keys: make([]sortValue, len(o.keys)), values: make([]sortValue, len(o.indices))}
	o.rows = append(o.rows, candidate)
	o.descriptor += o.rowDescriptor
	if err := o.copyCandidate(candidate); err != nil {
		return err
	}
	o.stats.AcceptedRows++
	o.stats.Candidates = uint64(len(o.rows))
	o.stats.PeakCandidates = max(o.stats.PeakCandidates, o.stats.Candidates)
	o.stats.CandidateCapacity = uint64(cap(o.rows))
	o.stats.DescriptorBytes = uint64(o.descriptor)
	recordSortWork(SortStats{AcceptedRows: 1})
	return nil
}

func (o *sortOperator) compare(left, right *sortCandidate) int {
	o.stats.Comparisons++
	for index, key := range o.keys {
		if order := key.compare(left.keys[index], right.keys[index]); order != 0 {
			return order
		}
	}
	return 0
}

// Heapsort has a fixed O(N log N) comparison bound and checks cancellation at
// each sift, including adversarial tied or reverse-ordered inputs.
func (o *sortOperator) sortRows(ctx context.Context) error {
	for root := len(o.rows)/2 - 1; root >= 0; root-- {
		if err := o.siftDown(ctx, root, len(o.rows)); err != nil {
			return err
		}
	}
	for end := len(o.rows) - 1; end > 0; end-- {
		o.rows[0], o.rows[end] = o.rows[end], o.rows[0]
		if err := o.siftDown(ctx, 0, end); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (o *sortOperator) flushComparisons() {
	recordSortWork(SortStats{Comparisons: o.stats.Comparisons - o.comparisonsRecorded})
	o.comparisonsRecorded = o.stats.Comparisons
}

func (o *sortOperator) siftDown(ctx context.Context, root, end int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for root < end/2 {
		child := root*2 + 1
		if child+1 < end && o.compare(o.rows[child], o.rows[child+1]) < 0 {
			child++
		}
		if o.compare(o.rows[root], o.rows[child]) >= 0 {
			break
		}
		o.rows[root], o.rows[child] = o.rows[child], o.rows[root]
		root = child
	}
	return nil
}

func (o *sortOperator) emit(ctx context.Context) (*vectorized.RecordBatch, error) {
	size := int(min(uint64(vectorized.DefaultBatchSize), o.end-o.position))
	o.output.Reset()
	for row := 0; row < size; row++ {
		if row%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		candidate := o.rows[o.position]
		for index, value := range candidate.values {
			sortAppendValue(o.output.Columns[index], value)
		}
		o.position++
	}
	o.output.Len = size
	o.stats.OutputRows += uint64(size)
	recordSortWork(SortStats{OutputRows: uint64(size)})
	return o.output, nil
}

func (o *sortOperator) prepareOutput(size int) error {
	// Cell headers and validity bitmaps dominate all supported output types.
	bytes := int64(len(o.indices))*int64(size)*40 + 1024
	if err := o.reserve(bytes); err != nil {
		return err
	}
	o.outputBytes = bytes
	o.descriptor += bytes
	o.stats.DescriptorBytes = uint64(o.descriptor)
	o.output = vectorized.NewRecordBatch(o.schema, size)
	return nil
}

func (o *sortOperator) reserve(bytes int64) error {
	if err := o.tracker.Reserve(bytes); err != nil {
		return err
	}
	o.reserved += bytes
	o.recordReserve(bytes)
	return nil
}

func (o *sortOperator) unreserve(bytes int64) {
	o.tracker.Release(bytes)
	o.reserved -= bytes
	o.recordRelease(bytes)
}
