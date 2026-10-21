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

package ql

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// SlotID identifies one independently named projection result.
type SlotID uint32

// Slot describes the source and visible response identity of a typed projection.
type Slot struct {
	Source   vectorized.ColumnDef
	Output   vectorized.ColumnDef
	ID       SlotID
	Nullable bool
}

// MillisRange is a half-open timestamp interval in Unix milliseconds.
type MillisRange struct{ Begin, End int64 }

// RawPlan is the immutable bound recipe for raw measure batch execution.
type RawPlan struct {
	Limit    *uint32
	Slots    []Slot
	Ranges   []MillisRange
	Offset   uint32
	Residual bool
	Empty    bool
}

type rawOperator struct {
	tracker          *vectorized.MemoryTracker
	source           vectorized.PullOperator
	schema           *vectorized.BatchSchema
	indices          []int
	timestampAliases []int
	ranges           []MillisRange
	selection        []uint16
	view             vectorized.RecordBatch
	selectionBytes   int64
	metadataBytes    int64
	offset           uint64
	remaining        uint64
	timestamp        int
	residual         bool
	limited          bool
	empty            bool
	closed           bool
}

// WrapRaw takes ownership of source, including on failure, and resolves projection indices once.
// Its output lends immutable source columns until the next pull or Close.
func WrapRaw(ctx context.Context, source vectorized.PullOperator, plan *RawPlan) (_ vectorized.PullOperator, err error) {
	defer func() {
		if err != nil && source != nil {
			_ = source.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || plan == nil || source.OutputSchema() == nil {
		return nil, fmt.Errorf("raw query requires a source schema and plan")
	}

	schema := source.OutputSchema()
	metadataColumns := 0
	for _, def := range schema.Columns {
		if def.Role != vectorized.RoleTag && def.Role != vectorized.RoleField {
			metadataColumns++
		}
	}
	bytes, boundErr := rawDescriptorBound(len(plan.Slots), metadataColumns, len(plan.Ranges))
	if boundErr != nil {
		return nil, boundErr
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256<<20)
	if err = tracker.Reserve(bytes); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			tracker.Release(bytes)
		}
	}()
	width := len(plan.Slots) + metadataColumns
	op := &rawOperator{
		tracker: tracker, metadataBytes: bytes, indices: make([]int, 0, width), timestampAliases: make([]int, 0, len(plan.Slots)),
		source: source, timestamp: schema.TimestampIndex(), ranges: slices.Clone(plan.Ranges),
		residual: plan.Residual, offset: uint64(plan.Offset), empty: plan.Empty,
	}
	if plan.Limit != nil {
		op.limited = true
		op.remaining = uint64(*plan.Limit)
		op.empty = op.empty || op.remaining == 0
	}
	defs := make([]vectorized.ColumnDef, 0, width)
	for _, slot := range plan.Slots {
		index := sourceIndex(schema, slot.Source)
		if index < 0 {
			return nil, fmt.Errorf("raw source column %q in family %q is absent", slot.Source.Name, slot.Source.TagFamily)
		}
		def := slot.Output
		def.Type = schema.Columns[index].Type
		if slot.Source.Role == vectorized.RoleTimestamp && slot.Output.Role == vectorized.RoleTag {
			op.timestampAliases = append(op.timestampAliases, len(defs))
		}
		op.indices = append(op.indices, index)
		defs = append(defs, def)
	}
	// Identity metadata remains available for final egress and never becomes a visible value.
	for index, def := range schema.Columns {
		if def.Role != vectorized.RoleTag && def.Role != vectorized.RoleField {
			op.indices = append(op.indices, index)
			defs = append(defs, def)
		}
	}
	if op.residual && op.timestamp < 0 {
		return nil, fmt.Errorf("raw time residual requires timestamp metadata")
	}
	op.schema = vectorized.NewBatchSchema(defs)
	op.view = vectorized.RecordBatch{Schema: op.schema, Columns: make([]vectorized.Column, len(defs)), Lent: true}
	return op, nil
}

// rawDescriptorBound covers column descriptors, lookup maps and their buckets,
// family layouts, index/column slices, iterator state and cloned range capacity.
// 1 KiB per column includes separate tag/family map entries in the worst layout;
// value buffers are borrowed and have their own native or final-egress charges.
func rawDescriptorBound(visible, metadata, ranges int) (int64, error) {
	const baseBytes, columnBytes, rangeBytes = int64(2048), int64(1024), int64(32)
	if int64(visible) > (math.MaxInt64-baseBytes)/columnBytes-int64(metadata) {
		return 0, fmt.Errorf("%w: raw column descriptor size overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	bytes := baseBytes + (int64(visible)+int64(metadata))*columnBytes
	if int64(ranges) > (math.MaxInt64-bytes)/rangeBytes {
		return 0, fmt.Errorf("%w: raw range descriptor size overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	return bytes + int64(ranges)*rangeBytes, nil
}

func sourceIndex(schema *vectorized.BatchSchema, def vectorized.ColumnDef) int {
	switch def.Role {
	case vectorized.RoleTimestamp:
		return schema.TimestampIndex()
	case vectorized.RoleVersion, vectorized.RoleSeriesID, vectorized.RoleShardID, vectorized.RoleElementID, vectorized.RoleOrderKey:
		return -1
	case vectorized.RoleTag:
		index, ok := schema.TagIndex(def.TagFamily, def.Name)
		if ok {
			return index
		}
	case vectorized.RoleField:
		index, ok := schema.FieldIndex(def.Name)
		if ok {
			return index
		}
	}
	return -1
}

func (o *rawOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if o.empty {
		return nil
	}
	if err := o.source.Init(ctx); err != nil {
		_ = o.Close()
		return err
	}
	return nil
}
func (o *rawOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }
func (o *rawOperator) Close() error {
	if o.closed {
		return nil
	}
	o.closed = true
	o.view.Columns = nil
	o.view.Selection = nil
	o.view.Schema = nil
	o.selection = nil
	o.indices = nil
	o.timestampAliases = nil
	o.ranges = nil
	o.schema = nil
	if o.metadataBytes != 0 {
		o.tracker.Release(o.metadataBytes)
		o.metadataBytes = 0
	}
	if o.selectionBytes != 0 {
		o.tracker.Release(o.selectionBytes)
		o.selectionBytes = 0
	}
	return o.source.Close()
}

func (o *rawOperator) NextBatch(ctx context.Context) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return nil, err
	}
	if o.closed || o.empty || (o.limited && o.remaining == 0) {
		return nil, nil
	}
	for {
		batch, err := o.source.NextBatch(ctx)
		if err != nil {
			_ = o.Close()
			return nil, err
		}
		if batch == nil {
			return nil, nil
		}
		if batch.ActiveLen() == 0 {
			continue
		}
		if batch.Len > math.MaxUint16+1 {
			_ = o.Close()
			return nil, fmt.Errorf("raw source batch has %d rows; maximum is %d", batch.Len, math.MaxUint16+1)
		}
		selection, selectErr := o.selectRows(ctx, batch)
		if selectErr != nil {
			_ = o.Close()
			return nil, selectErr
		}
		if selection != nil && len(selection) == 0 {
			if o.limited && o.remaining == 0 {
				return nil, nil
			}
			continue
		}
		for index, sourceIndex := range o.indices {
			o.view.Columns[index] = batch.Columns[sourceIndex]
		}
		o.view.Len = batch.Len
		o.view.Selection = selection
		o.view.Hints = batch.Hints
		recordRawProjection()
		return &o.view, nil
	}
}

func (o *rawOperator) selectRows(ctx context.Context, batch *vectorized.RecordBatch) ([]uint16, error) {
	if !o.residual && o.offset == 0 && (!o.limited || o.remaining >= uint64(batch.ActiveLen())) {
		if o.limited {
			o.remaining -= uint64(batch.ActiveLen())
		}
		recordRawSelection(0, 0, uint64(batch.ActiveLen()), uint64(batch.ActiveLen()))
		return batch.Selection, nil
	}
	if cap(o.selection) < batch.ActiveLen() {
		bytes := int64(batch.ActiveLen()) * 2
		if err := o.tracker.Reserve(bytes); err != nil {
			return nil, err
		}
		if o.selectionBytes != 0 {
			o.tracker.Release(o.selectionBytes)
		}
		o.selectionBytes = bytes
		o.selection = make([]uint16, 0, batch.ActiveLen())
	}
	o.selection = o.selection[:0]
	var timestamps []int64
	if o.residual {
		column, ok := batch.Columns[o.timestamp].(*vectorized.TypedColumn[int64])
		if !ok {
			return nil, fmt.Errorf("raw timestamp metadata is not int64")
		}
		timestamps = column.Data()
	}
	var filterInput, filterOutput, limitInput uint64
	for pos := 0; pos < batch.ActiveLen(); pos++ {
		if o.limited && o.remaining == 0 {
			break
		}
		if pos%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		row := pos
		if batch.Selection != nil {
			row = int(batch.Selection[pos])
		}
		if o.residual {
			filterInput++
			if !inMillisRanges(timestamps[row], o.ranges) {
				continue
			}
			filterOutput++
		}
		limitInput++
		if o.offset > 0 {
			o.offset--
			continue
		}
		o.selection = append(o.selection, uint16(row))
		if o.limited {
			o.remaining--
		}
	}
	if o.selection == nil {
		o.selection = make([]uint16, 0)
	}
	recordRawSelection(filterInput, filterOutput, limitInput, uint64(len(o.selection)))
	return o.selection, nil
}

func inMillisRanges(nanos int64, ranges []MillisRange) bool {
	millis := nanos / int64(1000000)
	if nanos < 0 && nanos%1000000 != 0 {
		millis--
	}
	for _, interval := range ranges {
		if millis >= interval.Begin && millis < interval.End {
			return true
		}
	}
	return false
}
