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
	"unsafe"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// AggregateFunction identifies an aggregate over one named scalar column.
type AggregateFunction uint8

// Aggregate functions use SQL non-null counting and empty-input semantics.
const (
	AggregateCount AggregateFunction = iota
	AggregateSum
	AggregateMin
	AggregateMax
	AggregateAvg
	AggregateCountDistinct
)

// AggregateCall describes one aggregate input independently of output aliases.
type AggregateCall struct {
	Source   vectorized.ColumnDef
	Function AggregateFunction
}

// AggregateOutput names one selected grouping key or aggregate call.
type AggregateOutput struct {
	Column    vectorized.ColumnDef
	Index     int
	IsGroup   bool
	Timestamp bool // Timestamp requests timestamp-tag encoding at final egress.
}

// AggregatePlan is an immutable recipe for complete local aggregation.
type AggregatePlan struct {
	GroupExpressions *ExpressionPlan
	TimeBucketWidth  string // Resolved existing native width; empty means no bucket.
	GroupBy          []vectorized.ColumnDef
	Calls            []AggregateCall
	Outputs          []AggregateOutput
	TimeBucketIndex  int // GroupBy index of the bucket key when TimeBucketWidth is nonempty.
}

// AggregateStats separates retained group keys and fixed-width state from native input residency.
type AggregateStats struct {
	InputRows           uint64
	Groups              uint64
	GroupCapacity       uint64
	KeyBytes            uint64
	KeyCapacity         uint64
	StateBytes          uint64 // Used fixed-width state bytes for actual groups.
	StateCapacity       uint64 // Reserved fixed-width state capacity in bytes.
	StateLanes          uint64
	DistinctKeys        uint64 // Retained (group, value) pairs across distinct states.
	DistinctKeyBytes    uint64 // Used encoded distinct key bytes.
	DistinctKeyCapacity uint64 // Reserved distinct indexes, key arenas and scratch bytes.
}

type aggregateCallRef struct {
	state    int
	function AggregateFunction
}

type aggregateOperator struct {
	metrics          *groupMetrics
	source           vectorized.PullOperator
	tracker          *vectorized.MemoryTracker
	schema           *vectorized.BatchSchema
	output           *vectorized.RecordBatch
	bucketWidth      string
	states           []aggregateState
	calls            []aggregateCallRef
	outputs          []AggregateOutput
	groupValues      []aggregateValue
	groupReaders     []aggregateReader
	stateReaders     []aggregateReader
	groups           aggregateGroups
	stats            AggregateStats
	distinctLimit    uint64
	metadataBytes    int64
	outputBytes      int64
	outputValueBytes int64
	position         int
	complete         bool
	closed           bool
	sourceClosed     bool
	distinct         bool
	bucketIndex      int
}

// WrapAggregate takes source ownership, including on construction failure.
// It consumes every active input row before emitting synthesized groups, without native identity metadata.
func WrapAggregate(ctx context.Context, source vectorized.PullOperator, plan *AggregatePlan) (_ vectorized.PullOperator, err error) {
	if plan != nil && plan.GroupExpressions != nil {
		source, err = wrapGroupExpressions(ctx, source, plan)
		if err != nil {
			return nil, err
		}
	}
	return wrapAggregate(ctx, source, plan, false)
}

func wrapAggregate(ctx context.Context, source vectorized.PullOperator, plan *AggregatePlan, distinct bool) (_ vectorized.PullOperator, err error) {
	metrics := &aggregateGroupMetrics
	if distinct {
		metrics = &distinctGroupMetrics
	}
	op := &aggregateOperator{source: source, distinct: distinct, metrics: metrics, bucketIndex: -1}
	defer func() {
		if err != nil {
			_ = op.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if source == nil || source.OutputSchema() == nil || plan == nil || len(plan.Outputs) == 0 {
		return nil, fmt.Errorf("aggregate requires a source schema, plan and output columns")
	}
	bytes, boundErr := rawDescriptorBound(len(plan.GroupBy)+len(plan.Calls)+len(plan.Outputs), 0, 0)
	if boundErr != nil {
		return nil, boundErr
	}
	op.tracker = vectorized.QueryMemoryTracker(ctx, 256<<20)
	if err = op.reserve(bytes); err != nil {
		return nil, err
	}
	op.metadataBytes = bytes
	op.groups = aggregateGroups{tracker: op.tracker, metrics: metrics}
	op.groupValues = make([]aggregateValue, len(plan.GroupBy))
	op.groupReaders = make([]aggregateReader, len(plan.GroupBy))
	for index, def := range plan.GroupBy {
		if !aggregateScalarType(def.Type) {
			return nil, fmt.Errorf("aggregate grouping column %q has unsupported type %s", def.Name, def.Type)
		}
		reader, readerErr := newAggregateReader(source.OutputSchema(), def)
		if readerErr != nil {
			return nil, readerErr
		}
		op.groupReaders[index] = reader
		if index == plan.TimeBucketIndex && plan.TimeBucketWidth != "" {
			op.bucketIndex, op.bucketWidth = index, plan.TimeBucketWidth
		}
	}
	if err = op.compileCalls(source.OutputSchema(), plan.Calls); err != nil {
		return nil, err
	}
	defs := make([]vectorized.ColumnDef, len(plan.Outputs))
	op.outputs = append([]AggregateOutput(nil), plan.Outputs...)
	for index, output := range op.outputs {
		if output.Column.Role != vectorized.RoleTag && output.Column.Role != vectorized.RoleField {
			return nil, fmt.Errorf("aggregate output %q must be a tag or field", output.Column.Name)
		}
		var expected vectorized.ColumnType
		if output.IsGroup {
			if output.Index < 0 || output.Index >= len(plan.GroupBy) {
				return nil, fmt.Errorf("aggregate group output index %d is absent", output.Index)
			}
			expected = plan.GroupBy[output.Index].Type
		} else {
			if output.Index < 0 || output.Index >= len(plan.Calls) {
				return nil, fmt.Errorf("aggregate call output index %d is absent", output.Index)
			}
			expected = aggregateOutputType(plan.Calls[output.Index])
		}
		if output.Column.Type != expected {
			return nil, fmt.Errorf("aggregate output %q has type %s; expected %s", output.Column.Name, output.Column.Type, expected)
		}
		defs[index] = output.Column
		defs[index].Entity = false
	}
	if op.bucketIndex >= 0 {
		defs = append(defs, vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64})
	}
	op.schema = vectorized.NewBatchSchema(defs)
	return op, nil
}

func (o *aggregateOperator) compileCalls(schema *vectorized.BatchSchema, calls []AggregateCall) error {
	o.calls = make([]aggregateCallRef, len(calls))
	type stateKey struct {
		source   vectorized.ColumnDef
		distinct bool
	}
	bySource := make(map[stateKey]int)
	for index, call := range calls {
		if call.Function > AggregateCountDistinct || !aggregateScalarType(call.Source.Type) {
			return fmt.Errorf("unsupported aggregate function or source type for %q", call.Source.Name)
		}
		if call.Function != AggregateCount && call.Function != AggregateCountDistinct &&
			call.Source.Type != vectorized.ColumnTypeInt64 && call.Source.Type != vectorized.ColumnTypeFloat64 {
			return fmt.Errorf("aggregate on %q requires a numeric column", call.Source.Name)
		}
		def := call.Source
		def.Entity = false
		key := stateKey{source: def, distinct: call.Function == AggregateCountDistinct}
		state, exists := bySource[key]
		if !exists {
			reader, err := newAggregateReader(schema, def)
			if err != nil {
				return err
			}
			state = len(o.states)
			bySource[key] = state
			entry := aggregateState{source: def}
			if key.distinct {
				bytes, boundErr := groupAllocationBytes(int64(unsafe.Sizeof(aggregateGroups{})))
				if boundErr != nil {
					return boundErr
				}
				if reserveErr := o.reserve(bytes); reserveErr != nil {
					return reserveErr
				}
				o.metadataBytes += bytes
				entry.distinct = &aggregateGroups{tracker: o.tracker, metrics: o.metrics}
			}
			o.states = append(o.states, entry)
			o.stateReaders = append(o.stateReaders, reader)
		}
		o.states[state].require(call.Function)
		o.calls[index] = aggregateCallRef{state: state, function: call.Function}
	}
	return nil
}

func aggregateOutputType(call AggregateCall) vectorized.ColumnType {
	switch call.Function {
	case AggregateCount, AggregateCountDistinct:
		return vectorized.ColumnTypeInt64
	case AggregateAvg:
		return vectorized.ColumnTypeFloat64
	default:
		return call.Source.Type
	}
}

// AggregateStatistics returns the structural state of a local aggregate, including after Close.
func AggregateStatistics(source vectorized.PullOperator) (AggregateStats, bool) {
	for current := source; current != nil; current = operatorSource(current) {
		if op, ok := current.(*aggregateOperator); ok && !op.distinct {
			return op.statistics(), true
		}
	}
	return AggregateStats{}, false
}

func (o *aggregateOperator) statistics() AggregateStats {
	stats := o.stats
	if o.closed {
		return stats
	}
	stats.Groups = uint64(len(o.groups.keys))
	stats.GroupCapacity = uint64(o.groups.capacity)
	stats.KeyBytes = uint64(o.groups.keyBytes)
	stats.KeyCapacity = uint64(o.groups.tableBytes + o.groups.keyReserved + o.groups.scratchBytes)
	for index := range o.states {
		stats.StateLanes += uint64(o.states[index].lanes())
		if keys := o.states[index].distinct; keys != nil {
			stats.DistinctKeys += uint64(len(keys.keys))
			stats.DistinctKeyBytes += uint64(keys.keyBytes)
			stats.DistinctKeyCapacity += uint64(keys.tableBytes + keys.keyReserved + keys.scratchBytes)
		}
	}
	stats.StateBytes = stats.Groups * stats.StateLanes * 8
	stats.StateCapacity = stats.GroupCapacity * stats.StateLanes * 8
	return stats
}

func (o *aggregateOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }

func (o *aggregateOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if o.closed {
		return fmt.Errorf("aggregate is closed")
	}
	if err := o.source.Init(ctx); err != nil {
		_ = o.Close()
		return err
	}
	return nil
}

func (o *aggregateOperator) closeSource() error {
	if o.sourceClosed || o.source == nil {
		return nil
	}
	o.sourceClosed = true
	return o.source.Close()
}

func (o *aggregateOperator) Close() error {
	if o.closed {
		return nil
	}
	o.stats = o.statistics()
	o.closed = true
	o.groups.close()
	for index := range o.states {
		if o.states[index].distinct != nil {
			o.states[index].distinct.close()
		}
		o.states[index] = aggregateState{}
	}
	o.states = nil
	o.groupValues, o.groupReaders, o.stateReaders = nil, nil, nil
	o.output, o.schema, o.outputs, o.calls = nil, nil, nil, nil
	o.release(o.metadataBytes + o.outputBytes + o.outputValueBytes + int64(o.stats.StateCapacity))
	o.metadataBytes, o.outputBytes, o.outputValueBytes = 0, 0, 0
	return o.closeSource()
}

func (o *aggregateOperator) reserve(bytes int64) error {
	if err := o.tracker.Reserve(bytes); err != nil {
		return err
	}
	o.metrics.reserve(bytes)
	return nil
}

func (o *aggregateOperator) release(bytes int64) {
	if bytes != 0 && o.tracker != nil {
		o.tracker.Release(bytes)
		o.metrics.release(bytes)
	}
}

func (o *aggregateOperator) NextBatch(ctx context.Context) (_ *vectorized.RecordBatch, err error) {
	defer func() {
		if err != nil {
			_ = o.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if o.closed {
		return nil, nil
	}
	if !o.complete {
		if err = o.consumeAll(ctx); err != nil {
			return nil, err
		}
	}
	if o.position >= len(o.groups.keys) {
		return nil, nil
	}
	return o.emit(ctx)
}

func (o *aggregateOperator) consumeAll(ctx context.Context) error {
	if len(o.groupReaders) == 0 {
		if _, err := o.groupID(nil); err != nil {
			return err
		}
	}
	for {
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
		if batch.Len > math.MaxUint16+1 || batch.Len < 0 {
			return fmt.Errorf("aggregate source batch has invalid row count %d", batch.Len)
		}
		if err = o.consumeBatch(ctx, batch); err != nil {
			return err
		}
		if o.distinctLimit != 0 && uint64(len(o.groups.keys)) >= o.distinctLimit {
			break
		}
	}
	// Source columns can borrow native decode buffers. Encoded grouping/distinct
	// keys and fixed-width states own their data; discard readers before egress.
	clear(o.groupValues)
	for index := range o.groupReaders {
		o.groupReaders[index].column = nil
	}
	for index := range o.stateReaders {
		o.stateReaders[index].column = nil
	}
	if err := o.closeSource(); err != nil {
		return err
	}
	o.complete = true
	o.metrics.execution(o.statistics())
	return nil
}

func (o *aggregateOperator) consumeBatch(ctx context.Context, batch *vectorized.RecordBatch) error {
	for index := range o.groupReaders {
		if err := o.groupReaders[index].bind(batch); err != nil {
			return err
		}
	}
	for index := range o.stateReaders {
		if err := o.stateReaders[index].bind(batch); err != nil {
			return err
		}
	}
	for position := 0; position < batch.ActiveLen(); position++ {
		if position%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		row := position
		if batch.Selection != nil {
			row = int(batch.Selection[position])
		}
		if row >= batch.Len {
			return fmt.Errorf("aggregate selection row %d exceeds batch length %d", row, batch.Len)
		}
		for index := range o.groupReaders {
			value, err := o.groupReaders[index].value(row)
			if err != nil {
				return err
			}
			value.timestamp = o.groupReaders[index].timestamp
			o.groupValues[index] = value
		}
		group, err := o.groupID(o.groupValues)
		if err != nil {
			return err
		}
		for index := range o.states {
			value, valueErr := o.stateReaders[index].value(row)
			if valueErr != nil {
				return valueErr
			}
			if updateErr := o.states[index].update(group, value); updateErr != nil {
				return updateErr
			}
		}
		o.stats.InputRows++
		if o.distinctLimit != 0 && uint64(len(o.groups.keys)) >= o.distinctLimit {
			break
		}
	}
	return nil
}
