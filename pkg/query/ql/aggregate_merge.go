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

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// NativeAggregateLeaf is one finalized native aggregate over an original source column.
type NativeAggregateLeaf struct {
	Request  *measurev1.QueryRequest
	Source   vectorized.ColumnDef
	Function AggregateFunction
}

// NativeAggregatePlan combines native leaves using the complete grouping tuple.
// MaxCount is a planner-proven upper bound on non-null values in each group.
type NativeAggregatePlan struct {
	Aggregate *AggregatePlan
	Leaves    []NativeAggregateLeaf
	MaxCount  int64
	MaxGroups uint64 // Zero permits every group that fits in the memory budget.
}

// NativeAggregateSource opens one native leaf using the query's shared memory tracker.
type NativeAggregateSource func(context.Context, NativeAggregateLeaf) (vectorized.PullOperator, error)

type nativeAggregateState struct {
	// Low bits mark rows seen by COUNT/SUM/MIN/MAX; high bits mark non-null values.
	presence []uint16
	aggregateState
	required uint16
}

type nativeAggregateBranch struct {
	leaf  NativeAggregateLeaf
	state int
}

type nativeAggregateOperator struct {
	open        NativeAggregateSource
	current     vectorized.PullOperator
	tracker     *vectorized.MemoryTracker
	schema      *vectorized.BatchSchema
	output      *vectorized.RecordBatch
	bucketWidth string
	groupDefs   []vectorized.ColumnDef
	groupValues []aggregateValue
	states      []nativeAggregateState
	calls       []aggregateCallRef
	outputs     []AggregateOutput
	branches    []nativeAggregateBranch
	keys        []string
	index       []uint32
	scratch     []byte
	stats       NativeAggregateStats
	maxCount    int64
	maxGroups   uint64
	metadata    int64
	tableBytes  int64
	stateBytes  int64
	keyBytes    int64
	keyCapacity int64
	scratchSize int64
	outputBytes int64
	valueBytes  int64
	capacity    int
	position    int
	complete    bool
	closed      bool
	bucketIndex int
}

// MergeNativeAggregate executes leaves sequentially and owns each opened source.
// All leaves finish before any final SQL group is emitted.
func MergeNativeAggregate(ctx context.Context, plan *NativeAggregatePlan, open NativeAggregateSource) (_ vectorized.PullOperator, err error) {
	o := &nativeAggregateOperator{open: open, bucketIndex: -1}
	defer func() {
		if err != nil {
			_ = o.Close()
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if plan == nil || plan.Aggregate == nil || len(plan.Aggregate.Outputs) == 0 || len(plan.Aggregate.Calls) == 0 || open == nil {
		return nil, fmt.Errorf("native aggregate requires a plan, calls, outputs and a source opener")
	}
	if plan.MaxCount < 0 || plan.MaxCount > 1<<53 {
		return nil, fmt.Errorf("native aggregate COUNT bound must be within [0, 2^53]")
	}
	o.tracker = vectorized.QueryMemoryTracker(ctx, 256<<20)
	metadata, boundErr := rawDescriptorBound(len(plan.Aggregate.GroupBy)+len(plan.Aggregate.Calls)+len(plan.Aggregate.Outputs)+len(plan.Leaves), 0, 0)
	if boundErr != nil {
		return nil, boundErr
	}
	if err = o.reserve(metadata); err != nil {
		return nil, err
	}
	o.metadata, o.maxCount, o.maxGroups = metadata, plan.MaxCount, plan.MaxGroups
	o.groupDefs = append([]vectorized.ColumnDef(nil), plan.Aggregate.GroupBy...)
	o.groupValues = make([]aggregateValue, len(o.groupDefs))
	for index, def := range o.groupDefs {
		if !aggregateScalarType(def.Type) || (def.Role != vectorized.RoleTag && def.Role != vectorized.RoleField && def.Role != vectorized.RoleTimestamp) {
			return nil, fmt.Errorf("native aggregate grouping column %q has unsupported type or role", def.Name)
		}
		if index == plan.Aggregate.TimeBucketIndex && plan.Aggregate.TimeBucketWidth != "" {
			o.bucketIndex, o.bucketWidth = index, plan.Aggregate.TimeBucketWidth
		}
	}
	if err = o.compile(plan); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *nativeAggregateOperator) compile(plan *NativeAggregatePlan) error {
	o.calls = make([]aggregateCallRef, len(plan.Aggregate.Calls))
	for index, call := range plan.Aggregate.Calls {
		if call.Function > AggregateAvg || !aggregateScalarType(call.Source.Type) ||
			(call.Function != AggregateCount && call.Source.Type != vectorized.ColumnTypeInt64 && call.Source.Type != vectorized.ColumnTypeFloat64) {
			return fmt.Errorf("unsupported native aggregate call for %q", call.Source.Name)
		}
		if call.Source.Type == vectorized.ColumnTypeInt64 && (call.Function == AggregateSum || call.Function == AggregateAvg) && o.maxCount > 1 {
			return fmt.Errorf("native integer SUM/AVG requires a count bound at most one")
		}
		state := o.findState(call.Source)
		if state < 0 {
			def := call.Source
			def.Entity = false
			state = len(o.states)
			o.states = append(o.states, nativeAggregateState{aggregateState: aggregateState{source: def}, required: 1 << AggregateCount})
		}
		o.states[state].require(call.Function)
		function := call.Function
		if function == AggregateAvg {
			function = AggregateSum
		}
		o.states[state].required |= 1 << function
		o.calls[index] = aggregateCallRef{state: state, function: call.Function}
	}
	seen := make([]uint16, len(o.states))
	for _, leaf := range plan.Leaves {
		state := o.findState(leaf.Source)
		if state < 0 || leaf.Function > AggregateMax {
			return fmt.Errorf("native aggregate leaf for %q is absent from the aggregate calls", leaf.Source.Name)
		}
		bit := uint16(1) << leaf.Function
		if o.states[state].required&bit == 0 || seen[state]&bit != 0 {
			return fmt.Errorf("native aggregate leaf for %q is unnecessary or repeated", leaf.Source.Name)
		}
		seen[state] |= bit
		o.branches = append(o.branches, nativeAggregateBranch{leaf: leaf, state: state})
	}
	for index := range o.states {
		if seen[index] != o.states[index].required {
			return fmt.Errorf("native aggregate leaves for %q are incomplete", o.states[index].source.Name)
		}
	}
	o.outputs = append([]AggregateOutput(nil), plan.Aggregate.Outputs...)
	defs := make([]vectorized.ColumnDef, len(o.outputs))
	for index, output := range o.outputs {
		if output.Column.Role != vectorized.RoleTag && output.Column.Role != vectorized.RoleField {
			return fmt.Errorf("native aggregate output %q must be a tag or field", output.Column.Name)
		}
		var expected vectorized.ColumnType
		if output.IsGroup {
			if output.Index < 0 || output.Index >= len(o.groupDefs) {
				return fmt.Errorf("native aggregate grouping output index %d is absent", output.Index)
			}
			expected = o.groupDefs[output.Index].Type
		} else {
			if output.Index < 0 || output.Index >= len(o.calls) {
				return fmt.Errorf("native aggregate call output index %d is absent", output.Index)
			}
			expected = aggregateOutputType(plan.Aggregate.Calls[output.Index])
		}
		if output.Column.Type != expected {
			return fmt.Errorf("native aggregate output %q has type %s; expected %s", output.Column.Name, output.Column.Type, expected)
		}
		defs[index] = output.Column
		defs[index].Entity = false
	}
	if o.bucketIndex >= 0 {
		defs = append(defs, vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64})
	}
	o.schema = vectorized.NewBatchSchema(defs)
	return nil
}

func (o *nativeAggregateOperator) findState(def vectorized.ColumnDef) int {
	def.Entity = false
	for index := range o.states {
		if o.states[index].source == def {
			return index
		}
	}
	return -1
}

func (o *nativeAggregateOperator) OutputSchema() *vectorized.BatchSchema { return o.schema }

func (o *nativeAggregateOperator) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		_ = o.Close()
		return err
	}
	if o.closed {
		return fmt.Errorf("native aggregate is closed")
	}
	return nil
}

func (o *nativeAggregateOperator) closeCurrent() error {
	if o.current == nil {
		return nil
	}
	source := o.current
	o.current = nil
	return source.Close()
}

func (o *nativeAggregateOperator) Close() error {
	if o.closed {
		return nil
	}
	o.stats = o.statistics()
	o.closed = true
	err := o.closeCurrent()
	o.release(o.metadata + o.tableBytes + o.stateBytes + o.keyCapacity + o.scratchSize + o.outputBytes + o.valueBytes)
	o.states, o.calls, o.outputs, o.branches = nil, nil, nil, nil
	o.keys, o.index, o.scratch = nil, nil, nil
	o.groupDefs, o.groupValues, o.output, o.schema, o.open = nil, nil, nil, nil, nil
	return err
}

func (o *nativeAggregateOperator) reserve(bytes int64) error {
	if err := o.tracker.Reserve(bytes); err != nil {
		return err
	}
	recordNativeAggregateReserve(bytes)
	return nil
}

func (o *nativeAggregateOperator) release(bytes int64) {
	if bytes != 0 && o.tracker != nil {
		o.tracker.Release(bytes)
		recordNativeAggregateRelease(bytes)
	}
}

func (o *nativeAggregateOperator) NextBatch(ctx context.Context) (_ *vectorized.RecordBatch, err error) {
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
		if err = o.consume(ctx); err != nil {
			return nil, err
		}
	}
	if o.position == len(o.keys) {
		return nil, nil
	}
	return o.emit(ctx)
}
