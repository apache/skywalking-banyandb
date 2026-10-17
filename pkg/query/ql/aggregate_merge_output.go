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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// NativeAggregateStats describes keyed merge work and retained capacity.
type NativeAggregateStats struct {
	Branches      uint64
	InputRows     uint64
	Groups        uint64
	GroupCapacity uint64
	KeyBytes      uint64
	KeyCapacity   uint64
	StateBytes    uint64 // Includes two bytes of branch-presence flags per source/group.
	StateCapacity uint64
	StateLanes    uint64 // Fixed-width eight-byte lanes, excluding presence flags.
	MergeVisits   uint64
}

// NativeAggregateStatistics reports keyed merge work, including after Close.
func NativeAggregateStatistics(source vectorized.PullOperator) (NativeAggregateStats, bool) {
	for current := source; current != nil; current = operatorSource(current) {
		if op, ok := current.(*nativeAggregateOperator); ok {
			return op.statistics(), true
		}
	}
	return NativeAggregateStats{}, false
}

func (o *nativeAggregateOperator) statistics() NativeAggregateStats {
	stats := o.stats
	if o.closed {
		return stats
	}
	stats.Groups, stats.GroupCapacity = uint64(len(o.keys)), uint64(o.capacity)
	stats.KeyBytes, stats.KeyCapacity = uint64(o.keyBytes), uint64(o.tableBytes+o.keyCapacity+o.scratchSize)
	for index := range o.states {
		stats.StateLanes += uint64(o.states[index].lanes())
	}
	stats.StateBytes = stats.Groups * (stats.StateLanes*8 + uint64(len(o.states))*2)
	stats.StateCapacity = uint64(o.stateBytes)
	return stats
}

func (o *nativeAggregateOperator) emit(ctx context.Context) (*vectorized.RecordBatch, error) {
	if o.output == nil {
		capacity := min(vectorized.DefaultBatchSize, len(o.keys))
		bytes := int64(capacity)*int64(len(o.outputs))*25 + int64(len(o.outputs))*128
		if err := o.reserve(bytes); err != nil {
			return nil, err
		}
		o.outputBytes = bytes
		o.output = vectorized.NewRecordBatch(o.schema, capacity)
	} else {
		o.output.Reset()
	}
	o.release(o.valueBytes)
	o.valueBytes = 0
	end := min(o.position+vectorized.DefaultBatchSize, len(o.keys))
	var valueBytes int64
	for group := o.position; group < end; group++ {
		decodeGroupValues(o.keys[group], o.groupValues)
		for _, output := range o.outputs {
			if output.IsGroup && output.Column.Type == vectorized.ColumnTypeBytes {
				bytes, err := groupAllocationBytes(int64(len(o.groupValues[output.Index].text)))
				if err != nil {
					return nil, err
				}
				valueBytes += bytes
			}
		}
	}
	if err := o.reserve(valueBytes); err != nil {
		return nil, err
	}
	o.valueBytes = valueBytes
	for group := o.position; group < end; group++ {
		if (group-o.position)%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		decodeGroupValues(o.keys[group], o.groupValues)
		for index, output := range o.outputs {
			var value aggregateValue
			if output.IsGroup {
				value = o.groupValues[output.Index]
			} else {
				call := o.calls[output.Index]
				value = o.states[call.state].result(group, call.function)
			}
			appendAggregateValue(o.output.Columns[index], value)
		}
	}
	o.output.Len, o.position = end-o.position, end
	return o.output, nil
}
