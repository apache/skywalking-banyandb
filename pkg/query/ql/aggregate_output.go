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

func (o *aggregateOperator) emit(ctx context.Context) (*vectorized.RecordBatch, error) {
	if o.output == nil {
		capacity := min(vectorized.DefaultBatchSize, len(o.groups.keys))
		// Every column is at most a 24-byte slice cell plus its null bitmap.
		bytes := int64(capacity)*int64(len(o.outputs))*25 + int64(len(o.outputs))*128
		if err := o.reserve(bytes); err != nil {
			return nil, err
		}
		o.outputBytes = bytes
		o.output = vectorized.NewRecordBatch(o.schema, capacity)
	} else {
		o.output.Reset()
	}
	o.release(o.outputValueBytes)
	o.outputValueBytes = 0
	end := min(o.position+vectorized.DefaultBatchSize, len(o.groups.keys))
	// Byte grouping outputs need their own []byte cells. String grouping outputs
	// lend immutable key substrings and therefore require no payload allocation.
	var valueBytes int64
	for group := o.position; group < end; group++ {
		decodeGroupValues(o.groups.key(group), o.groupValues)
		for _, output := range o.outputs {
			if output.IsGroup && output.Column.Type == vectorized.ColumnTypeBytes {
				bytes, allocationErr := groupAllocationBytes(int64(len(o.groupValues[output.Index].text)))
				if allocationErr != nil {
					return nil, allocationErr
				}
				valueBytes += bytes
			}
		}
	}
	if err := o.reserve(valueBytes); err != nil {
		return nil, err
	}
	o.outputValueBytes = valueBytes
	for group := o.position; group < end; group++ {
		if (group-o.position)%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		decodeGroupValues(o.groups.key(group), o.groupValues)
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
	o.output.Len = end - o.position
	o.position = end
	return o.output, nil
}
