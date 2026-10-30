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

func (n *expressionNode) conditional(op string) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	if op == "coalesce" {
		return n.coalesce
	}
	start := 0
	var compare expressionCompare
	if op == "simple_case" {
		start = 1
		compare = expressionComparison("=", n.args[0].value.typ)
	}
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		var source *expressionVector
		var err error
		if start == 1 {
			source, err = n.args[0].eval(ctx, batch, mask)
			if err != nil {
				return nil, err
			}
		}
		if err = n.masks(max(batch.Len, expressionRows(batch, mask))); err != nil {
			return nil, err
		}
		n.mask = n.mask[:0]
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			n.mask = append(n.mask, uint16(expressionRow(mask, pos)))
		}
		buffered := false
		for index := start; index < len(n.args)-1 && len(n.mask) > 0; index += 2 {
			condition, conditionErr := n.args[index].eval(ctx, batch, n.mask)
			if conditionErr != nil {
				return nil, conditionErr
			}
			n.matched = n.matched[:0]
			remaining := n.mask[:0]
			for _, entry := range n.mask {
				row := int(entry)
				matches := !condition.isNull(row)
				if matches {
					if start == 1 {
						matches = !source.isNull(row) && compare(source, condition, row)
					} else {
						matches = condition.boolAt(row)
					}
				}
				if matches {
					n.matched = append(n.matched, entry)
				} else {
					remaining = append(remaining, entry)
				}
			}
			n.mask = remaining
			if len(n.matched) == 0 {
				continue
			}
			value, valueErr := n.args[index+1].eval(ctx, batch, n.matched)
			if valueErr != nil {
				return nil, valueErr
			}
			n.recordArm(len(n.matched), index > start)
			if !buffered && len(n.mask) == 0 && len(n.matched) == expressionRows(batch, mask) {
				return value, nil
			}
			if !buffered {
				if err = n.buffer(batch.Len); err != nil {
					return nil, err
				}
				buffered = true
			}
			n.value.copyRows(value, n.matched)
		}
		if len(n.mask) > 0 {
			value, valueErr := n.args[len(n.args)-1].eval(ctx, batch, n.mask)
			if valueErr != nil {
				return nil, valueErr
			}
			n.recordArm(len(n.mask), true)
			if !buffered {
				return value, nil
			}
			n.value.copyRows(value, n.mask)
		}
		return &n.value, nil
	}
}

func (n *expressionNode) coalesce(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
	if err := n.masks(max(batch.Len, expressionRows(batch, mask))); err != nil {
		return nil, err
	}
	n.mask = n.mask[:0]
	for pos := 0; pos < expressionRows(batch, mask); pos++ {
		n.mask = append(n.mask, uint16(expressionRow(mask, pos)))
	}
	buffered := false
	for index, arg := range n.args {
		if len(n.mask) == 0 {
			break
		}
		value, err := arg.eval(ctx, batch, n.mask)
		if err != nil {
			return nil, err
		}
		n.recordArm(len(n.mask), index > 0)
		if value.constant && value.isNull(0) {
			continue
		}
		n.matched = n.matched[:0]
		remaining := n.mask[:0]
		for _, entry := range n.mask {
			if value.isNull(int(entry)) {
				remaining = append(remaining, entry)
			} else {
				n.matched = append(n.matched, entry)
			}
		}
		n.mask = remaining
		if len(n.matched) == 0 {
			continue
		}
		if !buffered && len(n.mask) == 0 {
			return value, nil
		}
		if !buffered {
			if err = n.buffer(batch.Len); err != nil {
				return nil, err
			}
			buffered = true
		}
		n.value.copyRows(value, n.matched)
	}
	if !buffered {
		n.value.constant = true
		n.value.literal = ExpressionScalar{Null: true}
		return &n.value, nil
	}
	for _, row := range n.mask {
		n.value.nulls[int(row)] = true
	}
	return &n.value, nil
}

func (v *expressionVector) copyRows(source *expressionVector, mask []uint16) {
	for _, index := range mask {
		row := int(index)
		v.nulls[row] = source.isNull(row)
	}
	switch v.typ {
	case ExpressionDecimal:
		for _, row := range mask {
			if !v.nulls[row] {
				v.decimals[row] = source.decimalAt(int(row))
			}
		}
	case ExpressionFloat:
		for _, row := range mask {
			if !v.nulls[row] {
				v.floats[row] = source.floatAt(int(row))
			}
		}
	case ExpressionBool:
		for _, row := range mask {
			if !v.nulls[row] {
				v.bools[row] = source.boolAt(int(row))
			}
		}
	case ExpressionString:
		for _, row := range mask {
			if !v.nulls[row] {
				v.strings[row] = source.stringAt(int(row))
			}
		}
	case ExpressionBytes:
		for _, row := range mask {
			if !v.nulls[row] {
				if source.constant {
					v.bytes[row] = source.literal.Bytes
				} else {
					v.bytes[row] = source.bytes[row]
				}
			}
		}
	case ExpressionIntArray:
		for _, row := range mask {
			if !v.nulls[row] {
				if source.constant {
					v.intArrays[row] = source.literal.Ints
				} else {
					v.intArrays[row] = source.intArrays[row]
				}
			}
		}
	case ExpressionStringArray:
		for _, row := range mask {
			if !v.nulls[row] {
				if source.constant {
					v.stringArrays[row] = source.literal.Strings
				} else {
					v.stringArrays[row] = source.stringArrays[row]
				}
			}
		}
	default:
		for _, row := range mask {
			if !v.nulls[row] {
				v.ints[row] = source.intAt(int(row))
			}
		}
	}
}
