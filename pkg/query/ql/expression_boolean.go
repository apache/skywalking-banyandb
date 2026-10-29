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
	"bytes"
	"context"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type expressionCompare func(*expressionVector, *expressionVector, int) bool

func expressionComparison(op string, typ ExpressionType) expressionCompare {
	switch typ {
	case ExpressionDecimal:
		return func(a, b *expressionVector, row int) bool {
			order := a.decimalAt(row).CmpScaled(a.scale, b.decimalAt(row), b.scale)
			switch op {
			case "=":
				return order == 0
			case "!=":
				return order != 0
			case "<":
				return order < 0
			case "<=":
				return order <= 0
			case ">":
				return order > 0
			default:
				return order >= 0
			}
		}
	case ExpressionFloat:
		return expressionFloatComparison(op)
	case ExpressionInt, ExpressionTimestamp, ExpressionNull:
		return expressionIntComparison(op)
	case ExpressionBool:
		if op == "=" {
			return func(a, b *expressionVector, row int) bool { return a.boolAt(row) == b.boolAt(row) }
		}
		return func(a, b *expressionVector, row int) bool { return a.boolAt(row) != b.boolAt(row) }
	case ExpressionString:
		return expressionStringComparison(op)
	default:
		relation := expressionIntComparison(op)
		return func(a, b *expressionVector, row int) bool {
			var left, right []byte
			if a.constant {
				left = a.literal.Bytes
			} else {
				left = a.bytes[row]
			}
			if b.constant {
				right = b.literal.Bytes
			} else {
				right = b.bytes[row]
			}
			order := int64(bytes.Compare(left, right))
			return relation(&expressionVector{constant: true, literal: ExpressionScalar{Int64: order}}, &expressionVector{constant: true}, 0)
		}
	}
}

func expressionIntComparison(op string) expressionCompare {
	switch op {
	case "=":
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) == b.intAt(r) }
	case "!=":
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) != b.intAt(r) }
	case "<":
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) < b.intAt(r) }
	case "<=":
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) <= b.intAt(r) }
	case ">":
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) > b.intAt(r) }
	default:
		return func(a, b *expressionVector, r int) bool { return a.intAt(r) >= b.intAt(r) }
	}
}

func expressionFloatComparison(op string) expressionCompare {
	switch op {
	case "=":
		return func(a, b *expressionVector, r int) bool {
			left, right := a.floatAt(r), b.floatAt(r)
			return left == right || (math.IsNaN(left) && math.IsNaN(right))
		}
	case "!=":
		return func(a, b *expressionVector, r int) bool {
			left, right := a.floatAt(r), b.floatAt(r)
			return left != right && !(math.IsNaN(left) && math.IsNaN(right))
		}
	case "<":
		return func(a, b *expressionVector, r int) bool { return aggregateFloatLess(a.floatAt(r), b.floatAt(r)) }
	case "<=":
		return func(a, b *expressionVector, r int) bool { return !aggregateFloatLess(b.floatAt(r), a.floatAt(r)) }
	case ">":
		return func(a, b *expressionVector, r int) bool { return aggregateFloatLess(b.floatAt(r), a.floatAt(r)) }
	default:
		return func(a, b *expressionVector, r int) bool { return !aggregateFloatLess(a.floatAt(r), b.floatAt(r)) }
	}
}

func expressionStringComparison(op string) expressionCompare {
	switch op {
	case "=":
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) == b.stringAt(r) }
	case "!=":
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) != b.stringAt(r) }
	case "<":
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) < b.stringAt(r) }
	case "<=":
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) <= b.stringAt(r) }
	case ">":
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) > b.stringAt(r) }
	default:
		return func(a, b *expressionVector, r int) bool { return a.stringAt(r) >= b.stringAt(r) }
	}
}

func (n *expressionNode) comparison(op string, typ ExpressionType) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	compare := expressionComparison(op, typ)
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		left, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		right, err := n.args[1].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if left.constant && right.constant {
			n.value.constant = true
			n.value.literal = ExpressionScalar{Null: left.isNull(0) || right.isNull(0)}
			if !n.value.literal.Null {
				n.value.literal.Bool = compare(left, right, 0)
			}
			return &n.value, nil
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			if left.isNull(row) || right.isNull(row) {
				n.value.nulls[row] = true
			} else {
				n.value.bools[row] = compare(left, right, row)
			}
		}
		return &n.value, nil
	}
}

func (n *expressionNode) unaryBoolean(op string) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	compute := func(source *expressionVector, row int) (bool, bool) { return !source.boolAt(row), source.isNull(row) }
	if op == "is_null" {
		compute = func(source *expressionVector, row int) (bool, bool) { return source.isNull(row), false }
	}
	if op == "is_not_null" {
		compute = func(source *expressionVector, row int) (bool, bool) { return !source.isNull(row), false }
	}
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		source, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if source.constant {
			n.value.constant = true
			n.value.literal.Bool, n.value.literal.Null = compute(source, 0)
			return &n.value, nil
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			n.value.bools[row], n.value.nulls[row] = compute(source, row)
		}
		return &n.value, nil
	}
}

func (n *expressionNode) masks(size int) error {
	if cap(n.mask) >= size {
		return nil
	}
	bytes := int64(size) * 4
	if err := n.op.reserve(bytes); err != nil {
		return err
	}
	if cap(n.mask) > 0 {
		old := int64(cap(n.mask)) * 4
		defer n.op.unreserve(old)
	}
	n.mask = make([]uint16, 0, size)
	n.matched = make([]uint16, 0, size)
	return nil
}

func (n *expressionNode) boolean(and bool) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		left, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if left.constant && !left.isNull(0) && left.boolAt(0) != and {
			n.value.constant = true
			n.value.literal = left.literal
			return &n.value, nil
		}
		if err = n.masks(max(batch.Len, expressionRows(batch, mask))); err != nil {
			return nil, err
		}
		n.mask = n.mask[:0]
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			if left.isNull(row) || left.boolAt(row) == and {
				n.mask = append(n.mask, uint16(row))
			}
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			n.value.bools[row] = !and
		}
		if len(n.mask) == 0 {
			return &n.value, nil
		}
		right, err := n.args[1].eval(ctx, batch, n.mask)
		if err != nil {
			return nil, err
		}
		for _, index := range n.mask {
			row := int(index)
			if !right.isNull(row) && right.boolAt(row) != and {
				n.value.bools[row] = !and
				continue
			}
			n.value.nulls[row] = left.isNull(row) || right.isNull(row)
			n.value.bools[row] = and
		}
		return &n.value, nil
	}
}

func (n *expressionNode) membership(not bool, typ ExpressionType) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	compare := expressionComparison("=", typ)
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		if len(n.args) == 1 {
			n.value.constant = true
			n.value.literal = ExpressionScalar{Bool: not}
			return &n.value, nil
		}
		left, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			n.value.bools[row] = not
			n.value.nulls[row] = left.isNull(row)
		}
		for _, arg := range n.args[1:] {
			right, rightErr := arg.eval(ctx, batch, mask)
			if rightErr != nil {
				return nil, rightErr
			}
			for pos := 0; pos < expressionRows(batch, mask); pos++ {
				row := expressionRow(mask, pos)
				if left.isNull(row) || n.value.bools[row] != not {
					continue
				}
				if right.isNull(row) {
					n.value.nulls[row] = true
				} else if compare(left, right, row) {
					n.value.nulls[row] = false
					n.value.bools[row] = !not
				}
			}
		}
		return &n.value, nil
	}
}
