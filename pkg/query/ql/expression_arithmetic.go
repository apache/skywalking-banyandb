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
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type (
	expressionIntKernel   func(int64, int64) (int64, error)
	expressionFloatKernel func(float64, float64) (float64, error)
)

func expressionArithmeticKernels(op string) (expressionIntKernel, expressionFloatKernel) {
	switch op {
	case "+":
		return func(a, b int64) (int64, error) {
			v := a + b
			if (b > 0 && v < a) || (b < 0 && v > a) {
				return 0, expressionError("integer overflow")
			}
			return v, nil
		}, func(a, b float64) (float64, error) { return a + b, nil }
	case "-":
		return func(a, b int64) (int64, error) {
			v := a - b
			if (b < 0 && v < a) || (b > 0 && v > a) {
				return 0, expressionError("integer overflow")
			}
			return v, nil
		}, func(a, b float64) (float64, error) { return a - b, nil }
	case "*":
		return func(a, b int64) (int64, error) {
			v := a * b
			if (a == math.MinInt64 && b == -1) || (b != 0 && v/b != a) {
				return 0, expressionError("integer overflow")
			}
			return v, nil
		}, func(a, b float64) (float64, error) { return a * b, nil }
	case "%":
		return func(a, b int64) (int64, error) {
				if b == 0 {
					return 0, expressionError("division by zero")
				}
				return a % b, nil
			}, func(a, b float64) (float64, error) {
				if b == 0 {
					return 0, expressionError("division by zero")
				}
				return math.Mod(a, b), nil
			}
	default:
		return func(a, b int64) (int64, error) {
				if b == 0 {
					return 0, expressionError("division by zero")
				}
				if a == math.MinInt64 && b == -1 {
					return 0, expressionError("integer overflow")
				}
				return a / b, nil
			}, func(a, b float64) (float64, error) {
				if b == 0 {
					return 0, expressionError("division by zero")
				}
				return a / b, nil
			}
	}
}

func (n *expressionNode) arithmetic(op string) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	integer, floating := expressionArithmeticKernels(op)
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
			n.value.column = nil
			n.value.literal = ExpressionScalar{Null: left.isNull(0) || right.isNull(0)}
			if n.value.literal.Null {
				return &n.value, nil
			}
			if n.value.typ == ExpressionFloat {
				n.value.literal.Float64, err = floating(left.literal.Float64, right.literal.Float64)
			} else {
				n.value.literal.Int64, err = integer(left.literal.Int64, right.literal.Int64)
			}
			return &n.value, err
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		if n.value.typ == ExpressionFloat {
			for pos := 0; pos < expressionRows(batch, mask); pos++ {
				row := expressionRow(mask, pos)
				if left.isNull(row) || right.isNull(row) {
					n.value.nulls[row] = true
					continue
				}
				n.value.floats[row], err = floating(left.floatAt(row), right.floatAt(row))
				if err != nil {
					return nil, err
				}
			}
		} else {
			for pos := 0; pos < expressionRows(batch, mask); pos++ {
				row := expressionRow(mask, pos)
				if left.isNull(row) || right.isNull(row) {
					n.value.nulls[row] = true
					continue
				}
				n.value.ints[row], err = integer(left.intAt(row), right.intAt(row))
				if err != nil {
					return nil, err
				}
			}
		}
		return &n.value, nil
	}
}

func (n *expressionNode) unary(op string) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	if op == "positive" {
		return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
			return n.args[0].eval(ctx, batch, mask)
		}
	}
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		value, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if value.constant {
			n.value.constant = true
			n.value.literal = value.literal
			if value.isNull(0) {
				return &n.value, nil
			}
			if value.typ == ExpressionFloat {
				n.value.literal.Float64 = -value.literal.Float64
			} else {
				if value.literal.Int64 == math.MinInt64 {
					return nil, expressionError("integer overflow")
				}
				n.value.literal.Int64 = -value.literal.Int64
			}
			return &n.value, nil
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		if value.typ == ExpressionFloat {
			for pos := 0; pos < expressionRows(batch, mask); pos++ {
				row := expressionRow(mask, pos)
				n.value.nulls[row] = value.isNull(row)
				if !n.value.nulls[row] {
					n.value.floats[row] = -value.floatAt(row)
				}
			}
		} else {
			for pos := 0; pos < expressionRows(batch, mask); pos++ {
				row := expressionRow(mask, pos)
				n.value.nulls[row] = value.isNull(row)
				if !n.value.nulls[row] {
					v := value.intAt(row)
					if v == math.MinInt64 {
						return nil, expressionError("integer overflow")
					}
					n.value.ints[row] = -v
				}
			}
		}
		return &n.value, nil
	}
}
