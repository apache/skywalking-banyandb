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

func resolveExpressionTimeBucket(expression *Expression) error {
	if len(expression.Args) != 1 || expression.Args[0].Type != ExpressionTimestamp || expression.Literal.Int64 <= 0 {
		return expressionError("time_bucket requires a timestamp and positive width")
	}
	expression.Type = ExpressionTimestamp
	return nil
}

func (n *expressionNode) timeBucket(width int64) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		input, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if err = n.buffer(batch.Len); err != nil {
			return nil, err
		}
		for position := 0; position < expressionRows(batch, mask); position++ {
			if position%1024 == 0 {
				if err = ctx.Err(); err != nil {
					return nil, err
				}
			}
			row := expressionRow(mask, position)
			if input.isNull(row) {
				n.value.nulls[row] = true
				continue
			}
			value := input.intAt(row)
			remainder := value % width
			if remainder < 0 {
				remainder += width
			}
			if value < math.MinInt64+remainder {
				return nil, expressionError("time_bucket start is outside the timestamp range")
			}
			n.value.ints[row] = value - remainder
		}
		return &n.value, nil
	}
}
