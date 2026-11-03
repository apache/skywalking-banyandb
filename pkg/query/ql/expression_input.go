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

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Source protobuf values are decoded at the input boundary, before typed kernels.
// Each batch resolves its concrete column once; expression loops never inspect protobufs.
func decodeExpressionValues[P, T any](ctx context.Context, batch *vectorized.RecordBatch, mask []uint16,
	source vectorized.Column, input []*P, output []T, nulls []bool, decode func(*P) (T, bool, error),
) error {
	for pos := 0; pos < expressionRows(batch, mask); pos++ {
		if pos%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		row := expressionRow(mask, pos)
		if source.IsNull(row) {
			nulls[row] = true
			continue
		}
		value, null, err := decode(input[row])
		if err != nil {
			return err
		}
		output[row] = value
		nulls[row] = null
	}
	return nil
}

func expressionTagNull(value *modelv1.TagValue) bool {
	if value == nil || value.Value == nil {
		return true
	}
	_, null := value.Value.(*modelv1.TagValue_Null)
	return null
}

func expressionFieldNull(value *modelv1.FieldValue) bool {
	if value == nil || value.Value == nil {
		return true
	}
	_, null := value.Value.(*modelv1.FieldValue_Null)
	return null
}

func (n *expressionNode) decodeTag(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16, input []*modelv1.TagValue) error {
	if err := n.buffer(batch.Len); err != nil {
		return err
	}
	switch n.value.typ {
	case ExpressionInt:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.ints, n.value.nulls, func(cell *modelv1.TagValue) (int64, bool, error) {
			if expressionTagNull(cell) {
				return 0, true, nil
			}
			value, ok := cell.Value.(*modelv1.TagValue_Int)
			if !ok {
				return 0, false, expressionError("incompatible tag source value")
			}
			if value.Int == nil {
				return 0, true, nil
			}
			return value.Int.Value, false, nil
		})
	case ExpressionString:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.strings, n.value.nulls, func(cell *modelv1.TagValue) (string, bool, error) {
			if expressionTagNull(cell) {
				return "", true, nil
			}
			value, ok := cell.Value.(*modelv1.TagValue_Str)
			if !ok {
				return "", false, expressionError("incompatible tag source value")
			}
			if value.Str == nil {
				return "", true, nil
			}
			return value.Str.Value, false, nil
		})
	case ExpressionBytes:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.bytes, n.value.nulls, func(cell *modelv1.TagValue) ([]byte, bool, error) {
			if expressionTagNull(cell) {
				return nil, true, nil
			}
			value, ok := cell.Value.(*modelv1.TagValue_BinaryData)
			if !ok {
				return nil, false, expressionError("incompatible tag source value")
			}
			return value.BinaryData, false, nil
		})
	case ExpressionIntArray:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.intArrays, n.value.nulls, func(cell *modelv1.TagValue) ([]int64, bool, error) {
			if expressionTagNull(cell) {
				return nil, true, nil
			}
			value, ok := cell.Value.(*modelv1.TagValue_IntArray)
			if !ok {
				return nil, false, expressionError("incompatible tag source value")
			}
			if value.IntArray == nil {
				return nil, true, nil
			}
			return value.IntArray.Value, false, nil
		})
	case ExpressionStringArray:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.stringArrays, n.value.nulls,
			func(cell *modelv1.TagValue) ([]string, bool, error) {
				if expressionTagNull(cell) {
					return nil, true, nil
				}
				value, ok := cell.Value.(*modelv1.TagValue_StrArray)
				if !ok {
					return nil, false, expressionError("incompatible tag source value")
				}
				if value.StrArray == nil {
					return nil, true, nil
				}
				return value.StrArray.Value, false, nil
			})
	case ExpressionTimestamp:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.ints, n.value.nulls, func(cell *modelv1.TagValue) (int64, bool, error) {
			if expressionTagNull(cell) {
				return 0, true, nil
			}
			value, ok := cell.Value.(*modelv1.TagValue_Timestamp)
			if !ok {
				return 0, false, expressionError("incompatible timestamp tag value")
			}
			if value.Timestamp == nil {
				return 0, true, nil
			}
			if err := value.Timestamp.CheckValid(); err != nil {
				return 0, false, expressionError("invalid timestamp tag")
			}
			nanos, err := expressionTimestampNanos(value.Timestamp.AsTime())
			return nanos, false, err
		})
	default:
		return expressionError("incompatible tag source type")
	}
}

func (n *expressionNode) decodeField(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16, input []*modelv1.FieldValue) error {
	if err := n.buffer(batch.Len); err != nil {
		return err
	}
	switch n.value.typ {
	case ExpressionInt:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.ints, n.value.nulls, func(cell *modelv1.FieldValue) (int64, bool, error) {
			if expressionFieldNull(cell) {
				return 0, true, nil
			}
			value, ok := cell.Value.(*modelv1.FieldValue_Int)
			if !ok {
				return 0, false, expressionError("incompatible field source value")
			}
			if value.Int == nil {
				return 0, true, nil
			}
			return value.Int.Value, false, nil
		})
	case ExpressionFloat:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.floats, n.value.nulls, func(cell *modelv1.FieldValue) (float64, bool, error) {
			if expressionFieldNull(cell) {
				return 0, true, nil
			}
			value, ok := cell.Value.(*modelv1.FieldValue_Float)
			if !ok {
				return 0, false, expressionError("incompatible field source value")
			}
			if value.Float == nil {
				return 0, true, nil
			}
			return value.Float.Value, false, nil
		})
	case ExpressionString:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.strings, n.value.nulls, func(cell *modelv1.FieldValue) (string, bool, error) {
			if expressionFieldNull(cell) {
				return "", true, nil
			}
			value, ok := cell.Value.(*modelv1.FieldValue_Str)
			if !ok {
				return "", false, expressionError("incompatible field source value")
			}
			if value.Str == nil {
				return "", true, nil
			}
			return value.Str.Value, false, nil
		})
	case ExpressionBytes:
		return decodeExpressionValues(ctx, batch, mask, batch.Columns[n.index], input, n.value.bytes, n.value.nulls, func(cell *modelv1.FieldValue) ([]byte, bool, error) {
			if expressionFieldNull(cell) {
				return nil, true, nil
			}
			value, ok := cell.Value.(*modelv1.FieldValue_BinaryData)
			if !ok {
				return nil, false, expressionError("incompatible field source value")
			}
			return value.BinaryData, false, nil
		})
	default:
		return expressionError("incompatible field source type")
	}
}
