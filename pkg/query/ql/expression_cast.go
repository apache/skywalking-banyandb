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
	"strconv"
	"strings"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type expressionCastKernel func(*expressionVector, int, *expressionVector) error

func expressionCastInt(from ExpressionType) func(*expressionVector, int) (int64, error) {
	switch from {
	case ExpressionInt, ExpressionTimestamp:
		return func(v *expressionVector, row int) (int64, error) { return v.intAt(row), nil }
	case ExpressionFloat:
		return func(v *expressionVector, row int) (int64, error) {
			number := v.floatAt(row)
			if math.IsNaN(number) || math.IsInf(number, 0) || number < -0x1p63 || number >= 0x1p63 {
				return 0, expressionError("DOUBLE is outside INT range")
			}
			return int64(number), nil
		}
	case ExpressionBool:
		return func(v *expressionVector, row int) (int64, error) {
			if v.boolAt(row) {
				return 1, nil
			}
			return 0, nil
		}
	default:
		return func(v *expressionVector, row int) (int64, error) {
			number, err := strconv.ParseInt(strings.TrimSpace(v.stringAt(row)), 10, 64)
			if err != nil {
				return 0, expressionError("invalid INT cast")
			}
			return number, nil
		}
	}
}

func expressionCastFloat(from ExpressionType) func(*expressionVector, int) (float64, error) {
	switch from {
	case ExpressionFloat:
		return func(v *expressionVector, row int) (float64, error) { return v.floatAt(row), nil }
	case ExpressionInt:
		return func(v *expressionVector, row int) (float64, error) { return float64(v.intAt(row)), nil }
	case ExpressionBool:
		return func(v *expressionVector, row int) (float64, error) {
			if v.boolAt(row) {
				return 1, nil
			}
			return 0, nil
		}
	default:
		return func(v *expressionVector, row int) (float64, error) {
			number, err := strconv.ParseFloat(strings.TrimSpace(v.stringAt(row)), 64)
			if err != nil {
				return 0, expressionError("invalid DOUBLE cast")
			}
			return number, nil
		}
	}
}

func expressionCastBool(from ExpressionType) func(*expressionVector, int) (bool, error) {
	switch from {
	case ExpressionBool:
		return func(v *expressionVector, row int) (bool, error) { return v.boolAt(row), nil }
	case ExpressionInt:
		return func(v *expressionVector, row int) (bool, error) { return v.intAt(row) != 0, nil }
	case ExpressionFloat:
		return func(v *expressionVector, row int) (bool, error) { return v.floatAt(row) != 0, nil }
	default:
		return func(v *expressionVector, row int) (bool, error) {
			text := strings.TrimSpace(v.stringAt(row))
			if strings.EqualFold(text, "true") {
				return true, nil
			}
			if strings.EqualFold(text, "false") {
				return false, nil
			}
			return false, expressionError("invalid BOOLEAN cast")
		}
	}
}

func expressionCastString(from ExpressionType) func(*expressionVector, int) (string, error) {
	switch from {
	case ExpressionInt:
		return func(v *expressionVector, row int) (string, error) { return strconv.FormatInt(v.intAt(row), 10), nil }
	case ExpressionFloat:
		return func(v *expressionVector, row int) (string, error) {
			return strconv.FormatFloat(v.floatAt(row), 'g', -1, 64), nil
		}
	case ExpressionBool:
		return func(v *expressionVector, row int) (string, error) { return strconv.FormatBool(v.boolAt(row)), nil }
	case ExpressionTimestamp:
		return func(v *expressionVector, row int) (string, error) {
			return time.Unix(0, v.intAt(row)).UTC().Format(time.RFC3339Nano), nil
		}
	default:
		return func(v *expressionVector, row int) (string, error) { return v.stringAt(row), nil }
	}
}

func expressionCastTimestamp(from ExpressionType) func(*expressionVector, int) (int64, error) {
	if from == ExpressionInt || from == ExpressionTimestamp {
		return func(v *expressionVector, row int) (int64, error) { return v.intAt(row), nil }
	}
	return func(v *expressionVector, row int) (int64, error) {
		instant, err := time.Parse(time.RFC3339Nano, v.stringAt(row))
		if err != nil {
			return 0, expressionError("invalid TIMESTAMP cast")
		}
		return expressionTimestampNanos(instant)
	}
}

func expressionTimestampNanos(instant time.Time) (int64, error) {
	minimum, maximum := time.Unix(0, math.MinInt64), time.Unix(0, math.MaxInt64)
	if instant.Before(minimum) || instant.After(maximum) {
		return 0, expressionError("TIMESTAMP is outside nanosecond range")
	}
	return instant.UnixNano(), nil
}

func expressionCastKernelFor(from, to ExpressionType) expressionCastKernel {
	switch to {
	case ExpressionInt:
		convert := expressionCastInt(from)
		return func(src *expressionVector, row int, dst *expressionVector) error {
			value, err := convert(src, row)
			dst.ints[row] = value
			return err
		}
	case ExpressionTimestamp:
		convert := expressionCastTimestamp(from)
		return func(src *expressionVector, row int, dst *expressionVector) error {
			value, err := convert(src, row)
			dst.ints[row] = value
			return err
		}
	case ExpressionFloat:
		convert := expressionCastFloat(from)
		return func(src *expressionVector, row int, dst *expressionVector) error {
			value, err := convert(src, row)
			dst.floats[row] = value
			return err
		}
	case ExpressionBool:
		convert := expressionCastBool(from)
		return func(src *expressionVector, row int, dst *expressionVector) error {
			value, err := convert(src, row)
			dst.bools[row] = value
			return err
		}
	case ExpressionString:
		convert := expressionCastString(from)
		return func(src *expressionVector, row int, dst *expressionVector) error {
			value, err := convert(src, row)
			dst.strings[row] = value
			return err
		}
	default:
		return nil
	}
}

func (n *expressionNode) cast(from, to ExpressionType) func(context.Context, *vectorized.RecordBatch, []uint16) (*expressionVector, error) {
	if from == to {
		return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
			return n.args[0].eval(ctx, batch, mask)
		}
	}
	convert := expressionCastKernelFor(from, to)
	return func(ctx context.Context, batch *vectorized.RecordBatch, mask []uint16) (*expressionVector, error) {
		source, err := n.args[0].eval(ctx, batch, mask)
		if err != nil {
			return nil, err
		}
		if source.constant && source.isNull(0) {
			n.value.constant = true
			n.value.literal = ExpressionScalar{Null: true}
			return &n.value, nil
		}
		size := batch.Len
		if source.constant {
			size = 1
		}
		if err = n.buffer(size); err != nil {
			return nil, err
		}
		if to == ExpressionString {
			rows := expressionRows(batch, mask)
			if source.constant {
				rows = 1
			}
			bytes := int64(rows) * 40
			if err = n.op.reserve(bytes); err != nil {
				return nil, err
			}
			n.op.payloadBytes += bytes
		}
		if source.constant {
			if err = convert(source, 0, &n.value); err != nil {
				return nil, err
			}
			n.value.literal = n.value.scalarAt(0)
			n.value.constant = true
			return &n.value, nil
		}
		for pos := 0; pos < expressionRows(batch, mask); pos++ {
			row := expressionRow(mask, pos)
			if source.isNull(row) {
				n.value.nulls[row] = true
				continue
			}
			if err = convert(source, row, &n.value); err != nil {
				return nil, err
			}
		}
		return &n.value, nil
	}
}

func (v *expressionVector) scalarAt(row int) ExpressionScalar {
	value := ExpressionScalar{Null: v.isNull(row)}
	if value.Null {
		return value
	}
	switch v.typ {
	case ExpressionFloat:
		value.Float64 = v.floatAt(row)
	case ExpressionBool:
		value.Bool = v.boolAt(row)
	case ExpressionString:
		value.String = v.stringAt(row)
	case ExpressionBytes:
		if v.constant {
			value.Bytes = v.literal.Bytes
		} else {
			value.Bytes = v.bytes[row]
		}
	case ExpressionIntArray:
		if v.constant {
			value.Ints = v.literal.Ints
		} else {
			value.Ints = v.intArrays[row]
		}
	case ExpressionStringArray:
		if v.constant {
			value.Strings = v.literal.Strings
		} else {
			value.Strings = v.stringArrays[row]
		}
	default:
		value.Int64 = v.intAt(row)
	}
	return value
}
