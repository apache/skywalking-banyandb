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
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Numeric bits stay typed by their bound lane. Variable values borrow input only
// in the scratch row; accepted candidates clone every reachable variable value.
type sortValue struct {
	extra  any
	text   string
	raw    []byte
	number uint64
	null   bool
}

func (o *sortOperator) bindKey(expr *Expression, key SortKey) (sortBoundKey, error) {
	bound := sortBoundKey{}
	var compare func(sortValue, sortValue) int
	switch expr.Type {
	case ExpressionInt, ExpressionTimestamp:
		bound.read = func(value *expressionVector, row int) sortValue {
			return sortValue{number: uint64(value.intAt(row)), null: value.isNull(row)}
		}
		compare = func(left, right sortValue) int { return cmp.Compare(int64(left.number), int64(right.number)) }
	case ExpressionFloat:
		bound.read = func(value *expressionVector, row int) sortValue {
			return sortValue{number: math.Float64bits(value.floatAt(row)), null: value.isNull(row)}
		}
		compare = func(left, right sortValue) int {
			a, b := math.Float64frombits(left.number), math.Float64frombits(right.number)
			if math.IsNaN(a) {
				if math.IsNaN(b) {
					return 0
				}
				return 1
			}
			if math.IsNaN(b) {
				return -1
			}
			return cmp.Compare(a, b)
		}
	case ExpressionString:
		bound.read = func(value *expressionVector, row int) sortValue {
			return sortValue{text: value.stringAt(row), null: value.isNull(row)}
		}
		compare = func(left, right sortValue) int { return strings.Compare(left.text, right.text) }
	case ExpressionBytes:
		bound.read = func(value *expressionVector, row int) sortValue {
			if value.constant {
				return sortValue{raw: value.literal.Bytes, null: value.isNull(row)}
			}
			return sortValue{raw: value.bytes[row], null: value.isNull(row)}
		}
		compare = func(left, right sortValue) int { return bytes.Compare(left.raw, right.raw) }
	case ExpressionBool:
		bound.read = func(value *expressionVector, row int) sortValue {
			return sortValue{number: uint64(boolInt(value.boolAt(row))), null: value.isNull(row)}
		}
		compare = func(left, right sortValue) int { return cmp.Compare(left.number, right.number) }
	case ExpressionNull:
		bound.read = func(*expressionVector, int) sortValue { return sortValue{null: true} }
		compare = func(sortValue, sortValue) int { return 0 }
	case ExpressionIntArray, ExpressionStringArray:
		return bound, expressionError("ORDER BY array keys are not supported")
	}
	bound.compare = func(left, right sortValue) int {
		if left.null || right.null {
			if left.null == right.null {
				return 0
			}
			if left.null == key.NullsFirst {
				return -1
			}
			return 1
		}
		order := compare(left, right)
		if key.Descending {
			return -order
		}
		return order
	}
	root, err := o.kernel.compile(expr)
	if err != nil {
		return bound, err
	}
	root.eval = o.kernel.groupKeyEvaluator(root.eval)
	bound.root = root
	return bound, nil
}

func sortReadColumn(column vectorized.Column) (func(int) sortValue, error) {
	var read func(int) sortValue
	if constant, ok := vectorized.AsConst(column); ok {
		if constant.IsNullValue() {
			return func(int) sortValue { return sortValue{null: true} }, nil
		}
		if !vectorized.ConstSupported(constant.Type()) {
			return nil, fmt.Errorf("sort cannot copy constant type %s", constant.Type())
		}
		value := sortValue{null: constant.IsNullValue()}
		switch constant.Type() {
		case vectorized.ColumnTypeInt64:
			value.number = uint64(constant.Int64())
		case vectorized.ColumnTypeFloat64:
			value.number = math.Float64bits(constant.Float64())
		case vectorized.ColumnTypeString:
			value.text = constant.Str()
		case vectorized.ColumnTypeBytes:
			value.raw = constant.Bytes()
		case vectorized.ColumnTypeInt64Array, vectorized.ColumnTypeStrArray, vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
		}
		return func(int) sortValue { return value }, nil
	}
	switch typed := column.(type) {
	case *vectorized.TypedColumn[int64]:
		read = func(row int) sortValue { return sortValue{number: uint64(typed.Data()[row])} }
	case *vectorized.TypedColumn[float64]:
		read = func(row int) sortValue { return sortValue{number: math.Float64bits(typed.Data()[row])} }
	case *vectorized.TypedColumn[string]:
		read = func(row int) sortValue { return sortValue{text: typed.Data()[row]} }
	case *vectorized.TypedColumn[[]byte]:
		read = func(row int) sortValue { return sortValue{raw: typed.Data()[row]} }
	case *vectorized.TypedColumn[[]int64]:
		read = func(row int) sortValue { return sortValue{extra: typed.Data()[row]} }
	case *vectorized.TypedColumn[[]string]:
		read = func(row int) sortValue { return sortValue{extra: typed.Data()[row]} }
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		read = func(row int) sortValue { return sortValue{extra: typed.Data()[row]} }
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		read = func(row int) sortValue { return sortValue{extra: typed.Data()[row]} }
	default:
		return nil, fmt.Errorf("sort cannot copy column %T", column)
	}
	return func(row int) sortValue {
		if column.IsNull(row) {
			return sortValue{null: true}
		}
		return read(row)
	}, nil
}

func sortAppendValue(column vectorized.Column, value sortValue) {
	if value.null {
		column.AppendNull()
		return
	}
	switch typed := column.(type) {
	case *vectorized.TypedColumn[int64]:
		typed.Append(int64(value.number))
	case *vectorized.TypedColumn[float64]:
		typed.Append(math.Float64frombits(value.number))
	case *vectorized.TypedColumn[string]:
		typed.Append(value.text)
	case *vectorized.TypedColumn[[]byte]:
		typed.Append(value.raw)
	case *vectorized.TypedColumn[[]int64]:
		typed.Append(value.extra.([]int64))
	case *vectorized.TypedColumn[[]string]:
		typed.Append(value.extra.([]string))
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		typed.Append(value.extra.(*modelv1.TagValue))
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		typed.Append(value.extra.(*modelv1.FieldValue))
	}
}

func sortValueBound(value sortValue) (payload, descriptor int64) {
	if value.null {
		return 0, 0
	}
	payload = int64(len(value.text)) + int64(len(value.raw))
	switch extra := value.extra.(type) {
	case []int64:
		payload += int64(len(extra)) * 8
	case []string:
		descriptor += int64(len(extra)) * 32
		for _, item := range extra {
			payload += int64(len(item))
		}
	case *modelv1.TagValue:
		if extra != nil {
			// Wire size covers variable strings, bytes and unknown fields. Packed
			// integers still need native int64 capacity; repeated strings need headers.
			payload += int64(proto.Size(extra))
			descriptor += 1024 + int64(len(extra.GetStrArray().GetValue()))*32 + int64(len(extra.GetIntArray().GetValue()))*16
		}
	case *modelv1.FieldValue:
		if extra != nil {
			payload += int64(proto.Size(extra))
			descriptor += 1024
		}
	}
	return payload, descriptor
}

func cloneSortValue(value sortValue) sortValue {
	if value.null {
		return sortValue{null: true}
	}
	value.text = strings.Clone(value.text)
	value.raw = bytes.Clone(value.raw)
	switch extra := value.extra.(type) {
	case []int64:
		value.extra = slices.Clone(extra)
	case []string:
		copied := make([]string, len(extra))
		for index, item := range extra {
			copied[index] = strings.Clone(item)
		}
		value.extra = copied
	case *modelv1.TagValue:
		if extra != nil {
			copied := proto.Clone(extra).(*modelv1.TagValue)
			if text := copied.GetStr(); text != nil {
				text.Value = strings.Clone(text.Value)
			}
			if texts := copied.GetStrArray(); texts != nil {
				for index := range texts.Value {
					texts.Value[index] = strings.Clone(texts.Value[index])
				}
			}
			value.extra = copied
		}
	case *modelv1.FieldValue:
		if extra != nil {
			copied := proto.Clone(extra).(*modelv1.FieldValue)
			if text := copied.GetStr(); text != nil {
				text.Value = strings.Clone(text.Value)
			}
			value.extra = copied
		}
	}
	return value
}

func (o *sortOperator) copyCandidate(candidate *sortCandidate) error {
	var payload, descriptor int64
	for _, values := range [][]sortValue{o.scratch.keys, o.scratch.values} {
		for _, value := range values {
			cellPayload, cellDescriptor := sortValueBound(value)
			if cellPayload > math.MaxInt64/2-payload || cellDescriptor > math.MaxInt64-descriptor {
				return fmt.Errorf("%w: sort candidate payload overflows", vectorized.ErrMemoryBudgetExceeded)
			}
			payload += cellPayload
			descriptor += cellDescriptor
		}
	}
	if descriptor > math.MaxInt64-payload*2 {
		return fmt.Errorf("%w: sort candidate allocation overflows", vectorized.ErrMemoryBudgetExceeded)
	}
	if err := o.reserve(payload*2 + descriptor); err != nil {
		return err
	}
	for index, value := range o.scratch.keys {
		candidate.keys[index] = cloneSortValue(value)
	}
	for index, value := range o.scratch.values {
		candidate.values[index] = cloneSortValue(value)
	}
	candidate.payload = payload*2 + descriptor
	candidate.livePayload, candidate.descriptors = payload, descriptor
	o.descriptor += descriptor
	o.stats.PayloadBytes += uint64(payload)
	o.stats.PayloadCapacity += uint64(payload * 2)
	o.stats.PeakPayloadBytes = max(o.stats.PeakPayloadBytes, o.stats.PayloadBytes)
	return nil
}
