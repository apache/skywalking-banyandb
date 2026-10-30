// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package vectorized

import (
	"math"
	"unsafe"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/decimal"
)

// Per-value charges for protobuf-typed columns. A value costs its payload once plus a fixed object overhead.
const (
	// valueObjectBytes covers the value struct and its oneof wrapper.
	valueObjectBytes uint64 = 64
	// stringHeaderBytes is the Go string header kept per string-array element.
	stringHeaderBytes uint64 = 16
	// intElementBytes is the size of one int64 array element.
	intElementBytes uint64 = 8
)

// EstimatedRetainedBytes estimates row references, column capacity and payloads
// retained by a merge operator. Protobuf values are charged their payload once plus
// a fixed object overhead; this is not an exact Go heap measurement. Shared payloads may be counted twice.
func EstimatedRetainedBytes(batch *RecordBatch) uint64 {
	if batch == nil {
		return 0
	}
	total := saturatingAdd(64, saturatingMultiply(uint64(batch.ActiveLen()), 32))
	total = saturatingAdd(total, saturatingMultiply(uint64(cap(batch.Columns)), 16))
	total = saturatingAdd(total, saturatingMultiply(uint64(cap(batch.Selection)), 2))
	for _, column := range batch.Columns {
		if constCol, isConst := column.(*ConstColumn); isConst {
			// One value for every row, however many there are.
			total = saturatingAdd(total, saturatingAdd(uint64(unsafe.Sizeof(*constCol)), saturatingAdd(uint64(len(constCol.str)), uint64(cap(constCol.bytes)))))
			continue
		}
		switch column.Type() {
		case ColumnTypeDecimal:
			total = saturatingAdd(total, retainedColumnBytes(column.(*TypedColumn[decimal.Int128])))
		case ColumnTypeInt64:
			total = saturatingAdd(total, retainedColumnBytes(column.(*TypedColumn[int64])))
		case ColumnTypeFloat64:
			total = saturatingAdd(total, retainedColumnBytes(column.(*TypedColumn[float64])))
		case ColumnTypeString:
			typed := column.(*TypedColumn[string])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, value := range typed.Data() {
				total = saturatingAdd(total, uint64(len(value)))
			}
		case ColumnTypeBytes:
			typed := column.(*TypedColumn[[]byte])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, value := range typed.Data() {
				total = saturatingAdd(total, uint64(cap(value)))
			}
		case ColumnTypeInt64Array:
			typed := column.(*TypedColumn[[]int64])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, value := range typed.Data() {
				total = saturatingAdd(total, saturatingMultiply(uint64(cap(value)), 8))
			}
		case ColumnTypeStrArray:
			typed := column.(*TypedColumn[[]string])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, arrayValues := range typed.Data() {
				total = saturatingAdd(total, saturatingMultiply(uint64(cap(arrayValues)), 16))
				for _, value := range arrayValues {
					total = saturatingAdd(total, uint64(len(value)))
				}
			}
		case ColumnTypeTagValue:
			typed := column.(*TypedColumn[*modelv1.TagValue])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, value := range typed.Data() {
				if value != nil {
					total = saturatingAdd(total, estimatedTagValueBytes(value))
				}
			}
		case ColumnTypeFieldValue:
			typed := column.(*TypedColumn[*modelv1.FieldValue])
			total = saturatingAdd(total, retainedColumnBytes(typed))
			for _, value := range typed.Data() {
				if value != nil {
					total = saturatingAdd(total, estimatedFieldValueBytes(value))
				}
			}
		default:
			return math.MaxUint64
		}
	}
	return total
}

func retainedColumnBytes[T any](column *TypedColumn[T]) uint64 {
	var element T
	backing := saturatingMultiply(uint64(cap(column.data)), uint64(unsafe.Sizeof(element)))
	validity := saturatingMultiply(uint64(cap(column.validity.bits)), 8)
	return saturatingAdd(uint64(unsafe.Sizeof(*column)), saturatingAdd(backing, validity))
}

func estimatedTagValueBytes(value *modelv1.TagValue) uint64 {
	switch kind := value.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return saturatingAdd(uint64(len(kind.Str.GetValue())), valueObjectBytes)
	case *modelv1.TagValue_BinaryData:
		return saturatingAdd(uint64(len(kind.BinaryData)), valueObjectBytes)
	case *modelv1.TagValue_StrArray:
		total := valueObjectBytes
		for _, element := range kind.StrArray.GetValue() {
			total = saturatingAdd(total, saturatingAdd(uint64(len(element)), stringHeaderBytes))
		}
		return total
	case *modelv1.TagValue_IntArray:
		return saturatingAdd(saturatingMultiply(uint64(len(kind.IntArray.GetValue())), intElementBytes), valueObjectBytes)
	case *modelv1.TagValue_Int, *modelv1.TagValue_Null, *modelv1.TagValue_Timestamp:
		return valueObjectBytes
	default:
		return saturatingAdd(uint64(proto.Size(value)), valueObjectBytes)
	}
}

func estimatedFieldValueBytes(value *modelv1.FieldValue) uint64 {
	switch kind := value.GetValue().(type) {
	case *modelv1.FieldValue_Str:
		return saturatingAdd(uint64(len(kind.Str.GetValue())), valueObjectBytes)
	case *modelv1.FieldValue_BinaryData:
		return saturatingAdd(uint64(len(kind.BinaryData)), valueObjectBytes)
	case *modelv1.FieldValue_Int, *modelv1.FieldValue_Float, *modelv1.FieldValue_Null:
		return valueObjectBytes
	default:
		return saturatingAdd(uint64(proto.Size(value)), valueObjectBytes)
	}
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func saturatingMultiply(left, right uint64) uint64 {
	if right != 0 && left > math.MaxUint64/right {
		return math.MaxUint64
	}
	return left * right
}
