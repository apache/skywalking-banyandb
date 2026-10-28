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
	"fmt"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type aggregateValue struct {
	text      string
	raw       []byte
	i64       int64
	f64       float64
	typ       vectorized.ColumnType
	null      bool
	timestamp bool
}

type aggregateReader struct {
	column    vectorized.Column
	def       vectorized.ColumnDef
	index     int
	timestamp bool
}

func aggregateScalarType(typ vectorized.ColumnType) bool {
	return typ == vectorized.ColumnTypeInt64 || typ == vectorized.ColumnTypeFloat64 ||
		typ == vectorized.ColumnTypeString || typ == vectorized.ColumnTypeBytes
}

func newAggregateReader(schema *vectorized.BatchSchema, def vectorized.ColumnDef) (aggregateReader, error) {
	index := sourceIndex(schema, def)
	if index < 0 {
		return aggregateReader{}, fmt.Errorf("aggregate source column %q in family %q is absent", def.Name, def.TagFamily)
	}
	actual := schema.Columns[index].Type
	if actual != def.Type && actual != vectorized.ColumnTypeTagValue && actual != vectorized.ColumnTypeFieldValue {
		return aggregateReader{}, fmt.Errorf("aggregate source column %q has type %s; expected %s", def.Name, actual, def.Type)
	}
	return aggregateReader{def: def, index: index}, nil
}

func (r *aggregateReader) bind(batch *vectorized.RecordBatch) error {
	if r.index >= len(batch.Columns) || batch.Columns[r.index] == nil || batch.Columns[r.index].Len() < batch.Len {
		return fmt.Errorf("aggregate source column %q does not cover the input batch", r.def.Name)
	}
	r.column = batch.Columns[r.index]
	return nil
}

func (r *aggregateReader) value(row int) (aggregateValue, error) {
	value := aggregateValue{typ: r.def.Type, null: r.column.IsNull(row)}
	if value.null {
		return value, nil
	}
	if constant, ok := vectorized.AsConst(r.column); ok {
		if constant.Type() != value.typ {
			return value, r.typeError()
		}
		value.i64, value.f64, value.text, value.raw = constant.Int64(), constant.Float64(), constant.Str(), constant.Bytes()
		return value, nil
	}
	switch column := r.column.(type) {
	case *vectorized.TypedColumn[int64]:
		if value.typ != vectorized.ColumnTypeInt64 {
			return value, r.typeError()
		}
		value.i64 = column.Data()[row]
	case *vectorized.TypedColumn[float64]:
		if value.typ != vectorized.ColumnTypeFloat64 {
			return value, r.typeError()
		}
		value.f64 = column.Data()[row]
	case *vectorized.TypedColumn[string]:
		if value.typ != vectorized.ColumnTypeString {
			return value, r.typeError()
		}
		value.text = column.Data()[row]
	case *vectorized.TypedColumn[[]byte]:
		if value.typ != vectorized.ColumnTypeBytes {
			return value, r.typeError()
		}
		value.raw = column.Data()[row]
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		return r.tagValue(column.Data()[row])
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		return r.fieldValue(column.Data()[row])
	default:
		return value, r.typeError()
	}
	return value, nil
}

func (r *aggregateReader) typeError() error {
	return fmt.Errorf("aggregate source column %q has a value incompatible with %s", r.def.Name, r.def.Type)
}

func (r *aggregateReader) tagValue(cell *modelv1.TagValue) (aggregateValue, error) {
	value := aggregateValue{typ: r.def.Type}
	if cell == nil || cell.Value == nil {
		value.null = true
		return value, nil
	}
	switch item := cell.Value.(type) {
	case *modelv1.TagValue_Null:
		value.null = true
	case *modelv1.TagValue_Int:
		if item.Int == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeInt64 {
			return value, r.typeError()
		}
		value.i64 = item.Int.Value
	case *modelv1.TagValue_Str:
		if item.Str == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeString {
			return value, r.typeError()
		}
		value.text = item.Str.Value
	case *modelv1.TagValue_BinaryData:
		if value.typ != vectorized.ColumnTypeBytes {
			return value, r.typeError()
		}
		value.raw = item.BinaryData
	case *modelv1.TagValue_Timestamp:
		if item.Timestamp == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeInt64 || item.Timestamp.CheckValid() != nil {
			return value, r.typeError()
		}
		value.i64 = item.Timestamp.AsTime().UnixNano()
	case *modelv1.TagValue_IntArray, *modelv1.TagValue_StrArray:
		return value, r.typeError()
	}
	return value, nil
}

func (r *aggregateReader) fieldValue(cell *modelv1.FieldValue) (aggregateValue, error) {
	value := aggregateValue{typ: r.def.Type}
	if cell == nil || cell.Value == nil {
		value.null = true
		return value, nil
	}
	switch item := cell.Value.(type) {
	case *modelv1.FieldValue_Null:
		value.null = true
	case *modelv1.FieldValue_Int:
		if item.Int == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeInt64 {
			return value, r.typeError()
		}
		value.i64 = item.Int.Value
	case *modelv1.FieldValue_Float:
		if item.Float == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeFloat64 {
			return value, r.typeError()
		}
		value.f64 = item.Float.Value
	case *modelv1.FieldValue_Str:
		if item.Str == nil {
			value.null = true
			return value, nil
		}
		if value.typ != vectorized.ColumnTypeString {
			return value, r.typeError()
		}
		value.text = item.Str.Value
	case *modelv1.FieldValue_BinaryData:
		if value.typ != vectorized.ColumnTypeBytes {
			return value, r.typeError()
		}
		value.raw = item.BinaryData
	}
	return value, nil
}

func appendAggregateValue(column vectorized.Column, value aggregateValue) {
	if value.null {
		column.AppendNull()
		return
	}
	switch output := column.(type) {
	case *vectorized.TypedColumn[int64]:
		output.Append(value.i64)
	case *vectorized.TypedColumn[float64]:
		output.Append(value.f64)
	case *vectorized.TypedColumn[string]:
		output.Append(value.text)
	case *vectorized.TypedColumn[[]byte]:
		output.Append([]byte(value.text))
	}
}
