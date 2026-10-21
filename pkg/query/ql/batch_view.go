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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ql

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// RetainBatchView snapshots selected columns for use after the source advances or closes.
// Repeated indices share one immutable copied column. ReleaseOwners releases the view's budget.
// NativeScan otherwise lends batches only until the next pull or Close.
func RetainBatchView(ctx context.Context, batch *vectorized.RecordBatch, indices []int) (*vectorized.RecordBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if batch == nil || batch.Schema == nil {
		return nil, fmt.Errorf("batch view requires a batch schema")
	}
	for _, column := range batch.Columns {
		switch col := column.(type) {
		case *vectorized.ConstColumn:
			if !vectorized.ConstSupported(col.Type()) {
				return nil, fmt.Errorf("unsupported constant type %v", col.Type())
			}
		case *vectorized.TypedColumn[int64], *vectorized.TypedColumn[float64], *vectorized.TypedColumn[string], *vectorized.TypedColumn[[]byte],
			*vectorized.TypedColumn[[]int64], *vectorized.TypedColumn[[]string], *vectorized.TypedColumn[*modelv1.TagValue], *vectorized.TypedColumn[*modelv1.FieldValue]:
		default:
			return nil, fmt.Errorf("unsupported native batch column %T", column)
		}
	}
	defs := make([]vectorized.ColumnDef, len(indices))
	for pos, idx := range indices {
		if idx < 0 || idx >= len(batch.Columns) {
			return nil, fmt.Errorf("batch view column %d out of range", idx)
		}
		defs[pos] = batch.Schema.Columns[idx]
	}
	tracker := vectorized.QueryMemoryTracker(ctx, 256*1024*1024)
	// Double the conservative native retained estimate to cover copied protobuf object graphs and allocation slack.
	estimated := vectorized.EstimatedRetainedBytes(batch)
	overhead := uint64(len(indices))*128 + uint64(len(batch.Selection))*2 + 256
	if estimated > (math.MaxInt64-overhead)/2 {
		return nil, fmt.Errorf("batch view exceeds supported memory size")
	}
	reserved := int64(estimated*2 + overhead)
	if err := tracker.Reserve(reserved); err != nil {
		return nil, err
	}
	owner := &batchViewOwner{tracker: tracker, bytes: reserved}
	owner.refs.Store(1)
	view := &vectorized.RecordBatch{Schema: vectorized.NewBatchSchema(defs), Len: batch.Len, Selection: slices.Clone(batch.Selection), Lent: true}
	view.Owners.Pin(owner)
	success := false
	defer func() {
		if !success {
			view.ReleaseOwners()
		}
	}()
	owner.Release()
	copied := make(map[int]vectorized.Column)
	for _, idx := range indices {
		column, exists := copied[idx]
		if !exists {
			var copyErr error
			column, copyErr = copyViewColumn(batch.Columns[idx])
			if copyErr != nil {
				return nil, copyErr
			}
			copied[idx] = column
		}
		view.Columns = append(view.Columns, column)
	}
	success = true
	return view, nil
}

type batchViewOwner struct {
	tracker *vectorized.MemoryTracker
	bytes   int64
	refs    atomic.Int64
}

func (o *batchViewOwner) Retain() { o.refs.Add(1) }
func (o *batchViewOwner) Release() {
	if o.refs.Add(-1) == 0 {
		o.tracker.Release(o.bytes)
	}
}

func copyViewColumn(column vectorized.Column) (vectorized.Column, error) {
	if constant, ok := vectorized.AsConst(column); ok {
		if constant.IsNullValue() {
			return vectorized.NewConstNull(constant.Type(), constant.Len()), nil
		}
		switch constant.Type() {
		case vectorized.ColumnTypeInt64:
			return vectorized.NewConstInt64(constant.Int64(), constant.Len()), nil
		case vectorized.ColumnTypeFloat64:
			return vectorized.NewConstFloat64(constant.Float64(), constant.Len()), nil
		case vectorized.ColumnTypeString:
			return vectorized.NewConstString(strings.Clone(constant.Str()), constant.Len()), nil
		case vectorized.ColumnTypeBytes:
			return vectorized.NewConstBytes(bytes.Clone(constant.Bytes()), constant.Len()), nil
		case vectorized.ColumnTypeInt64Array, vectorized.ColumnTypeStrArray, vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
			return nil, fmt.Errorf("unsupported constant type %v", constant.Type())
		}
	}
	switch col := column.(type) {
	case *vectorized.TypedColumn[int64]:
		return copyTypedView(col, func(v int64) int64 { return v }), nil
	case *vectorized.TypedColumn[float64]:
		return copyTypedView(col, func(v float64) float64 { return v }), nil
	case *vectorized.TypedColumn[string]:
		return copyTypedView(col, strings.Clone), nil
	case *vectorized.TypedColumn[[]byte]:
		return copyTypedView(col, bytes.Clone), nil
	case *vectorized.TypedColumn[[]int64]:
		return copyTypedView(col, slices.Clone[[]int64]), nil
	case *vectorized.TypedColumn[[]string]:
		return copyTypedView(col, func(v []string) []string {
			out := slices.Clone(v)
			for i := range out {
				out[i] = strings.Clone(out[i])
			}
			return out
		}), nil
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		return copyTypedView(col, func(v *modelv1.TagValue) *modelv1.TagValue {
			if v == nil {
				return nil
			}
			return proto.Clone(v).(*modelv1.TagValue)
		}), nil
	case *vectorized.TypedColumn[*modelv1.FieldValue]:
		return copyTypedView(col, func(v *modelv1.FieldValue) *modelv1.FieldValue {
			if v == nil {
				return nil
			}
			return proto.Clone(v).(*modelv1.FieldValue)
		}), nil
	default:
		return nil, fmt.Errorf("unsupported native batch column %T", column)
	}
}

func copyTypedView[T any](column *vectorized.TypedColumn[T], clone func(T) T) vectorized.Column {
	out := vectorized.NewColumnForType(column.Type(), column.Len()).(*vectorized.TypedColumn[T])
	for row, value := range column.Data() {
		if column.IsNull(row) {
			out.AppendNull()
		} else {
			out.Append(clone(value))
		}
	}
	return out
}
