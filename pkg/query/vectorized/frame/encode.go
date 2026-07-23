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

package frame

import (
	"encoding/binary"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// Encode serializes a vec columnar RecordBatch into a non-proto raw frame body.
// The returned bytes begin with the codec's Magic (0x00-leading), so a flag-off
// node's proto.Unmarshal of the body deterministically fails loud.
//
// Only the batch's active rows are encoded: if b.Selection is nil, every row in
// [0, b.Len) is active; otherwise the rows listed in b.Selection are active in
// the order they appear. Empty Selection produces a 0-row frame.
//
// A column whose vectorized.ColumnType has no wire mapping yields
// ErrUnsupportedColumnType; an unmapped role yields ErrUnsupportedColumnRole —
// both surface here at encode time, not as silently-wrong wire bytes.
func (c Codec) Encode(b *vectorized.RecordBatch) ([]byte, error) {
	if b == nil || b.Schema == nil {
		return nil, errNilBatch
	}
	active := activeRowIndices(b)
	nrows := uint64(len(active))
	ncols := uint64(len(b.Schema.Columns))

	buf := make([]byte, 0, MinHeaderLen)
	buf = append(buf, c.Magic[:]...)
	buf = append(buf, c.WireVersion)
	buf = binary.AppendUvarint(buf, nrows)
	buf = binary.AppendUvarint(buf, ncols)

	for colIdx, def := range b.Schema.Columns {
		role, roleErr := c.RoleToWire(def.Role)
		if roleErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", colIdx, def.Name, roleErr)
		}
		ctype, typeErr := c.TypeToWire(def.Type)
		if typeErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", colIdx, def.Name, typeErr)
		}
		buf = append(buf, role, ctype)
		buf = binary.AppendUvarint(buf, uint64(len(def.Name)))
		buf = append(buf, def.Name...)
		buf = binary.AppendUvarint(buf, uint64(len(def.TagFamily)))
		buf = append(buf, def.TagFamily...)

		col := b.Columns[colIdx]
		buf = appendValidityBitmap(buf, col, active)
		var dataErr error
		buf, dataErr = appendColumnData(buf, col, def.Type, active)
		if dataErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", colIdx, def.Name, dataErr)
		}
	}
	return buf, nil
}

// activeRowIndices materializes the active-row index list per the
// RecordBatch.Selection contract: nil ⇒ [0, Len); empty ⇒ no rows; non-empty ⇒
// rows listed in Selection (in their order).
func activeRowIndices(b *vectorized.RecordBatch) []int {
	if b.Selection == nil {
		idx := make([]int, b.Len)
		for i := range idx {
			idx[i] = i
		}
		return idx
	}
	idx := make([]int, len(b.Selection))
	for i, s := range b.Selection {
		idx[i] = int(s)
	}
	return idx
}

// appendValidityBitmap appends the per-active-row null bitmap for col. Bit j is
// set ⇔ col.IsNull(active[j]) is true (1 = null). ⌈N/8⌉ bytes, little-endian
// bit packing. No bytes are appended when N=0.
func appendValidityBitmap(buf []byte, col vectorized.Column, active []int) []byte {
	n := len(active)
	if n == 0 {
		return buf
	}
	nbytes := (n + 7) / 8
	start := len(buf)
	buf = append(buf, make([]byte, nbytes)...)
	bits := buf[start : start+nbytes]
	for j, srcRow := range active {
		if col.IsNull(srcRow) {
			bits[j/8] |= 1 << uint(j%8)
		}
	}
	return buf
}

// appendColumnData appends the type-specific data section for col over the
// active rows. Fixed-width types (int64, float64) write N × 8 bytes regardless
// of nullness. Variable-width types (string, []byte, proto passthroughs) write
// uvarint(len) + len bytes per row; null rows write len=0 + 0 bytes.
// nolint:gocyclo // switch-dispatch over ColumnType variants is intentionally exhaustive; splitting per-case helpers would obscure the wire-format mapping
func appendColumnData(buf []byte, col vectorized.Column, t vectorized.ColumnType, active []int) ([]byte, error) {
	switch t { //nolint:exhaustive // array column types are handled via passthrough at the dispatcher; this function never receives them
	case vectorized.ColumnTypeInt64:
		tc, ok := col.(*vectorized.TypedColumn[int64])
		if !ok {
			return nil, fmt.Errorf("%w: declared int64 but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var v int64
			if srcRow >= 0 && srcRow < len(data) {
				v = data[srcRow]
			}
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v))
		}
		return buf, nil
	case vectorized.ColumnTypeFloat64:
		tc, ok := col.(*vectorized.TypedColumn[float64])
		if !ok {
			return nil, fmt.Errorf("%w: declared float64 but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var v float64
			if srcRow >= 0 && srcRow < len(data) {
				v = data[srcRow]
			}
			buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(v))
		}
		return buf, nil
	case vectorized.ColumnTypeString:
		tc, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			return nil, fmt.Errorf("%w: declared string but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var v string
			if !col.IsNull(srcRow) && srcRow >= 0 && srcRow < len(data) {
				v = data[srcRow]
			}
			buf = binary.AppendUvarint(buf, uint64(len(v)))
			buf = append(buf, v...)
		}
		return buf, nil
	case vectorized.ColumnTypeBytes:
		tc, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			return nil, fmt.Errorf("%w: declared bytes but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var v []byte
			if !col.IsNull(srcRow) && srcRow >= 0 && srcRow < len(data) {
				v = data[srcRow]
			}
			buf = binary.AppendUvarint(buf, uint64(len(v)))
			buf = append(buf, v...)
		}
		return buf, nil
	case vectorized.ColumnTypeTagValue:
		tc, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		if !ok {
			return nil, fmt.Errorf("%w: declared TagValue passthrough but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var raw []byte
			if !col.IsNull(srcRow) && srcRow >= 0 && srcRow < len(data) && data[srcRow] != nil {
				marshaled, marshalErr := proto.Marshal(data[srcRow])
				if marshalErr != nil {
					return nil, fmt.Errorf("vectorized.frame: TagValue cell marshal: %w", marshalErr)
				}
				raw = marshaled
			}
			buf = binary.AppendUvarint(buf, uint64(len(raw)))
			buf = append(buf, raw...)
		}
		return buf, nil
	case vectorized.ColumnTypeFieldValue:
		tc, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		if !ok {
			return nil, fmt.Errorf("%w: declared FieldValue passthrough but column is %T", ErrUnsupportedColumnType, col)
		}
		data := tc.Data()
		for _, srcRow := range active {
			var raw []byte
			if !col.IsNull(srcRow) && srcRow >= 0 && srcRow < len(data) && data[srcRow] != nil {
				marshaled, marshalErr := proto.Marshal(data[srcRow])
				if marshalErr != nil {
					return nil, fmt.Errorf("vectorized.frame: FieldValue cell marshal: %w", marshalErr)
				}
				raw = marshaled
			}
			buf = binary.AppendUvarint(buf, uint64(len(raw)))
			buf = append(buf, raw...)
		}
		return buf, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrUnsupportedColumnType, t.String())
}
