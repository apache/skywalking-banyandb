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
	"errors"
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// errNilBatch signals that Encode was handed a nil batch or nil schema.
var errNilBatch = errors.New("vectorized.measure.frame: nil batch or schema")

// Encode serializes a vec columnar RecordBatch into a non-proto raw frame
// body suitable for clusterv1.SendResponse.Body on TopicInternalMeasureQuery
// under the G9f throughout-vec design. The returned bytes begin with Magic
// (0x00-leading), so a flag-off node's proto.Unmarshal of the body
// deterministically fails loud (G9f spec Principle 3).
//
// Only the batch's active rows are encoded: if b.Selection is nil, every row
// in [0, b.Len) is active; otherwise the rows listed in b.Selection are
// active in the order they appear. Empty Selection produces a 0-row frame.
//
// Supported column types are int64, float64, string, and []byte. Any other
// type yields ErrUnsupportedColumnType so unsupported shapes surface here at
// encode time, not as silently-wrong wire bytes at the consumer. Likewise an
// unknown column role yields ErrUnsupportedColumnRole.
//
// The decoder lives in G9f.3; this package intentionally ships encode-only
// per the G9f spec (golden-byte exit, no decode(encode(x)) round-trip here).
func Encode(b *vectorized.RecordBatch) ([]byte, error) {
	if b == nil || b.Schema == nil {
		return nil, errNilBatch
	}
	active := activeRowIndices(b)
	nrows := uint64(len(active))
	ncols := uint64(len(b.Schema.Columns))

	buf := make([]byte, 0, MinHeaderLen)
	buf = append(buf, Magic[:]...)
	buf = append(buf, WireVersion)
	buf = binary.AppendUvarint(buf, nrows)
	buf = binary.AppendUvarint(buf, ncols)

	for colIdx, def := range b.Schema.Columns {
		role, roleErr := mapColumnRole(def.Role)
		if roleErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", colIdx, def.Name, roleErr)
		}
		ctype, typeErr := mapColumnType(def.Type)
		if typeErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", colIdx, def.Name, typeErr)
		}
		buf = append(buf, byte(role), byte(ctype))
		buf = binary.AppendUvarint(buf, uint64(len(def.Name)))
		buf = append(buf, def.Name...)
		// TagFamily survives the wire: the row path's BuildBatchSchema
		// groups RoleTag columns by family for serializeBatchToProto's
		// TagFamilyGroups output. Dropping it on the wire collapsed
		// every projected tag family into the empty-name family on the
		// receive side, producing `tagFamilies[].name == ""` and
		// breaking the integration fixture's expected YAML.
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
// RecordBatch.Selection contract: nil ⇒ [0, Len); empty ⇒ no rows;
// non-empty ⇒ rows listed in Selection (in their order).
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

// appendValidityBitmap appends the per-active-row null bitmap for col. Bit j
// is set ⇔ col.IsNull(active[j]) is true (1 = null), matching the project's
// validityBitmap convention. ⌈N/8⌉ bytes, little-endian bit packing: byte k
// holds rows k*8 … k*8+7, with bit (j%8) within that byte. No bytes are
// appended when N=0.
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
// of nullness — the validity bitmap is the source of truth, so null slots
// carry the underlying source value but the decoder ignores them.
// Variable-width types (string, []byte) write uvarint(len) + len bytes per row;
// null rows write len=0 + 0 bytes (the validity bitmap disambiguates "null"
// from "empty string"/"empty bytes").
func appendColumnData(buf []byte, col vectorized.Column, t vectorized.ColumnType, active []int) ([]byte, error) {
	switch t {
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
	}
	return nil, fmt.Errorf("%w: %s", ErrUnsupportedColumnType, t.String())
}

// mapColumnType translates a vectorized.ColumnType to its wire-stable
// frameColType. Unsupported types yield ErrUnsupportedColumnType so callers
// surface the bad input loudly at encode time.
func mapColumnType(t vectorized.ColumnType) (frameColType, error) {
	switch t {
	case vectorized.ColumnTypeInt64:
		return frameColInt64, nil
	case vectorized.ColumnTypeFloat64:
		return frameColFloat64, nil
	case vectorized.ColumnTypeString:
		return frameColString, nil
	case vectorized.ColumnTypeBytes:
		return frameColBytes, nil
	}
	return 0, fmt.Errorf("%w: %s", ErrUnsupportedColumnType, t.String())
}

// mapColumnRole translates a vectorized.ColumnRole to its wire-stable
// frameColRole. Unsupported roles yield ErrUnsupportedColumnRole.
func mapColumnRole(r vectorized.ColumnRole) (frameColRole, error) {
	switch r {
	case vectorized.RoleTimestamp:
		return frameRoleTimestamp, nil
	case vectorized.RoleVersion:
		return frameRoleVersion, nil
	case vectorized.RoleSeriesID:
		return frameRoleSeriesID, nil
	case vectorized.RoleShardID:
		return frameRoleShardID, nil
	case vectorized.RoleTag:
		return frameRoleTag, nil
	case vectorized.RoleField:
		return frameRoleField, nil
	}
	return 0, fmt.Errorf("%w: %d", ErrUnsupportedColumnRole, r)
}
