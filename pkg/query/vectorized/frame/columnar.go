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
	"unsafe"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// The columnar layout is the Arrow-compatible counterpart of the row-oriented layout
// described in doc.go. It shares the magic and the header prefix (magic, version,
// uvarint NumRows, uvarint NumCols) and differs in the version byte
// (Codec.ColumnarWireVersion) and in the column blocks, which send each buffer of a
// column as it sits in memory so a receiver builds the column by slicing the message.
//
// Every multi-byte value is little-endian. Every buffer starts on an 8-byte boundary
// counted from the first byte of the frame and is zero padded to the next one, so a
// receiver whose message buffer is 8-byte aligned reads the numeric buffers in place.
//
// Column block:
//
//	off  len        field        description
//	---  ---------  -----------  --------------------------------------------------
//	  0  1          Role         codec.RoleToWire(def.Role).
//	  1  1          Type         codec.TypeToWire(def.Type).
//	  2  1          Encoding     0 flat, 1 constant.
//	  3  uvarint    NameLen      length of the UTF-8 column name.
//	  ?  NameLen    Name         column name bytes.
//	  ?  uvarint    TagFamilyLen length of the UTF-8 tag family name (0 for non-tags).
//	  ?  TFL        TagFamily    tag family name bytes.
//	  ?  uvarint    NullCount    flat: rows that are null; constant: 1 if the value is null.
//	  ?  0-7        padding      to an 8-byte boundary.
//	  ?  ...        buffers      by encoding and type, below.
//
// Flat buffers, N = NumRows, each padded to 8 bytes:
//
//   - validity: ceil(N/8) bytes, bit i (least significant first) set when row i holds a
//     value, as in Arrow. Present only when NullCount > 0.
//   - int64, float64: N x 8 bytes.
//   - string, bytes, TagValue, FieldValue: offsets, (N+1) x int32, offsets[0] = 0 and
//     non-decreasing; then the values, offsets[N] bytes, row i being
//     values[offsets[i]:offsets[i+1]]. A TagValue or FieldValue cell is the proto
//     encoding of the message, empty for a null row or an empty message.
//
// Constant buffers (int64, float64, string and bytes only; absent when the value is null):
//
//   - int64, float64: the value, 8 bytes.
//   - string, bytes: uint32 length, the value, padded to 8 bytes in all.
//
// A constant column stands for NumRows copies of its value; a decoder expands it into the
// typed column the consumers expect (see DecodeColumnar).
const (
	columnEncFlat  uint8 = 0
	columnEncConst uint8 = 1

	// maxConstRows bounds the rows a constant column may stand for, so a corrupt header
	// cannot make a decoder expand a few bytes into gigabytes. An encoder expands a
	// constant column of more rows into a flat one.
	maxConstRows = 1 << 20

	// maxConstExpandBytes bounds the memory a decoder may allocate to expand ALL the
	// constant columns of one frame. The per-column row limit alone lets a tiny frame of
	// many constant columns amplify into tens of gigabytes before any query memory
	// tracker sees it. 256 MiB is the expansion of ten maxConstRows string columns, far
	// above what a query's constant columns (entity tags, a few fields) need. The
	// encoder applies the same budget and sends a constant column that would exceed it
	// as a flat column, whose size is paid for in frame bytes, so a legitimate frame
	// never trips the decoder. Flat columns need no budget: every buffer they allocate
	// from is bounded by bytes actually present in the frame, at a small constant ratio.
	maxConstExpandBytes = 256 << 20
)

// constExpandBytes returns the memory a decoder allocates to expand a constant column
// of n rows into its typed column: one element per row, plus the validity bitmap and
// the null set when the constant is null.
func constExpandBytes(ctype vectorized.ColumnType, n int, null bool) int {
	var elem int
	switch ctype { //nolint:exhaustive // ConstSupported admits only these four types
	case vectorized.ColumnTypeString:
		elem = int(unsafe.Sizeof(""))
	case vectorized.ColumnTypeBytes:
		elem = int(unsafe.Sizeof([]byte(nil)))
	default:
		elem = 8
	}
	size := n * elem
	if null {
		size += 2 * ((n + 7) / 8)
	}
	return size
}

var hostLittleEndian = func() bool {
	x := uint16(1)
	return *(*byte)(unsafe.Pointer(&x)) == 1
}()

func pad8(n int) int { return (n + 7) &^ 7 }

// colPlan is what the encoder learns about a column before it writes anything.
type colPlan struct {
	col      vectorized.Column
	constCol *vectorized.ConstColumn
	// cellSizes holds the proto size of every cell of a TagValue/FieldValue column.
	cellSizes []int
	nullCount int
	// dataLen is the total value bytes of a variable-width column.
	dataLen int
	ctype   vectorized.ColumnType
	role    uint8
	typ     uint8
}

// EncodeColumnar serializes b into a columnar frame body. Only the batch's active rows
// are encoded, as in Encode. A ConstColumn is sent as one value; every value a column
// borrows from another process's or pool's memory is copied into the frame, so the frame
// references nothing outside itself.
func (c Codec) EncodeColumnar(b *vectorized.RecordBatch) ([]byte, error) {
	if c.ColumnarWireVersion == 0 {
		return nil, errors.New("vectorized.frame: codec has no columnar wire version")
	}
	if b == nil || b.Schema == nil {
		return nil, errNilBatch
	}
	sel := b.Selection
	n := b.Len
	if sel != nil {
		n = len(sel)
	}
	ncols := len(b.Schema.Columns)
	plans := make([]colPlan, ncols)
	total := MagicLen + 1 + uvarintLen(uint64(n)) + uvarintLen(uint64(ncols))
	constBudget := maxConstExpandBytes
	for i, def := range b.Schema.Columns {
		plan := &plans[i]
		role, roleErr := c.RoleToWire(def.Role)
		if roleErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, roleErr)
		}
		wireType, typeErr := c.TypeToWire(def.Type)
		if typeErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, typeErr)
		}
		plan.role, plan.typ, plan.ctype, plan.col = role, wireType, def.Type, b.Columns[i]
		if constCol, isConst := plan.col.(*vectorized.ConstColumn); isConst {
			if n <= maxConstRows && constCol.Type() == def.Type && vectorized.ConstSupported(def.Type) &&
				constExpandBytes(def.Type, n, constCol.IsNullValue()) <= constBudget {
				constBudget -= constExpandBytes(def.Type, n, constCol.IsNullValue())
				plan.constCol = constCol
				if constCol.IsNullValue() {
					plan.nullCount = 1
				}
			} else {
				plan.col = constCol.Expand()
			}
		}
		if plan.constCol == nil {
			if planErr := planFlatColumn(plan, sel, n); planErr != nil {
				return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, planErr)
			}
		}
		total += 3 + uvarintLen(uint64(len(def.Name))) + len(def.Name) + uvarintLen(uint64(len(def.TagFamily))) + len(def.TagFamily) +
			uvarintLen(uint64(plan.nullCount))
		total = pad8(total) + plan.bufferBytes(n)
		if total > math.MaxInt32 {
			return nil, fmt.Errorf("vectorized.frame: columnar frame exceeds %d bytes", math.MaxInt32)
		}
	}

	out := make([]byte, 0, total)
	out = append(out, c.Magic[:]...)
	out = append(out, c.ColumnarWireVersion)
	out = binary.AppendUvarint(out, uint64(n))
	out = binary.AppendUvarint(out, uint64(ncols))
	for i, def := range b.Schema.Columns {
		plan := &plans[i]
		enc := columnEncFlat
		if plan.constCol != nil {
			enc = columnEncConst
		}
		out = append(out, plan.role, plan.typ, enc)
		out = binary.AppendUvarint(out, uint64(len(def.Name)))
		out = append(out, def.Name...)
		out = binary.AppendUvarint(out, uint64(len(def.TagFamily)))
		out = append(out, def.TagFamily...)
		out = binary.AppendUvarint(out, uint64(plan.nullCount))
		out = padTo8(out)
		var writeErr error
		if plan.constCol != nil {
			out = appendConst(out, plan.constCol)
		} else {
			out, writeErr = appendFlat(out, plan, sel, n)
		}
		if writeErr != nil {
			return nil, fmt.Errorf("column %d (%q): %w", i, def.Name, writeErr)
		}
	}
	if len(out) != total {
		return nil, fmt.Errorf("vectorized.frame: columnar frame size mismatch: wrote %d, planned %d", len(out), total)
	}
	return out, nil
}

// bufferBytes returns the size of the column's buffers, each padded to 8 bytes.
func (p *colPlan) bufferBytes(n int) int {
	if p.constCol != nil {
		if p.nullCount > 0 {
			return 0
		}
		switch p.ctype { //nolint:exhaustive // a ConstColumn only stands for these four types
		case vectorized.ColumnTypeString:
			return pad8(4 + len(p.constCol.Str()))
		case vectorized.ColumnTypeBytes:
			return pad8(4 + len(p.constCol.Bytes()))
		default:
			return 8
		}
	}
	size := 0
	if p.nullCount > 0 {
		size += pad8((n + 7) / 8)
	}
	switch p.ctype { //nolint:exhaustive // array column types are converted to TagValue cells before they reach the codec
	case vectorized.ColumnTypeInt64, vectorized.ColumnTypeFloat64:
		return size + n*8
	default:
		return size + pad8((n+1)*4) + pad8(p.dataLen)
	}
}

func padTo8(b []byte) []byte {
	for len(b)&7 != 0 {
		b = append(b, 0)
	}
	return b
}

func uvarintLen(v uint64) int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutUvarint(tmp[:], v)
}

type nullable interface{ MayHaveNulls() bool }

// planFlatColumn counts the nulls of a flat column and the bytes its values need.
func planFlatColumn(p *colPlan, sel []uint16, n int) error {
	col := p.col
	switch p.ctype { //nolint:exhaustive // array column types are converted to TagValue cells before they reach the codec
	case vectorized.ColumnTypeInt64:
		if _, ok := col.(*vectorized.TypedColumn[int64]); !ok {
			return fmt.Errorf("%w: declared int64 but column is %T", ErrUnsupportedColumnType, col)
		}
	case vectorized.ColumnTypeFloat64:
		if _, ok := col.(*vectorized.TypedColumn[float64]); !ok {
			return fmt.Errorf("%w: declared float64 but column is %T", ErrUnsupportedColumnType, col)
		}
	case vectorized.ColumnTypeString:
		typed, ok := col.(*vectorized.TypedColumn[string])
		if !ok {
			return fmt.Errorf("%w: declared string but column is %T", ErrUnsupportedColumnType, col)
		}
		data := typed.Data()
		for i := range n {
			if row := rowAt(sel, i); row < len(data) && !col.IsNull(row) {
				p.dataLen += len(data[row])
			}
		}
	case vectorized.ColumnTypeBytes:
		typed, ok := col.(*vectorized.TypedColumn[[]byte])
		if !ok {
			return fmt.Errorf("%w: declared bytes but column is %T", ErrUnsupportedColumnType, col)
		}
		data := typed.Data()
		for i := range n {
			if row := rowAt(sel, i); row < len(data) && !col.IsNull(row) {
				p.dataLen += len(data[row])
			}
		}
	case vectorized.ColumnTypeTagValue:
		typed, ok := col.(*vectorized.TypedColumn[*modelv1.TagValue])
		if !ok {
			return fmt.Errorf("%w: declared TagValue passthrough but column is %T", ErrUnsupportedColumnType, col)
		}
		p.cellSizes = protoCellSizes(typed.Data(), col, sel, n)
		p.dataLen = sumInts(p.cellSizes)
	case vectorized.ColumnTypeFieldValue:
		typed, ok := col.(*vectorized.TypedColumn[*modelv1.FieldValue])
		if !ok {
			return fmt.Errorf("%w: declared FieldValue passthrough but column is %T", ErrUnsupportedColumnType, col)
		}
		p.cellSizes = protoCellSizes(typed.Data(), col, sel, n)
		p.dataLen = sumInts(p.cellSizes)
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedColumnType, p.ctype.String())
	}
	if p.dataLen > math.MaxInt32 {
		return fmt.Errorf("vectorized.frame: column values exceed %d bytes", math.MaxInt32)
	}
	if nl, ok := col.(nullable); ok && !nl.MayHaveNulls() {
		return nil
	}
	for i := range n {
		if col.IsNull(rowAt(sel, i)) {
			p.nullCount++
		}
	}
	return nil
}

func rowAt(sel []uint16, i int) int {
	if sel == nil {
		return i
	}
	return int(sel[i])
}

func sumInts(v []int) int {
	total := 0
	for _, x := range v {
		total += x
	}
	return total
}

func protoCellSizes[T interface {
	comparable
	proto.Message
}](data []T, col vectorized.Column, sel []uint16, n int) []int {
	sizes := make([]int, n)
	var zero T
	for i := range n {
		row := rowAt(sel, i)
		if row >= len(data) || data[row] == zero || col.IsNull(row) {
			continue
		}
		sizes[i] = proto.Size(data[row])
	}
	return sizes
}

// appendConst appends the value buffer of a constant column.
func appendConst(out []byte, c *vectorized.ConstColumn) []byte {
	if c.IsNullValue() {
		return out
	}
	switch c.Type() { //nolint:exhaustive // a ConstColumn only stands for these four types
	case vectorized.ColumnTypeInt64:
		return binary.LittleEndian.AppendUint64(out, uint64(c.Int64()))
	case vectorized.ColumnTypeFloat64:
		return binary.LittleEndian.AppendUint64(out, math.Float64bits(c.Float64()))
	case vectorized.ColumnTypeString:
		out = binary.LittleEndian.AppendUint32(out, uint32(len(c.Str())))
		out = append(out, c.Str()...)
	default:
		out = binary.LittleEndian.AppendUint32(out, uint32(len(c.Bytes())))
		out = append(out, c.Bytes()...)
	}
	return padTo8(out)
}

// appendFlat appends the buffers of a flat column.
func appendFlat(out []byte, p *colPlan, sel []uint16, n int) ([]byte, error) {
	if p.nullCount > 0 {
		start := len(out)
		out = append(out, make([]byte, (n+7)/8)...)
		bits := out[start:]
		for i := range n {
			if !p.col.IsNull(rowAt(sel, i)) {
				bits[i/8] |= 1 << uint(i%8)
			}
		}
		out = padTo8(out)
	}
	switch p.ctype { //nolint:exhaustive // array column types are converted to TagValue cells before they reach the codec
	case vectorized.ColumnTypeInt64:
		data := p.col.(*vectorized.TypedColumn[int64]).Data()
		return appendInt64s(out, data, sel, n), nil
	case vectorized.ColumnTypeFloat64:
		data := p.col.(*vectorized.TypedColumn[float64]).Data()
		return appendFloat64s(out, data, sel, n), nil
	case vectorized.ColumnTypeString:
		data := p.col.(*vectorized.TypedColumn[string]).Data()
		return appendVarWidth(out, n, func(i int, dst []byte) []byte {
			if row := rowAt(sel, i); row < len(data) && !p.col.IsNull(row) {
				return append(dst, data[row]...)
			}
			return dst
		}), nil
	case vectorized.ColumnTypeBytes:
		data := p.col.(*vectorized.TypedColumn[[]byte]).Data()
		return appendVarWidth(out, n, func(i int, dst []byte) []byte {
			if row := rowAt(sel, i); row < len(data) && !p.col.IsNull(row) {
				return append(dst, data[row]...)
			}
			return dst
		}), nil
	case vectorized.ColumnTypeTagValue:
		data := p.col.(*vectorized.TypedColumn[*modelv1.TagValue]).Data()
		return appendProtoCells(out, p, sel, n, data)
	default:
		data := p.col.(*vectorized.TypedColumn[*modelv1.FieldValue]).Data()
		return appendProtoCells(out, p, sel, n, data)
	}
}

func appendInt64s(out []byte, data []int64, sel []uint16, n int) []byte {
	if sel == nil && hostLittleEndian && n <= len(data) {
		if n == 0 {
			return out
		}
		return append(out, unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data))), n*8)...)
	}
	for i := range n {
		var v int64
		if row := rowAt(sel, i); row < len(data) {
			v = data[row]
		}
		out = binary.LittleEndian.AppendUint64(out, uint64(v))
	}
	return out
}

func appendFloat64s(out []byte, data []float64, sel []uint16, n int) []byte {
	if sel == nil && hostLittleEndian && n <= len(data) {
		if n == 0 {
			return out
		}
		return append(out, unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(data))), n*8)...)
	}
	for i := range n {
		var v float64
		if row := rowAt(sel, i); row < len(data) {
			v = data[row]
		}
		out = binary.LittleEndian.AppendUint64(out, math.Float64bits(v))
	}
	return out
}

// appendVarWidth appends the offsets and values buffers of a variable-width column.
// appendValue appends the value of row i, and nothing for a null row.
func appendVarWidth(out []byte, n int, appendValue func(i int, dst []byte) []byte) []byte {
	offsetsStart := len(out)
	out = append(out, make([]byte, pad8((n+1)*4))...)
	valuesStart := len(out)
	for i := range n {
		out = appendValue(i, out)
		binary.LittleEndian.PutUint32(out[offsetsStart+(i+1)*4:], uint32(len(out)-valuesStart))
	}
	return padTo8(out)
}

// appendProtoCells appends the offsets and values buffers of a TagValue or FieldValue column.
func appendProtoCells[T interface {
	comparable
	proto.Message
}](out []byte, p *colPlan, sel []uint16, n int, data []T) ([]byte, error) {
	var zero T
	var marshalErr error
	out = appendVarWidth(out, n, func(i int, dst []byte) []byte {
		size := p.cellSizes[i]
		if size == 0 {
			return dst
		}
		cell := data[rowAt(sel, i)]
		if cell == zero {
			return dst
		}
		if marshalErr != nil {
			return dst
		}
		start := len(dst)
		grown, err := proto.MarshalOptions{}.MarshalAppend(dst, cell)
		if err != nil {
			marshalErr = err
			return dst
		}
		if len(grown)-start != size {
			marshalErr = fmt.Errorf("proto cell size changed from %d to %d while marshaling", size, len(grown)-start)
			return dst
		}
		return grown
	})
	if marshalErr != nil {
		return nil, fmt.Errorf("vectorized.frame: cell marshal: %w", marshalErr)
	}
	return out, nil
}
