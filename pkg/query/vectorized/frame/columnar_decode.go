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
	"unsafe"

	"google.golang.org/protobuf/proto"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// DecodeColumnar is the inverse of EncodeColumnar. It builds the columns by slicing the
// message instead of copying it: int64 and float64 columns are typed slices over the
// message bytes, string columns hold strings that point into it, and bytes columns hold
// sub-slices of it. The returned batch pins a MessageOwner for the message, so a
// consumer that keeps a cell past the batch's life must copy it or pin the same owners,
// exactly as for cells that borrow storage memory; ReleaseOwners drops the pin. TagValue
// and FieldValue cells are unmarshaled and borrow nothing.
//
// A constant column is expanded into the typed column the consumers expect: the cells
// share one value, so the cost is one slice header per row and no per-row allocation.
//
// The numeric buffers are read in place only when the message is 8-byte aligned;
// otherwise the message is first copied once into an aligned buffer. Every length and
// offset in the frame is validated before it is used, so a truncated or corrupt frame
// yields an error wrapping ErrTruncated rather than a panic or an out-of-range read.
func (c Codec) DecodeColumnar(msg []byte) (*vectorized.RecordBatch, error) {
	header, offset, headerErr := c.validateHeader(msg, c.ColumnarWireVersion, false)
	if headerErr != nil {
		return nil, headerErr
	}
	owner := newMessageOwner(msg)
	defer owner.Release()
	d := columnarDecoder{buf: owner.Bytes(), pos: offset, nrows: int(header.NumRows)}
	defs := make([]vectorized.ColumnDef, 0, header.NumCols)
	cols := make([]vectorized.Column, 0, header.NumCols)
	for colIdx := range header.NumCols {
		def, col, colErr := c.decodeColumnarColumn(&d)
		if colErr != nil {
			return nil, fmt.Errorf("column %d: %w", colIdx, colErr)
		}
		defs = append(defs, def)
		cols = append(cols, col)
	}
	if d.pos != len(d.buf) {
		return nil, fmt.Errorf("%w: %d trailing bytes after %d columns", ErrTruncated, len(d.buf)-d.pos, header.NumCols)
	}
	batch := &vectorized.RecordBatch{
		Schema:  vectorized.NewBatchSchema(defs),
		Columns: cols,
		Len:     d.nrows,
	}
	if d.borrows {
		batch.Owners.Pin(owner)
	}
	return batch, nil
}

type columnarDecoder struct {
	buf   []byte
	pos   int
	nrows int
	// constBytes is the memory the constant columns decoded so far expanded into.
	constBytes int
	// borrows is set once a decoded column points into buf.
	borrows bool
}

func (d *columnarDecoder) remaining() int { return len(d.buf) - d.pos }

// take returns the next size bytes and advances past them and their padding.
func (d *columnarDecoder) take(size int, what string) ([]byte, error) {
	if size < 0 || size > d.remaining() {
		return nil, fmt.Errorf("%w: %s needs %d bytes, have %d", ErrTruncated, what, size, d.remaining())
	}
	out := d.buf[d.pos : d.pos+size : d.pos+size]
	d.pos += size
	if padErr := d.skipPadding(what); padErr != nil {
		return nil, padErr
	}
	return out, nil
}

// skipPadding advances to the next 8-byte boundary, which the frame must contain.
func (d *columnarDecoder) skipPadding(what string) error {
	padded := pad8(d.pos)
	if padded > len(d.buf) {
		return fmt.Errorf("%w: padding after %s runs past the frame", ErrTruncated, what)
	}
	d.pos = padded
	return nil
}

func (d *columnarDecoder) uvarint(what string) (uint64, error) {
	v, size := binary.Uvarint(d.buf[d.pos:])
	if size <= 0 {
		return 0, fmt.Errorf("%w: malformed %s varint", ErrTruncated, what)
	}
	d.pos += size
	return v, nil
}

func (d *columnarDecoder) text(what string) (string, error) {
	length, lenErr := d.uvarint(what + "Len")
	if lenErr != nil {
		return "", lenErr
	}
	if length > uint64(d.remaining()) {
		return "", fmt.Errorf("%w: %sLen=%d exceeds remaining %d bytes", ErrTruncated, what, length, d.remaining())
	}
	text := string(d.buf[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return text, nil
}

// decodeColumnarColumn parses one column block.
func (c Codec) decodeColumnarColumn(d *columnarDecoder) (vectorized.ColumnDef, vectorized.Column, error) {
	var def vectorized.ColumnDef
	if d.remaining() < 3 {
		return def, nil, fmt.Errorf("%w: header needs 3 bytes for role, type and encoding, have %d", ErrTruncated, d.remaining())
	}
	role, roleErr := c.WireToRole(d.buf[d.pos])
	if roleErr != nil {
		return def, nil, roleErr
	}
	ctype, typeErr := c.WireToType(d.buf[d.pos+1])
	if typeErr != nil {
		return def, nil, typeErr
	}
	enc := d.buf[d.pos+2]
	d.pos += 3
	name, nameErr := d.text("Name")
	if nameErr != nil {
		return def, nil, nameErr
	}
	family, familyErr := d.text("TagFamily")
	if familyErr != nil {
		return def, nil, familyErr
	}
	nullCount, nullErr := d.uvarint("NullCount")
	if nullErr != nil {
		return def, nil, nullErr
	}
	if padErr := d.skipPadding("column header"); padErr != nil {
		return def, nil, padErr
	}
	def = vectorized.ColumnDef{Role: role, Type: ctype, Name: name, TagFamily: family}
	var col vectorized.Column
	var colErr error
	switch enc {
	case columnEncFlat:
		col, colErr = decodeFlat(d, ctype, nullCount)
	case columnEncConst:
		col, colErr = decodeConst(d, ctype, nullCount)
	default:
		colErr = fmt.Errorf("%w: unknown column encoding %d", ErrTruncated, enc)
	}
	if colErr != nil {
		return vectorized.ColumnDef{}, nil, colErr
	}
	return def, col, nil
}

// decodeFlat reads the buffers of a flat column.
func decodeFlat(d *columnarDecoder, ctype vectorized.ColumnType, nullCount uint64) (vectorized.Column, error) {
	n := d.nrows
	var valid []byte
	if nullCount > 0 {
		if nullCount > uint64(n) {
			return nil, fmt.Errorf("%w: NullCount=%d exceeds %d rows", ErrTruncated, nullCount, n)
		}
		var validErr error
		if valid, validErr = d.take((n+7)/8, "validity bitmap"); validErr != nil {
			return nil, validErr
		}
	}
	switch ctype { //nolint:exhaustive // array column types never appear on the wire; the codec's type mapping rejects them
	case vectorized.ColumnTypeInt64:
		raw, takeErr := d.take(n*8, "int64 data")
		if takeErr != nil {
			return nil, takeErr
		}
		var data []int64
		col := vectorized.NewInt64Column(0)
		if hostLittleEndian && n > 0 {
			d.borrows = true
			data = unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(raw))), n)
			col.Borrow(data)
		} else {
			data = make([]int64, n)
			for i := range data {
				data[i] = int64(binary.LittleEndian.Uint64(raw[i*8:]))
			}
			col = vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, data)
		}
		applyValidity(col, valid)
		return col, nil
	case vectorized.ColumnTypeFloat64:
		raw, takeErr := d.take(n*8, "float64 data")
		if takeErr != nil {
			return nil, takeErr
		}
		var data []float64
		col := vectorized.NewFloat64Column(0)
		if hostLittleEndian && n > 0 {
			d.borrows = true
			data = unsafe.Slice((*float64)(unsafe.Pointer(unsafe.SliceData(raw))), n)
			col.Borrow(data)
		} else {
			data = make([]float64, n)
			for i := range data {
				data[i] = math.Float64frombits(binary.LittleEndian.Uint64(raw[i*8:]))
			}
			col = vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, data)
		}
		applyValidity(col, valid)
		return col, nil
	case vectorized.ColumnTypeString, vectorized.ColumnTypeBytes, vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
		offsets, values, varErr := d.takeVarWidth(n)
		if varErr != nil {
			return nil, varErr
		}
		return buildVarWidth(d, ctype, offsets, values, valid, n)
	}
	return nil, fmt.Errorf("%w: %s", ErrUnsupportedColumnType, ctype.String())
}

// takeVarWidth reads the offsets and values buffers of a variable-width column and
// checks that the offsets are an ascending run from 0 to the length of the values.
func (d *columnarDecoder) takeVarWidth(n int) (offsets, values []byte, err error) {
	if offsets, err = d.take((n+1)*4, "offsets"); err != nil {
		return nil, nil, err
	}
	if first := binary.LittleEndian.Uint32(offsets); first != 0 {
		return nil, nil, fmt.Errorf("%w: first offset is %d, want 0", ErrTruncated, first)
	}
	end := binary.LittleEndian.Uint32(offsets[n*4:])
	if values, err = d.take(int(end), "values"); err != nil {
		return nil, nil, err
	}
	return offsets, values, nil
}

// isValid reports whether row i holds a value; a nil bitmap means every row does.
func isValid(valid []byte, i int) bool {
	return len(valid) == 0 || (i/8 < len(valid) && valid[i/8]&(1<<uint(i%8)) != 0)
}

func applyValidity[T any](col *vectorized.TypedColumn[T], valid []byte) {
	if valid != nil {
		col.SetNullsFromValidity(valid)
	}
}

// buildVarWidth builds a string, bytes, TagValue or FieldValue column from its offsets
// and values buffers.
func buildVarWidth(d *columnarDecoder, ctype vectorized.ColumnType, offsets, values, valid []byte, n int) (vectorized.Column, error) {
	// cell returns the bounds of row i, or an error when the offsets are not ascending
	// or run past the values.
	prev := uint32(0)
	cell := func(i int) (int, int, error) {
		next := binary.LittleEndian.Uint32(offsets[(i+1)*4:])
		if next < prev || int(next) > len(values) {
			return 0, 0, errOffset(i, next, len(values))
		}
		lo := prev
		prev = next
		return int(lo), int(next), nil
	}
	switch ctype { //nolint:exhaustive // only the variable-width types are routed here
	case vectorized.ColumnTypeString:
		data := make([]string, n)
		last := uint32(0)
		for i := range data {
			next := binary.LittleEndian.Uint32(offsets[(i+1)*4:])
			if next < last || int(next) > len(values) {
				return nil, errOffset(i, next, len(values))
			}
			if next > last {
				data[i] = unsafe.String(&values[last], int(next-last))
			}
			last = next
		}
		d.borrows = d.borrows || len(values) > 0
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, data)
		applyValidity(col, valid)
		return col, nil
	case vectorized.ColumnTypeBytes:
		data := make([][]byte, n)
		last := uint32(0)
		for i := range data {
			next := binary.LittleEndian.Uint32(offsets[(i+1)*4:])
			if next < last || int(next) > len(values) {
				return nil, errOffset(i, next, len(values))
			}
			if isValid(valid, i) {
				data[i] = values[last:next:next]
			}
			last = next
		}
		d.borrows = d.borrows || len(values) > 0
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, data)
		applyValidity(col, valid)
		return col, nil
	case vectorized.ColumnTypeTagValue:
		data := make([]*modelv1.TagValue, n)
		for i := range data {
			lo, hi, err := cell(i)
			if err != nil {
				return nil, err
			}
			if !isValid(valid, i) {
				continue
			}
			cellValue := &modelv1.TagValue{}
			if hi > lo {
				if unmarshalErr := proto.Unmarshal(values[lo:hi], cellValue); unmarshalErr != nil {
					return nil, fmt.Errorf("vectorized.frame: TagValue cell unmarshal row %d: %w", i, unmarshalErr)
				}
			}
			data[i] = cellValue
		}
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeTagValue, data)
		applyValidity(col, valid)
		return col, nil
	default:
		data := make([]*modelv1.FieldValue, n)
		for i := range data {
			lo, hi, err := cell(i)
			if err != nil {
				return nil, err
			}
			if !isValid(valid, i) {
				continue
			}
			cellValue := &modelv1.FieldValue{}
			if hi > lo {
				if unmarshalErr := proto.Unmarshal(values[lo:hi], cellValue); unmarshalErr != nil {
					return nil, fmt.Errorf("vectorized.frame: FieldValue cell unmarshal row %d: %w", i, unmarshalErr)
				}
			}
			data[i] = cellValue
		}
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeFieldValue, data)
		applyValidity(col, valid)
		return col, nil
	}
}

// decodeConst reads a constant column and expands it into the typed column of the
// column's rows.
func decodeConst(d *columnarDecoder, ctype vectorized.ColumnType, nullCount uint64) (vectorized.Column, error) {
	n := d.nrows
	if n > maxConstRows {
		return nil, fmt.Errorf("%w: constant column of %d rows exceeds the %d row limit", ErrTruncated, n, maxConstRows)
	}
	if nullCount > 1 {
		return nil, fmt.Errorf("%w: constant column NullCount=%d, want 0 or 1", ErrTruncated, nullCount)
	}
	if !vectorized.ConstSupported(ctype) {
		return nil, fmt.Errorf("%w: constant column of type %s", ErrUnsupportedColumnType, ctype.String())
	}
	// Charge the frame-wide expansion budget before allocating anything.
	d.constBytes += constExpandBytes(ctype, n, nullCount == 1)
	if d.constBytes > maxConstExpandBytes {
		return nil, fmt.Errorf("%w: constant columns expand to %d bytes, over the %d byte frame limit",
			ErrTruncated, d.constBytes, maxConstExpandBytes)
	}
	var valid []byte
	if nullCount == 1 {
		valid = make([]byte, (n+7)/8)
	}
	switch ctype { //nolint:exhaustive // ConstSupported admits only these four types
	case vectorized.ColumnTypeInt64:
		data := make([]int64, n)
		if nullCount == 0 {
			raw, err := d.take(8, "int64 constant")
			if err != nil {
				return nil, err
			}
			v := int64(binary.LittleEndian.Uint64(raw))
			for i := range data {
				data[i] = v
			}
		}
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeInt64, data)
		applyValidity(col, valid)
		return col, nil
	case vectorized.ColumnTypeFloat64:
		data := make([]float64, n)
		if nullCount == 0 {
			raw, err := d.take(8, "float64 constant")
			if err != nil {
				return nil, err
			}
			v := math.Float64frombits(binary.LittleEndian.Uint64(raw))
			for i := range data {
				data[i] = v
			}
		}
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeFloat64, data)
		applyValidity(col, valid)
		return col, nil
	}
	value, valueErr := d.takeConstBytes(nullCount == 0)
	if valueErr != nil {
		return nil, valueErr
	}
	if ctype == vectorized.ColumnTypeString {
		data := make([]string, n)
		if len(value) > 0 {
			d.borrows = true
			s := unsafe.String(unsafe.SliceData(value), len(value))
			for i := range data {
				data[i] = s
			}
		}
		col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeString, data)
		applyValidity(col, valid)
		return col, nil
	}
	data := make([][]byte, n)
	if nullCount == 0 {
		d.borrows = d.borrows || len(value) > 0
		for i := range data {
			data[i] = value[:len(value):len(value)]
		}
	}
	col := vectorized.NewColumnFromSlice(vectorized.ColumnTypeBytes, data)
	applyValidity(col, valid)
	return col, nil
}

// takeConstBytes reads the length-prefixed value of a string or bytes constant, or
// nothing when the constant is null.
func (d *columnarDecoder) takeConstBytes(present bool) ([]byte, error) {
	if !present {
		return nil, nil
	}
	if d.remaining() < 4 {
		return nil, fmt.Errorf("%w: constant length needs 4 bytes, have %d", ErrTruncated, d.remaining())
	}
	length := int(binary.LittleEndian.Uint32(d.buf[d.pos:]))
	d.pos += 4
	if length > d.remaining() {
		return nil, fmt.Errorf("%w: constant value needs %d bytes, have %d", ErrTruncated, length, d.remaining())
	}
	value := d.buf[d.pos : d.pos+length : d.pos+length]
	d.pos += length
	if padErr := d.skipPadding("constant value"); padErr != nil {
		return nil, padErr
	}
	return value, nil
}

func errOffset(row int, offset uint32, values int) error {
	return fmt.Errorf("%w: offset %d at row %d is out of order or past the %d value bytes", ErrTruncated, offset, row, values)
}
