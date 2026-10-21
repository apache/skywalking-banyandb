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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// encodedColumn is a validated view of one column's wire buffers.
type encodedColumn struct {
	validity, offsets, values []byte
	def                       vectorized.ColumnDef
	rows, nulls               int
	constant                  bool
	role, typ                 uint8
}

// ConcatColumnar joins frame rows into one columnar frame without rebuilding
// typed columns or decoding protobuf cells. Inputs stay borrowed until return.
// reserve is called with the exact output capacity before allocating it. The caller
// accounts separately for O(frames * columns) buffer descriptors and schema text.
func (c Codec) ConcatColumnar(frames [][]byte, reserve func(int) error) ([]byte, error) {
	if len(frames) == 0 {
		return nil, errNilBatch
	}
	parts, rows, err := c.columnarConcatParts(frames)
	if err != nil {
		return nil, err
	}
	total := MagicLen + 1 + uvarintLen(uint64(rows)) + uvarintLen(uint64(len(parts[0])))
	for col, base := range parts[0] {
		nulls, values := encodedColumnTotals(parts, col)
		total += 3 + uvarintLen(uint64(len(base.def.Name))) + len(base.def.Name) +
			uvarintLen(uint64(len(base.def.TagFamily))) + len(base.def.TagFamily) + uvarintLen(uint64(nulls))
		total = pad8(total)
		if nulls > 0 {
			total += pad8((rows + 7) / 8)
		}
		if encodedNumeric(base.def.Type) {
			total += rows * 8
		} else {
			total += pad8((rows+1)*4) + pad8(values)
		}
		if total > math.MaxInt32 {
			return nil, fmt.Errorf("vectorized.frame: columnar frame exceeds %d bytes", math.MaxInt32)
		}
	}
	if reserve != nil {
		if err := reserve(total); err != nil {
			return nil, err
		}
	}
	out := make([]byte, 0, total)
	out = append(out, c.Magic[:]...)
	out = append(out, c.ColumnarWireVersion)
	out = binary.AppendUvarint(out, uint64(rows))
	out = binary.AppendUvarint(out, uint64(len(parts[0])))
	for col, base := range parts[0] {
		nulls, _ := encodedColumnTotals(parts, col)
		out = append(out, base.role, base.typ, columnEncFlat)
		out = binary.AppendUvarint(out, uint64(len(base.def.Name)))
		out = append(out, base.def.Name...)
		out = binary.AppendUvarint(out, uint64(len(base.def.TagFamily)))
		out = append(out, base.def.TagFamily...)
		out = binary.AppendUvarint(out, uint64(nulls))
		out = padTo8(out)
		out = appendEncodedColumn(out, parts, col, rows, nulls)
	}
	return out, nil
}

func appendEncodedColumn(out []byte, parts [][]encodedColumn, col, rows, nulls int) []byte {
	base := parts[0][col]
	if nulls > 0 {
		start := len(out)
		out = append(out, make([]byte, pad8((rows+7)/8))...)
		row := 0
		for _, frameParts := range parts {
			part := frameParts[col]
			for i := range part.rows {
				if part.valid(i) {
					out[start+row/8] |= 1 << uint(row%8)
				}
				row++
			}
		}
	}
	if encodedNumeric(base.def.Type) {
		for _, frameParts := range parts {
			part := frameParts[col]
			if !part.constant {
				out = append(out, part.values...)
				continue
			}
			for range part.rows {
				if part.nulls > 0 {
					out = binary.LittleEndian.AppendUint64(out, 0)
				} else {
					out = append(out, part.values...)
				}
			}
		}
		return out
	}
	offsetsStart := len(out)
	out = append(out, make([]byte, pad8((rows+1)*4))...)
	valuesStart := len(out)
	row := 0
	for _, frameParts := range parts {
		part := frameParts[col]
		if part.constant {
			for range part.rows {
				out = append(out, part.values...)
				row++
				binary.LittleEndian.PutUint32(out[offsetsStart+row*4:], uint32(len(out)-valuesStart))
			}
			continue
		}
		baseOffset := len(out) - valuesStart
		out = append(out, part.values...)
		for i := range part.rows {
			row++
			offset := binary.LittleEndian.Uint32(part.offsets[(i+1)*4:])
			binary.LittleEndian.PutUint32(out[offsetsStart+row*4:], uint32(baseOffset)+offset)
		}
	}
	out = padTo8(out)
	return out
}

func (c Codec) columnarConcatParts(frames [][]byte) ([][]encodedColumn, int, error) {
	parts := make([][]encodedColumn, len(frames))
	rows := 0
	for idx, body := range frames {
		if len(body) > math.MaxInt32 {
			return nil, 0, fmt.Errorf("vectorized.frame: frame %d exceeds %d bytes", idx, math.MaxInt32)
		}
		header, offset, err := c.validateHeader(body, c.ColumnarWireVersion, false)
		if err != nil {
			return nil, 0, fmt.Errorf("frame %d: %w", idx, err)
		}
		d := columnarDecoder{buf: body, pos: offset, nrows: int(header.NumRows)}
		parts[idx] = make([]encodedColumn, int(header.NumCols))
		for col := range parts[idx] {
			part, partErr := c.readEncodedColumn(&d)
			if partErr != nil {
				return nil, 0, fmt.Errorf("frame %d column %d: %w", idx, col, partErr)
			}
			if idx > 0 && (len(parts[idx]) != len(parts[0]) || part.def != parts[0][col].def) {
				return nil, 0, fmt.Errorf("vectorized.frame: frame %d column %d schema mismatch", idx, col)
			}
			parts[idx][col] = part
		}
		if idx > 0 && len(parts[idx]) != len(parts[0]) {
			return nil, 0, fmt.Errorf("vectorized.frame: frame %d schema mismatch", idx)
		}
		if d.pos != len(body) {
			return nil, 0, fmt.Errorf("%w: trailing frame bytes", ErrTruncated)
		}
		if d.nrows > math.MaxInt32-rows {
			return nil, 0, fmt.Errorf("vectorized.frame: concatenated row count exceeds %d", math.MaxInt32)
		}
		rows += d.nrows
	}
	return parts, rows, nil
}

func encodedNumeric(ctype vectorized.ColumnType) bool {
	return ctype == vectorized.ColumnTypeInt64 || ctype == vectorized.ColumnTypeFloat64
}

func encodedColumnTotals(parts [][]encodedColumn, col int) (nulls, values int) {
	for _, frameParts := range parts {
		part := frameParts[col]
		nulls += part.nulls
		if part.constant {
			values += len(part.values) * part.rows
		} else {
			values += len(part.values)
		}
	}
	return nulls, values
}

func (p encodedColumn) valid(row int) bool {
	if p.constant {
		return p.nulls == 0
	}
	return len(p.validity) == 0 || p.validity[row/8]&(1<<uint(row%8)) != 0
}

func (c Codec) readEncodedColumn(d *columnarDecoder) (encodedColumn, error) {
	var p encodedColumn
	if d.remaining() < 3 {
		return p, ErrTruncated
	}
	p.role, p.typ = d.buf[d.pos], d.buf[d.pos+1]
	enc := d.buf[d.pos+2]
	d.pos += 3
	role, err := c.WireToRole(p.role)
	if err != nil {
		return p, err
	}
	ctype, err := c.WireToType(p.typ)
	if err != nil {
		return p, err
	}
	switch ctype {
	case vectorized.ColumnTypeInt64, vectorized.ColumnTypeFloat64, vectorized.ColumnTypeString, vectorized.ColumnTypeBytes,
		vectorized.ColumnTypeTagValue, vectorized.ColumnTypeFieldValue:
	default:
		return p, ErrUnsupportedColumnType
	}
	name, err := d.borrowedText("Name")
	if err != nil {
		return p, err
	}
	family, err := d.borrowedText("TagFamily")
	if err != nil {
		return p, err
	}
	nulls, err := d.uvarint("NullCount")
	if err != nil {
		return p, err
	}
	if err = d.skipPadding("column header"); err != nil {
		return p, err
	}
	p.def = vectorized.ColumnDef{Role: role, Type: ctype, Name: name, TagFamily: family}
	p.rows = d.nrows
	switch enc {
	case columnEncConst:
		if !vectorized.ConstSupported(ctype) || nulls > 1 || d.nrows > maxConstRows {
			return p, fmt.Errorf("%w: invalid constant column", ErrTruncated)
		}
		p.constant = true
		if nulls > 0 {
			p.nulls = p.rows
			return p, nil
		}
		size := 8
		if !encodedNumeric(ctype) {
			if d.remaining() < 4 {
				return p, ErrTruncated
			}
			size = int(binary.LittleEndian.Uint32(d.buf[d.pos:]))
			d.pos += 4
		}
		p.values, err = d.take(size, "constant value")
		return p, err
	case columnEncFlat:
		if nulls > uint64(d.nrows) {
			return p, fmt.Errorf("%w: invalid null count", ErrTruncated)
		}
		p.nulls = int(nulls)
		if nulls > 0 {
			p.validity, err = d.take((d.nrows+7)/8, "validity")
			if err != nil {
				return p, err
			}
		}
		if encodedNumeric(ctype) {
			p.values, err = d.take(d.nrows*8, "numeric data")
			return p, err
		}
		p.offsets, err = d.take((d.nrows+1)*4, "offsets")
		if err != nil {
			return p, err
		}
		previous := uint32(0)
		for row := 0; row <= d.nrows; row++ {
			offset := binary.LittleEndian.Uint32(p.offsets[row*4:])
			if offset < previous || (row == 0 && offset != 0) {
				return p, fmt.Errorf("%w: invalid offsets", ErrTruncated)
			}
			previous = offset
		}
		p.values, err = d.take(int(previous), "values")
		return p, err
	default:
		return p, fmt.Errorf("%w: unknown column encoding %d", ErrTruncated, enc)
	}
}

// borrowedText is used only while a validated input frame stays retained.
func (d *columnarDecoder) borrowedText(what string) (string, error) {
	length, err := d.uvarint(what + "Len")
	if err != nil {
		return "", err
	}
	if length > uint64(d.remaining()) {
		return "", fmt.Errorf("%w: %s exceeds remaining frame", ErrTruncated, what)
	}
	raw := d.buf[d.pos : d.pos+int(length)]
	d.pos += int(length)
	return unsafe.String(unsafe.SliceData(raw), len(raw)), nil
}
