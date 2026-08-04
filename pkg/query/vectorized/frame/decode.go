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

// Decode is the inverse of Encode: it parses a raw frame body produced by the
// same codec and returns an in-memory RecordBatch with the same schema, columns
// and (active) rows. The returned batch's Selection is nil and its Len equals
// the encoded NumRows.
//
// Decode is the load-bearing fail-loud guard on the consumer side:
// ValidateHeader rejects bad magic / wire version loudly before any column is
// touched, and the per-column readers reject truncation, unknown role / type
// bytes, and varint underflows the same way. There is no lossy fallback.
//
// A nil/empty body is rejected here with ErrTruncated; the codec-layer carve-out
// for legitimately-empty distributed results lives one level up.
func (c Codec) Decode(b []byte) (*vectorized.RecordBatch, error) {
	header, offset, headerErr := c.ValidateHeader(b)
	if headerErr != nil {
		return nil, headerErr
	}
	defs := make([]vectorized.ColumnDef, 0, header.NumCols)
	cols := make([]vectorized.Column, 0, header.NumCols)
	for colIdx := range header.NumCols {
		def, col, consumed, colErr := c.decodeColumn(b[offset:], header.NumRows)
		if colErr != nil {
			return nil, fmt.Errorf("column %d: %w", colIdx, colErr)
		}
		defs = append(defs, def)
		cols = append(cols, col)
		offset += consumed
	}
	if offset != len(b) {
		return nil, fmt.Errorf("%w: %d trailing bytes after %d columns", ErrTruncated, len(b)-offset, header.NumCols)
	}
	return &vectorized.RecordBatch{
		Schema:  vectorized.NewBatchSchema(defs),
		Columns: cols,
		Len:     int(header.NumRows),
	}, nil
}

// decodeColumn parses one column block (header + body) from b. It returns the
// column definition, the materialized column with nrows rows already appended
// (validity bits respected), and the number of bytes consumed.
func (c Codec) decodeColumn(b []byte, nrows uint64) (vectorized.ColumnDef, vectorized.Column, int, error) {
	if len(b) < 2 {
		return vectorized.ColumnDef{}, nil, 0, fmt.Errorf("%w: header needs 2 bytes for role+type, have %d", ErrTruncated, len(b))
	}
	roleByte := b[0]
	typeByte := b[1]
	offset := 2
	role, roleErr := c.WireToRole(roleByte)
	if roleErr != nil {
		return vectorized.ColumnDef{}, nil, 0, roleErr
	}
	ctype, typeErr := c.WireToType(typeByte)
	if typeErr != nil {
		return vectorized.ColumnDef{}, nil, 0, typeErr
	}
	nameLen, nameLenSize := binary.Uvarint(b[offset:])
	if nameLenSize <= 0 {
		return vectorized.ColumnDef{}, nil, 0, fmt.Errorf("%w: malformed NameLen varint", ErrTruncated)
	}
	offset += nameLenSize
	if uint64(len(b)-offset) < nameLen {
		return vectorized.ColumnDef{}, nil, 0, fmt.Errorf("%w: NameLen=%d exceeds remaining %d bytes", ErrTruncated, nameLen, len(b)-offset)
	}
	name := string(b[offset : offset+int(nameLen)])
	offset += int(nameLen)
	tagFamilyLen, tagFamilyLenSize := binary.Uvarint(b[offset:])
	if tagFamilyLenSize <= 0 {
		return vectorized.ColumnDef{}, nil, 0, fmt.Errorf("%w: malformed TagFamilyLen varint", ErrTruncated)
	}
	offset += tagFamilyLenSize
	if uint64(len(b)-offset) < tagFamilyLen {
		return vectorized.ColumnDef{}, nil, 0, fmt.Errorf("%w: TagFamilyLen=%d exceeds remaining %d bytes", ErrTruncated, tagFamilyLen, len(b)-offset)
	}
	tagFamily := string(b[offset : offset+int(tagFamilyLen)])
	offset += int(tagFamilyLen)
	def := vectorized.ColumnDef{Role: role, Type: ctype, Name: name, TagFamily: tagFamily}

	validity, validityConsumed, validityErr := readValidityBitmap(b[offset:], nrows)
	if validityErr != nil {
		return vectorized.ColumnDef{}, nil, 0, validityErr
	}
	offset += validityConsumed

	col, dataConsumed, dataErr := readColumnData(b[offset:], ctype, int(nrows), validity)
	if dataErr != nil {
		return vectorized.ColumnDef{}, nil, 0, dataErr
	}
	offset += dataConsumed
	return def, col, offset, nil
}

// readValidityBitmap parses ⌈nrows/8⌉ bytes of validity bits. Bit set ⇒ null.
// Returns one bool per row (true=null) plus the byte count consumed.
func readValidityBitmap(b []byte, nrows uint64) ([]bool, int, error) {
	if nrows == 0 {
		return nil, 0, nil
	}
	nbytes := int((nrows + 7) / 8)
	if len(b) < nbytes {
		return nil, 0, fmt.Errorf("%w: validity bitmap needs %d bytes, have %d", ErrTruncated, nbytes, len(b))
	}
	nulls := make([]bool, nrows)
	for i := range nrows {
		if b[i/8]&(1<<uint(i%8)) != 0 {
			nulls[i] = true
		}
	}
	return nulls, nbytes, nil
}

// readColumnData materializes one column of nrows rows from b given the validity
// vector. Fixed-width types read N × 8 bytes regardless of nullness;
// variable-width types read uvarint(len) + len bytes per row.
// nolint:gocyclo // switch-dispatch over ColumnType variants is intentionally exhaustive; splitting per-case helpers would obscure the wire-format mapping
func readColumnData(b []byte, t vectorized.ColumnType, nrows int, nulls []bool) (vectorized.Column, int, error) {
	switch t { //nolint:exhaustive // array column types never appear on the wire; the codec's type mapping rejects them at encode time
	case vectorized.ColumnTypeInt64:
		if len(b) < nrows*8 {
			return nil, 0, fmt.Errorf("%w: int64 column needs %d bytes, have %d", ErrTruncated, nrows*8, len(b))
		}
		col := vectorized.NewInt64Column(nrows)
		for i := range nrows {
			v := int64(binary.LittleEndian.Uint64(b[i*8 : i*8+8]))
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
				continue
			}
			col.Append(v)
		}
		return col, nrows * 8, nil
	case vectorized.ColumnTypeFloat64:
		if len(b) < nrows*8 {
			return nil, 0, fmt.Errorf("%w: float64 column needs %d bytes, have %d", ErrTruncated, nrows*8, len(b))
		}
		col := vectorized.NewFloat64Column(nrows)
		for i := range nrows {
			v := math.Float64frombits(binary.LittleEndian.Uint64(b[i*8 : i*8+8]))
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
				continue
			}
			col.Append(v)
		}
		return col, nrows * 8, nil
	case vectorized.ColumnTypeString:
		col := vectorized.NewStringColumn(nrows)
		offset := 0
		for i := range nrows {
			vlen, vlenSize := binary.Uvarint(b[offset:])
			if vlenSize <= 0 {
				return nil, 0, fmt.Errorf("%w: malformed string length varint at row %d", ErrTruncated, i)
			}
			offset += vlenSize
			if uint64(len(b)-offset) < vlen {
				return nil, 0, fmt.Errorf("%w: string row %d needs %d bytes, have %d", ErrTruncated, i, vlen, len(b)-offset)
			}
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
			} else {
				col.Append(string(b[offset : offset+int(vlen)]))
			}
			offset += int(vlen)
		}
		return col, offset, nil
	case vectorized.ColumnTypeBytes:
		col := vectorized.NewBytesColumn(nrows)
		offset := 0
		for i := range nrows {
			vlen, vlenSize := binary.Uvarint(b[offset:])
			if vlenSize <= 0 {
				return nil, 0, fmt.Errorf("%w: malformed bytes length varint at row %d", ErrTruncated, i)
			}
			offset += vlenSize
			if uint64(len(b)-offset) < vlen {
				return nil, 0, fmt.Errorf("%w: bytes row %d needs %d bytes, have %d", ErrTruncated, i, vlen, len(b)-offset)
			}
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
			} else {
				row := make([]byte, vlen)
				copy(row, b[offset:offset+int(vlen)])
				col.Append(row)
			}
			offset += int(vlen)
		}
		return col, offset, nil
	case vectorized.ColumnTypeTagValue:
		col := vectorized.NewTagValueColumn(nrows)
		offset := 0
		for i := range nrows {
			vlen, vlenSize := binary.Uvarint(b[offset:])
			if vlenSize <= 0 {
				return nil, 0, fmt.Errorf("%w: malformed TagValue length varint at row %d", ErrTruncated, i)
			}
			offset += vlenSize
			if uint64(len(b)-offset) < vlen {
				return nil, 0, fmt.Errorf("%w: TagValue row %d needs %d bytes, have %d", ErrTruncated, i, vlen, len(b)-offset)
			}
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
			} else {
				tv := &modelv1.TagValue{}
				if vlen > 0 {
					if unmarshalErr := proto.Unmarshal(b[offset:offset+int(vlen)], tv); unmarshalErr != nil {
						return nil, 0, fmt.Errorf("vectorized.frame: TagValue cell unmarshal row %d: %w", i, unmarshalErr)
					}
				}
				col.Append(tv)
			}
			offset += int(vlen)
		}
		return col, offset, nil
	case vectorized.ColumnTypeFieldValue:
		col := vectorized.NewFieldValueColumn(nrows)
		offset := 0
		for i := range nrows {
			vlen, vlenSize := binary.Uvarint(b[offset:])
			if vlenSize <= 0 {
				return nil, 0, fmt.Errorf("%w: malformed FieldValue length varint at row %d", ErrTruncated, i)
			}
			offset += vlenSize
			if uint64(len(b)-offset) < vlen {
				return nil, 0, fmt.Errorf("%w: FieldValue row %d needs %d bytes, have %d", ErrTruncated, i, vlen, len(b)-offset)
			}
			if i < len(nulls) && nulls[i] {
				col.AppendNull()
			} else {
				fv := &modelv1.FieldValue{}
				if vlen > 0 {
					if unmarshalErr := proto.Unmarshal(b[offset:offset+int(vlen)], fv); unmarshalErr != nil {
						return nil, 0, fmt.Errorf("vectorized.frame: FieldValue cell unmarshal row %d: %w", i, unmarshalErr)
					}
				}
				col.Append(fv)
			}
			offset += int(vlen)
		}
		return col, offset, nil
	}
	return nil, 0, fmt.Errorf("%w: %s", ErrUnsupportedColumnType, t.String())
}
