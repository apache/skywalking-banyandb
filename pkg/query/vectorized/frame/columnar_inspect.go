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
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// ColumnarLayout describes validated buffers without allocating decoded columns.
type ColumnarLayout struct {
	Header               Header
	ConstantPayloadBytes uint64
	SchemaBytes          uint64
	Scalar               bool
}

// InspectColumnar validates a frame and reports whether every cell uses a native
// scalar encoding. ConstantPayloadBytes bounds per-row materialization of shared
// string/bytes constants; SchemaBytes counts column and family name bytes.
func (c Codec) InspectColumnar(body []byte) (ColumnarLayout, error) {
	header, offset, err := c.validateHeader(body, c.ColumnarWireVersion, false)
	if err != nil {
		return ColumnarLayout{}, err
	}
	layout := ColumnarLayout{Header: header, Scalar: true}
	decoder := columnarDecoder{buf: body, pos: offset, nrows: int(header.NumRows)}
	constBytes := 0
	for col := range header.NumCols {
		part, partErr := c.readEncodedColumn(&decoder)
		if partErr != nil {
			return ColumnarLayout{}, fmt.Errorf("column %d: %w", col, partErr)
		}
		layout.SchemaBytes += uint64(len(part.def.Name)) + uint64(len(part.def.TagFamily))
		if part.def.Type == vectorized.ColumnTypeTagValue || part.def.Type == vectorized.ColumnTypeFieldValue {
			layout.Scalar = false
		}
		if !part.constant {
			continue
		}
		constBytes += constExpandBytes(part.def.Type, part.rows, part.nulls != 0)
		if constBytes > maxConstExpandBytes {
			return ColumnarLayout{}, fmt.Errorf("%w: constant expansion exceeds frame limit", ErrTruncated)
		}
		if part.def.Type != vectorized.ColumnTypeString && part.def.Type != vectorized.ColumnTypeBytes {
			continue
		}
		amount := uint64(len(part.values)) * header.NumRows
		if amount > math.MaxInt64-layout.ConstantPayloadBytes {
			return ColumnarLayout{}, fmt.Errorf("%w: constant payload expansion overflows", ErrTruncated)
		}
		layout.ConstantPayloadBytes += amount
	}
	if decoder.pos != len(body) {
		return ColumnarLayout{}, fmt.Errorf("%w: trailing frame bytes", ErrTruncated)
	}
	return layout, nil
}

// SameColumnarSchema compares the complete column layouts of two validated frames
// without constructing schemas or decoded columns. Both inputs stay borrowed.
func (c Codec) SameColumnarSchema(left, right []byte) (bool, error) {
	lh, lo, err := c.validateHeader(left, c.ColumnarWireVersion, false)
	if err != nil {
		return false, err
	}
	rh, ro, err := c.validateHeader(right, c.ColumnarWireVersion, false)
	if err != nil {
		return false, err
	}
	if lh.NumCols != rh.NumCols {
		return false, nil
	}
	ld := columnarDecoder{buf: left, pos: lo, nrows: int(lh.NumRows)}
	rd := columnarDecoder{buf: right, pos: ro, nrows: int(rh.NumRows)}
	for range lh.NumCols {
		lp, leftErr := c.readEncodedColumn(&ld)
		if leftErr != nil {
			return false, leftErr
		}
		rp, rightErr := c.readEncodedColumn(&rd)
		if rightErr != nil {
			return false, rightErr
		}
		if lp.def != rp.def {
			return false, nil
		}
	}
	return true, nil
}
