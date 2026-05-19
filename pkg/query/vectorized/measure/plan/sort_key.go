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

package plan

import (
	"encoding/binary"
	"fmt"
	"math"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// encodeSortKey returns a lex-sortable []byte encoding of column[rowIdx]
// that mirrors the row-path comparable element bytes (pbv1.MarshalTagValue
// for TagValue passthrough), and uses monotonic bit-twiddles for the
// natively-typed columns so the heap merger can compare byte-wise and still
// get numeric order across negatives and floats.
//
// Encoding contracts:
//   - int64: 8-byte big-endian with the sign bit flipped (XOR high byte with
//     0x80). This maps signed int64 to unsigned big-endian so negative
//     values lex-sort below non-negative ones.
//   - float64: 8-byte big-endian IEEE 754 with the monotonic trick — for
//     non-negative values flip the sign bit, for negative values invert
//     every bit. NaN sorts last (consistent with the IEEE bit pattern),
//     -Inf < -finite < +/-0 (the two zeros compare equal) < +finite < +Inf.
//   - string: raw UTF-8 bytes.
//   - []byte: raw bytes.
//   - *modelv1.TagValue passthrough: pbv1.MarshalTagValue applied to the
//     cell. This is the same encoding the row path's newComparableElement
//     uses, so equal-sort-field windows align with the row baseline.
//
// Returns an error on unsupported column types (e.g. []int64, []string,
// FieldValue) — those should be rejected at plan-analysis time before the
// merger ever sees them. A null cell encodes as nil so it sorts before any
// non-null value in lex order.
func encodeSortKey(col vectorized.Column, rowIdx int) ([]byte, error) {
	if col.IsNull(rowIdx) {
		return nil, nil
	}
	switch c := col.(type) {
	case *vectorized.TypedColumn[int64]:
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(c.Data()[rowIdx]))
		out[0] ^= 0x80
		return out, nil
	case *vectorized.TypedColumn[float64]:
		v := c.Data()[rowIdx]
		// Normalize negative zero to positive zero so -0 and +0 encode to
		// identical sort keys (the row path treats them as equal too).
		if v == 0 {
			v = 0
		}
		bits := math.Float64bits(v)
		if bits&(1<<63) != 0 {
			bits = ^bits
		} else {
			bits ^= 1 << 63
		}
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, bits)
		return out, nil
	case *vectorized.TypedColumn[string]:
		s := c.Data()[rowIdx]
		return []byte(s), nil
	case *vectorized.TypedColumn[[]byte]:
		src := c.Data()[rowIdx]
		out := make([]byte, len(src))
		copy(out, src)
		return out, nil
	case *vectorized.TypedColumn[*modelv1.TagValue]:
		tv := c.Data()[rowIdx]
		if tv == nil {
			return nil, nil
		}
		return pbv1.MarshalTagValue(tv)
	}
	return nil, fmt.Errorf("encodeSortKey: unsupported column type %s", col.Type())
}
