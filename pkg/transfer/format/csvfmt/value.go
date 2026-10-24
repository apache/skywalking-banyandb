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

package csvfmt

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// NullCell is the sentinel cell of a null value.
const NullCell = `\N`

const (
	binaryPrefix  = `\B`
	intArraySep   = ";"
	backslashByte = '\\'
)

// EncodeString encodes a string cell. A value containing '\r' is downgraded to
// `\B`+base64 because encoding/csv rewrites "\r\n" to "\n" on read; otherwise a value
// starting with a backslash gets one extra leading backslash so that it never collides
// with the `\N` and `\B` sentinels.
func EncodeString(s string) string {
	if strings.IndexByte(s, '\r') >= 0 {
		return binaryPrefix + base64.StdEncoding.EncodeToString([]byte(s))
	}
	if s != "" && s[0] == backslashByte {
		return `\` + s
	}
	return s
}

// EncodeInt encodes an int cell in decimal.
func EncodeInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// EncodeFloat encodes a float cell with the shortest exact 'f' representation; NaN and
// infinities become "NaN", "+Inf" and "-Inf".
func EncodeFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// EncodeBinary encodes a binary cell with standard padded base64, matching protojson.
// An empty slice encodes to "" which stays distinct from NullCell.
func EncodeBinary(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// EncodeStringArray encodes a string array as a JSON array. A nil slice encodes as "[]".
// Invalid UTF-8 bytes are replaced with U+FFFD by encoding/json.
func EncodeStringArray(v []string) string {
	if v == nil {
		v = []string{}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Encoding a []string cannot fail.
		panic(fmt.Sprintf("encode string array: %v", err))
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

// EncodeIntArray encodes an int array as ';'-joined decimals; an empty array is "".
func EncodeIntArray(v []int64) string {
	if len(v) == 0 {
		return ""
	}
	b := make([]byte, 0, len(v)*8)
	for i, x := range v {
		if i > 0 {
			b = append(b, intArraySep...)
		}
		b = strconv.AppendInt(b, x, 10)
	}
	return string(b)
}

// elementIDDigits is the width of an encoded stream element id: 8 bytes in hex.
const elementIDDigits = 16

// EncodeElementID encodes a stream element id as the 16 lowercase hex digits of its
// big-endian bytes, the form a stream query returns as element_id.
func EncodeElementID(id uint64) string {
	return hex.EncodeToString(convert.Uint64ToBytes(id))
}

// DecodeElementID parses a cell written by EncodeElementID back into the element id.
func DecodeElementID(cell string) (uint64, error) {
	if len(cell) != elementIDDigits {
		return 0, fmt.Errorf("element id %q is not %d hex digits", cell, elementIDDigits)
	}
	id, err := strconv.ParseUint(cell, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("element id %q: %w", cell, err)
	}
	return id, nil
}

// EncodeTimestamp encodes a time column as int64 UnixNano decimal.
func EncodeTimestamp(unixNano int64) string {
	return strconv.FormatInt(unixNano, 10)
}

// EncodeTagValue encodes a tag value according to its oneof; nil and null become NullCell.
func EncodeTagValue(v *modelv1.TagValue) string {
	switch x := v.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return EncodeString(x.Str.GetValue())
	case *modelv1.TagValue_Int:
		return EncodeInt(x.Int.GetValue())
	case *modelv1.TagValue_StrArray:
		return EncodeStringArray(x.StrArray.GetValue())
	case *modelv1.TagValue_IntArray:
		return EncodeIntArray(x.IntArray.GetValue())
	case *modelv1.TagValue_BinaryData:
		return EncodeBinary(x.BinaryData)
	case *modelv1.TagValue_Timestamp:
		if x.Timestamp == nil {
			return NullCell
		}
		return EncodeTimestamp(x.Timestamp.AsTime().UnixNano())
	default:
		return NullCell
	}
}

// Timestamps are written as int64 nanoseconds since the epoch, which span roughly the years
// 1678 to 2262; whole seconds strictly inside that span always fit.
const (
	minTimestampSeconds = math.MinInt64/int64(time.Second) + 1
	maxTimestampSeconds = math.MaxInt64/int64(time.Second) - 1
)

// FitsColumn reports whether EncodeTagValue writes v losslessly into a column of type t: a
// null value fits every column, any other value only a column of its own type, and a
// timestamp only within the int64 nanosecond range.
func FitsColumn(v *modelv1.TagValue, t ValueType) bool {
	switch x := v.GetValue().(type) {
	case *modelv1.TagValue_Str:
		return t == ValueString
	case *modelv1.TagValue_Int:
		return t == ValueInt
	case *modelv1.TagValue_StrArray:
		return t == ValueStringArray
	case *modelv1.TagValue_IntArray:
		return t == ValueIntArray
	case *modelv1.TagValue_BinaryData:
		return t == ValueBinary
	case *modelv1.TagValue_Timestamp:
		if x.Timestamp == nil {
			return true
		}
		secs := x.Timestamp.GetSeconds()
		return t == ValueTimestamp && secs >= minTimestampSeconds && secs <= maxTimestampSeconds
	default:
		return true
	}
}

// EncodeFieldValue encodes a field value according to its oneof; nil and null become NullCell.
func EncodeFieldValue(v *modelv1.FieldValue) string {
	switch x := v.GetValue().(type) {
	case *modelv1.FieldValue_Str:
		return EncodeString(x.Str.GetValue())
	case *modelv1.FieldValue_Int:
		return EncodeInt(x.Int.GetValue())
	case *modelv1.FieldValue_Float:
		return EncodeFloat(x.Float.GetValue())
	case *modelv1.FieldValue_BinaryData:
		return EncodeBinary(x.BinaryData)
	default:
		return NullCell
	}
}

// DecodeCell is the inverse of the Encode functions. The value is a string, int64 (int and
// timestamp), float64, []byte, []string or []int64 depending on t; isNull reports NullCell.
func DecodeCell(cell string, t ValueType) (value any, isNull bool, err error) {
	if cell == NullCell {
		return nil, true, nil
	}
	switch t {
	case ValueString:
		s, err := decodeString(cell)
		return s, false, err
	case ValueInt, ValueTimestamp:
		v, err := strconv.ParseInt(cell, 10, 64)
		if err != nil {
			return nil, false, fmt.Errorf("decode %s cell: %w", t, err)
		}
		return v, false, nil
	case ValueFloat:
		v, err := strconv.ParseFloat(cell, 64)
		if err != nil {
			return nil, false, fmt.Errorf("decode double cell: %w", err)
		}
		return v, false, nil
	case ValueBinary:
		b, err := base64.StdEncoding.DecodeString(cell)
		if err != nil {
			return nil, false, fmt.Errorf("decode binary cell: %w", err)
		}
		return b, false, nil
	case ValueStringArray:
		var v []string
		if err := json.Unmarshal([]byte(cell), &v); err != nil {
			return nil, false, fmt.Errorf("decode string array cell: %w", err)
		}
		if v == nil {
			return nil, false, fmt.Errorf("decode string array cell: %q is not an array", cell)
		}
		return v, false, nil
	case ValueIntArray:
		if cell == "" {
			return []int64{}, false, nil
		}
		parts := strings.Split(cell, intArraySep)
		v := make([]int64, len(parts))
		for i, p := range parts {
			x, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				return nil, false, fmt.Errorf("decode int array cell element %d: %w", i, err)
			}
			v[i] = x
		}
		return v, false, nil
	default:
		return nil, false, fmt.Errorf("unknown value type %d", uint8(t))
	}
}

func decodeString(cell string) (string, error) {
	switch {
	case strings.HasPrefix(cell, `\\`):
		return cell[1:], nil
	case strings.HasPrefix(cell, binaryPrefix):
		b, err := base64.StdEncoding.DecodeString(cell[len(binaryPrefix):])
		if err != nil {
			return "", fmt.Errorf("decode \\B string cell: %w", err)
		}
		return string(b), nil
	default:
		return cell, nil
	}
}

// EscapePathComponent makes s safe as one path component: every byte outside
// [A-Za-z0-9._-] is written as %XX (upper-case hex). The names "." and ".." have their
// dots escaped too so the result can never traverse directories.
func EscapePathComponent(s string) string {
	if s == "." || s == ".." {
		return strings.Repeat("%2E", len(s))
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}
