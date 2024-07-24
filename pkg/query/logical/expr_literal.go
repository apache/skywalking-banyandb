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

package logical

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	_ LiteralExpr    = (*Int64Literal)(nil)
	_ ComparableExpr = (*Int64Literal)(nil)
)

// Int64Literal represents a wrapper for the int64 type.
type Int64Literal struct {
	int64
}

// NewInt64Literal creates a new instance of Int64Literal with the provided int64 value.
func NewInt64Literal(val int64) *Int64Literal {
	return &Int64Literal{
		int64: val,
	}
}

func (i *Int64Literal) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*Int64Literal); ok {
		return int(i.int64 - o.int64), true
	}
	return 0, false
}

func (i *Int64Literal) contains(other LiteralExpr) bool {
	if o, ok := other.(*Int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*Int64ArrLiteral); ok {
		if len(o.arr) == 1 && o.arr[0] == i.int64 {
			return true
		}
	}
	return false
}

func (i *Int64Literal) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*Int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*Int64ArrLiteral); ok {
		return slices.Contains(o.arr, i.int64)
	}
	return false
}

// Bytes converts the Int64Literal's int64 value to a slice of byte slices.
func (i *Int64Literal) Bytes() [][]byte {
	return [][]byte{convert.Int64ToBytes(i.int64)}
}

func (i *Int64Literal) equal(expr Expr) bool {
	if other, ok := expr.(*Int64Literal); ok {
		return other.int64 == i.int64
	}

	return false
}

func (i *Int64Literal) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT)
}

// String converts the Int64Literal's int64 value to its string representation.
func (i *Int64Literal) String() string {
	return strconv.FormatInt(i.int64, 10)
}

var (
	_ LiteralExpr    = (*Int64ArrLiteral)(nil)
	_ ComparableExpr = (*Int64ArrLiteral)(nil)
)

// Int64ArrLiteral represents a wrapper for a slice of int64 values.
type Int64ArrLiteral struct {
	arr []int64
}

// NewInt64ArrLiteral creates a new instance of Int64ArrLiteral with the provided slice of int64 values.
func NewInt64ArrLiteral(val []int64) *Int64ArrLiteral {
	return &Int64ArrLiteral{
		arr: val,
	}
}

func (i *Int64ArrLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*Int64ArrLiteral); ok {
		return 0, slices.Equal(i.arr, o.arr)
	}
	return 0, false
}

func (i *Int64ArrLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*Int64Literal); ok {
		return slices.Contains(i.arr, o.int64)
	}
	if o, ok := other.(*Int64ArrLiteral); ok {
		for _, v := range o.arr {
			if !slices.Contains(i.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

func (i *Int64ArrLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*Int64Literal); ok {
		if len(i.arr) == 1 && i.arr[0] == o.int64 {
			return true
		}
		return false
	}
	if o, ok := other.(*Int64ArrLiteral); ok {
		for _, v := range i.arr {
			if !slices.Contains(o.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

// Bytes converts the Int64ArrLiteral's slice of int64 values to a slice of byte slices.
func (i *Int64ArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(i.arr))
	for _, i := range i.arr {
		b = append(b, convert.Int64ToBytes(i))
	}
	return b
}

func (i *Int64ArrLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*Int64ArrLiteral); ok {
		return slices.Equal(other.arr, i.arr)
	}

	return false
}

func (i *Int64ArrLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT_ARRAY)
}

// String converts the Int64ArrLiteral's slice of int64 values to a string representation.
func (i *Int64ArrLiteral) String() string {
	return fmt.Sprintf("%v", i.arr)
}

var (
	_ LiteralExpr    = (*StrLiteral)(nil)
	_ ComparableExpr = (*StrLiteral)(nil)
)

// StrLiteral represents a wrapper for the string type.
type StrLiteral struct {
	string
}

// NewStrLiteral creates a new instance of StrLiteral with the provided string.
func NewStrLiteral(val string) *StrLiteral {
	return &StrLiteral{
		string: val,
	}
}

func (s *StrLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*StrLiteral); ok {
		return strings.Compare(s.string, o.string), true
	}
	return 0, false
}

func (s *StrLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*StrLiteral); ok {
		return s == o
	}
	if o, ok := other.(*StrArrLiteral); ok {
		if len(o.arr) == 1 && o.arr[0] == s.string {
			return true
		}
	}
	return false
}

func (s *StrLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*StrLiteral); ok {
		return s == o
	}
	if o, ok := other.(*StrArrLiteral); ok {
		return slices.Contains(o.arr, s.string)
	}
	return false
}

// Bytes converts the StrLiteral's string to a slice of byte slices.
func (s *StrLiteral) Bytes() [][]byte {
	return [][]byte{[]byte(s.string)}
}

func (s *StrLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*StrLiteral); ok {
		return other.string == s.string
	}

	return false
}

// Str creates a new StrLiteral instance containing the given string.
func Str(str string) LiteralExpr {
	return &StrLiteral{str}
}

func (s *StrLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING)
}

// String returns the string representation of the StrLiteral.
func (s *StrLiteral) String() string {
	return s.string
}

var (
	_ LiteralExpr    = (*StrArrLiteral)(nil)
	_ ComparableExpr = (*StrArrLiteral)(nil)
)

// StrArrLiteral represents a wrapper for a slice of strings.
type StrArrLiteral struct {
	arr []string
}

// NewStrArrLiteral creates a new instance of StrArrLiteral with the provided slice of strings.
func NewStrArrLiteral(val []string) *StrArrLiteral {
	return &StrArrLiteral{
		arr: val,
	}
}

func (s *StrArrLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*StrArrLiteral); ok {
		return 0, StringSlicesEqual(s.arr, o.arr)
	}
	return 0, false
}

func (s *StrArrLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*StrLiteral); ok {
		return slices.Contains(s.arr, o.string)
	}
	if o, ok := other.(*StrArrLiteral); ok {
		for _, v := range o.arr {
			if !slices.Contains(s.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

func (s *StrArrLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*StrLiteral); ok {
		if len(s.arr) == 1 && s.arr[0] == o.string {
			return true
		}
		return false
	}
	if o, ok := other.(*StrArrLiteral); ok {
		for _, v := range s.arr {
			if !slices.Contains(o.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

// Bytes converts the StrArrLiteral's slice of strings to a slice of byte slices.
func (s *StrArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(s.arr))
	for _, str := range s.arr {
		b = append(b, []byte(str))
	}
	return b
}

func (s *StrArrLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*StrArrLiteral); ok {
		return slices.Equal(other.arr, s.arr)
	}

	return false
}

func (s *StrArrLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING_ARRAY)
}

// String converts the StrArrLiteral's slice of strings to a string representation.
func (s *StrArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}

// BytesLiteral represents a wrapper for a slice of bytes.
type BytesLiteral struct {
	bb []byte
}

// NewBytesLiteral creates a new instance of BytesLiteral with the provided slice of bytes.
func NewBytesLiteral(bb []byte) *BytesLiteral {
	return &BytesLiteral{bb: bb}
}

// Bytes returns a 2D slice of bytes where the inner slice contains the byte slice stored in the BytesLiteral.
func (b *BytesLiteral) Bytes() [][]byte {
	return [][]byte{b.bb}
}

func (b *BytesLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*BytesLiteral); ok {
		return bytes.Equal(other.bb, b.bb)
	}

	return false
}

func (b *BytesLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_DATA_BINARY)
}

// String converts the BytesLiteral's slice of bytes to a string representation.
func (b *BytesLiteral) String() string {
	return hex.EncodeToString(b.bb)
}

var (
	_               LiteralExpr    = (*NullLiteral)(nil)
	_               ComparableExpr = (*NullLiteral)(nil)
	nullLiteralExpr                = &NullLiteral{}
)

// NullLiteral represents an empty struct.
type NullLiteral struct{}

// NewNullLiteral returns a new instance of NullLiteral.
func NewNullLiteral() *NullLiteral {
	return nullLiteralExpr
}

func (s NullLiteral) compare(_ LiteralExpr) (int, bool) {
	return 0, false
}

func (s NullLiteral) belongTo(_ LiteralExpr) bool {
	return false
}

func (s NullLiteral) contains(_ LiteralExpr) bool {
	return false
}

// Bytes returns nil for the NullLiteral.
func (s NullLiteral) Bytes() [][]byte {
	return nil
}

func (s NullLiteral) equal(_ Expr) bool {
	return false
}

func (s NullLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_UNSPECIFIED)
}

// String returns the string "null" for the NullLiteral.
func (s NullLiteral) String() string {
	return "null"
}
