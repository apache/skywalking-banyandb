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

type Int64Literal struct {
	int64
}

// NewInt64Literal creates a new instance of int64Literal with the provided int64 value.
func NewInt64Literal(val int64) *Int64Literal {
	return &Int64Literal{
		int64: val,
	}
}

func (i *Int64Literal) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*Int64Literal); ok {
		return int(i.int64 - o.int64), true
	}
	return 0, false
}

func (i *Int64Literal) Contains(other LiteralExpr) bool {
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

func (i *Int64Literal) BelongTo(other LiteralExpr) bool {
	if o, ok := other.(*Int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*Int64ArrLiteral); ok {
		return slices.Contains(o.arr, i.int64)
	}
	return false
}

func (i *Int64Literal) Bytes() [][]byte {
	return [][]byte{convert.Int64ToBytes(i.int64)}
}

func (i *Int64Literal) Equal(expr Expr) bool {
	if other, ok := expr.(*Int64Literal); ok {
		return other.int64 == i.int64
	}

	return false
}

func (i *Int64Literal) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT)
}

func (i *Int64Literal) String() string {
	return strconv.FormatInt(i.int64, 10)
}

var (
	_ LiteralExpr    = (*Int64ArrLiteral)(nil)
	_ ComparableExpr = (*Int64ArrLiteral)(nil)
)

type Int64ArrLiteral struct {
	arr []int64
}

// NewInt64ArrLiteral creates a new instance of int64ArrLiteral with the provided slice of int64 values.
func NewInt64ArrLiteral(val []int64) *Int64ArrLiteral {
	return &Int64ArrLiteral{
		arr: val,
	}
}

func (i *Int64ArrLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*Int64ArrLiteral); ok {
		return 0, slices.Equal(i.arr, o.arr)
	}
	return 0, false
}

func (i *Int64ArrLiteral) Contains(other LiteralExpr) bool {
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

func (i *Int64ArrLiteral) BelongTo(other LiteralExpr) bool {
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

func (i *Int64ArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(i.arr))
	for _, i := range i.arr {
		b = append(b, convert.Int64ToBytes(i))
	}
	return b
}

func (i *Int64ArrLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*Int64ArrLiteral); ok {
		return slices.Equal(other.arr, i.arr)
	}

	return false
}

func (i *Int64ArrLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT_ARRAY)
}

func (i *Int64ArrLiteral) String() string {
	return fmt.Sprintf("%v", i.arr)
}

var (
	_ LiteralExpr    = (*StrLiteral)(nil)
	_ ComparableExpr = (*StrLiteral)(nil)
)

type StrLiteral struct {
	string
}

// NewStrLiteral creates a new instance of strLiteral with the provided string value.
func NewStrLiteral(val string) *StrLiteral {
	return &StrLiteral{
		string: val,
	}
}

func (s *StrLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*StrLiteral); ok {
		return strings.Compare(s.string, o.string), true
	}
	return 0, false
}

func (s *StrLiteral) Contains(other LiteralExpr) bool {
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

func (s *StrLiteral) BelongTo(other LiteralExpr) bool {
	if o, ok := other.(*StrLiteral); ok {
		return s == o
	}
	if o, ok := other.(*StrArrLiteral); ok {
		return slices.Contains(o.arr, s.string)
	}
	return false
}

func (s *StrLiteral) Bytes() [][]byte {
	return [][]byte{[]byte(s.string)}
}

func (s *StrLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*StrLiteral); ok {
		return other.string == s.string
	}

	return false
}

func Str(str string) LiteralExpr {
	return &StrLiteral{str}
}

func (s *StrLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING)
}

func (s *StrLiteral) String() string {
	return s.string
}

var (
	_ LiteralExpr    = (*StrArrLiteral)(nil)
	_ ComparableExpr = (*StrArrLiteral)(nil)
)

type StrArrLiteral struct {
	arr []string
}

// NewStrArrLiteral creates a new instance of strArrLiteral with the provided slice of string values.
func NewStrArrLiteral(val []string) *StrArrLiteral {
	return &StrArrLiteral{
		arr: val,
	}
}

func (s *StrArrLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*StrArrLiteral); ok {
		return 0, StringSlicesEqual(s.arr, o.arr)
	}
	return 0, false
}

func (s *StrArrLiteral) Contains(other LiteralExpr) bool {
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

func (s *StrArrLiteral) BelongTo(other LiteralExpr) bool {
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

func (s *StrArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(s.arr))
	for _, str := range s.arr {
		b = append(b, []byte(str))
	}
	return b
}

func (s *StrArrLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*StrArrLiteral); ok {
		return slices.Equal(other.arr, s.arr)
	}

	return false
}

func (s *StrArrLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING_ARRAY)
}

func (s *StrArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}

type BytesLiteral struct {
	bb []byte
}

// NewBytesLiteral creates a new instance of BytesLiteral with the provided slice of bytes.
func NewBytesLiteral(bb []byte) *BytesLiteral {
	return &BytesLiteral{bb: bb}
}

func (b *BytesLiteral) Bytes() [][]byte {
	return [][]byte{b.bb}
}

func (b *BytesLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*BytesLiteral); ok {
		return bytes.Equal(other.bb, b.bb)
	}

	return false
}

func (b *BytesLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_DATA_BINARY)
}

func (b *BytesLiteral) String() string {
	return hex.EncodeToString(b.bb)
}

var (
	_               LiteralExpr    = (*NullLiteral)(nil)
	_               ComparableExpr = (*NullLiteral)(nil)
	nullLiteralExpr                = &NullLiteral{}
)

type NullLiteral struct{}

// NewNullLiteral returns a new instance of nullLiteral.
func NewNullLiteral() *NullLiteral {
	return nullLiteralExpr
}

func (s NullLiteral) Compare(_ LiteralExpr) (int, bool) {
	return 0, false
}

func (s NullLiteral) BelongTo(_ LiteralExpr) bool {
	return false
}

func (s NullLiteral) Contains(_ LiteralExpr) bool {
	return false
}

func (s NullLiteral) Bytes() [][]byte {
	return nil
}

func (s NullLiteral) Equal(_ Expr) bool {
	return false
}

func (s NullLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_UNSPECIFIED)
}

func (s NullLiteral) String() string {
	return "null"
}
