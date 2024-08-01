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
	_ LiteralExpr    = (*int64Literal)(nil)
	_ ComparableExpr = (*int64Literal)(nil)
)

// int64Literal represents a wrapper for the int64 type.
type int64Literal struct {
	int64
}

// newInt64Literal creates a new instance of Int64Literal with the provided int64 value.
func newInt64Literal(val int64) *int64Literal {
	return &int64Literal{
		int64: val,
	}
}

func (i *int64Literal) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*int64Literal); ok {
		return int(i.int64 - o.int64), true
	}
	return 0, false
}

func (i *int64Literal) contains(other LiteralExpr) bool {
	if o, ok := other.(*int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*int64ArrLiteral); ok {
		if len(o.arr) == 1 && o.arr[0] == i.int64 {
			return true
		}
	}
	return false
}

func (i *int64Literal) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*int64ArrLiteral); ok {
		return slices.Contains(o.arr, i.int64)
	}
	return false
}

// Bytes converts the Int64Literal's int64 value to a slice of byte slices.
func (i *int64Literal) Bytes() [][]byte {
	return [][]byte{convert.Int64ToBytes(i.int64)}
}

func (i *int64Literal) equal(expr Expr) bool {
	if other, ok := expr.(*int64Literal); ok {
		return other.int64 == i.int64
	}

	return false
}

func (i *int64Literal) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT)
}

// String converts the Int64Literal's int64 value to its string representation.
func (i *int64Literal) String() string {
	return strconv.FormatInt(i.int64, 10)
}

func (i *int64Literal) Elements() []string {
	return []string{strconv.FormatInt(i.int64, 10)}
}

var (
	_ LiteralExpr    = (*int64ArrLiteral)(nil)
	_ ComparableExpr = (*int64ArrLiteral)(nil)
)

// int64ArrLiteral represents a wrapper for a slice of int64 values.
type int64ArrLiteral struct {
	arr []int64
}

// newInt64ArrLiteral creates a new instance of Int64ArrLiteral with the provided slice of int64 values.
func newInt64ArrLiteral(val []int64) *int64ArrLiteral {
	return &int64ArrLiteral{
		arr: val,
	}
}

func (i *int64ArrLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*int64ArrLiteral); ok {
		return 0, slices.Equal(i.arr, o.arr)
	}
	return 0, false
}

func (i *int64ArrLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*int64Literal); ok {
		return slices.Contains(i.arr, o.int64)
	}
	if o, ok := other.(*int64ArrLiteral); ok {
		for _, v := range o.arr {
			if !slices.Contains(i.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

func (i *int64ArrLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*int64Literal); ok {
		if len(i.arr) == 1 && i.arr[0] == o.int64 {
			return true
		}
		return false
	}
	if o, ok := other.(*int64ArrLiteral); ok {
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
func (i *int64ArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(i.arr))
	for _, i := range i.arr {
		b = append(b, convert.Int64ToBytes(i))
	}
	return b
}

func (i *int64ArrLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*int64ArrLiteral); ok {
		return slices.Equal(other.arr, i.arr)
	}

	return false
}

func (i *int64ArrLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT_ARRAY)
}

// String converts the Int64ArrLiteral's slice of int64 values to a string representation.
func (i *int64ArrLiteral) String() string {
	return fmt.Sprintf("%v", i.arr)
}

func (i *int64ArrLiteral) Elements() []string {
	var elements []string
	for _, v := range i.arr {
		elements = append(elements, strconv.FormatInt(v, 10))
	}
	return elements
}

var (
	_ LiteralExpr    = (*strLiteral)(nil)
	_ ComparableExpr = (*strLiteral)(nil)
)

// strLiteral represents a wrapper for the string type.
type strLiteral struct {
	string
}

func (s *strLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*strLiteral); ok {
		return strings.Compare(s.string, o.string), true
	}
	return 0, false
}

func (s *strLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		return s == o
	}
	if o, ok := other.(*strArrLiteral); ok {
		if len(o.arr) == 1 && o.arr[0] == s.string {
			return true
		}
	}
	return false
}

func (s *strLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		return s == o
	}
	if o, ok := other.(*strArrLiteral); ok {
		return slices.Contains(o.arr, s.string)
	}
	return false
}

// Bytes converts the StrLiteral's string to a slice of byte slices.
func (s *strLiteral) Bytes() [][]byte {
	return [][]byte{[]byte(s.string)}
}

func (s *strLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*strLiteral); ok {
		return other.string == s.string
	}

	return false
}

// Str creates a new StrLiteral instance containing the given string.
func Str(str string) LiteralExpr {
	return &strLiteral{str}
}

func (s *strLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING)
}

// String returns the string representation of the StrLiteral.
func (s *strLiteral) String() string {
	return s.string
}

func (s *strLiteral) Elements() []string {
	return []string{s.string}
}

var (
	_ LiteralExpr    = (*strArrLiteral)(nil)
	_ ComparableExpr = (*strArrLiteral)(nil)
)

// strArrLiteral represents a wrapper for a slice of strings.
type strArrLiteral struct {
	arr []string
}

// newStrArrLiteral creates a new instance of StrArrLiteral with the provided slice of strings.
func newStrArrLiteral(val []string) *strArrLiteral {
	return &strArrLiteral{
		arr: val,
	}
}

func (s *strArrLiteral) compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*strArrLiteral); ok {
		return 0, StringSlicesEqual(s.arr, o.arr)
	}
	return 0, false
}

func (s *strArrLiteral) contains(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		return slices.Contains(s.arr, o.string)
	}
	if o, ok := other.(*strArrLiteral); ok {
		for _, v := range o.arr {
			if !slices.Contains(s.arr, v) {
				return false
			}
		}
		return true
	}
	return false
}

func (s *strArrLiteral) belongTo(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		if len(s.arr) == 1 && s.arr[0] == o.string {
			return true
		}
		return false
	}
	if o, ok := other.(*strArrLiteral); ok {
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
func (s *strArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(s.arr))
	for _, str := range s.arr {
		b = append(b, []byte(str))
	}
	return b
}

func (s *strArrLiteral) equal(expr Expr) bool {
	if other, ok := expr.(*strArrLiteral); ok {
		return slices.Equal(other.arr, s.arr)
	}

	return false
}

func (s *strArrLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING_ARRAY)
}

// String converts the StrArrLiteral's slice of strings to a string representation.
func (s *strArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}

func (s *strArrLiteral) Elements() []string {
	return s.arr
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

// Elements returns a slice containing the string representation of the byte slice.
func (b *BytesLiteral) Elements() []string {
	return []string{hex.EncodeToString(b.bb)}
}

var (
	_               LiteralExpr    = (*nullLiteral)(nil)
	_               ComparableExpr = (*nullLiteral)(nil)
	nullLiteralExpr                = &nullLiteral{}
)

// nullLiteral represents an empty struct.
type nullLiteral struct{}

// newNullLiteral returns a new instance of NullLiteral.
func newNullLiteral() *nullLiteral {
	return nullLiteralExpr
}

func (s nullLiteral) compare(_ LiteralExpr) (int, bool) {
	return 0, false
}

func (s nullLiteral) belongTo(_ LiteralExpr) bool {
	return false
}

func (s nullLiteral) contains(_ LiteralExpr) bool {
	return false
}

// Bytes returns nil for the NullLiteral.
func (s nullLiteral) Bytes() [][]byte {
	return nil
}

func (s nullLiteral) equal(_ Expr) bool {
	return false
}

func (s nullLiteral) dataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_UNSPECIFIED)
}

// String returns the string "null" for the NullLiteral.
func (s nullLiteral) String() string {
	return "null"
}

func (s nullLiteral) Elements() []string {
	return []string{"null"}
}
