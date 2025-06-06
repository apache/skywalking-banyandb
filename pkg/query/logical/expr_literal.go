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
	"fmt"
	"math"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	_ LiteralExpr    = (*int64Literal)(nil)
	_ ComparableExpr = (*int64Literal)(nil)
)

type int64Literal struct {
	int64
}

func (i *int64Literal) Field(key index.FieldKey) index.Field {
	return index.NewIntField(key, i.int64)
}

func (i *int64Literal) RangeOpts(isUpper bool, includeLower bool, includeUpper bool) index.RangeOpts {
	if isUpper {
		return index.NewIntRangeOpts(math.MinInt64, i.int64, includeLower, includeUpper)
	}
	return index.NewIntRangeOpts(i.int64, math.MaxInt64, includeLower, includeUpper)
}

func (i *int64Literal) SubExprs() []LiteralExpr {
	return []LiteralExpr{i}
}

func newInt64Literal(val int64) *int64Literal {
	return &int64Literal{
		int64: val,
	}
}

func (i *int64Literal) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*int64Literal); ok {
		return int(i.int64 - o.int64), true
	}
	return 0, false
}

func (i *int64Literal) Contains(other LiteralExpr) bool {
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

func (i *int64Literal) BelongTo(other LiteralExpr) bool {
	if o, ok := other.(*int64Literal); ok {
		return i == o
	}
	if o, ok := other.(*int64ArrLiteral); ok {
		return slices.Contains(o.arr, i.int64)
	}
	return false
}

func (i *int64Literal) Bytes() [][]byte {
	return [][]byte{convert.Int64ToBytes(i.int64)}
}

func (i *int64Literal) Equal(expr Expr) bool {
	if other, ok := expr.(*int64Literal); ok {
		return other.int64 == i.int64
	}

	return false
}

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

type int64ArrLiteral struct {
	arr []int64
}

func (i *int64ArrLiteral) Field(_ index.FieldKey) index.Field {
	logger.Panicf("unsupported generate an index field for int64 array")
	return index.Field{}
}

func (i *int64ArrLiteral) RangeOpts(_ bool, _ bool, _ bool) index.RangeOpts {
	logger.Panicf("unsupported generate an index range opts for int64 array")
	return index.RangeOpts{}
}

func (i *int64ArrLiteral) SubExprs() []LiteralExpr {
	exprs := make([]LiteralExpr, 0, len(i.arr))
	for _, v := range i.arr {
		exprs = append(exprs, newInt64Literal(v))
	}
	return exprs
}

func newInt64ArrLiteral(val []int64) *int64ArrLiteral {
	return &int64ArrLiteral{
		arr: val,
	}
}

func (i *int64ArrLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*int64ArrLiteral); ok {
		return 0, slices.Equal(i.arr, o.arr)
	}
	return 0, false
}

func (i *int64ArrLiteral) Contains(other LiteralExpr) bool {
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

func (i *int64ArrLiteral) BelongTo(other LiteralExpr) bool {
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

func (i *int64ArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(i.arr))
	for _, i := range i.arr {
		b = append(b, convert.Int64ToBytes(i))
	}
	return b
}

func (i *int64ArrLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*int64ArrLiteral); ok {
		return slices.Equal(other.arr, i.arr)
	}

	return false
}

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
	_            LiteralExpr    = (*strLiteral)(nil)
	_            ComparableExpr = (*strLiteral)(nil)
	defaultUpper                = convert.Uint64ToBytes(math.MaxUint64)
	defaultLower                = convert.Uint64ToBytes(0)
)

type strLiteral struct {
	string
}

func (s *strLiteral) Field(key index.FieldKey) index.Field {
	return index.NewStringField(key, s.string)
}

func (s *strLiteral) RangeOpts(isUpper bool, includeLower bool, includeUpper bool) index.RangeOpts {
	if isUpper {
		return index.NewStringRangeOpts(convert.BytesToString(defaultLower), s.string, includeLower, includeUpper)
	}
	return index.NewStringRangeOpts(s.string, convert.BytesToString(defaultUpper), includeLower, includeUpper)
}

func (s *strLiteral) SubExprs() []LiteralExpr {
	return []LiteralExpr{s}
}

func (s *strLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*strLiteral); ok {
		return strings.Compare(s.string, o.string), true
	}
	return 0, false
}

func (s *strLiteral) Contains(other LiteralExpr) bool {
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

func (s *strLiteral) BelongTo(other LiteralExpr) bool {
	if o, ok := other.(*strLiteral); ok {
		return s == o
	}
	if o, ok := other.(*strArrLiteral); ok {
		return slices.Contains(o.arr, s.string)
	}
	return false
}

func (s *strLiteral) Bytes() [][]byte {
	return [][]byte{[]byte(s.string)}
}

func (s *strLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*strLiteral); ok {
		return other.string == s.string
	}

	return false
}

func str(str string) LiteralExpr {
	return &strLiteral{str}
}

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

type strArrLiteral struct {
	arr []string
}

func (s *strArrLiteral) Field(_ index.FieldKey) index.Field {
	logger.Panicf("unsupported generate an index field for string array")
	return index.Field{}
}

func (s *strArrLiteral) RangeOpts(_ bool, _ bool, _ bool) index.RangeOpts {
	logger.Panicf("unsupported generate an index range opts for string array")
	return index.RangeOpts{}
}

func (s *strArrLiteral) SubExprs() []LiteralExpr {
	exprs := make([]LiteralExpr, 0, len(s.arr))
	for _, v := range s.arr {
		exprs = append(exprs, str(v))
	}
	return exprs
}

func newStrArrLiteral(val []string) *strArrLiteral {
	return &strArrLiteral{
		arr: val,
	}
}

func (s *strArrLiteral) Compare(other LiteralExpr) (int, bool) {
	if o, ok := other.(*strArrLiteral); ok {
		return 0, StringSlicesEqual(s.arr, o.arr)
	}
	return 0, false
}

func (s *strArrLiteral) Contains(other LiteralExpr) bool {
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

func (s *strArrLiteral) BelongTo(other LiteralExpr) bool {
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

func (s *strArrLiteral) Bytes() [][]byte {
	b := make([][]byte, 0, len(s.arr))
	for _, str := range s.arr {
		b = append(b, []byte(str))
	}
	return b
}

func (s *strArrLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*strArrLiteral); ok {
		return slices.Equal(other.arr, s.arr)
	}

	return false
}

func (s *strArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}

func (s *strArrLiteral) Elements() []string {
	return s.arr
}

var (
	_               LiteralExpr    = (*nullLiteral)(nil)
	_               ComparableExpr = (*nullLiteral)(nil)
	nullLiteralExpr                = &nullLiteral{}
	nullIndexField                 = index.Field{}
	nullRangeOpts                  = index.RangeOpts{}
)

type nullLiteral struct{}

func (s *nullLiteral) Field(_ index.FieldKey) index.Field {
	return nullIndexField
}

func (s *nullLiteral) RangeOpts(_ bool, _ bool, _ bool) index.RangeOpts {
	return nullRangeOpts
}

func (s *nullLiteral) SubExprs() []LiteralExpr {
	panic("unimplemented")
}

func newNullLiteral() *nullLiteral {
	return nullLiteralExpr
}

func (s nullLiteral) Compare(_ LiteralExpr) (int, bool) {
	return 0, false
}

func (s nullLiteral) BelongTo(_ LiteralExpr) bool {
	return false
}

func (s nullLiteral) Contains(_ LiteralExpr) bool {
	return false
}

func (s nullLiteral) Bytes() [][]byte {
	return nil
}

func (s nullLiteral) Equal(_ Expr) bool {
	return false
}

func (s nullLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_UNSPECIFIED)
}

func (s nullLiteral) String() string {
	return "null"
}

func (s nullLiteral) Elements() []string {
	return []string{"null"}
}
