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
	"strconv"

	"github.com/google/go-cmp/cmp"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var _ LiteralExpr = (*int64Literal)(nil)

type int64Literal struct {
	int64
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

func Int(num int64) Expr {
	return &int64Literal{num}
}

func (i *int64Literal) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT)
}

func (i *int64Literal) String() string {
	return strconv.FormatInt(i.int64, 10)
}

var _ LiteralExpr = (*int64ArrLiteral)(nil)

type int64ArrLiteral struct {
	arr []int64
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
		return cmp.Equal(other.arr, i.arr)
	}

	return false
}

func Ints(ints ...int64) Expr {
	return &int64ArrLiteral{
		arr: ints,
	}
}

func (i *int64ArrLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_INT_ARRAY)
}

func (i *int64ArrLiteral) String() string {
	return fmt.Sprintf("%v", i.arr)
}

var _ LiteralExpr = (*strLiteral)(nil)

type strLiteral struct {
	string
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

func Str(str string) Expr {
	return &strLiteral{str}
}

func (s *strLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING)
}

func (s *strLiteral) String() string {
	return s.string
}

var _ LiteralExpr = (*strArrLiteral)(nil)

type strArrLiteral struct {
	arr []string
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
		return cmp.Equal(other.arr, s.arr)
	}

	return false
}

func Strs(strs ...string) Expr {
	return &strArrLiteral{
		arr: strs,
	}
}

func (s *strArrLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_STRING_ARRAY)
}

func (s *strArrLiteral) String() string {
	return fmt.Sprintf("%v", s.arr)
}
