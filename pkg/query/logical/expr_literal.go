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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var _ LiteralExpr = (*int64Literal)(nil)
var _ ComparableExpr = (*int64Literal)(nil)

type int64Literal struct {
	int64
}

func (i *int64Literal) Compare(tagValue *modelv1.TagValue) (int, bool) {
	intValue := tagValue.GetInt()
	if intValue == nil {
		return 0, false
	}
	return int(i.int64 - intValue.Value), true
}

func (i *int64Literal) BelongTo(tagValue *modelv1.TagValue) bool {
	intValue := tagValue.GetInt()
	if intValue != nil {
		return i.int64 == intValue.Value

	}
	intArray := tagValue.GetIntArray()
	if intArray == nil {
		return false
	}
	return int64SliceContains(i.int64, intArray.Value)
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
var _ ComparableExpr = (*int64ArrLiteral)(nil)

type int64ArrLiteral struct {
	arr []int64
}

func (i *int64ArrLiteral) Compare(tagValue *modelv1.TagValue) (int, bool) {
	intArray := tagValue.GetIntArray()
	if intArray == nil {
		return 0, false
	}
	if int64SlicesEqual(i.arr, intArray.Value) {
		return 0, true
	}
	return 0, false
}

func (i *int64ArrLiteral) BelongTo(tagValue *modelv1.TagValue) bool {
	intValue := tagValue.GetInt()
	if intValue != nil {
		return int64SliceContains(intValue.Value, i.arr)

	}
	intArray := tagValue.GetIntArray()
	if intArray == nil {
		return false
	}
	for _, v := range intArray.Value {
		if !int64SliceContains(v, i.arr) {
			return false
		}
	}
	return true
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
var _ ComparableExpr = (*strLiteral)(nil)

type strLiteral struct {
	string
}

func (s *strLiteral) Compare(tagValue *modelv1.TagValue) (int, bool) {
	strValue := tagValue.GetStr()
	if strValue == nil {
		return 0, false
	}
	if strValue.Value == s.string {
		return 0, true
	}
	return 0, false
}

func (s *strLiteral) BelongTo(tagValue *modelv1.TagValue) bool {
	strValue := tagValue.GetStr()
	if strValue != nil {
		return s.string == strValue.Value

	}
	strArray := tagValue.GetStrArray()
	if strArray == nil {
		return false
	}
	return stringSliceContains(s.string, strArray.Value)
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
var _ ComparableExpr = (*strArrLiteral)(nil)

type strArrLiteral struct {
	arr []string
}

func (s *strArrLiteral) Compare(tagValue *modelv1.TagValue) (int, bool) {
	strArray := tagValue.GetStrArray()
	if strArray == nil {
		return 0, false
	}
	if stringSlicesEqual(s.arr, strArray.Value) {
		return 0, true
	}
	return 0, false
}

func (s *strArrLiteral) BelongTo(tagValue *modelv1.TagValue) bool {
	strValue := tagValue.GetStr()
	if strValue != nil {
		return stringSliceContains(strValue.Value, s.arr)

	}
	strArray := tagValue.GetStrArray()
	if strArray == nil {
		return false
	}
	for _, v := range strArray.Value {
		if !stringSliceContains(v, s.arr) {
			return false
		}
	}
	return true
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

var _ LiteralExpr = (*idLiteral)(nil)

type idLiteral struct {
	string
}

func (s *idLiteral) Bytes() [][]byte {
	return [][]byte{[]byte(s.string)}
}

func (s *idLiteral) Equal(expr Expr) bool {
	if other, ok := expr.(*idLiteral); ok {
		return other.string == s.string
	}

	return false
}

func ID(id string) Expr {
	return &idLiteral{id}
}

func (s *idLiteral) DataType() int32 {
	return int32(databasev1.TagType_TAG_TYPE_ID)
}

func (s *idLiteral) String() string {
	return s.string
}

func int64SlicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func int64SliceContains(a int64, slice []int64) bool {
	for _, v := range slice {
		if v == a {
			return true
		}
	}
	return false
}

func stringSliceContains(a string, slice []string) bool {
	for _, v := range slice {
		if v == a {
			return true
		}
	}
	return false
}
