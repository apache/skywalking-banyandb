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

package convert

import (
	"encoding/binary"
)

func Uint64ToBytes(u uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, u)
	return bs
}

func Int64ToBytes(i int64) []byte {
	abs := i
	if i < 0 {
		abs = -abs
	}
	u := uint64(abs)
	if i >= 0 {
		u = u | 1<<63
	} else {
		u = 1<<63 - u
	}
	return Uint64ToBytes(u)
}

func Uint16ToBytes(u uint16) []byte {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, u)
	return bs
}

func Uint32ToBytes(u uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, u)
	return bs
}

func BytesToInt64(b []byte) int64 {
	u := binary.BigEndian.Uint64(b)
	if b[0] >= 128 {
		u = u ^ 1<<63
	} else {
		u = 1<<63 - u
	}
	abs := int64(u)
	if b[0] < 128 {
		abs = -abs
	}
	return abs
}

func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToUint16(b []byte) uint16 {
	return binary.BigEndian.Uint16(b)
}

func IntToInt64(numbers ...int) []int64 {
	var arr []int64
	for i := 0; i < len(numbers); i++ {
		arr = append(arr, int64(numbers[i]))
	}
	return arr
}

func Int8ToInt64(numbers ...int8) []int64 {
	var arr []int64
	for i := 0; i < len(numbers); i++ {
		arr = append(arr, int64(numbers[i]))
	}
	return arr
}

func Int16ToInt64(numbers ...int16) []int64 {
	var arr []int64
	for i := 0; i < len(numbers); i++ {
		arr = append(arr, int64(numbers[i]))
	}
	return arr
}

func Int32ToInt64(numbers ...int32) []int64 {
	var arr []int64
	for i := 0; i < len(numbers); i++ {
		arr = append(arr, int64(numbers[i]))
	}
	return arr
}
