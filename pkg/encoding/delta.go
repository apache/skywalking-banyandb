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

package encoding

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func int64ListDeltaToBytes(dst []byte, src []int64) (result []byte, firstValue int64) {
	if len(src) < 1 {
		logger.Panicf("BUG: src must contain at least 1 item; got %d items", len(src))
	}

	firstValue = src[0]
	v := src[0]
	src = src[1:]
	is := GenerateInt64List(len(src))
	for i, next := range src {
		d := next - v
		v += d
		is.L[i] = d
	}
	dst = VarInt64ListToBytes(dst, is.L)
	ReleaseInt64List(is)
	return dst, firstValue
}

func bytesDeltaToInt64List(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 1 {
		logger.Panicf("BUG: itemsCount must be greater than 0; got %d", itemsCount)
	}

	is := GenerateInt64List(itemsCount - 1)
	defer ReleaseInt64List(is)

	tail, err := BytesToVarInt64List(is.L, src)
	if err != nil {
		return nil, fmt.Errorf("cannot decode nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after decodeing %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	dst = append(dst, v)
	for _, d := range is.L {
		v += d
		dst = append(dst, v)
	}
	return dst, nil
}

func int64sDeltaOfDeltaToBytes(dst []byte, src []int64) (result []byte, firstValue int64) {
	if len(src) < 2 {
		logger.Panicf("src must contain at least 2 items; got %d items", len(src))
	}
	firstValue = src[0]
	d1 := src[1] - src[0]
	dst = VarInt64ToBytes(dst, d1)
	v := src[1]
	src = src[2:]
	is := GenerateInt64List(len(src))
	for i, next := range src {
		d2 := next - v - d1
		d1 += d2
		v += d1
		is.L[i] = d2
	}
	dst = VarInt64ListToBytes(dst, is.L)
	ReleaseInt64List(is)
	return dst, firstValue
}

func bytesDeltaOfDeltaToInt64s(dst []int64, src []byte, firstValue int64, itemsCount int) ([]int64, error) {
	if itemsCount < 2 {
		logger.Panicf("itemsCount must be greater than 1; got %d", itemsCount)
	}

	is := GenerateInt64List(itemsCount - 1)
	defer ReleaseInt64List(is)

	tail, err := BytesToVarInt64List(is.L, src)
	if err != nil {
		return nil, fmt.Errorf("cannot decode nearest delta from %d bytes; src=%X: %w", len(src), src, err)
	}
	if len(tail) > 0 {
		return nil, fmt.Errorf("unexpected tail left after decodeing %d items from %d bytes; tail size=%d; src=%X; tail=%X", itemsCount, len(src), len(tail), src, tail)
	}

	v := firstValue
	d1 := is.L[0]
	dst = append(dst, v)
	v += d1
	dst = append(dst, v)
	for _, d2 := range is.L[1:] {
		d1 += d2
		v += d1
		dst = append(dst, v)
	}
	return dst, nil
}
