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

// Int64ListToBytes encodes a list of int64 into bytes.
func Int64ListToBytes(dst []byte, a []int64) (result []byte, mt EncodeType, firstValue int64) {
	if len(a) == 0 {
		logger.Panicf("a must contain at least one item")
	}
	if isConst(a) {
		firstValue = a[0]
		return dst, EncodeTypeConst, firstValue
	}
	isDelta, isDeltaConst := isDelta(a)
	if isDeltaConst {
		firstValue = a[0]
		dst = VarInt64ToBytes(dst, a[1]-a[0])
		return dst, EncodeTypeDeltaConst, firstValue
	}
	if isDelta {
		dst, firstValue = int64sDeltaOfDeltaToBytes(dst, a)
		return dst, EncodeTypeDeltaOfDelta, firstValue
	}

	if isIncremental(a) {
		mt = EncodeTypeDeltaOfDelta
		dst, firstValue = int64sDeltaOfDeltaToBytes(dst, a)
		return dst, mt, firstValue
	}
	mt = EncodeTypeDelta
	dst, firstValue = int64ListDeltaToBytes(dst, a)
	return dst, mt, firstValue
}

// BytesToInt64List decodes bytes into a list of int64.
func BytesToInt64List(dst []int64, src []byte, mt EncodeType, firstValue int64, itemsCount int) ([]int64, error) {
	dst = extendInt64ListCapacity(dst, itemsCount)

	var err error
	switch mt {
	case EncodeTypeDelta:
		dst, err = bytesDeltaToInt64List(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot decode nearest delta data: %w", err)
		}
		return dst, nil
	case EncodeTypeDeltaOfDelta:
		dst, err = bytesDeltaOfDeltaToInt64s(dst, src, firstValue, itemsCount)
		if err != nil {
			return nil, fmt.Errorf("cannot decode nearest delta2 data: %w", err)
		}
		return dst, nil
	case EncodeTypeConst:
		if len(src) > 0 {
			return nil, fmt.Errorf("unexpected data left in const encoding: %d bytes", len(src))
		}
		for itemsCount > 0 {
			dst = append(dst, firstValue)
			itemsCount--
		}
		return dst, nil
	case EncodeTypeDeltaConst:
		v := firstValue
		tail, d, err := BytesToVarInt64(src)
		if err != nil {
			return nil, fmt.Errorf("cannot decode delta value for delta const: %w", err)
		}
		if len(tail) > 0 {
			return nil, fmt.Errorf("unexpected trailing data after delta const (d=%d): %d bytes", d, len(tail))
		}
		for itemsCount > 0 {
			dst = append(dst, v)
			itemsCount--
			v += d
		}
		return dst, nil
	default:
		return nil, fmt.Errorf("unknown EncodeType=%d", mt)
	}
}

func extendInt64ListCapacity(dst []int64, additionalItems int) []int64 {
	dstLen := len(dst)
	if n := dstLen + additionalItems - cap(dst); n > 0 {
		dst = append(dst[:cap(dst)], make([]int64, n)...)
	}
	return dst[:dstLen]
}

func isConst(a []int64) bool {
	if len(a) == 0 {
		return false
	}
	v1 := a[0]
	for _, v := range a {
		if v != v1 {
			return false
		}
	}
	return true
}

func isDelta(a []int64) (bool, bool) {
	if len(a) < 2 {
		return false, false
	}
	ct := true
	d1 := a[1] - a[0]
	asc := getSignBit(d1)
	prev := a[1]
	for _, next := range a[2:] {
		d := next - prev
		if (getSignBit(d) ^ asc) == 1 {
			return false, false
		}
		if ct && d != d1 {
			ct = false
		}
		prev = next
	}
	return true, ct
}

func getSignBit(n int64) int64 {
	return n >> 63 & 1
}

func isIncremental(a []int64) bool {
	if len(a) < 2 {
		return false
	}

	resets := 0
	vPrev := a[0]
	if vPrev < 0 {
		return true
	}
	for _, v := range a[1:] {
		if v < vPrev {
			if v < 0 {
				return false
			}
			if v > (vPrev >> 3) {
				// Decremental data is found.
				return false
			}
			resets++
		}
		vPrev = v
	}
	if resets <= 2 {
		return true
	}

	// Assume an incremental list has less than len(a)/8 resets .
	return resets < (len(a) >> 3)
}
