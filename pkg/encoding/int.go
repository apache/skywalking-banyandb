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
	"encoding/binary"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// Uint16ToBytes appends the bytes of the given uint16 to the given byte slice.
func Uint16ToBytes(dst []byte, u uint16) []byte {
	return append(dst, byte(u>>8), byte(u))
}

// BytesToUint16 converts the first two bytes of the given byte slice to a uint16.
func BytesToUint16(src []byte) uint16 {
	return binary.BigEndian.Uint16(src[:2])
}

// Uint32ToBytes appends the bytes of the given uint32 to the given byte slice.
func Uint32ToBytes(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// BytesToUint32 converts the first four bytes of the given byte slice to a uint32.
func BytesToUint32(src []byte) uint32 {
	return binary.BigEndian.Uint32(src[:4])
}

// Uint64ToBytes appends the bytes of the given uint64 to the given byte slice.
func Uint64ToBytes(dst []byte, u uint64) []byte {
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// BytesToUint64 converts the first eight bytes of the given byte slice to a uint64.
func BytesToUint64(src []byte) uint64 {
	return binary.BigEndian.Uint64(src[:8])
}

// Int64ToBytes appends the bytes of the given int64 to the given byte slice.
func Int64ToBytes(dst []byte, v int64) []byte {
	v = (v << 1) ^ (v >> 63)
	u := uint64(v)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32), byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// BytesToInt64 converts the first eight bytes of the given byte slice to an int64.
func BytesToInt64(src []byte) int64 {
	u := binary.BigEndian.Uint64(src[:8])
	v := int64(u>>1) ^ (int64(u<<63) >> 63)
	return v
}

// VarInt64ToBytes appends the bytes of the given int64 to the given byte slice.
// It uses variable-length encoding.
func VarInt64ToBytes(dst []byte, v int64) []byte {
	var tmp [1]int64
	tmp[0] = v
	return VarInt64ListToBytes(dst, tmp[:])
}

// VarInt64ListToBytes appends the bytes of the given int64s to the given byte slice.
// It uses variable-length encoding.
func VarInt64ListToBytes(dst []byte, vs []int64) []byte {
	for _, v := range vs {
		if v < 0x40 && v > -0x40 {
			c := int8(v)
			v := (c << 1) ^ (c >> 7)
			dst = append(dst, byte(v))
			continue
		}

		v = (v << 1) ^ (v >> 63)
		u := uint64(v)
		for u > 0x7f {
			dst = append(dst, 0x80|byte(u))
			u >>= 7
		}
		dst = append(dst, byte(u))
	}
	return dst
}

// BytesToVarInt64 converts the first bytes of the given byte slice to an int64.
// It uses variable-length encoding.
func BytesToVarInt64(src []byte) ([]byte, int64, error) {
	var tmp [1]int64
	tail, err := BytesToVarInt64List(tmp[:], src)
	return tail, tmp[0], err
}

// BytesToVarInt64List converts the first bytes of the given byte slice to an int64s.
// It uses variable-length encoding.
func BytesToVarInt64List(dst []int64, src []byte) ([]byte, error) {
	idx := uint(0)
	for i := range dst {
		// Check if there's enough data to decode
		if idx >= uint(len(src)) {
			return nil, fmt.Errorf("cannot decode varint from empty data")
		}
		c := src[idx]
		idx++
		// If c < 128, the integer is represented by a single byte
		if c < 0x80 {
			v := int8(c>>1) ^ (int8(c<<7) >> 7)
			dst[i] = int64(v)
			continue
		}

		// If c >= 128, the integer is represented by multiple bytes
		u := uint64(c & 0x7f)
		startIdx := idx - 1
		shift := uint8(0)
		for c >= 0x80 {
			if idx >= uint(len(src)) {
				return nil, fmt.Errorf("unexpected end of encoded varint at byte %d; src=%x", idx-startIdx, src[startIdx:])
			}
			if idx-startIdx > 9 {
				return src[idx:], fmt.Errorf("too long encoded varint; the maximum allowed length is 10 bytes; got %d bytes; src=%x",
					(idx-startIdx)+1, src[startIdx:])
			}
			c = src[idx]
			idx++
			shift += 7
			u |= uint64(c&0x7f) << shift
		}
		v := int64(u>>1) ^ (int64(u<<63) >> 63)
		dst[i] = v
	}
	return src[idx:], nil
}

// VarUint64ToBytes appends the bytes of the given uint64 to the given byte slice.
// It uses variable-length encoding.
func VarUint64ToBytes(dst []byte, u uint64) []byte {
	if u < (1 << 7) {
		return append(dst, byte(u))
	}
	if u < (1 << (2 * 7)) {
		return append(dst, byte(u|0x80), byte(u>>7))
	}
	if u < (1 << (3 * 7)) {
		return append(dst, byte(u|0x80), byte((u>>7)|0x80), byte(u>>(2*7)))
	}

	var tmp [1]uint64
	tmp[0] = u
	return VarUint64sToBytes(dst, tmp[:])
}

// VarUint64sToBytes appends the bytes of the given uint64s to the given byte slice.
// It uses variable-length encoding.
func VarUint64sToBytes(dst []byte, us []uint64) []byte {
	for _, u := range us {
		if u < 0x80 {
			dst = append(dst, byte(u))
			continue
		}
		for u > 0x7f {
			dst = append(dst, 0x80|byte(u))
			u >>= 7
		}
		dst = append(dst, byte(u))
	}
	return dst
}

// BytesToVarUint64 converts the first bytes of the given byte slice to a uint64.
// It uses variable-length encoding.
func BytesToVarUint64(src []byte) ([]byte, uint64) {
	if len(src) == 0 {
		return src, 0
	}
	if src[0] < 0x80 {
		// Fast path for a single byte
		return src[1:], uint64(src[0])
	}
	if len(src) == 1 {
		return src, 0
	}
	if src[1] < 0x80 {
		// Fast path for two bytes
		return src[2:], uint64(src[0]&0x7f) | uint64(src[1])<<7
	}

	// Slow path for other number of bytes
	x, o := binary.Uvarint(src)
	if o <= 0 {
		return src, 0
	}
	return src[o:], x
}

// BytesToVarUint64s converts the first bytes of the given byte slice to a uint64s.
// It uses variable-length encoding.
func BytesToVarUint64s(dst []uint64, src []byte) ([]byte, error) {
	idx := uint(0)
	for i := range dst {
		if idx >= uint(len(src)) {
			return nil, fmt.Errorf("cannot decode varuint from empty data")
		}
		c := src[idx]
		idx++
		if c < 0x80 {
			dst[i] = uint64(c)
			continue
		}

		u := uint64(c & 0x7f)
		startIdx := idx - 1
		shift := uint8(0)
		for c >= 0x80 {
			if idx >= uint(len(src)) {
				return nil, fmt.Errorf("unexpected end of encoded varint at byte %d; src=%x", idx-startIdx, src[startIdx:])
			}
			if idx-startIdx > 9 {
				return src[idx:], fmt.Errorf("too long encoded varint; the maximum allowed length is 10 bytes; got %d bytes; src=%x",
					(idx-startIdx)+1, src[startIdx:])
			}
			c = src[idx]
			idx++
			shift += 7
			u |= uint64(c&0x7f) << shift
		}
		dst[i] = u
	}
	return src[idx:], nil
}

// GenerateInt64List generates a list of int64 with the given size.
// The returned list may be from a pool and should be released after use.
func GenerateInt64List(size int) *Int64List {
	v := int64ListPool.Get()
	if v == nil {
		return &Int64List{
			L: make([]int64, size),
		}
	}
	is := v
	if n := size - cap(is.L); n > 0 {
		is.L = append(is.L[:cap(is.L)], make([]int64, n)...)
	}
	is.L = is.L[:size]
	return is
}

// ReleaseInt64List releases the given list of int64.
// The list may be put into a pool for reuse.
func ReleaseInt64List(is *Int64List) {
	int64ListPool.Put(is)
}

// Int64List is a list of int64.
type Int64List struct {
	L []int64
}

var int64ListPool = pool.Register[*Int64List]("encoding-int64List")

// GenerateUint64List generates a list of uint64 with the given size.
// The returned list may be from a pool and should be released after use.
func GenerateUint64List(size int) *Uint64List {
	v := uint64ListPool.Get()
	if v == nil {
		return &Uint64List{
			L: make([]uint64, size),
		}
	}
	is := v
	if n := size - cap(is.L); n > 0 {
		is.L = append(is.L[:cap(is.L)], make([]uint64, n)...)
	}
	is.L = is.L[:size]
	return is
}

// ReleaseUint64List releases the given list of uint64.
// The list may be put into a pool for reuse.
func ReleaseUint64List(is *Uint64List) {
	uint64ListPool.Put(is)
}

// Uint64List is a list of uint64.
type Uint64List struct {
	L []uint64
}

var uint64ListPool = pool.Register[*Uint64List]("encoding-uin64List")
