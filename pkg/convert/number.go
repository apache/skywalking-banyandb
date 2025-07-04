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
	"math"
)

// Uint64ToBytes converts uint64 to bytes.
func Uint64ToBytes(u uint64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, u)
	return bs
}

// Int64ToBytes converts int64 to bytes.
func Int64ToBytes(i int64) []byte {
	abs := i
	if i < 0 {
		abs = -abs
	}
	u := uint64(abs)
	if i >= 0 {
		u |= 1 << 63
	} else {
		u = 1<<63 - u
	}
	return Uint64ToBytes(u)
}

// Uint32ToBytes converts uint32 to bytes.
func Uint32ToBytes(u uint32) []byte {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, u)
	return bs
}

// Int32ToBytes converts int32 to bytes.
func Int32ToBytes(i int32) []byte {
	abs := i
	if i < 0 {
		abs = -abs
	}
	u := uint32(abs)
	if i >= 0 {
		u |= 1 << 31
	} else {
		u = (1 << 31) - u
	}
	return Uint32ToBytes(u)
}

// Int16ToBytes converts int16 to bytes using BigEndian.
func Int16ToBytes(i int16) []byte {
	bs := make([]byte, 2)
	binary.BigEndian.PutUint16(bs, uint16(i))
	return bs
}

// BytesToInt16 converts a 2-byte slice to int16 using BigEndian.
func BytesToInt16(b []byte) int16 {
	if len(b) < 2 {
		panic("BytesToInt16: byte slice too short")
	}
	return int16(binary.BigEndian.Uint16(b))
}

// BoolToBytes converts bool to bytes.
func BoolToBytes(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

// BytesToInt64 converts bytes to int64.
func BytesToInt64(b []byte) int64 {
	u := binary.BigEndian.Uint64(b)
	if b[0] >= 128 {
		u ^= 1 << 63
	} else {
		u = 1<<63 - u
	}
	abs := int64(u)
	if b[0] < 128 {
		abs = -abs
	}
	return abs
}

// BytesToInt32 converts bytes to int32 based on the encoding used in Int32ToBytes.
func BytesToInt32(b []byte) int32 {
	u := binary.BigEndian.Uint32(b)
	if (u >> 31) == 1 {
		// Positive number
		return int32(u &^ (1 << 31)) // clear the sign bit
	}
	return -int32((1 << 31) - u)
}

// BytesToUint64 converts bytes to uint64.
func BytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// BytesToUint32 converts bytes to uint32.
func BytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// Float64ToBytes converts float64 to byes.
func Float64ToBytes(f float64) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, math.Float64bits(f))
	return bs
}

// BytesToFloat64 converts bytes to float64.
func BytesToFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

// BytesToBool converts bytes to bool.
func BytesToBool(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	return b[0] != 0
}
