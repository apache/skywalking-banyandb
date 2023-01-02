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
	"io"
)

// Reader reads bits from buffer.
type Reader struct {
	in    io.ByteReader
	cache byte
	len   byte
}

// NewReader crate bit reader.
func NewReader(in io.ByteReader) *Reader {
	return &Reader{
		in: in,
	}
}

// ReadBool reads a bit, 1 returns true, 0 returns false.
func (r *Reader) ReadBool() (bool, error) {
	if r.len == 0 {
		b, err := r.in.ReadByte()
		if err != nil {
			return false, err
		}
		r.cache = b
		r.len = 8
	}
	r.len--
	b := r.cache & 0x80
	r.cache <<= 1
	return b != 0, nil
}

// ReadBits read number of bits.
func (r *Reader) ReadBits(numBits int) (uint64, error) {
	var result uint64

	for ; numBits >= 8; numBits -= 8 {
		b, err := r.ReadByte()
		if err != nil {
			return 0, err
		}

		result = (result << 8) | uint64(b)
	}

	for ; numBits > 0; numBits-- {
		byt, err := r.ReadBool()
		if err != nil {
			return 0, err
		}
		result <<= 1
		if byt {
			result |= 1
		}
	}

	return result, nil
}

// ReadByte reads a byte.
func (r *Reader) ReadByte() (byte, error) {
	if r.len == 0 {
		b, err := r.in.ReadByte()
		if err != nil {
			return b, err
		}
		r.cache = b
		return b, err
	}
	b, err := r.in.ReadByte()
	if err != nil {
		return b, err
	}
	result := r.cache | b>>r.len
	r.cache = b << (8 - r.len)
	return result, nil
}

// Reset resets the reader to read from a new slice.
func (r *Reader) Reset() {
	r.len = 0
	r.cache = 0
}
