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
	"math/bits"
)

const (
	ctrlBitsNoContainMeaningful = 0x2
	ctrlBitsContainMeaningful   = 0x3
)

// XOREncoder intends to compress uint64 data
// https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
type XOREncoder struct {
	bw       *Writer
	preVal   uint64
	leading  int
	trailing int

	first bool
}

// NewXOREncoder creates xor zstdEncoder for compressing uint64 data.
func NewXOREncoder(bw *Writer) *XOREncoder {
	return &XOREncoder{
		bw:    bw,
		first: true,
	}
}

func (e *XOREncoder) Write(val uint64) {
	if e.first {
		e.first = false
		e.preVal = val
		e.bw.WriteBits(val, 64)
		return
	}

	delta := val ^ e.preVal
	e.preVal = val
	if delta == 0 {
		e.bw.WriteBool(false)
		return
	}

	leading := bits.LeadingZeros64(delta)
	trailing := bits.TrailingZeros64(delta)
	if leading >= e.leading && trailing >= e.trailing {
		// write control '10' to reuse previous block meaningful bits
		e.bw.WriteBits(ctrlBitsNoContainMeaningful, 2)
		e.bw.WriteBits(delta>>uint(e.trailing), 64-e.leading-e.trailing)
	} else {
		// write control '11' to create a new block meaningful bits
		e.bw.WriteBits(ctrlBitsContainMeaningful, 2)
		meaningfulLen := 64 - leading - trailing
		e.bw.WriteBits(uint64(leading), 6)
		// meaningfulLen is at least 1, so we can subtract 1 from it and encode it in 6 bits
		e.bw.WriteBits(uint64(meaningfulLen-1), 6)
		e.bw.WriteBits(delta>>uint(trailing), meaningfulLen)

		e.leading = leading
		e.trailing = trailing
	}
}

// XORDecoder decodes buffer to uint64 values using xor compress.
type XORDecoder struct {
	err      error
	br       *Reader
	val      uint64
	leading  uint64
	trailing uint64
	first    bool
}

// NewXORDecoder create zstdDecoder decompress buffer using xor.
func NewXORDecoder(br *Reader) *XORDecoder {
	s := &XORDecoder{
		br:    br,
		first: true,
	}
	return s
}

// Reset resets the underlying buffer to decode.
func (d *XORDecoder) Reset() {
	d.first = true
	d.leading = 0
	d.trailing = 0
	d.val = 0
}

// Next return if zstdDecoder has value in buffer using xor, do uncompress logic in next method,
// data format reference zstdEncoder format.
func (d *XORDecoder) Next() bool {
	if d.first {
		// read first value
		d.first = false
		d.val, d.err = d.br.ReadBits(64)
		return d.err == nil
	}

	var b bool
	// read delta control bit
	b, d.err = d.br.ReadBool()
	if d.err != nil {
		return false
	}
	if !b {
		return true
	}
	ctrlBits := ctrlBitsNoContainMeaningful
	// read control bit
	b, d.err = d.br.ReadBool()
	if d.err != nil {
		return false
	}
	if b {
		ctrlBits |= 1
	}
	var blockSize uint64
	if ctrlBits == ctrlBitsNoContainMeaningful {
		blockSize = 64 - d.leading - d.trailing
	} else {
		// read leading and trailing, because block is diff with previous
		d.leading, d.err = d.br.ReadBits(6)
		if d.err != nil {
			return false
		}
		blockSize, d.err = d.br.ReadBits(6)
		if d.err != nil {
			return false
		}
		blockSize++
		d.trailing = 64 - d.leading - blockSize
	}
	delta, err := d.br.ReadBits(int(blockSize))
	if err != nil {
		d.err = err
		return false
	}
	val := delta << d.trailing
	d.val ^= val
	return true
}

// Value returns uint64 from buffer.
func (d *XORDecoder) Value() uint64 {
	return d.val
}

// Err returns error raised in Next().
func (d *XORDecoder) Err() error {
	return d.err
}
