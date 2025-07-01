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
	"bytes"
	"fmt"
	"math/bits"
)

const maxUniqueValues = 256

// Dictionary is used for dictionary encoding.
type Dictionary struct {
	values  [][]byte
	indices []uint32
}

// NewDictionary creates a dictionary.
func NewDictionary() *Dictionary {
	return &Dictionary{
		values:  make([][]byte, 0),
		indices: make([]uint32, 0),
	}
}

// Reset resets the dictionary.
func (d *Dictionary) Reset() {
	d.values = d.values[:0]
	d.indices = d.indices[:0]
}

// Add adds a value to the dictionary.
func (d *Dictionary) Add(value []byte) bool {
	for i, v := range d.values {
		if bytes.Equal(v, value) {
			d.indices = append(d.indices, uint32(i))
			return true
		}
	}
	if len(d.values) == maxUniqueValues {
		return false
	}
	d.values = append(d.values, value)
	index := uint32(len(d.values) - 1)
	d.indices = append(d.indices, index)
	return true
}

// Encode encodes the dictionary.
func (d *Dictionary) Encode(dst []byte, tmp []uint32) []byte {
	dst = VarUint64ToBytes(dst, uint64(len(d.values)))
	dst = EncodeBytesBlock(dst, d.values)
	re := encodeRLE(tmp, d.indices)
	be := encodeBitPacking(re)
	dst = append(dst, be...)
	return dst
}

// Decode decodes the dictionary.
func (d *Dictionary) Decode(src []byte, tmp []uint32) error {
	src, count := BytesToVarUint64(src)
	if count == 0 {
		return nil
	}

	values, src, err := d.decodeBytesBlockWithTail(src, count)
	if err != nil {
		return err
	}
	d.values = values

	tmp, err = decodeBitPacking(tmp, src)
	if err != nil {
		return err
	}
	d.indices = decodeRLE(d.indices[:0], tmp)
	return nil
}

func (d *Dictionary) decodeBytesBlockWithTail(src []byte, itemsCount uint64) ([][]byte, []byte, error) {
	u64List := GenerateUint64List(0)
	defer ReleaseUint64List(u64List)

	var tail []byte
	var err error
	u64List.L, tail, err = DecodeUint64Block(u64List.L[:0], src, itemsCount)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode string lengths: %w", err)
	}
	aLens := u64List.L
	src = tail

	var decompressedData []byte
	decompressedData, tail, err = decompressBlock(nil, src)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode bytes block with strings: %w", err)
	}

	dst := d.values[:0]
	data := decompressedData
	for _, sLen := range aLens {
		if uint64(len(data)) < sLen {
			return nil, nil, fmt.Errorf("cannot decode a string with the length %d bytes from %d bytes", sLen, len(data))
		}
		if sLen == 0 {
			dst = append(dst, nil)
			continue
		}
		dst = append(dst, data[:sLen])
		data = data[sLen:]
	}

	return dst, tail, nil
}

func encodeRLE(dst []uint32, src []uint32) []uint32 {
	if len(src) == 0 {
		return nil
	}

	currentValue := src[0]
	count := uint32(1)
	for i := 1; i < len(src); i++ {
		if src[i] == currentValue {
			count++
		} else {
			dst = append(dst, currentValue, count)
			currentValue = src[i]
			count = 1
		}
	}
	dst = append(dst, currentValue, count)
	return dst
}

func decodeRLE(dst []uint32, src []uint32) []uint32 {
	if len(src) == 0 {
		return nil
	}

	for i := 0; i < len(src); i += 2 {
		value := src[i]
		count := src[i+1]
		for j := uint32(1); j <= count; j++ {
			dst = append(dst, value)
		}
	}
	return dst
}

type bitPackingEncoder struct {
	bw *Writer
}

func newBitPackingEncoder(bw *Writer) *bitPackingEncoder {
	return &bitPackingEncoder{
		bw: bw,
	}
}

func (bpe *bitPackingEncoder) encode(src []uint32) {
	if len(src) == 0 {
		bpe.bw.WriteBits(0, 32)
		return
	}

	bpe.bw.WriteBits(uint64(len(src)), 32)
	maxValue := uint32(0)
	for _, v := range src {
		if v > maxValue {
			maxValue = v
		}
	}
	bitsWidth := 1
	if maxValue > 0 {
		bitsWidth = bits.Len32(maxValue)
	}
	bpe.bw.WriteBits(uint64(bitsWidth), 8)
	for _, v := range src {
		bpe.bw.WriteBits(uint64(v), bitsWidth)
	}
}

type bitPackingDecoder struct {
	br *Reader
}

func newBitPackingDecoder(br *Reader) *bitPackingDecoder {
	return &bitPackingDecoder{
		br: br,
	}
}

func (bpd *bitPackingDecoder) decode(dst []uint32) ([]uint32, error) {
	length, err := bpd.br.ReadBits(32)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []uint32{}, nil
	}

	bitsWidth, err := bpd.br.ReadBits(8)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < length; i++ {
		value, err := bpd.br.ReadBits(int(bitsWidth))
		if err != nil {
			return nil, err
		}
		dst = append(dst, uint32(value))
	}
	return dst, nil
}

func encodeBitPacking(src []uint32) []byte {
	bw := NewWriter()
	var buf bytes.Buffer
	bw.Reset(&buf)
	encoder := newBitPackingEncoder(bw)
	encoder.encode(src)
	bw.Flush()
	return buf.Bytes()
}

func decodeBitPacking(dst []uint32, src []byte) ([]uint32, error) {
	reader := bytes.NewReader(src)
	br := NewReader(reader)
	decoder := newBitPackingDecoder(br)
	return decoder.decode(dst)
}
