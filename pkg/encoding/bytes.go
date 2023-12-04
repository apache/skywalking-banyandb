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

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
)

func EncodeBytes(dst, b []byte) []byte {
	dst = VarUint64ToBytes(dst, uint64(len(b)))
	dst = append(dst, b...)
	return dst
}

func DecodeBytes(src []byte) ([]byte, []byte, error) {
	tail, n, err := BytesToVarUint64(src)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot decode string size: %w", err)
	}
	src = tail
	if uint64(len(src)) < n {
		return nil, nil, fmt.Errorf("src is too short for reading string with size %d; len(src)=%d", n, len(src))
	}
	return src[n:], src[:n], nil
}

func EncodeBytesBlock(dst []byte, a [][]byte) []byte {
	u64s := GenerateUint64List(len(a))
	aLens := u64s.L[:0]
	for _, s := range a {
		aLens = append(aLens, uint64(len(s)))
	}
	u64s.L = aLens
	dst = encodeUint64Block(dst, u64s.L)
	ReleaseUint64List(u64s)

	bb := bbPool.Get()
	b := bb.Buf
	for _, s := range a {
		b = append(b, s...)
	}
	bb.Buf = b
	dst = compressBlock(dst, bb.Buf)
	bbPool.Put(bb)

	return dst
}

type BytesBlockDecoder struct {
	data []byte
}

func (bbd *BytesBlockDecoder) Reset() {
	bbd.data = bbd.data[:0]
}

func (bbd *BytesBlockDecoder) Decode(dst [][]byte, src []byte, itemsCount uint64) ([][]byte, error) {
	u64List := GenerateUint64List(0)
	defer ReleaseUint64List(u64List)

	var tail []byte
	var err error
	u64List.L, tail, err = decodeUint64Block(u64List.L[:0], src, itemsCount)
	if err != nil {
		return dst, fmt.Errorf("cannot decode string lengths: %w", err)
	}
	aLens := u64List.L
	src = tail

	dataLen := len(bbd.data)
	bbd.data, tail, err = decompressBlock(bbd.data, src)
	if err != nil {
		return dst, fmt.Errorf("cannot decode bytes block with strings: %w", err)
	}
	if len(tail) > 0 {
		return dst, fmt.Errorf("unexpected non-empty tail after reading bytes block with strings; len(tail)=%d", len(tail))
	}

	data := bbd.data[dataLen:]
	for _, sLen := range aLens {
		if uint64(len(data)) < sLen {
			return dst, fmt.Errorf("cannot decode a string with the length %d bytes from %d bytes", sLen, len(data))
		}
		dst = append(dst, data[:sLen])
		data = data[sLen:]
	}

	return dst, nil
}

func encodeUint64Block(dst []byte, a []uint64) []byte {
	bb := bbPool.Get()
	bb.Buf = encodeUint64List(bb.Buf[:0], a)
	dst = compressBlock(dst, bb.Buf)
	bbPool.Put(bb)
	return dst
}

func decodeUint64Block(dst []uint64, src []byte, itemsCount uint64) ([]uint64, []byte, error) {
	bb := bbPool.Get()
	defer bbPool.Put(bb)

	var err error
	bb.Buf, src, err = decompressBlock(bb.Buf[:0], src)
	if err != nil {
		return dst, src, fmt.Errorf("cannot decode bytes block: %w", err)
	}

	dst, err = decodeUint64List(dst, bb.Buf, itemsCount)
	if err != nil {
		return dst, src, fmt.Errorf("cannot decode %d uint64 items from bytes block of length %d bytes: %w", itemsCount, len(bb.Buf), err)
	}
	return dst, src, nil
}

const (
	uintBlockType8  = 0
	uintBlockType16 = 1
	uintBlockType32 = 2
	uintBlockType64 = 3
)

func encodeUint64List(dst []byte, a []uint64) []byte {
	nMax := uint64(0)
	for _, n := range a {
		if n > nMax {
			nMax = n
		}
	}
	switch {
	case nMax < (1 << 8):
		dst = append(dst, uintBlockType8)
		for _, n := range a {
			dst = append(dst, byte(n))
		}
	case nMax < (1 << 16):
		dst = append(dst, uintBlockType16)
		for _, n := range a {
			dst = Uint16ToBytes(dst, uint16(n))
		}
	case nMax < (1 << 32):
		dst = append(dst, uintBlockType32)
		for _, n := range a {
			dst = Uint32ToBytes(dst, uint32(n))
		}
	default:
		dst = append(dst, uintBlockType64)
		for _, n := range a {
			dst = Uint64ToBytes(dst, uint64(n))
		}
	}
	return dst
}

func decodeUint64List(dst []uint64, src []byte, itemsCount uint64) ([]uint64, error) {
	if len(src) < 1 {
		return dst, fmt.Errorf("cannot decode uint64 block type from empty src")
	}
	blockType := src[0]
	src = src[1:]

	switch blockType {
	case uintBlockType8:
		if uint64(len(src)) != itemsCount {
			return dst, fmt.Errorf("unexpected block length for %d items; got %d bytes; want %d bytes", itemsCount, len(src), itemsCount)
		}
		for _, v := range src {
			dst = append(dst, uint64(v))
		}
	case uintBlockType16:
		if uint64(len(src)) != 2*itemsCount {
			return dst, fmt.Errorf("unexpected block length for %d items; got %d bytes; want %d bytes", itemsCount, len(src), 2*itemsCount)
		}
		for len(src) > 0 {
			v := BytesToUint16(src)
			src = src[2:]
			dst = append(dst, uint64(v))
		}
	case uintBlockType32:
		if uint64(len(src)) != 4*itemsCount {
			return dst, fmt.Errorf("unexpected block length for %d items; got %d bytes; want %d bytes", itemsCount, len(src), 4*itemsCount)
		}
		for len(src) > 0 {
			v := BytesToUint32(src)
			src = src[4:]
			dst = append(dst, uint64(v))
		}
	case uintBlockType64:
		if uint64(len(src)) != 8*itemsCount {
			return dst, fmt.Errorf("unexpected block length for %d items; got %d bytes; want %d bytes", itemsCount, len(src), 8*itemsCount)
		}
		for len(src) > 0 {
			v := BytesToUint64(src)
			src = src[8:]
			dst = append(dst, v)
		}
	default:
		return dst, fmt.Errorf("unexpected uint64 block type: %d; want 0, 1, 2 or 3", blockType)
	}
	return dst, nil
}

const (
	compressTypePlain = 0
	compressTypeZSTD  = 1
)

func compressBlock(dst, src []byte) []byte {
	if len(src) < 128 {
		dst = append(dst, compressTypePlain)
		dst = append(dst, byte(len(src)))
		return append(dst, src...)
	}

	dst = append(dst, compressTypeZSTD)
	bb := bbPool.Get()
	bb.Buf = zstd.Compress(bb.Buf[:0], src, 1)
	dst = VarUint64ToBytes(dst, uint64(len(bb.Buf)))
	dst = append(dst, bb.Buf...)
	bbPool.Put(bb)
	return dst
}

func decompressBlock(dst, src []byte) ([]byte, []byte, error) {
	if len(src) < 1 {
		return dst, src, fmt.Errorf("cannot decode block type from empty src")
	}
	blockType := src[0]
	src = src[1:]
	switch blockType {
	case compressTypePlain:
		if len(src) < 1 {
			return dst, src, fmt.Errorf("cannot decode plain block size from empty src")
		}
		blockLen := int(src[0])
		src = src[1:]
		if len(src) < blockLen {
			return dst, src, fmt.Errorf("cannot read plain block with the size %d bytes from %b bytes", blockLen, len(src))
		}

		dst = append(dst, src[:blockLen]...)
		src = src[blockLen:]
		return dst, src, nil
	case compressTypeZSTD:
		tail, blockLen, err := BytesToVarUint64(src)
		if err != nil {
			return dst, src, fmt.Errorf("cannot decode compressed block size: %w", err)
		}
		src = tail
		if uint64(len(src)) < blockLen {
			return dst, src, fmt.Errorf("cannot read compressed block with the size %d bytes from %d bytes", blockLen, len(src))
		}
		compressedBlock := src[:blockLen]
		src = src[blockLen:]

		// Decompress the block
		bb := bbPool.Get()
		bb.Buf, err = zstd.Decompress(bb.Buf[:0], compressedBlock)
		if err != nil {
			return dst, src, fmt.Errorf("cannot decompress block: %w", err)
		}

		// Copy the decompressed block to dst.
		dst = append(dst, bb.Buf...)
		bbPool.Put(bb)
		return dst, src, nil
	default:
		return dst, src, fmt.Errorf("unexpected block type: %d; supported types: 0, 1", blockType)
	}
}

var bbPool bytes.BufferPool
