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

// Package encoding provides tag value encoding functionality with optimal compression
// for different data types including int64, float64, and other types using dictionary
// encoding with fallback to plain encoding with zstd compression.
package encoding

import (
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

var (
	int64SlicePool   = pool.Register[*[]int64]("tag-encoder-int64Slice")
	float64SlicePool = pool.Register[*[]float64]("tag-encoder-float64Slice")
	dictionaryPool   = pool.Register[*encoding.Dictionary]("tag-encoder-dictionary")
)

func generateInt64Slice(length int) *[]int64 {
	v := int64SlicePool.Get()
	if v == nil {
		s := make([]int64, length)
		return &s
	}
	if cap(*v) < length {
		*v = make([]int64, length)
	} else {
		*v = (*v)[:length]
	}
	return v
}

func releaseInt64Slice(int64Slice *[]int64) {
	*int64Slice = (*int64Slice)[:0]
	int64SlicePool.Put(int64Slice)
}

func generateFloat64Slice(length int) *[]float64 {
	v := float64SlicePool.Get()
	if v == nil {
		s := make([]float64, length)
		return &s
	}
	if cap(*v) < length {
		*v = make([]float64, length)
	} else {
		*v = (*v)[:length]
	}
	return v
}

func releaseFloat64Slice(float64Slice *[]float64) {
	*float64Slice = (*float64Slice)[:0]
	float64SlicePool.Put(float64Slice)
}

func generateDictionary() *encoding.Dictionary {
	v := dictionaryPool.Get()
	if v == nil {
		return &encoding.Dictionary{}
	}
	return v
}

func releaseDictionary(d *encoding.Dictionary) {
	d.Reset()
	dictionaryPool.Put(d)
}

// EncodeTagValues encodes tag values based on the value type with optimal compression.
// For int64: uses delta encoding with first value storage.
// For float64: converts to decimal integers with exponent, then delta encoding.
// For other types: uses dictionary encoding, falls back to plain with zstd compression.
func EncodeTagValues(bb *bytes.Buffer, values [][]byte, valueType pbv1.ValueType) error {
	if len(values) == 0 {
		return nil
	}

	switch valueType {
	case pbv1.ValueTypeInt64:
		return encodeInt64TagValues(bb, values)
	case pbv1.ValueTypeFloat64:
		return encodeFloat64TagValues(bb, values)
	default:
		return encodeDefaultTagValues(bb, values)
	}
}

// DecodeTagValues decodes tag values based on the value type.
func DecodeTagValues(dst [][]byte, decoder *encoding.BytesBlockDecoder, bb *bytes.Buffer, valueType pbv1.ValueType, count int) ([][]byte, error) {
	if len(bb.Buf) == 0 {
		return nil, nil
	}

	switch valueType {
	case pbv1.ValueTypeInt64:
		return decodeInt64TagValues(dst, decoder, bb, uint64(count))
	case pbv1.ValueTypeFloat64:
		return decodeFloat64TagValues(dst, decoder, bb, uint64(count))
	default:
		return decodeDefaultTagValues(dst, decoder, bb, uint64(count))
	}
}

func encodeInt64TagValues(bb *bytes.Buffer, values [][]byte) error {
	intValuesPtr := generateInt64Slice(len(values))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)
	var encodeType encoding.EncodeType

	for i, v := range values {
		if v == nil || string(v) == "null" {
			// Handle null values by falling back to default encoding
			bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], values)
			// Prepend EncodeTypePlain at the head of compressed data
			bb.Buf = append([]byte{byte(encoding.EncodeTypePlain)}, bb.Buf...)
			return nil
		}
		if len(v) != 8 {
			logger.Panicf("invalid value length at index %d: expected 8 bytes, got %d", i, len(v))
		}
		intValues[i] = convert.BytesToInt64(v)
	}

	var firstValue int64
	bb.Buf, encodeType, firstValue = encoding.Int64ListToBytes(bb.Buf[:0], intValues)
	if encodeType == encoding.EncodeTypeUnknown {
		logger.Panicf("invalid encode type for int64 values")
	}
	firstValueBytes := convert.Int64ToBytes(firstValue)

	// Prepend encodeType (1 byte) and firstValue (8 bytes) to the beginning
	bb.Buf = append(
		append([]byte{byte(encodeType)}, firstValueBytes...),
		bb.Buf...,
	)
	return nil
}

func encodeFloat64TagValues(bb *bytes.Buffer, values [][]byte) error {
	intValuesPtr := generateInt64Slice(len(values))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)

	floatValuesPtr := generateFloat64Slice(len(values))
	floatValues := *floatValuesPtr
	defer releaseFloat64Slice(floatValuesPtr)

	var encodeType encoding.EncodeType

	for i, v := range values {
		if v == nil || string(v) == "null" {
			// Handle null values by falling back to default encoding
			bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], values)
			// Prepend EncodeTypePlain at the head of compressed data
			bb.Buf = append([]byte{byte(encoding.EncodeTypePlain)}, bb.Buf...)
			return nil
		}
		if len(v) != 8 {
			logger.Panicf("invalid value length at index %d: expected 8 bytes, got %d", i, len(v))
		}
		floatValues[i] = convert.BytesToFloat64(v)
	}

	intValues, exp, err := encoding.Float64ListToDecimalIntList(intValues[:0], floatValues)
	if err != nil {
		logger.Errorf("cannot convert Float64List to DecimalIntList: %v", err)
		bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], values)
		// Prepend EncodeTypePlain at the head of compressed data
		bb.Buf = append([]byte{byte(encoding.EncodeTypePlain)}, bb.Buf...)
		return nil
	}

	var firstValue int64
	bb.Buf, encodeType, firstValue = encoding.Int64ListToBytes(bb.Buf[:0], intValues)
	if encodeType == encoding.EncodeTypeUnknown {
		logger.Panicf("invalid encode type for int64 values")
	}
	firstValueBytes := convert.Int64ToBytes(firstValue)
	expBytes := convert.Int16ToBytes(exp)

	// Prepend encodeType (1 byte), exp (2 bytes) and firstValue (8 bytes) to the beginning
	bb.Buf = append(
		append(append([]byte{byte(encodeType)}, expBytes...), firstValueBytes...),
		bb.Buf...,
	)
	return nil
}

func encodeDefaultTagValues(bb *bytes.Buffer, values [][]byte) error {
	dict := generateDictionary()
	defer releaseDictionary(dict)

	for _, v := range values {
		if !dict.Add(v) {
			// Dictionary encoding failed, use plain encoding with zstd compression
			bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], values)
			bb.Buf = append([]byte{byte(encoding.EncodeTypePlain)}, bb.Buf...)
			return nil
		}
	}

	// Dictionary encoding succeeded
	bb.Buf = dict.Encode(bb.Buf[:0])
	bb.Buf = append([]byte{byte(encoding.EncodeTypeDictionary)}, bb.Buf...)
	return nil
}

func decodeInt64TagValues(dst [][]byte, decoder *encoding.BytesBlockDecoder, bb *bytes.Buffer, count uint64) ([][]byte, error) {
	intValuesPtr := generateInt64Slice(int(count))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)

	if len(bb.Buf) < 1 {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", 1, len(bb.Buf))
	}

	// Check the first byte to determine the encoding type
	firstByte := encoding.EncodeType(bb.Buf[0])

	if firstByte == encoding.EncodeTypePlain {
		// Decode the decompressed data
		var decodeErr error
		dst, decodeErr = decoder.Decode(dst[:0], bb.Buf[1:], count)
		if decodeErr != nil {
			logger.Panicf("cannot decode values: %v", decodeErr)
		}
		return dst, nil
	}

	// Otherwise, this is int list data with EncodeType at the beginning
	encodeType := firstByte
	const expectedLen = 9
	if len(bb.Buf) < expectedLen {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", expectedLen, len(bb.Buf))
	}
	firstValue := convert.BytesToInt64(bb.Buf[1:9])
	bb.Buf = bb.Buf[9:]

	var err error
	intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, int(count))
	if err != nil {
		logger.Panicf("cannot decode int values: %v", err)
	}

	// Convert int64 array to byte array
	if len(dst) < len(intValues) {
		dst = append(dst, make([][]byte, len(intValues)-len(dst))...)
	}
	dst = dst[:len(intValues)]
	for i, v := range intValues {
		dst[i] = convert.Int64ToBytes(v)
	}
	return dst, nil
}

func decodeFloat64TagValues(dst [][]byte, decoder *encoding.BytesBlockDecoder, bb *bytes.Buffer, count uint64) ([][]byte, error) {
	intValuesPtr := generateInt64Slice(int(count))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)

	floatValuesPtr := generateFloat64Slice(int(count))
	floatValues := *floatValuesPtr
	defer releaseFloat64Slice(floatValuesPtr)

	if len(bb.Buf) < 1 {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", 1, len(bb.Buf))
	}

	// Check the first byte to determine the encoding type
	firstByte := encoding.EncodeType(bb.Buf[0])

	if firstByte == encoding.EncodeTypePlain {
		var decodeErr error
		dst, decodeErr = decoder.Decode(dst[:0], bb.Buf[1:], count)
		if decodeErr != nil {
			logger.Panicf("cannot decode values: %v", decodeErr)
		}
		return dst, nil
	}

	// Otherwise, this is float64 int list data with EncodeType at the beginning
	encodeType := firstByte
	const expectedLen = 11
	if len(bb.Buf) < expectedLen {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", expectedLen, len(bb.Buf))
	}
	exp := convert.BytesToInt16(bb.Buf[1:3])
	firstValue := convert.BytesToInt64(bb.Buf[3:11])
	bb.Buf = bb.Buf[11:]

	var err error
	intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, int(count))
	if err != nil {
		logger.Panicf("cannot decode int values: %v", err)
	}

	floatValues, err = encoding.DecimalIntListToFloat64List(floatValues[:0], intValues, exp, int(count))
	if err != nil {
		logger.Panicf("cannot convert DecimalIntList to Float64List: %v", err)
	}

	if uint64(len(floatValues)) != count {
		logger.Panicf("unexpected floatValues length: got %d, expected %d", len(floatValues), count)
	}

	// Convert float64 array to byte array
	if len(dst) < len(floatValues) {
		dst = append(dst, make([][]byte, len(floatValues)-len(dst))...)
	}
	dst = dst[:len(floatValues)]
	for i, v := range floatValues {
		dst[i] = convert.Float64ToBytes(v)
	}
	return dst, nil
}

func decodeDefaultTagValues(dst [][]byte, decoder *encoding.BytesBlockDecoder, bb *bytes.Buffer, count uint64) ([][]byte, error) {
	if len(bb.Buf) < 1 {
		return dst, nil
	}

	encodeType := encoding.EncodeType(bb.Buf[0])
	var err error

	switch encodeType {
	case encoding.EncodeTypeDictionary:
		dict := generateDictionary()
		defer releaseDictionary(dict)
		dst, err = dict.Decode(dst[:0], bb.Buf[1:], count)
	case encoding.EncodeTypePlain:
		dst, err = decoder.Decode(dst[:0], bb.Buf[1:], count)
	default:
		dst, err = decoder.Decode(dst[:0], bb.Buf[1:], count)
	}

	if err != nil {
		logger.Panicf("cannot decode values: %v", err)
	}
	return dst, nil
}
