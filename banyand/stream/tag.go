// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	stdbytes "bytes"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
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

func generateEncodingWriter(buffer encoding.BufferWriter) *encoding.Writer {
	v := encodingWriterPool.Get()
	if v == nil {
		v = encoding.NewWriter()
	}
	v.Reset(buffer)
	return v
}

func releaseEncodingWriter(writer *encoding.Writer) {
	writer.Flush()
	encodingWriterPool.Put(writer)
}

var (
	int64SlicePool     = pool.Register[*[]int64]("stream-int64Slice")
	float64SlicePool   = pool.Register[*[]float64]("stream-float64Slice")
	encodingWriterPool = pool.Register[*encoding.Writer]("stream-encodingWriter")
)

type tag struct {
	name      string
	values    [][]byte
	valueType pbv1.ValueType
}

func (t *tag) reset() {
	t.name = ""

	values := t.values
	for i := range values {
		values[i] = nil
	}
	t.values = values[:0]
}

func (t *tag) resizeValues(valuesLen int) [][]byte {
	values := t.values
	if n := valuesLen - cap(values); n > 0 {
		values = append(values[:cap(values)], make([][]byte, n)...)
	}
	values = values[:valuesLen]
	t.values = values
	return values
}

func (t *tag) mustWriteTo(tm *tagMetadata, tagWriter *writer) {
	tm.reset()

	tm.name = t.name
	tm.valueType = t.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	t.encodeTagValues(tm, bb)
	tm.size = uint64(len(bb.Buf))
	if tm.size > maxValuesBlockSize {
		logger.Panicf("too large valuesSize: %d bytes; mustn't exceed %d bytes", tm.size, maxValuesBlockSize)
	}
	tm.offset = tagWriter.bytesWritten
	tagWriter.MustWrite(bb.Buf)
}

func (t *tag) encodeTagValues(tm *tagMetadata, bb *bytes.Buffer) {
	encodeDefault := func() {
		bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], t.values)
	}
	// select encoding based on data type
	switch t.valueType {
	case pbv1.ValueTypeInt64:
		// convert byte array to int64 array
		intValuesPtr := generateInt64Slice(len(t.values))
		intValues := *intValuesPtr
		defer func() {
			releaseInt64Slice(intValuesPtr)
		}()

		for i, v := range t.values {
			if v == nil || string(v) == "null" {
				// TODO t.valueType = pbv1.ValueTypeStr
				tm.valueType = pbv1.ValueTypeStr
				encodeDefault()
				return
			}
			if len(v) != 8 {
				var val int64
				for j := 0; j < len(v); j++ {
					val = (val << 8) | int64(v[j])
				}
				intValues[i] = val
			} else {
				intValues[i] = convert.BytesToInt64(v)
			}
		}
		// use delta encoding for integer column
		var encodeType encoding.EncodeType
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
	case pbv1.ValueTypeFloat64:
		// convert byte array to float64 array
		floatValuesPtr := generateFloat64Slice(len(t.values))
		floatValues := *floatValuesPtr
		defer func() {
			releaseFloat64Slice(floatValuesPtr)
		}()

		for i, v := range t.values {
			floatValues[i] = convert.BytesToFloat64(v)
		}
		// use XOR encoding for float column
		bb.Buf = bb.Buf[:0]
		writer := generateEncodingWriter(bb)
		xorEncoder := encoding.NewXOREncoder(writer)
		// convert float64 to uint64 for encoding
		for _, v := range floatValues {
			xorEncoder.Write(math.Float64bits(v))
		}
		releaseEncodingWriter(writer)
	default:
		encodeDefault()
	}
}

func (t *tag) mustReadValues(decoder *encoding.BytesBlockDecoder, reader fs.Reader, cm tagMetadata, count uint64) {
	t.name = cm.name
	t.valueType = cm.valueType
	if t.valueType == pbv1.ValueTypeUnknown {
		for i := uint64(0); i < count; i++ {
			t.values = append(t.values, nil)
		}
		return
	}

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}
	bb.Buf = bytes.ResizeOver(bb.Buf, int(valuesSize))
	fs.MustReadData(reader, int64(cm.offset), bb.Buf)
	t.decodeTagValues(decoder, reader.Path(), count, bb)
}

func (t *tag) mustSeqReadValues(decoder *encoding.BytesBlockDecoder, reader *seqReader, cm tagMetadata, count uint64) {
	t.name = cm.name
	t.valueType = cm.valueType
	if cm.offset != reader.bytesRead {
		logger.Panicf("%s: offset mismatch: %d vs %d", reader.Path(), cm.offset, reader.bytesRead)
	}
	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	bb.Buf = bytes.ResizeOver(bb.Buf, int(valuesSize))
	reader.mustReadFull(bb.Buf)
	t.decodeTagValues(decoder, reader.Path(), count, bb)
}

func (t *tag) decodeTagValues(decoder *encoding.BytesBlockDecoder, path string, count uint64, bb *bytes.Buffer) {
	switch t.valueType {
	case pbv1.ValueTypeInt64:
		// decode integer type
		intValuesPtr := generateInt64Slice(int(count))
		intValues := *intValuesPtr
		defer func() {
			releaseInt64Slice(intValuesPtr)
		}()

		const expectedLen = 9
		if len(bb.Buf) < expectedLen {
			logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", expectedLen, len(bb.Buf))
		}
		encodeType := encoding.EncodeType(bb.Buf[0])
		firstValue := convert.BytesToInt64(bb.Buf[1:9])
		bb.Buf = bb.Buf[9:]
		var err error
		intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, int(count))
		if err != nil {
			logger.Panicf("%s: cannot decode int values: %v", path, err)
		}
		// convert int64 array to byte array
		t.values = make([][]byte, count)
		for i, v := range intValues {
			t.values[i] = convert.Int64ToBytes(v)
		}
	case pbv1.ValueTypeFloat64:
		// decode float type
		reader := encoding.NewReader(stdbytes.NewReader(bb.Buf))
		xorDecoder := encoding.NewXORDecoder(reader)
		t.values = make([][]byte, count)
		for i := uint64(0); i < count; i++ {
			if !xorDecoder.Next() {
				logger.Panicf("cannot decode float value at index %d: %v", i, xorDecoder.Err())
			}
			val := xorDecoder.Value()
			// convert uint64 back to float64
			floatVal := math.Float64frombits(val)
			t.values[i] = convert.Float64ToBytes(floatVal)
		}
	default:
		var err error
		t.values, err = decoder.Decode(t.values[:0], bb.Buf, count)
		if err != nil {
			logger.Panicf("%s: cannot decode values: %v", path, err)
		}
	}
}

var bigValuePool = bytes.NewBufferPool("stream-big-value")

type tagFamily struct {
	name string
	tags []tag
}

func (tf *tagFamily) reset() {
	tf.name = ""

	tags := tf.tags
	for i := range tags {
		tags[i].reset()
	}
	tf.tags = tags[:0]
}

func (tf *tagFamily) resizeTags(tagsLen int) []tag {
	tags := tf.tags
	if n := tagsLen - cap(tags); n > 0 {
		tags = append(tags[:cap(tags)], make([]tag, n)...)
	}
	tags = tags[:tagsLen]
	tf.tags = tags
	return tags
}
