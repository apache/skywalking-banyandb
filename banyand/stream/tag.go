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
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

var (
	int64SlicePool = sync.Pool{
		New: func() interface{} {
			return make([]int64, 0, 1024)
		},
	}
	float64SlicePool = sync.Pool{
		New: func() interface{} {
			return make([]float64, 0, 1024)
		},
	}
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
	// buffer
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// marshal values
	// select encoding based on data type
	switch t.valueType {
	case pbv1.ValueTypeInt64:
		// convert byte array to int64 array
		intValues := int64SlicePool.Get().([]int64)
		intValues = intValues[:0]
		if cap(intValues) < len(t.values) {
			intValues = make([]int64, len(t.values))
		} else {
			intValues = intValues[:len(t.values)]
		}
		defer int64SlicePool.Put(intValues)

		for i, v := range t.values {
			if len(v) != 8 {
				// 如果长度不是8，直接使用原始数据
				// 将原始数据转换为int64
				var val int64
				for j := 0; j < len(v); j++ {
					val = (val << 8) | int64(v[j])
				}
				intValues[i] = val
			} else {
				intValues[i] = encoding.BytesToInt64(v)
			}
		}
		// use delta encoding for integer column
		var encodeType encoding.EncodeType
		var firstValue int64
		bb.Buf, encodeType, firstValue = encoding.Int64ListToBytes(bb.Buf[:0], intValues)
		tm.encodeType = encodeType
		tm.firstValue = firstValue
		if tm.encodeType == encoding.EncodeTypeUnknown {
			logger.Panicf("invalid encode type for int64 values")
		}
	case pbv1.ValueTypeFloat64:
		// convert byte array to float64 array
		floatValues := float64SlicePool.Get().([]float64)
		floatValues = floatValues[:0]
		if cap(floatValues) < len(t.values) {
			floatValues = make([]float64, len(t.values))
		} else {
			floatValues = floatValues[:len(t.values)]
		}
		defer float64SlicePool.Put(floatValues)

		for i, v := range t.values {
			floatValues[i] = encoding.BytesToFloat64(v)
		}
		// use XOR encoding for float column
		bb.Buf = bb.Buf[:0]
		writer := encoding.NewWriter()
		writer.Reset(bytes.NewByteSliceWriter(&bb.Buf))
		xorEncoder := encoding.NewXOREncoder(writer)
		// convert float64 to uint64 for encoding
		for _, v := range floatValues {
			xorEncoder.Write(encoding.Float64ToUint64(v))
		}
		xorEncoder.Close()
	default:
		bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], t.values)
	}

	tm.size = uint64(len(bb.Buf))
	if tm.size > maxValuesBlockSize {
		logger.Panicf("too valuesSize: %d bytes; mustn't exceed %d bytes", tm.size, maxValuesBlockSize)
	}
	tm.offset = tagWriter.bytesWritten
	tagWriter.MustWrite(bb.Buf)
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

	switch t.valueType {
	case pbv1.ValueTypeInt64:
		// decode integer type
		intValues := int64SlicePool.Get().([]int64)
		intValues = intValues[:0]
		if cap(intValues) < int(count) {
			intValues = make([]int64, count)
		} else {
			intValues = intValues[:count]
		}
		defer int64SlicePool.Put(intValues)

		var err error
		intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, cm.encodeType, cm.firstValue, int(count))
		if err != nil {
			logger.Panicf("%s: cannot decode int values: %v", reader.Path(), err)
		}
		// convert int64 array to byte array
		t.values = make([][]byte, count)
		for i, v := range intValues {
			t.values[i] = encoding.Int64ToBytes(nil, v)
		}
	case pbv1.ValueTypeFloat64:
		// decode float type
		reader := encoding.NewReader(bytes.NewByteSliceReader(bb.Buf))
		xorDecoder := encoding.NewXORDecoder(reader)
		t.values = make([][]byte, count)
		for i := uint64(0); i < count; i++ {
			if !xorDecoder.Next() {
				logger.Panicf("cannot decode float value at index %d: %v", i, xorDecoder.Err())
			}
			val := xorDecoder.Value()
			// convert uint64 back to float64
			floatVal := encoding.Uint64ToFloat64(val)
			t.values[i] = encoding.Float64ToBytes(nil, floatVal)
		}
	default:
		var err error
		t.values, err = decoder.Decode(t.values[:0], bb.Buf, count)
		if err != nil {
			logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
		}
	}
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

	switch t.valueType {
	case pbv1.ValueTypeInt64:
		// decode integer type
		intValues := int64SlicePool.Get().([]int64)
		intValues = intValues[:0]
		if cap(intValues) < int(count) {
			intValues = make([]int64, count)
		} else {
			intValues = intValues[:count]
		}
		defer int64SlicePool.Put(intValues)

		var err error
		intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, cm.encodeType, cm.firstValue, int(count))
		if err != nil {
			logger.Panicf("%s: mustSeqReadValues cannot decode int values: %v", reader.Path(), err)
		}
		// convert int64 array to byte array
		t.values = make([][]byte, count)
		for i, v := range intValues {
			t.values[i] = encoding.Int64ToBytes(nil, v)
		}
	case pbv1.ValueTypeFloat64:
		// decode float type
		reader := encoding.NewReader(bytes.NewByteSliceReader(bb.Buf))
		xorDecoder := encoding.NewXORDecoder(reader)
		t.values = make([][]byte, count)
		for i := uint64(0); i < count; i++ {
			if !xorDecoder.Next() {
				logger.Panicf("mustSeqReadValues cannot decode float value at index %d: %v", i, xorDecoder.Err())
			}
			val := xorDecoder.Value()
			floatVal := encoding.Uint64ToFloat64(val)
			t.values[i] = encoding.Float64ToBytes(nil, floatVal)
		}
	default:
		var err error
		t.values, err = decoder.Decode(t.values[:0], bb.Buf, count)
		if err != nil {
			logger.Panicf("%s: mustSeqReadValues cannot decode values: %v", reader.Path(), err)
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
