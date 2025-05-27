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
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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

type tagMetadata struct {
	name string
	dataBlock
	valueType pbv1.ValueType
	encodeBlock
}

type dataBlock struct {
	offset uint64
	size   uint64
}

type encodeBlock struct {
	encodeType encoding.EncodeType
	firstValue int64
}

func (t *tag) mustWriteTo(tm *tagMetadata, tagWriter *writer) {
	tm.reset()

	tm.name = t.name
	tm.valueType = t.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// marshal values
	switch t.valueType {
	case pbv1.ValueTypeInt:
		// convert byte array to int64 array
		intValues := make([]int64, len(t.values))
		for i, v := range t.values {
			intValues[i] = encoding.BytesToInt64(v)
		}
		// use delta encoding for integer column
		var encodeType encoding.EncodeType
		var firstValue int64
		bb.Buf, encodeType, firstValue = encoding.Int64ListToBytes(bb.Buf[:0], intValues)
		tm.encodeType = encodeType
		tm.firstValue = firstValue
	case pbv1.ValueTypeIntFloat:
		// convert byte array to float64 array
		floatValues := make([]float64, len(t.values))
		for i, v := range t.values {
			floatValues[i] = encoding.BytesToFloat64(v)
		}
		// use XOR encoding for float column
		bb.Buf = bb.Buf[:0]
		xorEncoder := encoding.NewXOREncoder(encoding.NewWriter(&bb.Buf))
		// convert float64 to uint64 for encoding
		for _, v := range floatValues {
			xorEncoder.Write(encoding.Float64ToUint64(v))
		}
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
	case pbv1.ValueTypeInt:
		// decode integer type
		intValues := make([]int64, count)
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
	case pbv1.ValueTypeIntFloat:
		// decode float type
		xorDecoder := encoding.NewXORDecoder(encoding.NewReader(bb.Buf))
		t.values = make([][]byte, count)
		for i := uint64(0); i < count; i++ {
			if !xorDecoder.Next() {
				logger.Panicf("%s: cannot decode float value at index %d: %v", reader.Path(), i, xorDecoder.Err())
			}
			// convert uint64 back to float64
			floatVal := encoding.Uint64ToFloat64(xorDecoder.Value())
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
	var err error
	t.values, err = decoder.Decode(t.values[:0], bb.Buf, count)
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
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
