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

package measure

import (
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

var (
	int64SlicePool   = pool.Register[*[]int64]("measure-int64Slice")
	float64SlicePool = pool.Register[*[]float64]("measure-float64Slice")
	dictionaryPool   = pool.Register[*encoding.Dictionary]("measure-dictionary")
)

type column struct {
	name      string
	values    [][]byte
	valueType pbv1.ValueType
}

func (c *column) reset() {
	c.name = ""

	values := c.values
	for i := range values {
		values[i] = nil
	}
	c.values = values[:0]
}

func (c *column) resizeValues(valuesLen int) [][]byte {
	values := c.values
	if n := valuesLen - cap(values); n > 0 {
		values = append(values[:cap(values)], make([][]byte, n)...)
	}
	values = values[:valuesLen]
	c.values = values
	return values
}

func (c *column) mustWriteTo(cm *columnMetadata, columnWriter *writer) {
	cm.reset()

	cm.name = c.name
	cm.valueType = c.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// select encoding based on data type
	switch c.valueType {
	case pbv1.ValueTypeInt64:
		c.encodeInt64Column(bb)
	case pbv1.ValueTypeFloat64:
		c.encodeFloat64Column(bb)
	default:
		c.encodeDefault(bb)
	}
	cm.size = uint64(len(bb.Buf))
	if cm.size > maxValuesBlockSize {
		logger.Panicf("too large valuesSize: %d bytes; mustn't exceed %d bytes", cm.size, maxValuesBlockSize)
	}
	cm.offset = columnWriter.bytesWritten
	columnWriter.MustWrite(bb.Buf)
}

func (c *column) encodeInt64Column(bb *bytes.Buffer) {
	// convert byte array to int64 array
	intValuesPtr := generateInt64Slice(len(c.values))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)
	var encodeType encoding.EncodeType

	for i, v := range c.values {
		if v == nil || string(v) == "null" {
			c.encodeDefault(bb)
			encodeType = encoding.EncodeTypePlain
			// Prepend encodeType (1 byte) to the beginning
			bb.Buf = append([]byte{byte(encodeType)}, bb.Buf...)
			return
		}
		if len(v) != 8 {
			logger.Panicf("invalid value length at index %d: expected 8 bytes, got %d", i, len(v))
		}
		intValues[i] = convert.BytesToInt64(v)
	}
	// use delta encoding for integer column
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
}

func (c *column) encodeFloat64Column(bb *bytes.Buffer) {
	// convert byte array to float64 array
	intValuesPtr := generateInt64Slice(len(c.values))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)

	floatValuesPtr := generateFloat64Slice(len(c.values))
	floatValues := *floatValuesPtr
	defer releaseFloat64Slice(floatValuesPtr)

	var encodeType encoding.EncodeType

	doEncodeDefault := func() {
		c.encodeDefault(bb)
		encodeType = encoding.EncodeTypePlain
		// Prepend encodeType (1 byte) to the beginning
		bb.Buf = append([]byte{byte(encodeType)}, bb.Buf...)
	}

	for i, v := range c.values {
		if v == nil || string(v) == "null" {
			doEncodeDefault()
			return
		}
		if len(v) != 8 {
			logger.Panicf("invalid value length at index %d: expected 8 bytes, got %d", i, len(v))
		}
		floatValues[i] = convert.BytesToFloat64(v)
	}
	intValues, exp, err := encoding.Float64ListToDecimalIntList(intValues[:0], floatValues)
	if err != nil {
		logger.Errorf("cannot convert Float64List to DecimalIntList : %v", err)
		doEncodeDefault()
		return
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
}

func (c *column) encodeDefault(bb *bytes.Buffer) {
	dict := generateDictionary()
	defer releaseDictionary(dict)
	for _, v := range c.values {
		if !dict.Add(v) {
			bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], c.values)
			bb.Buf = append([]byte{byte(encoding.EncodeTypePlain)}, bb.Buf...)
			return
		}
	}
	bb.Buf = dict.Encode(bb.Buf[:0])
	bb.Buf = append([]byte{byte(encoding.EncodeTypeDictionary)}, bb.Buf...)
}

func (c *column) mustReadValues(decoder *encoding.BytesBlockDecoder, reader fs.Reader, cm columnMetadata, count uint64) {
	c.name = cm.name
	c.valueType = cm.valueType
	if c.valueType == pbv1.ValueTypeUnknown {
		for i := uint64(0); i < count; i++ {
			c.values = append(c.values, nil)
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
	c.decodeColumnValues(decoder, reader.Path(), count, bb)
}

func (c *column) mustSeqReadValues(decoder *encoding.BytesBlockDecoder, reader *seqReader, cm columnMetadata, count uint64) {
	c.name = cm.name
	c.valueType = cm.valueType
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
	c.decodeColumnValues(decoder, reader.Path(), count, bb)
}

func (c *column) decodeColumnValues(decoder *encoding.BytesBlockDecoder, path string, count uint64, bb *bytes.Buffer) {
	switch c.valueType {
	case pbv1.ValueTypeInt64:
		c.decodeInt64Column(decoder, path, count, bb)
	case pbv1.ValueTypeFloat64:
		c.decodeFloat64Column(decoder, path, count, bb)
	default:
		c.decodeDefault(decoder, bb, count, path)
	}
}

func (c *column) decodeInt64Column(decoder *encoding.BytesBlockDecoder, path string, count uint64, bb *bytes.Buffer) {
	// decode integer type
	intValuesPtr := generateInt64Slice(int(count))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)

	if len(bb.Buf) < 1 {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", 1, len(bb.Buf))
	}
	encodeType := encoding.EncodeType(bb.Buf[0])
	if encodeType == encoding.EncodeTypePlain {
		bb.Buf = bb.Buf[1:]
		c.decodeDefault(decoder, bb, count, path)
		return
	}

	const expectedLen = 9
	if len(bb.Buf) < expectedLen {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", expectedLen, len(bb.Buf))
	}
	firstValue := convert.BytesToInt64(bb.Buf[1:9])
	bb.Buf = bb.Buf[9:]
	var err error
	intValues, err = encoding.BytesToInt64List(intValues[:0], bb.Buf, encodeType, firstValue, int(count))
	if err != nil {
		logger.Panicf("%s: cannot decode int values: %v", path, err)
	}
	// convert int64 array to byte array
	c.values = make([][]byte, count)
	for i, v := range intValues {
		c.values[i] = convert.Int64ToBytes(v)
	}
}

func (c *column) decodeFloat64Column(decoder *encoding.BytesBlockDecoder, path string, count uint64, bb *bytes.Buffer) {
	// decode float type
	intValuesPtr := generateInt64Slice(int(count))
	intValues := *intValuesPtr
	defer releaseInt64Slice(intValuesPtr)
	floatValuesPtr := generateFloat64Slice(int(count))
	floatValues := *floatValuesPtr
	defer releaseFloat64Slice(floatValuesPtr)

	if len(bb.Buf) < 1 {
		logger.Panicf("bb.Buf length too short: expect at least %d bytes, but got %d bytes", 1, len(bb.Buf))
	}
	encodeType := encoding.EncodeType(bb.Buf[0])
	if encodeType == encoding.EncodeTypePlain {
		bb.Buf = bb.Buf[1:]
		c.decodeDefault(decoder, bb, count, path)
		return
	}

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
		logger.Panicf("%s: cannot decode int values: %v", path, err)
	}
	floatValues, err = encoding.DecimalIntListToFloat64List(floatValues[:0], intValues, exp, int(count))
	if err != nil {
		logger.Panicf("cannot convert DecimalIntList to Float64List: %v", err)
	}
	if uint64(len(floatValues)) != count {
		logger.Panicf("unexpected floatValues length: got %d, expected %d", len(floatValues), count)
	}
	// convert float64 array to byte array
	c.values = make([][]byte, count)
	for i, v := range floatValues {
		c.values[i] = convert.Float64ToBytes(v)
	}
}

func (c *column) decodeDefault(decoder *encoding.BytesBlockDecoder, bb *bytes.Buffer, count uint64, path string) {
	encodeType := encoding.EncodeType(bb.Buf[0])
	var err error
	if encodeType == encoding.EncodeTypeDictionary {
		dict := generateDictionary()
		defer releaseDictionary(dict)
		c.values, err = dict.Decode(c.values[:0], bb.Buf[1:], count)
	} else {
		c.values, err = decoder.Decode(c.values[:0], bb.Buf, count)
	}
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", path, err)
	}
}

var bigValuePool = bytes.NewBufferPool("measure-big-value")

type columnFamily struct {
	name    string
	columns []column
}

func (cf *columnFamily) reset() {
	cf.name = ""

	columns := cf.columns
	for i := range columns {
		columns[i].reset()
	}
	cf.columns = columns[:0]
}

func (cf *columnFamily) resizeColumns(columnsLen int) []column {
	columns := cf.columns
	if n := columnsLen - cap(columns); n > 0 {
		columns = append(columns[:cap(columns)], make([]column, n)...)
	}
	columns = columns[:columnsLen]
	cf.columns = columns
	return columns
}
