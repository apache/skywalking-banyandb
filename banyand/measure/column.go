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
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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

	// TODO: encoding values based on value type

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// marshal values
	bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], c.values)
	cm.size = uint64(len(bb.Buf))
	if cm.size > maxValuesBlockSize {
		logger.Panicf("too valuesSize: %d bytes; mustn't exceed %d bytes", cm.size, maxValuesBlockSize)
	}
	cm.offset = columnWriter.bytesWritten
	columnWriter.MustWrite(bb.Buf)
}

func (c *column) mustReadValues(decoder *encoding.BytesBlockDecoder, reader fs.Reader, cm columnMetadata, count uint64) {
	c.name = cm.name
	c.valueType = cm.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}
	bb.Buf = bytes.ResizeOver(bb.Buf, int(valuesSize))
	fs.MustReadData(reader, int64(cm.offset), bb.Buf)
	var err error
	c.values, err = decoder.Decode(c.values[:0], bb.Buf, count)
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
	}
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
	var err error
	c.values, err = decoder.Decode(c.values[:0], bb.Buf, count)
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
	}
}

var bigValuePool bytes.BufferPool

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
