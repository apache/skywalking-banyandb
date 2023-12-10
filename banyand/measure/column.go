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

type Column struct {
	Name      string
	ValueType pbv1.ValueType
	Values    [][]byte
}

func (c *Column) reset() {
	c.Name = ""

	values := c.Values
	for i := range values {
		values[i] = nil
	}
	c.Values = values[:0]
}

func (c *Column) resizeValues(valuesLen int) [][]byte {
	values := c.Values
	if n := valuesLen - cap(values); n > 0 {
		values = append(values[:cap(values)], make([][]byte, n)...)
	}
	values = values[:valuesLen]
	c.Values = values
	return values
}

func (c *Column) mustWriteTo(ch *columnMetadata, columnWriter *writer) {
	ch.reset()

	ch.name = c.Name
	ch.valueType = c.ValueType

	// remove value type from values
	for i := range c.Values {
		c.Values[i] = c.Values[i][1:]
	}

	// TODO: encoding values based on value type

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// marshal values
	bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], c.Values)
	ch.size = uint64(len(bb.Buf))
	if ch.size > maxValuesBlockSize {
		logger.Panicf("too valuesSize: %d bytes; mustn't exceed %d bytes", ch.size, maxValuesBlockSize)
	}
	ch.offset = columnWriter.bytesWritten
	columnWriter.MustWrite(bb.Buf)
}

func (c *Column) mustReadValues(decoder *encoding.BytesBlockDecoder, reader fs.Reader, cm columnMetadata, count uint64) {
	c.Name = cm.name
	c.ValueType = cm.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}
	bb.Buf = bytes.ResizeOver(bb.Buf, int(valuesSize))
	fs.MustReadData(reader, int64(cm.offset), bb.Buf)
	var err error
	c.Values, err = decoder.Decode(c.Values[:0], bb.Buf, count)
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
	}
}

var bigValuePool bytes.BufferPool

type ColumnFamily struct {
	Name    string
	Columns []Column
}

func (cf *ColumnFamily) reset() {
	cf.Name = ""

	columns := cf.Columns
	for i := range columns {
		columns[i].reset()
	}
	cf.Columns = columns[:0]
}

func (cf *ColumnFamily) resizeColumns(columnsLen int) []Column {
	columns := cf.Columns
	if n := columnsLen - cap(columns); n > 0 {
		columns = append(columns[:cap(columns)], make([]Column, n)...)
	}
	columns = columns[:columnsLen]
	cf.Columns = columns
	return columns
}
