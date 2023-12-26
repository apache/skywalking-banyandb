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

func (t *tag) mustWriteTo(ch *tagMetadata, tagWriter *writer) {
	ch.reset()

	ch.name = t.name
	ch.valueType = t.valueType

	// TODO: encoding values based on value type

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)

	// marshal values
	bb.Buf = encoding.EncodeBytesBlock(bb.Buf[:0], t.values)
	ch.size = uint64(len(bb.Buf))
	if ch.size > maxValuesBlockSize {
		logger.Panicf("too valuesSize: %d bytes; mustn't exceed %d bytes", ch.size, maxValuesBlockSize)
	}
	ch.offset = tagWriter.bytesWritten
	tagWriter.MustWrite(bb.Buf)
}

func (t *tag) mustReadValues(decoder *encoding.BytesBlockDecoder, reader fs.Reader, cm tagMetadata, count uint64) {
	t.name = cm.name
	t.valueType = cm.valueType

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}
	bb.Buf = bytes.ResizeOver(bb.Buf, int(valuesSize))
	fs.MustReadData(reader, int64(cm.offset), bb.Buf)
	var err error
	t.values, err = decoder.Decode(t.values[:0], bb.Buf, count)
	if err != nil {
		logger.Panicf("%s: cannot decode values: %v", reader.Path(), err)
	}
}

var bigValuePool bytes.BufferPool

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
