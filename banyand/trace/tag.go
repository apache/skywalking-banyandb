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

package trace

import (
	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	pkgencoding "github.com/apache/skywalking-banyandb/pkg/encoding"
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

func (t *tag) mustWriteTo(tm *tagMetadata, tagWriter *writer) {
	tm.reset()

	tm.name = t.name
	tm.valueType = t.valueType

	// Use shared encoding module
	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	err := internalencoding.EncodeTagValues(bb, t.values, t.valueType)
	if err != nil {
		logger.Panicf("failed to encode tag values: %v", err)
	}

	tm.size = uint64(len(bb.Buf))
	if tm.size > maxValuesBlockSize {
		logger.Panicf("too large valuesSize: %d bytes; mustn't exceed %d bytes", tm.size, maxValuesBlockSize)
	}
	tm.offset = tagWriter.bytesWritten
	tagWriter.MustWrite(bb.Buf)
}

func (t *tag) mustReadValues(decoder *pkgencoding.BytesBlockDecoder, reader fs.Reader, cm tagMetadata, count uint64) {
	t.name = cm.name
	t.valueType = cm.valueType
	if t.valueType == pbv1.ValueTypeUnknown {
		for range count {
			t.values = append(t.values, nil)
		}
		return
	}

	valuesSize := cm.size
	if valuesSize > maxValuesBlockSize {
		logger.Panicf("%s: block size cannot exceed %d bytes; got %d bytes", reader.Path(), maxValuesBlockSize, valuesSize)
	}

	bb := bigValuePool.Generate()
	defer bigValuePool.Release(bb)
	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(valuesSize))
	fs.MustReadData(reader, int64(cm.offset), bb.Buf)

	// Use shared decoding module
	var err error
	t.values, err = internalencoding.DecodeTagValues(t.values, decoder, bb, t.valueType, int(count))
	if err != nil {
		logger.Panicf("%s: failed to decode tag values: %v", reader.Path(), err)
	}
}

func (t *tag) mustSeqReadValues(decoder *pkgencoding.BytesBlockDecoder, reader *seqReader, cm tagMetadata, count uint64) {
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
	bb.Buf = bytes.ResizeOver(bb.Buf[:0], int(valuesSize))
	reader.mustReadFull(bb.Buf)

	// Use shared decoding module
	var err error
	t.values, err = internalencoding.DecodeTagValues(t.values, decoder, bb, t.valueType, int(count))
	if err != nil {
		logger.Panicf("%s: failed to decode tag values: %v", reader.Path(), err)
	}
}

var bigValuePool = bytes.NewBufferPool("trace-big-value")
