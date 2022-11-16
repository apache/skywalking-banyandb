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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/bit"
	"github.com/apache/skywalking-banyandb/pkg/buffer"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	intEncoderPool = sync.Pool{
		New: newIntEncoder,
	}
	intDecoderPool = sync.Pool{
		New: func() interface{} {
			return &intDecoder{}
		},
	}
)

type intEncoderPoolDelegator struct {
	name string
	pool *sync.Pool
	size int
	fn   ParseInterval
}

func NewIntEncoderPool(name string, size int, fn ParseInterval) SeriesEncoderPool {
	return &intEncoderPoolDelegator{
		name: name,
		pool: &intEncoderPool,
		size: size,
		fn:   fn,
	}
}

func (b *intEncoderPoolDelegator) Get(metadata []byte) SeriesEncoder {
	encoder := b.pool.Get().(*intEncoder)
	encoder.name = b.name
	encoder.size = b.size
	encoder.fn = b.fn
	encoder.Reset(metadata)
	return encoder
}

func (b *intEncoderPoolDelegator) Put(encoder SeriesEncoder) {
	_, ok := encoder.(*intEncoder)
	if ok {
		b.pool.Put(encoder)
	}
}

type intDecoderPoolDelegator struct {
	pool *sync.Pool
	fn   ParseInterval
	name string
	size int
}

func NewIntDecoderPool(name string, size int, fn ParseInterval) SeriesDecoderPool {
	return &intDecoderPoolDelegator{
		name: name,
		pool: &intDecoderPool,
		size: size,
		fn:   fn,
	}
}

func (b *intDecoderPoolDelegator) Get(_ []byte) SeriesDecoder {
	decoder := b.pool.Get().(*intDecoder)
	decoder.name = b.name
	decoder.size = b.size
	decoder.fn = b.fn
	return decoder
}

func (b *intDecoderPoolDelegator) Put(decoder SeriesDecoder) {
	_, ok := decoder.(*intDecoder)
	if ok {
		b.pool.Put(decoder)
	}
}

var _ SeriesEncoder = (*intEncoder)(nil)

type ParseInterval = func(key []byte) time.Duration

type intEncoder struct {
	name      string
	buff      *bytes.Buffer
	bw        *bit.Writer
	values    *XOREncoder
	fn        ParseInterval
	interval  time.Duration
	startTime uint64
	prevTime  uint64
	num       int
	size      int
}

func newIntEncoder() interface{} {
	buff := &bytes.Buffer{}
	bw := bit.NewWriter(buff)
	return &intEncoder{
		buff:   buff,
		bw:     bw,
		values: NewXOREncoder(bw),
	}
}

func (ie *intEncoder) Append(ts uint64, value []byte) {
	if len(value) > 8 {
		return
	}
	if ie.startTime == 0 {
		ie.startTime = ts
		ie.prevTime = ts
	}
	gap := int(ts) - int(ie.prevTime)
	if gap < 0 {
		return
	}
	zeroNum := gap/int(ie.interval) - 1
	for i := 0; i < zeroNum; i++ {
		ie.bw.WriteBool(false)
		ie.num++
	}
	ie.prevTime = ts
	l := len(value)
	ie.bw.WriteBool(l > 0)
	ie.values.Write(convert.BytesToUint64(value))
	ie.num++
	itemsNum.WithLabelValues(ie.name, "int").Inc()
	rawSize.WithLabelValues(ie.name, "int").Add(float64(l + 8))
}

func (ie *intEncoder) IsFull() bool {
	return ie.num >= ie.size
}

func (ie *intEncoder) Reset(key []byte) {
	ie.bw.Reset(nil)
	ie.interval = ie.fn(key)
	ie.startTime = 0
	ie.prevTime = 0
}

func (ie *intEncoder) Encode() ([]byte, error) {
	ie.bw.Flush()
	buffWriter := buffer.NewBufferWriter(ie.buff)
	buffWriter.PutUint64(ie.startTime)
	buffWriter.PutUint16(uint16(ie.size))
	bb := buffWriter.Bytes()
	encodedSize.WithLabelValues(ie.name, "int").Add(float64(len(bb)))
	return bb, nil
}

func (ie *intEncoder) StartTime() uint64 {
	return ie.startTime
}

var _ SeriesDecoder = (*intDecoder)(nil)

type intDecoder struct {
	name      string
	fn        ParseInterval
	size      int
	interval  time.Duration
	startTime uint64
	num       int
	area      []byte
}

func (i *intDecoder) Decode(key, data []byte) error {
	i.interval = i.fn(key)
	i.startTime = binary.LittleEndian.Uint64(data[len(data)-10 : len(data)-2])
	i.num = int(binary.LittleEndian.Uint16(data[len(data)-2:]))
	i.area = data[:len(data)-10]
	return nil
}

func (i intDecoder) Len() int {
	return i.num
}

func (i intDecoder) IsFull() bool {
	return i.num >= i.size
}

func (i intDecoder) Get(ts uint64) ([]byte, error) {
	for iter := i.Iterator(); iter.Next(); {
		if iter.Time() == ts {
			return iter.Val(), nil
		}
	}
	return zeroBytes, nil
}

func (i intDecoder) Iterator() SeriesIterator {
	br := bit.NewReader(bytes.NewReader(i.area))
	return &intIterator{
		startTime: i.startTime,
		interval:  int(i.interval),
		br:        br,
		values:    NewXORDecoder(br),
		size:      i.size,
	}
}

var (
	_         SeriesIterator = (*intIterator)(nil)
	zeroBytes                = convert.Int64ToBytes(0)
	Zero                     = convert.BytesToUint64(zeroBytes)
)

type intIterator struct {
	startTime uint64
	interval  int
	size      int
	br        *bit.Reader
	values    *XORDecoder

	currVal  uint64
	currTime uint64
	index    int
	err      error
}

func (i *intIterator) Next() bool {
	if i.index >= i.size {
		return false
	}
	b, err := i.br.ReadBool()
	if errors.Is(err, io.EOF) {
		return false
	}
	if err != nil {
		i.err = err
		return false
	}
	if b {
		if i.values.Next() {
			i.currVal = i.values.Value()
		}
	} else {
		i.currVal = Zero
	}
	i.currTime = i.startTime + uint64(i.interval*i.index)
	i.index++
	return true
}

func (i *intIterator) Val() []byte {
	return convert.Uint64ToBytes(i.currVal)
}

func (i *intIterator) Time() uint64 {
	return i.currTime
}

func (i *intIterator) Error() error {
	return i.err
}
