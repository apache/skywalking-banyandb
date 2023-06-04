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
	"io"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var (
	encoderPool = sync.Pool{
		New: newEncoder,
	}
	decoderPool = sync.Pool{
		New: func() interface{} {
			return &decoder{}
		},
	}

	errInvalidValue = errors.New("invalid encoded value")
	errNoData       = errors.New("there is no data")
)

type encoderPoolDelegator struct {
	pool *sync.Pool
	fn   ParseInterval
	name string
	size int
}

// NewEncoderPool returns a SeriesEncoderPool which provides int-based xor encoders.
func NewEncoderPool(name string, size int, fn ParseInterval) SeriesEncoderPool {
	return &encoderPoolDelegator{
		name: name,
		pool: &encoderPool,
		size: size,
		fn:   fn,
	}
}

func (b *encoderPoolDelegator) Get(metadata []byte, buffer BufferWriter) SeriesEncoder {
	encoder := b.pool.Get().(*encoder)
	encoder.name = b.name
	encoder.size = b.size
	encoder.fn = b.fn
	encoder.Reset(metadata, buffer)
	return encoder
}

func (b *encoderPoolDelegator) Put(seriesEncoder SeriesEncoder) {
	_, ok := seriesEncoder.(*encoder)
	if ok {
		b.pool.Put(seriesEncoder)
	}
}

type decoderPoolDelegator struct {
	pool *sync.Pool
	fn   ParseInterval
	name string
	size int
}

// NewDecoderPool returns a SeriesDecoderPool which provides int-based xor decoders.
func NewDecoderPool(name string, size int, fn ParseInterval) SeriesDecoderPool {
	return &decoderPoolDelegator{
		name: name,
		pool: &decoderPool,
		size: size,
		fn:   fn,
	}
}

func (b *decoderPoolDelegator) Get(_ []byte) SeriesDecoder {
	decoder := b.pool.Get().(*decoder)
	decoder.name = b.name
	decoder.size = b.size
	decoder.fn = b.fn
	return decoder
}

func (b *decoderPoolDelegator) Put(seriesDecoder SeriesDecoder) {
	_, ok := seriesDecoder.(*decoder)
	if ok {
		b.pool.Put(seriesDecoder)
	}
}

var _ SeriesEncoder = (*encoder)(nil)

// ParseInterval parses the interval rule from the key in a kv pair.
type ParseInterval = func(key []byte) time.Duration

type encoder struct {
	buff      BufferWriter
	bw        *Writer
	values    *XOREncoder
	fn        ParseInterval
	name      string
	interval  time.Duration
	startTime uint64
	prevTime  uint64
	num       int
	size      int
}

func newEncoder() interface{} {
	bw := NewWriter()
	return &encoder{
		bw:     bw,
		values: NewXOREncoder(bw),
	}
}

func (ie *encoder) Append(ts uint64, value []byte) {
	if len(value) > 8 {
		return
	}
	if ie.startTime == 0 {
		ie.startTime = ts
		ie.prevTime = ts
	} else if ie.startTime > ts {
		ie.startTime = ts
	}
	gap := int(ie.prevTime) - int(ts)
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
}

func (ie *encoder) IsFull() bool {
	return ie.num >= ie.size
}

func (ie *encoder) Reset(key []byte, buffer BufferWriter) {
	ie.buff = buffer
	ie.bw.Reset(buffer)
	ie.interval = ie.fn(key)
	ie.startTime = 0
	ie.prevTime = 0
	ie.num = 0
	ie.values = NewXOREncoder(ie.bw)
}

func (ie *encoder) Encode() error {
	ie.bw.Flush()
	buffWriter := NewPacker(ie.buff)
	buffWriter.PutUint64(ie.startTime)
	buffWriter.PutUint16(uint16(ie.num))
	return nil
}

func (ie *encoder) StartTime() uint64 {
	return ie.startTime
}

var _ SeriesDecoder = (*decoder)(nil)

type decoder struct {
	fn        ParseInterval
	name      string
	area      []byte
	size      int
	interval  time.Duration
	startTime uint64
	num       int
}

func (i *decoder) Decode(key, data []byte) error {
	if len(data) < 10 {
		return errInvalidValue
	}
	i.interval = i.fn(key)
	i.startTime = binary.LittleEndian.Uint64(data[len(data)-10 : len(data)-2])
	i.num = int(binary.LittleEndian.Uint16(data[len(data)-2:]))
	i.area = data[:len(data)-10]
	return nil
}

func (i decoder) Len() int {
	return i.num
}

func (i decoder) IsFull() bool {
	return i.num >= i.size
}

func (i decoder) Get(ts uint64) ([]byte, error) {
	for iter := i.Iterator(); iter.Next(); {
		if iter.Time() == ts {
			return iter.Val(), nil
		}
	}
	return nil, errors.WithMessagef(errNoData, "ts:%d", ts)
}

func (i decoder) Range() (start, end uint64) {
	return i.startTime, i.startTime + uint64(i.num-1)*uint64(i.interval)
}

func (i decoder) Iterator() SeriesIterator {
	br := NewReader(bytes.NewReader(i.area))
	return &intIterator{
		endTime:  i.startTime + uint64(i.num*int(i.interval)),
		interval: int(i.interval),
		br:       br,
		values:   NewXORDecoder(br),
		size:     i.num,
	}
}

var _ SeriesIterator = (*intIterator)(nil)

type intIterator struct {
	err      error
	br       *Reader
	values   *XORDecoder
	endTime  uint64
	interval int
	size     int
	currVal  uint64
	currTime uint64
	index    int
}

func (i *intIterator) Next() bool {
	if i.index >= i.size {
		return false
	}
	var b bool
	var err error
	for !b {
		b, err = i.br.ReadBool()
		if errors.Is(err, io.EOF) {
			return false
		}
		if err != nil {
			i.err = err
			return false
		}
		i.index++
		i.currTime = i.endTime - uint64(i.interval*i.index)
	}
	if i.values.Next() {
		i.currVal = i.values.Value()
	}
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
