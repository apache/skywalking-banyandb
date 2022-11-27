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
	"fmt"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/buffer"
)

var (
	plainEncoderPool = sync.Pool{
		New: newPlainEncoder,
	}
	plainDecoderPool = sync.Pool{
		New: func() interface{} {
			return &plainDecoder{}
		},
	}
)

type plainEncoderPoolDelegator struct {
	name string
	pool *sync.Pool
	size int
}

func NewPlainEncoderPool(name string, size int) SeriesEncoderPool {
	return &plainEncoderPoolDelegator{
		name: name,
		pool: &plainEncoderPool,
		size: size,
	}
}

func (b *plainEncoderPoolDelegator) Get(metadata []byte) SeriesEncoder {
	encoder := b.pool.Get().(*plainEncoder)
	encoder.name = b.name
	encoder.Reset(metadata)
	encoder.valueSize = b.size
	return encoder
}

func (b *plainEncoderPoolDelegator) Put(encoder SeriesEncoder) {
	_, ok := encoder.(*plainEncoder)
	if ok {
		b.pool.Put(encoder)
	}
}

type plainDecoderPoolDelegator struct {
	name string
	pool *sync.Pool
	size int
}

func NewPlainDecoderPool(name string, size int) SeriesDecoderPool {
	return &plainDecoderPoolDelegator{
		name: name,
		pool: &plainDecoderPool,
		size: size,
	}
}

func (b *plainDecoderPoolDelegator) Get(_ []byte) SeriesDecoder {
	decoder := b.pool.Get().(*plainDecoder)
	decoder.name = b.name
	decoder.valueSize = b.size
	return decoder
}

func (b *plainDecoderPoolDelegator) Put(decoder SeriesDecoder) {
	_, ok := decoder.(*plainDecoder)
	if ok {
		b.pool.Put(decoder)
	}
}

var (
	zstdDecoder, _               = zstd.NewReader(nil)
	zstdEncoder, _               = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	_              SeriesEncoder = (*plainEncoder)(nil)
	_              SeriesDecoder = (*plainDecoder)(nil)
)

// plainEncoder backport to reduced value
type plainEncoder struct {
	name      string
	tsBuff    *buffer.Writer
	valBuff   *buffer.Writer
	len       uint32
	num       uint32
	startTime uint64
	valueSize int
}

func newPlainEncoder() interface{} {
	return &plainEncoder{
		tsBuff:  buffer.NewBufferWriter(&bytes.Buffer{}),
		valBuff: buffer.NewBufferWriter(&bytes.Buffer{}),
	}
}

func (t *plainEncoder) Append(ts uint64, value []byte) {
	if t.startTime == 0 {
		t.startTime = ts
	} else if t.startTime > ts {
		t.startTime = ts
	}
	vLen := len(value)
	offset := uint32(t.valBuff.Len())
	t.valBuff.PutUint32(uint32(vLen))
	t.valBuff.Write(value)
	t.tsBuff.PutUint64(ts)
	t.tsBuff.PutUint32(offset)
	t.num++
}

func (t *plainEncoder) IsFull() bool {
	return t.valBuff.Len() >= t.valueSize
}

func (t *plainEncoder) Reset(_ []byte) {
	t.tsBuff.Reset()
	t.valBuff.Reset()
	t.num = 0
	t.startTime = 0
}

func (t *plainEncoder) Encode() ([]byte, error) {
	if t.tsBuff.Len() < 1 {
		return nil, ErrEncodeEmpty
	}
	val := t.valBuff.Bytes()
	t.len = uint32(len(val))
	t.tsBuff.WriteTo(t.valBuff)
	t.valBuff.PutUint32(t.num)
	t.valBuff.PutUint32(t.len)
	data := t.valBuff.Bytes()
	l := len(data)
	dst := make([]byte, 0, compressBound(l))
	dst = zstdEncoder.EncodeAll(data, dst)
	result := buffer.NewBufferWriter(bytes.NewBuffer(make([]byte, 0, len(dst)+2)))
	result.Write(dst)
	result.PutUint16(uint16(l))
	dd := result.Bytes()
	itemsNum.WithLabelValues(t.name, "plain").Inc()
	rawSize.WithLabelValues(t.name, "plain").Add(float64(l))
	encodedSize.WithLabelValues(t.name, "plain").Add(float64(len(dd)))
	return dd, nil
}

func compressBound(srcSize int) int {
	return srcSize + (srcSize >> 8)
}

func (t *plainEncoder) StartTime() uint64 {
	return t.startTime
}

const (
	// TsLen equals ts(uint64) + data_offset(uint32)
	TsLen = 8 + 4
)

var ErrInvalidValue = errors.New("invalid encoded value")

// plainDecoder decodes encoded time index
type plainDecoder struct {
	name      string
	ts        []byte
	val       []byte
	len       uint32
	num       uint32
	valueSize int
}

func (t *plainDecoder) Len() int {
	return int(t.num)
}

func (t *plainDecoder) Decode(_, rawData []byte) (err error) {
	if len(rawData) < 2 {
		return ErrInvalidValue
	}
	var data []byte
	size := binary.LittleEndian.Uint16(rawData[len(rawData)-2:])
	if data, err = zstdDecoder.DecodeAll(rawData[:len(rawData)-2], make([]byte, 0, size)); err != nil {
		return errors.Wrap(err, "plain decoder fails to decode")
	}
	l := uint32(len(data))
	if l <= 8 {
		return ErrInvalidValue
	}
	lenOffset := len(data) - 4
	numOffset := lenOffset - 4
	t.num = binary.LittleEndian.Uint32(data[numOffset:lenOffset])
	t.len = binary.LittleEndian.Uint32(data[lenOffset:])
	if l <= t.len+8 {
		return ErrInvalidValue
	}
	t.val = data[:t.len]
	t.ts = data[t.len:numOffset]
	return nil
}

func (t *plainDecoder) IsFull() bool {
	return int(t.len) >= t.valueSize
}

func (t *plainDecoder) Get(ts uint64) ([]byte, error) {
	i := sort.Search(int(t.num), func(i int) bool {
		slot := getTSSlot(t.ts, i)
		return parseTS(slot) <= ts
	})
	if i >= int(t.num) {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	slot := getTSSlot(t.ts, i)
	if parseTS(slot) != ts {
		return nil, fmt.Errorf("%d is wrong", ts)
	}
	return getVal(t.val, parseOffset(slot))
}

func (t *plainDecoder) Range() (start, end uint64) {
	startSlot := getTSSlot(t.ts, int(t.num)-1)
	endSlot := getTSSlot(t.ts, 0)
	return parseTS(startSlot), parseTS(endSlot)
}

func (t *plainDecoder) Iterator() SeriesIterator {
	return newBlockItemIterator(t)
}

func getVal(buf []byte, offset uint32) ([]byte, error) {
	if uint32(len(buf)) <= offset+4 {
		return nil, ErrInvalidValue
	}
	dataLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
	return buf[offset+4 : offset+4+dataLen], nil
}

func getTSSlot(data []byte, index int) []byte {
	return data[index*TsLen : (index+1)*TsLen]
}

func parseTS(tsSlot []byte) uint64 {
	return binary.LittleEndian.Uint64(tsSlot[:8])
}

func parseOffset(tsSlot []byte) uint32 {
	return binary.LittleEndian.Uint32(tsSlot[8:])
}

var _ SeriesIterator = (*plainIterator)(nil)

type plainIterator struct {
	index []byte
	data  []byte
	idx   int
	num   int
}

func newBlockItemIterator(decoder *plainDecoder) SeriesIterator {
	return &plainIterator{
		idx:   -1,
		index: decoder.ts,
		data:  decoder.val,
		num:   int(decoder.num),
	}
}

func (b *plainIterator) Next() bool {
	b.idx++
	return b.idx >= 0 && b.idx < b.num
}

func (b *plainIterator) Val() []byte {
	v, _ := getVal(b.data, parseOffset(getTSSlot(b.index, b.idx)))
	return v
}

func (b *plainIterator) Time() uint64 {
	return parseTS(getTSSlot(b.index, b.idx))
}

func (b *plainIterator) Error() error {
	return nil
}
