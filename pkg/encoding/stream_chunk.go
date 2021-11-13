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

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

var (
	decoder, _               = zstd.NewReader(nil)
	encoder, _               = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	_          SeriesEncoder = (*streamChunkEncoder)(nil)
	_          SeriesDecoder = (*StreamChunkDecoder)(nil)
)

//streamChunkEncoder backport to reduced value
type streamChunkEncoder struct {
	tsBuff    bytes.Buffer
	valBuff   bytes.Buffer
	scratch   [binary.MaxVarintLen64]byte
	len       uint32
	num       uint32
	startTime uint64
	valueSize int
}

func NewStreamChunkEncoder(size int) SeriesEncoder {
	return &streamChunkEncoder{
		valueSize: size,
	}
}

func (t *streamChunkEncoder) Append(ts uint64, value []byte) {
	if t.startTime == 0 {
		t.startTime = ts
	} else if t.startTime > ts {
		t.startTime = ts
	}
	vLen := len(value)
	offset := uint32(len(t.valBuff.Bytes()))
	t.valBuff.Write(t.putUint32(uint32(vLen)))
	t.valBuff.Write(value)
	t.tsBuff.Write(t.putUint64(ts))
	t.tsBuff.Write(t.putUint32(offset))
	t.num = t.num + 1
}

func (t *streamChunkEncoder) IsFull() bool {
	return t.valBuff.Len() >= t.valueSize
}

func (t *streamChunkEncoder) Reset() {
	t.tsBuff.Reset()
	t.valBuff.Reset()
	t.num = 0
	t.startTime = 0
}

func (t *streamChunkEncoder) Encode() ([]byte, error) {
	if t.tsBuff.Len() < 1 {
		return nil, ErrEncodeEmpty
	}
	val := t.valBuff.Bytes()
	t.len = uint32(len(val))
	_, err := t.tsBuff.WriteTo(&t.valBuff)
	if err != nil {
		return nil, err
	}
	t.valBuff.Write(t.putUint32(t.num))
	t.valBuff.Write(t.putUint32(t.len))
	data := t.valBuff.Bytes()
	l := len(data)
	dst := make([]byte, 0, compressBound(l))
	dst = encoder.EncodeAll(data, dst)
	result := make([]byte, len(dst)+2)
	copy(result, dst)
	copy(result[len(dst):], t.putUint16(uint16(l)))
	return result, nil
}

func compressBound(srcSize int) int {
	return srcSize + (srcSize >> 8)
}

func (t *streamChunkEncoder) StartTime() uint64 {
	return t.startTime
}

func (t *streamChunkEncoder) putUint16(v uint16) []byte {
	binary.LittleEndian.PutUint16(t.scratch[:], v)
	return t.scratch[:2]
}

func (t *streamChunkEncoder) putUint32(v uint32) []byte {
	binary.LittleEndian.PutUint32(t.scratch[:], v)
	return t.scratch[:4]
}

func (t *streamChunkEncoder) putUint64(v uint64) []byte {
	binary.LittleEndian.PutUint64(t.scratch[:], v)
	return t.scratch[:8]
}

const (
	// TsLen equals ts(uint64) + data_offset(uint32)
	TsLen = 8 + 4
)

var ErrInvalidValue = errors.New("invalid encoded value")

//StreamChunkDecoder decodes encoded time index
type StreamChunkDecoder struct {
	ts        []byte
	val       []byte
	len       uint32
	num       uint32
	valueSize int
}

func NewStreamChunkDecoder(size int) SeriesDecoder {
	return &StreamChunkDecoder{
		valueSize: size,
	}
}

func (t *StreamChunkDecoder) Len() int {
	return int(t.num)
}

func (t *StreamChunkDecoder) Decode(rawData []byte) (err error) {
	var data []byte
	size := binary.LittleEndian.Uint16(rawData[len(rawData)-2:])
	if data, err = decoder.DecodeAll(rawData[:len(rawData)-2], make([]byte, 0, size)); err != nil {
		return err
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

func (t *StreamChunkDecoder) IsFull() bool {
	return int(t.len) >= t.valueSize
}

func (t *StreamChunkDecoder) Get(ts uint64) ([]byte, error) {
	i := sort.Search(int(t.num), func(i int) bool {
		slot := getTSSlot(t.ts, i)
		return parseTS(slot) <= ts
	})
	if i >= int(t.num) {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	slot := getTSSlot(t.ts, i)
	if parseTS(slot) != ts {
		return nil, fmt.Errorf("%d doesn't exist", ts)
	}
	return getVal(t.val, parseOffset(slot))
}

func (t *StreamChunkDecoder) Iterator() SeriesIterator {
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

var _ SeriesIterator = (*chunkIterator)(nil)

type chunkIterator struct {
	index []byte
	data  []byte
	idx   int
	num   int
}

func newBlockItemIterator(decoder *StreamChunkDecoder) SeriesIterator {
	return &chunkIterator{
		idx:   -1,
		index: decoder.ts,
		data:  decoder.val,
		num:   int(decoder.num),
	}
}

func (b *chunkIterator) Next() bool {
	b.idx++
	return b.idx >= 0 && b.idx < b.num
}

func (b *chunkIterator) Val() []byte {
	v, _ := getVal(b.data, parseOffset(getTSSlot(b.index, b.idx)))
	return v
}

func (b *chunkIterator) Time() uint64 {
	return parseTS(getTSSlot(b.index, b.idx))
}

func (b *chunkIterator) Error() error {
	return nil
}
