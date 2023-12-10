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
	"errors"
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

type dataBlock struct {
	offset uint64
	size   uint64
}

func (h *dataBlock) reset() {
	h.offset = 0
	h.size = 0
}

func (h *dataBlock) copyFrom(src *dataBlock) {
	h.offset = src.offset
	h.size = src.size
}

func (h *dataBlock) marshal(dst []byte) []byte {
	dst = encoding.VarUint64ToBytes(dst, h.offset)
	dst = encoding.VarUint64ToBytes(dst, h.size)
	return dst
}

func (h *dataBlock) unmarshal(src []byte) ([]byte, error) {
	src, n, err := encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal offset: %w", err)
	}
	h.offset = n

	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal size: %w", err)
	}
	h.size = n
	return src, nil
}

type blockMetadata struct {
	seriesID common.SeriesID

	uncompressedSizeBytes uint64

	count uint64

	timestamps  timestampsMetadata
	field       columnFamilyMetadata
	tagFamilies map[string]*dataBlock
}

func (bh *blockMetadata) getTagFamilyMetadata(name string) *dataBlock {
	if bh.tagFamilies == nil {
		bh.tagFamilies = make(map[string]*dataBlock)
	}
	tf, ok := bh.tagFamilies[name]
	if !ok {
		tf = &dataBlock{}
		bh.tagFamilies[name] = tf
	}
	return tf
}

func (bh *blockMetadata) reset() {
	bh.seriesID = 0
	bh.uncompressedSizeBytes = 0
	bh.count = 0
	bh.timestamps.reset()
	bh.field.reset()
	for k := range bh.tagFamilies {
		bh.tagFamilies[k].reset()
		delete(bh.tagFamilies, k)
	}
}

func (bh *blockMetadata) copyFrom(src *blockMetadata) {
	bh.reset()
	bh.seriesID = src.seriesID
	bh.uncompressedSizeBytes = src.uncompressedSizeBytes
	bh.count = src.count
	bh.timestamps.copyFrom(&src.timestamps)
	bh.field.copyFrom(&src.field)
	for k, v := range src.tagFamilies {
		bh.tagFamilies[k] = v
	}
}

func (bh *blockMetadata) marshal(dst []byte) []byte {
	dst = bh.seriesID.AppendToBytes(dst)
	dst = encoding.VarUint64ToBytes(dst, bh.uncompressedSizeBytes)
	dst = encoding.VarUint64ToBytes(dst, bh.count)
	dst = bh.timestamps.marshal(dst)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(bh.tagFamilies)))
	for name, cf := range bh.tagFamilies {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(name))
		dst = cf.marshal(dst)
	}
	if len(bh.field.columnMetadata) > 0 {
		dst = bh.field.marshal(dst)
	}
	return dst
}

func (bh *blockMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, errors.New("cannot unmarshal blockMetadata from less than 8 bytes")
	}
	bh.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	src, n, err := encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal uncompressedSizeBytes: %w", err)
	}
	bh.uncompressedSizeBytes = n

	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal count: %w", err)
	}
	bh.count = n
	src, err = bh.timestamps.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal timestampsMetadata: %w", err)
	}
	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagFamilies count: %w", err)
	}
	var nameBytes []byte
	for i := uint64(0); i < n; i++ {
		src, nameBytes, err = encoding.DecodeBytes(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tagFamily name: %w", err)
		}
		// TODO: cache dataBlock
		tf := &dataBlock{}
		src, err = tf.unmarshal(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal tagFamily dataBlock: %w", err)
		}
		bh.tagFamilies[convert.BytesToString(nameBytes)] = tf
	}
	if len(src) > 0 {
		src, err = bh.field.unmarshal(src)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal columnFamilyMetadata: %w", err)
		}
	}
	return src, nil
}

func (bh *blockMetadata) less(other *blockMetadata) bool {
	if bh.seriesID == other.seriesID {
		return bh.timestamps.min < other.timestamps.min
	}
	return bh.seriesID < other.seriesID
}

func getBlockMetadata() *blockMetadata {
	v := blockMetadataPool.Get()
	if v == nil {
		return &blockMetadata{}
	}
	return v.(*blockMetadata)
}

func putBlockMetadata(bh *blockMetadata) {
	bh.reset()
	blockMetadataPool.Put(bh)
}

var blockMetadataPool sync.Pool

type timestampsMetadata struct {
	dataBlock
	min         int64
	max         int64
	marshalType encoding.EncodeType
}

func (th *timestampsMetadata) reset() {
	th.dataBlock.reset()
	th.min = 0
	th.max = 0
	th.marshalType = 0
}

func (th *timestampsMetadata) copyFrom(src *timestampsMetadata) {
	th.dataBlock.copyFrom(&src.dataBlock)
	th.min = src.min
	th.max = src.max
	th.marshalType = src.marshalType
}

func (th *timestampsMetadata) marshal(dst []byte) []byte {
	dst = th.dataBlock.marshal(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(th.min))
	dst = encoding.Uint64ToBytes(dst, uint64(th.max))
	dst = append(dst, byte(th.marshalType))
	return dst
}

func (th *timestampsMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 25 {
		return nil, errors.New("cannot unmarshal timestampsMetadata from less than 25 bytes")
	}
	th.offset = encoding.BytesToUint64(src)
	th.size = encoding.BytesToUint64(src[8:])
	th.min = int64(encoding.BytesToUint64(src))
	src = src[8:]
	th.max = int64(encoding.BytesToUint64(src))
	src = src[8:]
	th.marshalType = encoding.EncodeType(src[0])
	return src[1:], nil
}

func unmarshalBlockMetadata(dst []blockMetadata, src []byte) ([]blockMetadata, error) {
	dstOrig := dst
	for len(src) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, blockMetadata{})
		}
		bm := &dst[len(dst)-1]
		tail, err := bm.unmarshal(src)
		if err != nil {
			return dstOrig, fmt.Errorf("cannot unmarshal blockMetadata entries: %w", err)
		}
		src = tail
	}
	if err := validateBlockHeaders(dst[len(dstOrig):]); err != nil {
		return dstOrig, err
	}
	return dst, nil
}

func validateBlockHeaders(bhs []blockMetadata) error {
	for i := 1; i < len(bhs); i++ {
		bhCurr := &bhs[i]
		bhPrev := &bhs[i-1]
		if bhCurr.seriesID < bhPrev.seriesID {
			return fmt.Errorf("unexpected blockMetadata with smaller seriesID=%d after bigger seriesID=%d at position %d", &bhCurr.seriesID, &bhPrev.seriesID, i)
		}
		if bhCurr.seriesID != bhPrev.seriesID {
			continue
		}
		thCurr := bhCurr.timestamps
		thPrev := bhPrev.timestamps
		if thCurr.min < thPrev.min {
			return fmt.Errorf("unexpected blockMetadata with smaller timestamp=%d after bigger timestamp=%d at position %d", thCurr.min, thPrev.min, i)
		}
	}
	return nil
}
