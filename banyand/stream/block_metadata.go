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
	"errors"
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
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
	tagFamilies           map[string]*dataBlock
	tagProjection         []pbv1.TagProjection
	timestamps            timestampsMetadata
	elementIDs            elementIDsMetadata
	seriesID              common.SeriesID
	uncompressedSizeBytes uint64
	count                 uint64
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
	bh.elementIDs.reset()
	for k := range bh.tagFamilies {
		bh.tagFamilies[k].reset()
		delete(bh.tagFamilies, k)
	}
	bh.tagProjection = bh.tagProjection[:0]
}

func (bh *blockMetadata) marshal(dst []byte) []byte {
	dst = bh.seriesID.AppendToBytes(dst)
	dst = encoding.VarUint64ToBytes(dst, bh.uncompressedSizeBytes)
	dst = encoding.VarUint64ToBytes(dst, bh.count)
	dst = bh.timestamps.marshal(dst)
	dst = bh.elementIDs.marshal(dst)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(bh.tagFamilies)))
	for name, cf := range bh.tagFamilies {
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(name))
		dst = cf.marshal(dst)
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
	src, err = bh.elementIDs.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal elementIDsMetadata: %w", err)
	}
	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagFamilies count: %w", err)
	}
	if n > 0 {
		if bh.tagFamilies == nil {
			bh.tagFamilies = make(map[string]*dataBlock, n)
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
	}
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal columnFamilyMetadata: %w", err)
	}
	return src, nil
}

func (bh blockMetadata) less(other blockMetadata) bool {
	if bh.seriesID == other.seriesID {
		return bh.timestamps.min < other.timestamps.min
	}
	return bh.seriesID < other.seriesID
}

func generateBlockMetadata() *blockMetadata {
	v := blockMetadataPool.Get()
	if v == nil {
		return &blockMetadata{}
	}
	return v.(*blockMetadata)
}

func releaseBlockMetadata(bh *blockMetadata) {
	bh.reset()
	blockMetadataPool.Put(bh)
}

var blockMetadataPool sync.Pool

type timestampsMetadata struct {
	dataBlock
	min        int64
	max        int64
	encodeType encoding.EncodeType
}

func (th *timestampsMetadata) reset() {
	th.dataBlock.reset()
	th.min = 0
	th.max = 0
	th.encodeType = 0
}

func (th *timestampsMetadata) copyFrom(src *timestampsMetadata) {
	th.dataBlock.copyFrom(&src.dataBlock)
	th.min = src.min
	th.max = src.max
	th.encodeType = src.encodeType
}

func (th *timestampsMetadata) marshal(dst []byte) []byte {
	dst = th.dataBlock.marshal(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(th.min))
	dst = encoding.Uint64ToBytes(dst, uint64(th.max))
	dst = append(dst, byte(th.encodeType))
	return dst
}

func (th *timestampsMetadata) unmarshal(src []byte) ([]byte, error) {
	src, err := th.dataBlock.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal dataBlock: %w", err)
	}
	th.min = int64(encoding.BytesToUint64(src))
	src = src[8:]
	th.max = int64(encoding.BytesToUint64(src))
	src = src[8:]
	th.encodeType = encoding.EncodeType(src[0])
	return src[1:], nil
}

type elementIDsMetadata struct {
	dataBlock
	encodeType encoding.EncodeType
}

func (th *elementIDsMetadata) reset() {
	th.dataBlock.reset()
	th.encodeType = 0
}

func (th *elementIDsMetadata) copyFrom(src *elementIDsMetadata) {
	th.dataBlock.copyFrom(&src.dataBlock)
	th.encodeType = src.encodeType
}

func (th *elementIDsMetadata) marshal(dst []byte) []byte {
	dst = th.dataBlock.marshal(dst)
	dst = append(dst, byte(th.encodeType))
	return dst
}

func (th *elementIDsMetadata) unmarshal(src []byte) ([]byte, error) {
	src, err := th.dataBlock.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal dataBlock: %w", err)
	}
	// src = src[8:]
	th.encodeType = encoding.EncodeType(src[0])
	return src[1:], nil
}

func unmarshalBlockMetadata(dst []blockMetadata, src []byte) ([]blockMetadata, error) {
	dstOrig := dst
	var pre *blockMetadata
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

		// Validate the order of blockMetadata during unmarshalling
		if pre != nil {
			if err := validateBlockMetadataOrder(pre, bm); err != nil {
				return dstOrig, err
			}
		}
		pre = bm
	}
	return dst, nil
}

func validateBlockMetadataOrder(pre, cur *blockMetadata) error {
	if cur.seriesID < pre.seriesID {
		return fmt.Errorf("unexpected blockMetadata with smaller seriesID=%d after bigger seriesID=%d", cur.seriesID, pre.seriesID)
	}
	if cur.seriesID != pre.seriesID {
		return nil
	}
	tmCur := cur.timestamps
	tmPre := pre.timestamps
	if tmCur.min < tmPre.min {
		return fmt.Errorf("unexpected blockMetadata with smaller timestamp=%d after bigger timestamp=%d", tmCur.min, tmPre.min)
	}
	return nil
}
