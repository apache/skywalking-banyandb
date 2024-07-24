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
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type dataBlock struct {
	offset uint64
	size   uint64
}

func (b *dataBlock) reset() {
	b.offset = 0
	b.size = 0
}

func (b *dataBlock) copyFrom(src *dataBlock) {
	b.offset = src.offset
	b.size = src.size
}

func (b *dataBlock) marshal(dst []byte) []byte {
	dst = encoding.VarUint64ToBytes(dst, b.offset)
	dst = encoding.VarUint64ToBytes(dst, b.size)
	return dst
}

func (b *dataBlock) unmarshal(src []byte) ([]byte, error) {
	src, n, err := encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal offset: %w", err)
	}
	b.offset = n

	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal size: %w", err)
	}
	b.size = n
	return src, nil
}

type blockMetadata struct {
	tagFamilies           map[string]*dataBlock
	field                 columnFamilyMetadata
	tagProjection         []model.TagProjection
	timestamps            timestampsMetadata
	seriesID              common.SeriesID
	uncompressedSizeBytes uint64
	count                 uint64
}

func (bm *blockMetadata) copyFrom(src *blockMetadata) {
	bm.seriesID = src.seriesID
	bm.uncompressedSizeBytes = src.uncompressedSizeBytes
	bm.count = src.count
	bm.timestamps.copyFrom(&src.timestamps)
	for k, db := range src.tagFamilies {
		if bm.tagFamilies == nil {
			bm.tagFamilies = make(map[string]*dataBlock)
		}
		bm.tagFamilies[k] = &dataBlock{}
		bm.tagFamilies[k].copyFrom(db)
	}
	bm.field.copyFrom(&src.field)
}

func (bm *blockMetadata) getTagFamilyMetadata(name string) *dataBlock {
	if bm.tagFamilies == nil {
		bm.tagFamilies = make(map[string]*dataBlock)
	}
	tf, ok := bm.tagFamilies[name]
	if !ok {
		tf = &dataBlock{}
		bm.tagFamilies[name] = tf
	}
	return tf
}

func (bm *blockMetadata) reset() {
	bm.seriesID = 0
	bm.uncompressedSizeBytes = 0
	bm.count = 0
	bm.timestamps.reset()
	bm.field.reset()
	for k := range bm.tagFamilies {
		bm.tagFamilies[k].reset()
		delete(bm.tagFamilies, k)
	}
	bm.tagProjection = bm.tagProjection[:0]
}

func (bm *blockMetadata) marshal(dst []byte) []byte {
	dst = bm.seriesID.AppendToBytes(dst)
	dst = encoding.VarUint64ToBytes(dst, bm.uncompressedSizeBytes)
	dst = encoding.VarUint64ToBytes(dst, bm.count)
	dst = bm.timestamps.marshal(dst)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(bm.tagFamilies)))
	// make sure the order of tagFamilies is stable
	keys := make([]string, 0, len(bm.tagFamilies))
	for k := range bm.tagFamilies {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		cf := bm.tagFamilies[name]
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(name))
		dst = cf.marshal(dst)
	}
	return bm.field.marshal(dst)
}

func (bm *blockMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, errors.New("cannot unmarshal blockMetadata from less than 8 bytes")
	}
	bm.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	src, n, err := encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal uncompressedSizeBytes: %w", err)
	}
	bm.uncompressedSizeBytes = n

	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal count: %w", err)
	}
	bm.count = n
	src, err = bm.timestamps.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal timestampsMetadata: %w", err)
	}
	src, n, err = encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagFamilies count: %w", err)
	}
	if n > 0 {
		if bm.tagFamilies == nil {
			bm.tagFamilies = make(map[string]*dataBlock, n)
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
			bm.tagFamilies[string(nameBytes)] = tf
		}
	}
	src, err = bm.field.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal columnFamilyMetadata: %w", err)
	}
	return src, nil
}

func (bm *blockMetadata) less(other *blockMetadata) bool {
	if bm.seriesID == other.seriesID {
		return bm.timestamps.min < other.timestamps.min
	}
	return bm.seriesID < other.seriesID
}

func generateBlockMetadata() *blockMetadata {
	v := blockMetadataPool.Get()
	if v == nil {
		return &blockMetadata{}
	}
	return v.(*blockMetadata)
}

func releaseBlockMetadata(bm *blockMetadata) {
	bm.reset()
	blockMetadataPool.Put(bm)
}

var blockMetadataPool sync.Pool

type blockMetadataArray struct {
	arr []blockMetadata
}

func (bma *blockMetadataArray) reset() {
	for i := range bma.arr {
		bma.arr[i].reset()
	}
	bma.arr = bma.arr[:0]
}

var blockMetadataArrayPool sync.Pool

func generateBlockMetadataArray() *blockMetadataArray {
	v := blockMetadataArrayPool.Get()
	if v == nil {
		return &blockMetadataArray{}
	}
	return v.(*blockMetadataArray)
}

func releaseBlockMetadataArray(bma *blockMetadataArray) {
	bma.reset()
	blockMetadataArrayPool.Put(bma)
}

type timestampsMetadata struct {
	dataBlock
	min               int64
	max               int64
	versionOffset     uint64
	versionFirst      int64
	encodeType        encoding.EncodeType
	versionEncodeType encoding.EncodeType
}

func (tm *timestampsMetadata) reset() {
	tm.dataBlock.reset()
	tm.min = 0
	tm.max = 0
	tm.encodeType = 0
	tm.versionOffset = 0
	tm.versionFirst = 0
	tm.versionEncodeType = 0
}

func (tm *timestampsMetadata) copyFrom(src *timestampsMetadata) {
	tm.dataBlock.copyFrom(&src.dataBlock)
	tm.min = src.min
	tm.max = src.max
	tm.encodeType = src.encodeType
	tm.versionOffset = src.versionOffset
	tm.versionFirst = src.versionFirst
	tm.versionEncodeType = src.versionEncodeType
}

func (tm *timestampsMetadata) marshal(dst []byte) []byte {
	dst = tm.dataBlock.marshal(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(tm.min))
	dst = encoding.Uint64ToBytes(dst, uint64(tm.max))
	dst = append(dst, byte(tm.encodeType))
	dst = encoding.VarUint64ToBytes(dst, tm.versionOffset)
	dst = encoding.Uint64ToBytes(dst, uint64(tm.versionFirst))
	dst = append(dst, byte(tm.versionEncodeType))
	return dst
}

func (tm *timestampsMetadata) unmarshal(src []byte) ([]byte, error) {
	src, err := tm.dataBlock.unmarshal(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal ts dataBlock: %w", err)
	}
	tm.min = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.max = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.encodeType = encoding.EncodeType(src[0])
	src = src[1:]
	src, n, err := encoding.BytesToVarUint64(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal ts offset: %w", err)
	}
	tm.versionOffset = n
	tm.versionFirst = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.versionEncodeType = encoding.EncodeType(src[0])
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
		bm.reset()
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
