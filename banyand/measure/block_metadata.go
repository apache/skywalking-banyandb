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
	"sync"

	"github.com/apache/skywalking-banyandb/api/common"
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
	dst = encoding.Uint64ToBytes(dst, h.offset)
	dst = encoding.Uint64ToBytes(dst, h.size)
	return dst
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
	for _, cf := range bh.tagFamilies {
		dst = cf.marshal(dst)
	}
	if len(bh.field.columnMetadata) > 0 {
		dst = bh.field.marshal(dst)
	}
	return dst
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
