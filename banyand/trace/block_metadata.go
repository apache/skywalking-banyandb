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
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

type dataBlock struct {
	offset uint64
	size   uint64
}

func (d *dataBlock) reset() {
	d.offset = 0
	d.size = 0
}

func (d *dataBlock) copyFrom(src *dataBlock) {
	d.offset = src.offset
	d.size = src.size
}

func (d *dataBlock) marshal(dst []byte) []byte {
	dst = encoding.VarUint64ToBytes(dst, d.offset)
	dst = encoding.VarUint64ToBytes(dst, d.size)
	return dst
}

func (d *dataBlock) unmarshal(src []byte) []byte {
	src, n := encoding.BytesToVarUint64(src)
	d.offset = n

	src, n = encoding.BytesToVarUint64(src)
	d.size = n
	return src
}

type blockMetadata struct {
	tags          map[string]*dataBlock
	tagType       map[string]pbv1.ValueType
	spans         *dataBlock
	timestamps    timestampsMetadata
	tagProjection *model.TagProjection
	traceID       string
	spanSize      uint64
	count         uint64
}

func (bm *blockMetadata) copyFrom(src *blockMetadata) {
	bm.traceID = src.traceID
	bm.spanSize = src.spanSize
	bm.count = src.count
	bm.spans.copyFrom(src.spans)
	bm.timestamps.copyFrom(&src.timestamps)
	for k, db := range src.tags {
		if bm.tags == nil {
			bm.tags = make(map[string]*dataBlock)
		}
		bm.tags[k] = &dataBlock{}
		bm.tags[k].copyFrom(db)
	}
	for k, vt := range src.tagType {
		if bm.tagType == nil {
			bm.tagType = make(map[string]pbv1.ValueType)
		}
		bm.tagType[k] = vt
	}
}

func (bm *blockMetadata) getTagMetadata(name string) *dataBlock {
	if bm.tags == nil {
		bm.tags = make(map[string]*dataBlock)
	}
	t, ok := bm.tags[name]
	if !ok {
		t = &dataBlock{}
		bm.tags[name] = t
	}
	return t
}

func (bm *blockMetadata) reset() {
	bm.traceID = ""
	bm.spanSize = 0
	bm.count = 0
	bm.tagProjection = nil
	bm.spans.reset()
	bm.timestamps.reset()
	for k := range bm.tags {
		bm.tags[k].reset()
		delete(bm.tags, k)
	}
	for k := range bm.tagType {
		delete(bm.tagType, k)
	}
}

func (bm *blockMetadata) marshal(dst []byte, traceIDLen uint32) []byte {
	dst = append(dst, bm.traceID...)
	paddingLen := traceIDLen - uint32(len(bm.traceID))
	if paddingLen > 0 {
		dst = append(dst, bytes.Repeat([]byte{0}, int(paddingLen))...)
	}
	dst = encoding.VarUint64ToBytes(dst, bm.spanSize)
	dst = encoding.VarUint64ToBytes(dst, bm.count)
	dst = encoding.VarUint64ToBytes(dst, uint64(len(bm.tags)))
	// make sure the order of tags is stable
	keys := make([]string, 0, len(bm.tags))
	for k := range bm.tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, name := range keys {
		t := bm.tags[name]
		dst = encoding.EncodeBytes(dst, convert.StringToBytes(name))
		dst = t.marshal(dst)
	}
	return dst
}

func (bm *blockMetadata) unmarshal(src []byte, tagType map[string]pbv1.ValueType, traceIDLen int) ([]byte, error) {
	if len(src) < traceIDLen {
		return nil, fmt.Errorf("cannot unmarshal blockMetadata from less than %d bytes", traceIDLen)
	}
	bm.traceID = strings.TrimRight(string(src[:traceIDLen]), "\x00")
	bm.tagType = tagType
	src = src[traceIDLen:]
	src, n := encoding.BytesToVarUint64(src)
	bm.spanSize = n
	src, n = encoding.BytesToVarUint64(src)
	bm.count = n

	src, n = encoding.BytesToVarUint64(src)
	if n > 0 {
		if bm.tags == nil {
			bm.tags = make(map[string]*dataBlock, n)
		}
		var nameBytes []byte
		var err error
		for i := uint64(0); i < n; i++ {
			src, nameBytes, err = encoding.DecodeBytes(src)
			if err != nil {
				return nil, fmt.Errorf("cannot unmarshal tag name: %w", err)
			}
			t := &dataBlock{}
			src = t.unmarshal(src)
			bm.tags[string(nameBytes)] = t
		}
	}
	return src, nil
}

func (bm *blockMetadata) less(other *blockMetadata) bool {
	return bm.traceID < other.traceID
}

func generateBlockMetadata() *blockMetadata {
	v := blockMetadataPool.Get()
	if v == nil {
		return &blockMetadata{}
	}
	return v
}

func releaseBlockMetadata(bm *blockMetadata) {
	bm.reset()
	blockMetadataPool.Put(bm)
}

var blockMetadataPool = pool.Register[*blockMetadata]("trace-blockMetadata")

type blockMetadataArray struct {
	arr []blockMetadata
}

func (bma *blockMetadataArray) reset() {
	for i := range bma.arr {
		bma.arr[i].reset()
	}
	bma.arr = bma.arr[:0]
}

var blockMetadataArrayPool = pool.Register[*blockMetadataArray]("trace-blockMetadataArray")

func generateBlockMetadataArray() *blockMetadataArray {
	v := blockMetadataArrayPool.Get()
	if v == nil {
		return &blockMetadataArray{}
	}
	return v
}

func releaseBlockMetadataArray(bma *blockMetadataArray) {
	bma.reset()
	blockMetadataArrayPool.Put(bma)
}

type timestampsMetadata struct {
	min int64
	max int64
}

func (tm *timestampsMetadata) reset() {
	tm.min = 0
	tm.max = 0
}

func (tm *timestampsMetadata) copyFrom(src *timestampsMetadata) {
	tm.min = src.min
	tm.max = src.max
}

func (tm *timestampsMetadata) marshal(dst []byte) []byte {
	dst = encoding.Uint64ToBytes(dst, uint64(tm.min))
	dst = encoding.Uint64ToBytes(dst, uint64(tm.max))
	return dst
}

func (tm *timestampsMetadata) unmarshal(src []byte) []byte {
	tm.min = int64(encoding.BytesToUint64(src))
	src = src[8:]
	tm.max = int64(encoding.BytesToUint64(src))
	src = src[8:]
	return src
}

func unmarshalBlockMetadata(dst []blockMetadata, src []byte, tagType map[string]pbv1.ValueType, traceIDLen int) ([]blockMetadata, error) {
	dstOrig := dst
	var pre *blockMetadata
	for len(src) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, blockMetadata{})
		}
		bm := &dst[len(dst)-1]
		tail, err := bm.unmarshal(src, tagType, traceIDLen)
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
	if cur.traceID < pre.traceID {
		return fmt.Errorf("unexpected blockMetadata with smaller traceID=%s after bigger traceID=%s", cur.traceID, pre.traceID)
	}
	return nil
}
