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

package sidx

import (
	"fmt"

	internalencoding "github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	pkgencoding "github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/filter"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// dataBlock represents a reference to data in a file.
type dataBlock struct {
	offset uint64
	size   uint64
}

// tagMetadata contains persistent metadata for a tag.
type tagMetadata struct {
	name        string
	min         []byte    // For int64 tags
	max         []byte    // For int64 tags
	dataBlock   dataBlock // Offset/size in .td file
	filterBlock dataBlock // Offset/size in .tf file
	valueType   pbv1.ValueType
}

// tagData represents the runtime data for a tag with filtering capabilities.
type tagData struct {
	uniqueValues map[string]struct{}
	name         string
	values       []tagRow
	tmpBytes     [][]byte
	valueType    pbv1.ValueType
}

type tagRow struct {
	value    []byte
	valueArr [][]byte
}

func (tr *tagRow) reset() {
	tr.value = nil
	tr.valueArr = tr.valueArr[:0]
}

var (
	tagDataPool     = pool.Register[*tagData]("sidx-tagData")
	tagMetadataPool = pool.Register[*tagMetadata]("sidx-tagMetadata")
	bloomFilterPool = pool.Register[*filter.BloomFilter]("sidx-bloomFilter")
)

// generateTagData gets a tagData from pool or creates new.
func generateTagData() *tagData {
	v := tagDataPool.Get()
	if v == nil {
		return &tagData{}
	}
	return v
}

// releaseTagData returns tagData to pool after reset.
func releaseTagData(td *tagData) {
	if td == nil {
		return
	}
	td.reset()
	tagDataPool.Put(td)
}

// generateTagMetadata gets a tagMetadata from pool or creates new.
func generateTagMetadata() *tagMetadata {
	v := tagMetadataPool.Get()
	if v == nil {
		return &tagMetadata{}
	}
	return v
}

// releaseTagMetadata returns tagMetadata to pool after reset.
func releaseTagMetadata(tm *tagMetadata) {
	if tm == nil {
		return
	}
	tm.reset()
	tagMetadataPool.Put(tm)
}

// reset clears tagData for reuse in object pool.
func (td *tagData) reset() {
	td.name = ""
	td.valueType = pbv1.ValueTypeUnknown

	// Reset values slice
	for i := range td.values {
		td.values[i].reset()
	}
	td.values = td.values[:0]

	// Reset tmpBytes slice
	for i := range td.tmpBytes {
		td.tmpBytes[i] = nil
	}
	td.tmpBytes = td.tmpBytes[:0]

	// Reset uniqueValues map for reuse
	for k := range td.uniqueValues {
		delete(td.uniqueValues, k)
	}
}

// reset clears tagMetadata for reuse in object pool.
func (tm *tagMetadata) reset() {
	tm.name = ""
	tm.valueType = pbv1.ValueTypeUnknown
	tm.dataBlock = dataBlock{}
	tm.filterBlock = dataBlock{}
	tm.min = nil
	tm.max = nil
}

// generateBloomFilter gets a bloom filter from pool or creates new.
func generateBloomFilter(expectedElements int) *filter.BloomFilter {
	v := bloomFilterPool.Get()
	if v == nil {
		return filter.NewBloomFilter(expectedElements)
	}
	// Reset and resize for new expected elements
	v.SetN(expectedElements)
	v.ResizeBits(filter.OptimalBitsSize(expectedElements))
	return v
}

// releaseBloomFilter returns bloom filter to pool after reset.
func releaseBloomFilter(bf *filter.BloomFilter) {
	if bf == nil {
		return
	}
	bf.Reset()
	bloomFilterPool.Put(bf)
}

// encodeBloomFilter encodes a bloom filter to bytes.
func encodeBloomFilter(dst []byte, bf *filter.BloomFilter) []byte {
	if bf == nil {
		return dst
	}
	dst = pkgencoding.Int64ToBytes(dst, int64(bf.N()))
	dst = pkgencoding.EncodeUint64Block(dst, bf.Bits())
	return dst
}

// decodeBloomFilter decodes bytes to bloom filter.
func decodeBloomFilter(src []byte) (*filter.BloomFilter, error) {
	if len(src) < 8 {
		return nil, fmt.Errorf("invalid bloom filter data: too short")
	}

	n := pkgencoding.BytesToInt64(src)
	bf := generateBloomFilter(int(n))
	bitsLen := len(bf.Bits())

	bits, _, err := pkgencoding.DecodeUint64Block(bf.Bits()[:0], src[8:], uint64(bitsLen))
	if err != nil {
		releaseBloomFilter(bf)
		return nil, fmt.Errorf("failed to decode bloom filter bits: %w", err)
	}
	bf.SetBits(bits)

	return bf, nil
}

// marshalTagRow marshals the tagRow value to a byte slice.
func marshalTagRow(tr *tagRow, valueType pbv1.ValueType) []byte {
	if tr.valueArr != nil {
		var dst []byte
		for i := range tr.valueArr {
			if valueType == pbv1.ValueTypeInt64Arr {
				dst = append(dst, tr.valueArr[i]...)
				continue
			}
			dst = internalencoding.MarshalVarArray(dst, tr.valueArr[i])
		}
		return dst
	}
	return tr.value
}

// decodeAndConvertTagValues decodes encoded tag values and converts them to tagRow format.
// This is a common operation shared between reading from disk and querying.
func decodeAndConvertTagValues(td *tagData, decoder *pkgencoding.BytesBlockDecoder, encodedData *bytes.Buffer, valueType pbv1.ValueType, count int) error {
	var err error

	// Decode to tmpBytes buffer, reusing the existing slice to avoid allocations
	td.tmpBytes, err = internalencoding.DecodeTagValues(td.tmpBytes[:0], decoder, encodedData, valueType, count)
	if err != nil {
		return fmt.Errorf("cannot decode tag values: %w", err)
	}

	// Convert [][]byte to []tagRow based on valueType
	if cap(td.values) < len(td.tmpBytes) {
		td.values = make([]tagRow, len(td.tmpBytes))
	} else {
		td.values = td.values[:len(td.tmpBytes)]
	}

	for i, encodedValue := range td.tmpBytes {
		if encodedValue == nil {
			td.values[i] = tagRow{}
			continue
		}

		if valueType == pbv1.ValueTypeStrArr || valueType == pbv1.ValueTypeInt64Arr {
			// For array types, unmarshal to valueArr
			td.values[i].valueArr, err = unmarshalTag(td.values[i].valueArr[:0], encodedValue, valueType)
			if err != nil {
				return fmt.Errorf("cannot unmarshal tag array: %w", err)
			}
		} else {
			// For scalar types, set value directly
			td.values[i].value = encodedValue
		}
	}

	return nil
}

// marshal serializes tag metadata to bytes using encoding package.
func (tm *tagMetadata) marshal(dst []byte) []byte {
	dst = pkgencoding.EncodeBytes(dst, []byte(tm.name))
	dst = append(dst, byte(tm.valueType))
	dst = pkgencoding.VarUint64ToBytes(dst, tm.dataBlock.offset)
	dst = pkgencoding.VarUint64ToBytes(dst, tm.dataBlock.size)
	dst = pkgencoding.VarUint64ToBytes(dst, tm.filterBlock.offset)
	dst = pkgencoding.VarUint64ToBytes(dst, tm.filterBlock.size)

	dst = pkgencoding.EncodeBytes(dst, tm.min)
	dst = pkgencoding.EncodeBytes(dst, tm.max)
	return dst
}

// unmarshal deserializes tag metadata from bytes using encoding package.
func (tm *tagMetadata) unmarshal(src []byte) ([]byte, error) {
	var nameBytes []byte
	var err error

	src, nameBytes, err = pkgencoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.name: %w", err)
	}
	tm.name = string(nameBytes)

	if len(src) < 1 {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.valueType: src is too short")
	}
	tm.valueType = pbv1.ValueType(src[0])
	src = src[1:]

	src, tm.dataBlock.offset = pkgencoding.BytesToVarUint64(src)
	src, tm.dataBlock.size = pkgencoding.BytesToVarUint64(src)
	src, tm.filterBlock.offset = pkgencoding.BytesToVarUint64(src)
	src, tm.filterBlock.size = pkgencoding.BytesToVarUint64(src)

	if len(src) < 1 {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata flags: src is too short")
	}

	src, tm.min, err = pkgencoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.min: %w", err)
	}
	if len(tm.min) == 0 {
		tm.min = nil
	}

	src, tm.max, err = pkgencoding.DecodeBytes(src)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tagMetadata.max: %w", err)
	}
	if len(tm.max) == 0 {
		tm.max = nil
	}

	return src, nil
}

// marshalAppend serializes tagMetadata to bytes and appends to dst (panic version for mustWriteTag).
func (tm *tagMetadata) marshalAppend(dst []byte) []byte {
	return tm.marshal(dst)
}

// unmarshalTagMetadata deserializes tag metadata from bytes.
func unmarshalTagMetadata(data []byte) (*tagMetadata, error) {
	tm := generateTagMetadata()
	_, err := tm.unmarshal(data)
	if err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	return tm, nil
}
