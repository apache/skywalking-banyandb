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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
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
	indexed     bool
	compressed  bool
}

// tagData represents the runtime data for a tag with filtering capabilities.
type tagData struct {
	values    [][]byte
	filter    *filter.BloomFilter // For indexed tags
	name      string
	min       []byte // For int64 tags
	max       []byte // For int64 tags
	valueType pbv1.ValueType
	indexed   bool
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
	td.indexed = false

	// Reset values slice
	for i := range td.values {
		td.values[i] = nil
	}
	td.values = td.values[:0]

	// Reset filter
	if td.filter != nil {
		releaseBloomFilter(td.filter)
		td.filter = nil
	}

	// Reset min/max
	td.min = nil
	td.max = nil
}

// reset clears tagMetadata for reuse in object pool.
func (tm *tagMetadata) reset() {
	tm.name = ""
	tm.valueType = pbv1.ValueTypeUnknown
	tm.indexed = false
	tm.compressed = false
	tm.dataBlock = dataBlock{}
	tm.filterBlock = dataBlock{}
	tm.min = nil
	tm.max = nil
}

const (
	// defaultCompressionLevel for zstd compression.
	defaultCompressionLevel = 3
)

// compressTagData compresses tag data using zstd compression.
func compressTagData(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	return zstd.Compress(nil, data, defaultCompressionLevel)
}

// decompressTagData decompresses tag data using zstd decompression.
func decompressTagData(compressedData []byte) ([]byte, error) {
	if len(compressedData) == 0 {
		return nil, nil
	}
	return zstd.Decompress(nil, compressedData)
}

// generateBloomFilter gets a bloom filter from pool or creates new.
func generateBloomFilter(expectedElements int) *filter.BloomFilter {
	v := bloomFilterPool.Get()
	if v == nil {
		return filter.NewBloomFilter(expectedElements)
	}
	// Reset and resize for new expected elements
	v.SetN(expectedElements)
	m := expectedElements * filter.B
	v.ResizeBits((m + 63) / 64)
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

	m := n * filter.B
	bits := make([]uint64, 0, (m+63)/64)
	var err error
	bits, _, err = pkgencoding.DecodeUint64Block(bits, src[8:], uint64((m+63)/64))
	if err != nil {
		releaseBloomFilter(bf)
		return nil, fmt.Errorf("failed to decode bloom filter bits: %w", err)
	}
	bf.SetBits(bits)

	return bf, nil
}

// generateTagFilter creates a bloom filter for indexed tags.
func generateTagFilter(values [][]byte, expectedElements int) *filter.BloomFilter {
	if len(values) == 0 {
		return nil
	}

	bloomFilter := generateBloomFilter(expectedElements)

	for _, value := range values {
		bloomFilter.Add(value)
	}

	return bloomFilter
}

// EncodeTagValues encodes tag values using the shared encoding module.
func EncodeTagValues(values [][]byte, valueType pbv1.ValueType) ([]byte, error) {
	return encoding.EncodeTagValues(values, valueType)
}

// DecodeTagValues decodes tag values using the shared encoding module.
func DecodeTagValues(data []byte, valueType pbv1.ValueType, count int) ([][]byte, error) {
	return encoding.DecodeTagValues(data, valueType, count)
}

// updateMinMax updates min/max values for int64 tags.
func (td *tagData) updateMinMax() {
	if td.valueType != pbv1.ValueTypeInt64 || len(td.values) == 0 {
		return
	}

	var minVal, maxVal int64
	first := true

	for _, value := range td.values {
		if len(value) != 8 {
			continue // Skip invalid int64 values
		}

		val := pkgencoding.BytesToInt64(value)

		if first {
			minVal = val
			maxVal = val
			first = false
		} else {
			if val < minVal {
				minVal = val
			}
			if val > maxVal {
				maxVal = val
			}
		}
	}

	if !first {
		td.min = pkgencoding.Int64ToBytes(nil, minVal)
		td.max = pkgencoding.Int64ToBytes(nil, maxVal)
	}
}

// addValue adds a value to the tag data.
func (td *tagData) addValue(value []byte) {
	td.values = append(td.values, value)

	// Update filter for indexed tags
	if td.indexed && td.filter != nil {
		td.filter.Add(value)
	}
}

// hasValue checks if a value exists in the tag using the bloom filter.
func (td *tagData) hasValue(value []byte) bool {
	if !td.indexed || td.filter == nil {
		// For non-indexed tags, do linear search
		for _, v := range td.values {
			if bytes.Equal(v, value) {
				return true
			}
		}
		return false
	}

	return td.filter.MightContain(value)
}

// marshalTagMetadata serializes tag metadata to bytes.
func (tm *tagMetadata) marshal() ([]byte, error) {
	buf := &bytes.Buffer{}

	// Write name length and name
	nameBytes := []byte(tm.name)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, err
	}

	// Write value type
	if err := binary.Write(buf, binary.LittleEndian, uint32(tm.valueType)); err != nil {
		return nil, err
	}

	// Write data block
	if err := binary.Write(buf, binary.LittleEndian, tm.dataBlock.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, tm.dataBlock.size); err != nil {
		return nil, err
	}

	// Write filter block
	if err := binary.Write(buf, binary.LittleEndian, tm.filterBlock.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, tm.filterBlock.size); err != nil {
		return nil, err
	}

	// Write flags
	var flags uint8
	if tm.indexed {
		flags |= 1
	}
	if tm.compressed {
		flags |= 2
	}
	if err := binary.Write(buf, binary.LittleEndian, flags); err != nil {
		return nil, err
	}

	// Write min length and min
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tm.min))); err != nil {
		return nil, err
	}
	if len(tm.min) > 0 {
		if _, err := buf.Write(tm.min); err != nil {
			return nil, err
		}
	}

	// Write max length and max
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tm.max))); err != nil {
		return nil, err
	}
	if len(tm.max) > 0 {
		if _, err := buf.Write(tm.max); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// unmarshalTagMetadata deserializes tag metadata from bytes.
func unmarshalTagMetadata(data []byte) (*tagMetadata, error) {
	tm := generateTagMetadata()
	buf := bytes.NewReader(data)

	// Read name
	var nameLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &nameLen); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(buf, nameBytes); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	tm.name = string(nameBytes)

	// Read value type
	var valueType uint32
	if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	tm.valueType = pbv1.ValueType(valueType)

	// Read data block
	if err := binary.Read(buf, binary.LittleEndian, &tm.dataBlock.offset); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &tm.dataBlock.size); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}

	// Read filter block
	if err := binary.Read(buf, binary.LittleEndian, &tm.filterBlock.offset); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &tm.filterBlock.size); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}

	// Read flags
	var flags uint8
	if err := binary.Read(buf, binary.LittleEndian, &flags); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	tm.indexed = (flags & 1) != 0
	tm.compressed = (flags & 2) != 0

	// Read min
	var minLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &minLen); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	if minLen > 0 {
		tm.min = make([]byte, minLen)
		if _, err := io.ReadFull(buf, tm.min); err != nil {
			releaseTagMetadata(tm)
			return nil, err
		}
	}

	// Read max
	var maxLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &maxLen); err != nil {
		releaseTagMetadata(tm)
		return nil, err
	}
	if maxLen > 0 {
		tm.max = make([]byte, maxLen)
		if _, err := io.ReadFull(buf, tm.max); err != nil {
			releaseTagMetadata(tm)
			return nil, err
		}
	}

	return tm, nil
}

// marshalAppend serializes tagMetadata to bytes and appends to dst (panic version for mustWriteTag).
func (tm *tagMetadata) marshalAppend(dst []byte) []byte {
	data, err := tm.marshal()
	if err != nil {
		panic(fmt.Sprintf("failed to marshal tag metadata: %v", err))
	}
	return append(dst, data...)
}

// marshalTagMetadata is the error-returning version of marshal.
func (tm *tagMetadata) marshalTagMetadata() ([]byte, error) {
	buf := &bytes.Buffer{}

	// Write name length and name
	nameBytes := []byte(tm.name)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nameBytes); err != nil {
		return nil, err
	}

	// Write value type
	if err := binary.Write(buf, binary.LittleEndian, uint32(tm.valueType)); err != nil {
		return nil, err
	}

	// Write data block
	if err := binary.Write(buf, binary.LittleEndian, tm.dataBlock.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, tm.dataBlock.size); err != nil {
		return nil, err
	}

	// Write filter block
	if err := binary.Write(buf, binary.LittleEndian, tm.filterBlock.offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, tm.filterBlock.size); err != nil {
		return nil, err
	}

	// Write flags
	var flags uint8
	if tm.indexed {
		flags |= 1
	}
	if tm.compressed {
		flags |= 2
	}
	if err := binary.Write(buf, binary.LittleEndian, flags); err != nil {
		return nil, err
	}

	// Write min length and min
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tm.min))); err != nil {
		return nil, err
	}
	if len(tm.min) > 0 {
		if _, err := buf.Write(tm.min); err != nil {
			return nil, err
		}
	}

	// Write max length and max
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(tm.max))); err != nil {
		return nil, err
	}
	if len(tm.max) > 0 {
		if _, err := buf.Write(tm.max); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}
