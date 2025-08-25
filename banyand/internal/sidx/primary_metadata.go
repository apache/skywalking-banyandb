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
	"io"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// primaryBlockMetadata tracks metadata for a primary block in SIDX.
// This structure mirrors the stream module's primaryBlockMetadata but uses
// user-provided int64 keys instead of timestamps.
type primaryBlockMetadata struct {
	seriesID common.SeriesID
	minKey   int64 // Minimum user key in the block
	maxKey   int64 // Maximum user key in the block
	dataBlock
}

// reset resets primaryBlockMetadata for subsequent re-use.
func (pm *primaryBlockMetadata) reset() {
	pm.seriesID = 0
	pm.minKey = 0
	pm.maxKey = 0
	pm.offset = 0
	pm.size = 0
}

// mustWriteBlock writes a compressed primary block with the specified key range.
func (pm *primaryBlockMetadata) mustWriteBlock(data []byte, sidFirst common.SeriesID, minKey, maxKey int64, sw *writers) {
	pm.seriesID = sidFirst
	pm.minKey = minKey
	pm.maxKey = maxKey

	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	bb.Buf = zstd.Compress(bb.Buf[:0], data, 1)
	pm.offset = sw.primaryWriter.bytesWritten
	pm.size = uint64(len(bb.Buf))
	sw.primaryWriter.MustWrite(bb.Buf)
	bb.Buf = bb.Buf[:0] // Reset for reuse
	bigValuePool.Put(bb)
}

// marshal serializes primaryBlockMetadata to bytes.
func (pm *primaryBlockMetadata) marshal(dst []byte) []byte {
	dst = pm.seriesID.AppendToBytes(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(pm.minKey))
	dst = encoding.Uint64ToBytes(dst, uint64(pm.maxKey))
	dst = encoding.Uint64ToBytes(dst, pm.offset)
	dst = encoding.Uint64ToBytes(dst, pm.size)
	return dst
}

// unmarshal deserializes primaryBlockMetadata from bytes.
func (pm *primaryBlockMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 40 {
		return nil, fmt.Errorf("cannot unmarshal primaryBlockMetadata from %d bytes; expect at least 40 bytes", len(src))
	}
	pm.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	pm.minKey = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pm.maxKey = int64(encoding.BytesToUint64(src))
	src = src[8:]
	pm.offset = encoding.BytesToUint64(src)
	src = src[8:]
	pm.size = encoding.BytesToUint64(src)
	return src[8:], nil
}

// mustReadPrimaryBlockMetadata reads primary block metadata from a file reader.
func mustReadPrimaryBlockMetadata(dst []primaryBlockMetadata, r fs.Reader) []primaryBlockMetadata {
	sr := r.SequentialRead()
	data, err := io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read primaryBlockMetadata entries from %s: %s", r.Path(), err)
	}
	fs.MustClose(sr)

	bb := bigValuePool.Get()
	if bb == nil {
		bb = &bytes.Buffer{}
	}
	bb.Buf, err = zstd.Decompress(bb.Buf[:0], data)
	if err != nil {
		logger.Panicf("cannot decompress primaryBlockMetadata entries from %s: %s", r.Path(), err)
	}
	dst, err = unmarshalPrimaryBlockMetadata(dst, bb.Buf)
	bb.Buf = bb.Buf[:0] // Reset for reuse
	bigValuePool.Put(bb)
	if err != nil {
		logger.Panicf("cannot parse primaryBlockMetadata entries from %s: %s", r.Path(), err)
	}
	return dst
}

// unmarshalPrimaryBlockMetadata deserializes multiple primaryBlockMetadata from bytes.
func unmarshalPrimaryBlockMetadata(dst []primaryBlockMetadata, src []byte) ([]primaryBlockMetadata, error) {
	dstOrig := dst
	for len(src) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, primaryBlockMetadata{})
		}
		pm := &dst[len(dst)-1]
		tail, err := pm.unmarshal(src)
		if err != nil {
			return dstOrig, fmt.Errorf("cannot unmarshal primaryBlockMetadata %d: %w", len(dst)-len(dstOrig), err)
		}
		src = tail
	}
	if err := validatePrimaryBlockMetadata(dst[len(dstOrig):]); err != nil {
		return dstOrig, err
	}
	return dst, nil
}

// validatePrimaryBlockMetadata ensures primary block metadata is properly ordered.
func validatePrimaryBlockMetadata(pms []primaryBlockMetadata) error {
	for i := 1; i < len(pms); i++ {
		if pms[i].seriesID < pms[i-1].seriesID {
			return fmt.Errorf("unexpected primaryBlockMetadata with smaller seriesID=%d after bigger seriesID=%d", &pms[i].seriesID, &pms[i-1].seriesID)
		}
	}
	return nil
}
