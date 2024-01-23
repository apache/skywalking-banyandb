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
	"fmt"
	"io"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type primaryBlockMetadata struct {
	seriesID     common.SeriesID
	minTimestamp int64
	maxTimestamp int64
	dataBlock
}

// reset resets ih for subsequent re-use.
func (ph *primaryBlockMetadata) reset() {
	ph.seriesID = 0
	ph.minTimestamp = 0
	ph.maxTimestamp = 0
	ph.offset = 0
	ph.size = 0
}

func (ph *primaryBlockMetadata) mustWriteBlock(data []byte, sidFirst common.SeriesID, minTimestamp, maxTimestamp int64, sw *writers) {
	ph.seriesID = sidFirst
	ph.minTimestamp = minTimestamp
	ph.maxTimestamp = maxTimestamp

	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], data, 1)
	ph.offset = sw.primaryWriter.bytesWritten
	ph.size = uint64(len(bb.Buf))
	sw.primaryWriter.MustWrite(bb.Buf)
	bigValuePool.Release(bb)
}

func (ph *primaryBlockMetadata) marshal(dst []byte) []byte {
	dst = ph.seriesID.AppendToBytes(dst)
	dst = encoding.Uint64ToBytes(dst, uint64(ph.minTimestamp))
	dst = encoding.Uint64ToBytes(dst, uint64(ph.maxTimestamp))
	dst = encoding.Uint64ToBytes(dst, ph.offset)
	dst = encoding.Uint64ToBytes(dst, ph.size)
	return dst
}

func (ph *primaryBlockMetadata) unmarshal(src []byte) ([]byte, error) {
	if len(src) < 40 {
		return nil, fmt.Errorf("cannot unmarshal primaryBlockMetadata from %d bytes; expect at least 40 bytes", len(src))
	}
	ph.seriesID = common.SeriesID(encoding.BytesToUint64(src))
	src = src[8:]
	ph.minTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	ph.maxTimestamp = int64(encoding.BytesToUint64(src))
	src = src[8:]
	ph.offset = encoding.BytesToUint64(src)
	src = src[8:]
	ph.size = encoding.BytesToUint64(src)
	return src[8:], nil
}

func mustReadPrimaryBlockMetadata(dst []primaryBlockMetadata, r fs.Reader) []primaryBlockMetadata {
	sr := r.SequentialRead()
	data, err := io.ReadAll(sr)
	if err != nil {
		logger.Panicf("cannot read primaryBlockMetadata entries from %s: %s", r.Path(), err)
	}
	fs.MustClose(sr)

	bb := bigValuePool.Generate()
	bb.Buf, err = zstd.Decompress(bb.Buf[:0], data)
	if err != nil {
		logger.Panicf("cannot decompress indexBlockHeader entries from %s: %s", r.Path(), err)
	}
	dst, err = unmarshalPrimaryBlockMetadata(dst, bb.Buf)
	bigValuePool.Release(bb)
	if err != nil {
		logger.Panicf("cannot parse indexBlockHeader entries from %s: %s", r.Path(), err)
	}
	return dst
}

func unmarshalPrimaryBlockMetadata(dst []primaryBlockMetadata, src []byte) ([]primaryBlockMetadata, error) {
	dstOrig := dst
	for len(src) > 0 {
		if len(dst) < cap(dst) {
			dst = dst[:len(dst)+1]
		} else {
			dst = append(dst, primaryBlockMetadata{})
		}
		ih := &dst[len(dst)-1]
		tail, err := ih.unmarshal(src)
		if err != nil {
			return dstOrig, fmt.Errorf("cannot unmarshal primaryBlockHeader %d: %w", len(dst)-len(dstOrig), err)
		}
		src = tail
	}
	if err := validatePrimaryBlockMetadata(dst[len(dstOrig):]); err != nil {
		return dstOrig, err
	}
	return dst, nil
}

func validatePrimaryBlockMetadata(ihs []primaryBlockMetadata) error {
	for i := 1; i < len(ihs); i++ {
		if ihs[i].seriesID < ihs[i-1].seriesID {
			return fmt.Errorf("unexpected primaryBlockMetadata with smaller seriesID=%d after bigger seriesID=%d", &ihs[i].seriesID, &ihs[i-1].seriesID)
		}
	}
	return nil
}
