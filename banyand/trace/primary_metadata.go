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
	"io"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type primaryBlockMetadata struct {
	traceID string
	dataBlock
	traceIDLen uint32
}

// reset resets pbm for subsequent re-use.
func (pbm *primaryBlockMetadata) reset() {
	pbm.traceIDLen = 0
	pbm.traceID = ""
	pbm.offset = 0
	pbm.size = 0
}

func (pbm *primaryBlockMetadata) mustWriteBlock(data []byte, traceIDLen uint32, traceID string, sw *writers) {
	pbm.traceIDLen = traceIDLen
	pbm.traceID = traceID

	bb := bigValuePool.Generate()
	bb.Buf = zstd.Compress(bb.Buf[:0], data, 1)
	pbm.offset = sw.primaryWriter.bytesWritten
	pbm.size = uint64(len(bb.Buf))
	sw.primaryWriter.MustWrite(bb.Buf)
	bigValuePool.Release(bb)
}

func (pbm *primaryBlockMetadata) marshal(dst []byte) []byte {
	dst = encoding.Uint32ToBytes(dst, pbm.traceIDLen)
	dst = append(dst, pbm.traceID...)
	paddingLen := pbm.traceIDLen - uint32(len(pbm.traceID))
	if paddingLen > 0 {
		dst = append(dst, bytes.Repeat([]byte{0}, int(paddingLen))...)
	}
	dst = encoding.Uint64ToBytes(dst, pbm.offset)
	dst = encoding.Uint64ToBytes(dst, pbm.size)
	return dst
}

func (pbm *primaryBlockMetadata) unmarshal(src []byte) ([]byte, error) {
	pbm.traceIDLen = encoding.BytesToUint32(src)
	src = src[4:]
	if len(src) < int(16+pbm.traceIDLen) {
		return nil, fmt.Errorf("cannot unmarshal primaryBlockMetadata from %d bytes; expect at least %d bytes", len(src), 32+pbm.traceIDLen)
	}
	pbm.traceID = strings.TrimRight(string(src[:pbm.traceIDLen]), "\x00")
	src = src[pbm.traceIDLen:]
	pbm.offset = encoding.BytesToUint64(src)
	src = src[8:]
	pbm.size = encoding.BytesToUint64(src)
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
		pbm := &dst[len(dst)-1]
		tail, err := pbm.unmarshal(src)
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

func validatePrimaryBlockMetadata(pbm []primaryBlockMetadata) error {
	for i := 1; i < len(pbm); i++ {
		if pbm[i].traceID < pbm[i-1].traceID {
			return fmt.Errorf("unexpected primaryBlockMetadata with smaller traceID=%s after bigger traceID=%s", pbm[i].traceID, pbm[i-1].traceID)
		}
	}
	return nil
}
