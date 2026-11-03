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

// Package zstd provides ZSTD compression and decompression.
package zstd

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	decoder *zstd.Decoder

	mu sync.Mutex

	av atomic.Value
)

func init() {
	r := make(map[int]*zstd.Encoder)
	av.Store(r)

	var err error
	decoder, err = zstd.NewReader(nil)
	if err != nil {
		logger.Panicf("failed to create ZSTD reader: %v", err)
	}
}

// Decompress decompresses the src into dst.
func Decompress(dst, src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, dst)
}

// Compress compresses the src into dst.
func Compress(dst, src []byte, compressionLevel int) []byte {
	e := getEncoder(compressionLevel)
	return e.EncodeAll(src, dst)
}

func getEncoder(compressionLevel int) *zstd.Encoder {
	r := av.Load().(map[int]*zstd.Encoder)
	e := r[compressionLevel]
	if e != nil {
		return e
	}

	mu.Lock()
	r1 := av.Load().(map[int]*zstd.Encoder)
	if e = r1[compressionLevel]; e == nil {
		e = newEncoder(compressionLevel)
		r2 := make(map[int]*zstd.Encoder)
		for k, v := range r1 {
			r2[k] = v
		}
		r2[compressionLevel] = e
		av.Store(r2)
	}
	mu.Unlock()

	return e
}

func newEncoder(compressionLevel int) *zstd.Encoder {
	level := zstd.EncoderLevelFromZstd(compressionLevel)
	e, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(false), // Disable CRC for performance reasons.
		zstd.WithEncoderLevel(level))
	if err != nil {
		logger.Panicf("failed to create ZSTD writer: %v", err)
	}
	return e
}

// ErrDecompressionLimit identifies data whose decoded size exceeds a caller's budget.
var ErrDecompressionLimit = errors.New("decoded zstd block exceeds budget")

// DecompressBounded decompresses src while limiting the decoded bytes to limit.
// The returned buffer is independently owned. A nonpositive limit is invalid.
func DecompressBounded(src []byte, limit int) ([]byte, error) {
	if limit <= 0 {
		return nil, ErrDecompressionLimit
	}
	var header zstd.Header
	if err := header.Decode(src); err != nil {
		return nil, err
	}
	if header.HasFCS && header.FrameContentSize > uint64(limit) {
		return nil, ErrDecompressionLimit
	}
	bounded, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true),
		zstd.WithDecoderMaxMemory(uint64(max(limit, 1024))))
	if err != nil {
		return nil, err
	}
	defer bounded.Close()
	if !header.HasFCS {
		if err = bounded.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}
		decoded, readErr := io.ReadAll(io.LimitReader(bounded, int64(limit)+1))
		if len(decoded) > limit {
			return nil, ErrDecompressionLimit
		}
		return decoded, readErr
	}
	decoded, err := bounded.DecodeAll(src, make([]byte, 0, int(header.FrameContentSize)))
	if errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		return nil, ErrDecompressionLimit
	}
	return decoded, err
}
