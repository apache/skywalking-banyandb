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
