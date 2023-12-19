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

package zstd_test

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
)

func randString(n int) []byte {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]byte, n)
	for i := range b {
		max := big.NewInt(int64(len(letters)))
		randIndex, err := rand.Int(rand.Reader, max)
		if err != nil {
			panic(err)
		}
		b[i] = letters[randIndex.Int64()]
	}

	return b
}

func TestCompressAndDecompress(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "SingleByte",
			data: randString(1),
		},
		{
			name: "NormalSizeBytes",
			data: randString(1000), // 1000 bytes
		},
		{
			name: "SuperBigBytes",
			data: randString(1e6), // 1 million bytes
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressionLevel := 3

			// Test Compress
			compressed := zstd.Compress(nil, tc.data, compressionLevel)
			require.NotEmpty(t, compressed, "Compress should return non-empty result")

			// Test Decompress
			decompressed, err := zstd.Decompress(nil, compressed)
			require.NoError(t, err, "Decompress should not return an error")
			require.Equal(t, tc.data, decompressed, "Decompressed data should be equal to the original data")
		})
	}
}
