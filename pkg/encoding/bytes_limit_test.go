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

package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
)

func TestBytesBlockDecoderRejectsCompressedPayloadOverBudget(t *testing.T) {
	encoded := EncodeBytesBlock(nil, [][]byte{bytes.Repeat([]byte("x"), 1<<20)})
	decoder := &BytesBlockDecoder{}
	decoder.SetMaxBytes(1024)
	_, err := decoder.Decode(nil, encoded, 1)
	require.ErrorIs(t, err, zstd.ErrDecompressionLimit)
	require.LessOrEqual(t, cap(decoder.data), 1024)
	decoder.SetMaxBytes(0)
	decoded, err := decoder.Decode(nil, encoded, 1)
	require.NoError(t, err)
	require.Len(t, decoded[0], 1<<20)
}

func TestBytesBlockDecoderBudgetAccumulatesAndResetReleasesUsage(t *testing.T) {
	decoder := &BytesBlockDecoder{}
	decoder.SetMaxBytes(5)
	encoded := EncodeBytesBlock(nil, [][]byte{[]byte("abc")})
	_, err := decoder.Decode(nil, encoded, 1)
	require.NoError(t, err)
	_, err = decoder.Decode(nil, encoded, 1)
	require.ErrorIs(t, err, zstd.ErrDecompressionLimit)
	decoder.Reset()
	_, err = decoder.Decode(nil, encoded, 1)
	require.NoError(t, err)
}
