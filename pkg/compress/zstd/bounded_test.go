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

package zstd

import (
	"bytes"
	"testing"

	nativezstd "github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestDecompressBoundedKnownSizeAllocatesActualFrame(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 300)
	decoded, err := DecompressBounded(Compress(nil, data, 1), 32<<20)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
	require.Less(t, cap(decoded), 1024)
	_, err = DecompressBounded(Compress(nil, data, 1), 100)
	require.ErrorIs(t, err, ErrDecompressionLimit)
}

func TestDecompressBoundedUnknownFrameSize(t *testing.T) {
	var output bytes.Buffer
	writer, err := nativezstd.NewWriter(&output, nativezstd.WithEncoderConcurrency(1), nativezstd.WithWindowSize(1024))
	require.NoError(t, err)
	data := bytes.Repeat([]byte("x"), 2000)
	_, err = writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	var header nativezstd.Header
	require.NoError(t, header.Decode(output.Bytes()))
	require.False(t, header.HasFCS)
	decoded, err := DecompressBounded(output.Bytes(), 3000)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
	_, err = DecompressBounded(output.Bytes(), 1500)
	require.ErrorIs(t, err, ErrDecompressionLimit)
}
