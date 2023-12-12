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

package encoding_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/encoding"
)

func TestDecodeBytes(t *testing.T) {
	src := [][]byte{
		[]byte("Hello, "),
		[]byte("world!"),
	}
	encoded := make([]byte, 0)
	for _, d := range src {
		encoded = encoding.EncodeBytes(encoded, d)
	}
	var decoded []byte
	var err error
	for i := range src {
		encoded, decoded, err = encoding.DecodeBytes(encoded)
		require.Nil(t, err)
		assert.Equal(t, src[i], decoded)
	}
}

func TestEncodeBlockAndDecode(t *testing.T) {
	slices := [][]byte{
		[]byte("Hello, "),
		[]byte("world!"),
		[]byte("Testing bytes"),
	}

	encoded := encoding.EncodeBytesBlock(nil, slices)
	require.NotNil(t, encoded)
	blockDecoder := &encoding.BytesBlockDecoder{}
	decoded, err := blockDecoder.Decode(nil, encoded, uint64(len(slices)))
	require.Nil(t, err)
	for i, slice := range slices {
		assert.Equal(t, slice, decoded[i])
	}
}
