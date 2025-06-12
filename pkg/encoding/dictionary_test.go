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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeAndDecodeRLE(t *testing.T) {
	arr := []uint32{1, 1, 2, 2, 3, 2, 2, 2, 1, 2, 1}
	encoded := make([]uint32, 0)
	encoded = encodeRLE(encoded, arr)
	require.Equal(t, []uint32{1, 2, 2, 2, 3, 1, 2, 3, 1, 1, 2, 1, 1, 1}, encoded)
	decoded := make([]uint32, 0)
	decoded = decodeRLE(decoded, encoded)
	require.Equal(t, arr, decoded)
}

func TestEncodeAndDecodeBitPacking(t *testing.T) {
	arr := []uint32{1, 2, 2, 2, 3, 1, 2, 3, 1, 1, 2, 1, 1, 1}
	encoded := encodeBitPacking(arr)
	decoded := make([]uint32, 0)
	decoded, err := decodeBitPacking(decoded, encoded)
	require.NoError(t, err)
	require.Equal(t, arr, decoded)
}

func TestEncodeAndDecodeDictionary(t *testing.T) {
	dict := NewDictionary()
	values := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte("hello"),
		[]byte("world"),
		[]byte("hello"),
	}
	for _, value := range values {
		dict.Add(value)
	}

	encoded := dict.Encode(nil)
	decoded := NewDictionary()
	err := decoded.Decode(encoded)
	require.NoError(t, err)

	expectedValues := [][]byte{[]byte("skywalking"), []byte("banyandb"), []byte("hello"), []byte("world")}
	require.Equal(t, expectedValues, decoded.values)
	expectedIndices := []uint32{0, 1, 2, 3, 2}
	require.Equal(t, expectedIndices, decoded.indices)
	expectedMap := map[string]uint32{
		"skywalking": 0,
		"banyandb":   1,
		"hello":      2,
		"world":      3,
	}
	for value, index := range decoded.m {
		require.Equal(t, expectedMap[value], index)
	}
}

type parameter struct {
	size        int
	cardinality int
}

var pList = [3]parameter{
	{size: 1000, cardinality: 10},
	{size: 10000, cardinality: 100},
	{size: 100000, cardinality: 1000},
}

func BenchmarkEncodeDictionary(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		b.Run(fmt.Sprintf("size=%d_cardinality=%d", p.size, p.cardinality), func(b *testing.B) {
			values := generateData(p.size, p.cardinality)
			dict := NewDictionary()
			for _, value := range values {
				dict.Add(value)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded := make([]byte, 0)
				dict.Encode(encoded)
			}
		})
	}
}

func BenchmarkDncodeDictionary(b *testing.B) {
	b.ReportAllocs()
	for _, p := range pList {
		b.Run(fmt.Sprintf("size=%d_cardinality=%d", p.size, p.cardinality), func(b *testing.B) {
			values := generateData(p.size, p.cardinality)
			dict := NewDictionary()
			for _, value := range values {
				dict.Add(value)
			}
			encoded := make([]byte, 0)
			encoded = dict.Encode(encoded)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decoded := NewDictionary()
				err := decoded.Decode(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func generateData(size int, cardinality int) [][]byte {
	values := make([][]byte, 0)
	for i := 0; i < size; i++ {
		values = append(values, []byte(fmt.Sprintf("value_%d", i%cardinality)))
	}
	return values
}
