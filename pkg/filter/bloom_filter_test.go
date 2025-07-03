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

package filter

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter(t *testing.T) {
	assert := assert.New(t)

	bf := NewBloomFilter(3)
	assert.NotNil(bf)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	for i := 0; i < 3; i++ {
		res := bf.Add(items[i])
		assert.True(res)
	}

	for i := 0; i < 3; i++ {
		mightContain := bf.MightContain(items[i])
		assert.True(mightContain)
	}

	for i := 3; i < 5; i++ {
		mightContain := bf.MightContain(items[i])
		assert.False(mightContain)
	}
}

func BenchmarkFilterAdd(b *testing.B) {
	for _, n := range []int{1e3, 1e4, 1e5, 1e6, 1e7} {
		data := generateTestData(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkFilterAdd(b, n, data)
		})
	}
}

func benchmarkFilterAdd(b *testing.B, n int, data [][]byte) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		bf := NewBloomFilter(n)
		for pb.Next() {
			for i := 0; i < n; i++ {
				bf.Add(data[i])
			}
		}
	})
}

func BenchmarkFilterMightContainHit(b *testing.B) {
	for _, n := range []int{1e3, 1e4, 1e5, 1e6, 1e7} {
		data := generateTestData(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkFilterMightContainHit(b, n, data)
		})
	}
}

func benchmarkFilterMightContainHit(b *testing.B, n int, data [][]byte) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		bf := NewBloomFilter(n)
		for i := 0; i < n; i++ {
			bf.Add(data[i])
		}
		for pb.Next() {
			for i := 0; i < n; i++ {
				if !bf.MightContain(data[i]) {
					panic(fmt.Errorf("missing item %d", data[i]))
				}
			}
		}
	})
}

func BenchmarkFilterMightContainMiss(b *testing.B) {
	for _, n := range []int{1e3, 1e4, 1e5, 1e6, 1e7} {
		data := generateTestData(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkFilterMightContainMiss(b, n, data)
		})
	}
}

func benchmarkFilterMightContainMiss(b *testing.B, n int, data [][]byte) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		bf := NewBloomFilter(n)
		for pb.Next() {
			for i := 0; i < n; i++ {
				if bf.MightContain(data[i]) {
					panic(fmt.Errorf("unexpected item %d", data[i]))
				}
			}
		}
	})
}

func generateTestData(n int) [][]byte {
	data := make([][]byte, 0)
	for i := 0; i < n; i++ {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(i))
		data = append(data, buf)
	}
	return data
}
