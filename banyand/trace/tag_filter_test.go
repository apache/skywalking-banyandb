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

package trace

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/filter"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestEncodeAndDecodeBloomFilter(t *testing.T) {
	assert := assert.New(t)

	bf := filter.NewBloomFilter(3)

	items := [][]byte{
		[]byte("skywalking"),
		[]byte("banyandb"),
		[]byte(""),
		[]byte("hello"),
		[]byte("world"),
	}

	for i := 0; i < 3; i++ {
		bf.Add(items[i])
	}

	buf := make([]byte, 0)
	buf = encodeBloomFilter(buf, bf)
	bf2 := filter.NewBloomFilter(0)
	bf2 = decodeBloomFilter(buf, bf2)

	for i := 0; i < 3; i++ {
		mightContain := bf2.MightContain(items[i])
		assert.True(mightContain)
	}

	for i := 3; i < 5; i++ {
		mightContain := bf2.MightContain(items[i])
		assert.False(mightContain)
	}
}

type mockReader struct {
	data []byte
}

func (mr *mockReader) Path() string {
	return "mock"
}

func (mr *mockReader) Read(offset int64, buffer []byte) (int, error) {
	if offset >= int64(len(mr.data)) {
		return 0, nil
	}
	n := copy(buffer, mr.data[offset:])
	return n, nil
}

func (mr *mockReader) SequentialRead() fs.SeqReader {
	return nil
}

func (mr *mockReader) Close() error {
	return nil
}

func generateMetaAndFilter(itemsPerTag int) ([]byte, []byte) {
	filterBuf := bytes.Buffer{}

	bf := filter.NewBloomFilter(itemsPerTag)
	for j := 0; j < itemsPerTag; j++ {
		item := make([]byte, 8)
		binary.BigEndian.PutUint64(item, uint64(j))
		bf.Add(item)
	}
	buf := make([]byte, 0)
	buf = encodeBloomFilter(buf, bf)

	tm := &tagMetadata{
		name:      "test_tag",
		valueType: pbv1.ValueTypeInt64,
		min:       make([]byte, 8),
		max:       make([]byte, 8),
	}
	binary.BigEndian.PutUint64(tm.min, uint64(0))
	binary.BigEndian.PutUint64(tm.max, uint64(itemsPerTag-1))
	tm.filterBlock.offset = 0
	tm.filterBlock.size = uint64(len(buf))

	filterBuf.Write(buf)

	metaBuf := make([]byte, 0)
	metaBuf = tm.marshal(metaBuf)
	return metaBuf, filterBuf.Bytes()
}

func BenchmarkTagFiltersUnmarshal(b *testing.B) {
	testCases := []struct {
		tagCount    int
		itemsPerTag int
	}{
		{5, 10},
		{10, 100},
		{15, 1000},
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("tags=%d_items=%d", tc.tagCount, tc.itemsPerTag), func(b *testing.B) {
			tags := make(map[string]*dataBlock)
			metaReaders := make(map[string]fs.Reader)
			filterReaders := make(map[string]fs.Reader)
			for i := 0; i < tc.tagCount; i++ {
				tagName := fmt.Sprintf("tag_%d", i)
				metaBuf, filterBuf := generateMetaAndFilter(tc.itemsPerTag)
				tags[tagName] = &dataBlock{
					offset: 0,
					size:   uint64(len(metaBuf)),
				}
				metaReaders[tagName] = &mockReader{data: metaBuf}
				filterReaders[tagName] = &mockReader{data: filterBuf}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tfs := generateTagFilters()
				if tfs.tagFilters == nil {
					tfs.tagFilters = make(map[string]*tagFilter)
				}
				tfs.unmarshal(tags, metaReaders, filterReaders)
				releaseTagFilters(tfs)
			}
		})
	}
}
