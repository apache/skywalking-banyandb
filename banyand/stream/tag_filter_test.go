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

package stream

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

func generateMetaAndFilter(tagCount int, itemsPerTag int) ([]byte, []byte) {
	tfm := generateTagFamilyMetadata()
	defer releaseTagFamilyMetadata(tfm)
	filterBuf := bytes.Buffer{}

	for i := 0; i < tagCount; i++ {
		bf := filter.NewBloomFilter(itemsPerTag)
		for j := 0; j < itemsPerTag; j++ {
			item := make([]byte, 8)
			binary.BigEndian.PutUint64(item, uint64(i*itemsPerTag+j))
			bf.Add(item)
		}
		buf := make([]byte, 0)
		buf = encodeBloomFilter(buf, bf)

		tm := &tagMetadata{
			name:      fmt.Sprintf("tag_%d", i),
			valueType: pbv1.ValueTypeInt64,
			min:       make([]byte, 8),
			max:       make([]byte, 8),
		}
		binary.BigEndian.PutUint64(tm.min, uint64(i*itemsPerTag))
		binary.BigEndian.PutUint64(tm.max, uint64(i*itemsPerTag+itemsPerTag-1))
		tm.filterBlock.offset = uint64(filterBuf.Len())
		tm.filterBlock.size = uint64(len(buf))
		tfm.tagMetadata = append(tfm.tagMetadata, *tm)

		filterBuf.Write(buf)
	}

	metaBuf := make([]byte, 0)
	metaBuf = tfm.marshal(metaBuf)
	return metaBuf, filterBuf.Bytes()
}

func BenchmarkTagFamilyFiltersUnmarshal(b *testing.B) {
	testCases := []struct {
		tagFamilyCount int
		tagCount       int
		itemsPerTag    int
	}{
		{1, 5, 10},
		{2, 10, 100},
		{3, 15, 1000},
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("tagFamilies=%d_tags=%d_items=%d", tc.tagFamilyCount, tc.tagCount, tc.itemsPerTag), func(b *testing.B) {
			tagFamilies := make(map[string]*dataBlock)
			metaReaders := make(map[string]fs.Reader)
			filterReaders := make(map[string]fs.Reader)
			for i := 0; i < tc.tagFamilyCount; i++ {
				familyName := fmt.Sprintf("tagFamily_%d", i)
				metaBuf, filterBuf := generateMetaAndFilter(tc.tagCount, tc.itemsPerTag)
				tagFamilies[familyName] = &dataBlock{
					offset: 0,
					size:   uint64(len(metaBuf)),
				}
				metaReaders[familyName] = &mockReader{data: metaBuf}
				filterReaders[familyName] = &mockReader{data: filterBuf}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tfs := generateTagFamilyFilters()
				tfs.unmarshal(tagFamilies, metaReaders, filterReaders)
				releaseTagFamilyFilters(tfs)
			}
		})
	}
}
