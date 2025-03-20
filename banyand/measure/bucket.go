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

package measure

import (
	"sort"
	"unsafe"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

type timestampsBuf struct {
	values       []int64
	buf          []byte
	minTimestamp int64
	maxTimestamp int64
	encodeType   encoding.EncodeType
}

type versionsBuf struct {
	values       []int64
	buf          []byte
	firstVersion int64
	encodeType   encoding.EncodeType
}

type columnBuf struct {
	column
	buf []byte
}

type columnFamilyBuf struct {
	name    string
	columns []columnBuf
}

type bucket struct {
	timestampsBuf      timestampsBuf
	versionsBuf        versionsBuf
	fieldBuf           columnFamilyBuf
	count              int
	size               uintptr
	seriesID           common.SeriesID
	lastQueryTimestamp int64
}

func newBucket(seriesID common.SeriesID) *bucket {
	return &bucket{
		seriesID: seriesID,
		timestampsBuf: timestampsBuf{
			values: make([]int64, 0),
			buf:    make([]byte, 0),
		},
		versionsBuf: versionsBuf{
			values: make([]int64, 0),
			buf:    make([]byte, 0),
		},
		fieldBuf: columnFamilyBuf{
			name:    "",
			columns: make([]columnBuf, 0),
		},
	}
}

func (b *bucket) reset() {
	b.timestampsBuf.values = b.timestampsBuf.values[:0]
	b.timestampsBuf.buf = b.timestampsBuf.buf[:0]
	b.versionsBuf.values = b.versionsBuf.values[:0]
	b.versionsBuf.buf = b.versionsBuf.buf[:0]
	b.fieldBuf.name = ""
	b.fieldBuf.columns = b.fieldBuf.columns[:0]
	b.count = 0
	b.size = 0
	b.lastQueryTimestamp = 0
}

func (b *bucket) addOne(bc *blockCursor, idx int) {
	b.timestampsBuf.values = append(b.timestampsBuf.values, bc.timestamps[idx])
	b.versionsBuf.values = append(b.versionsBuf.values, bc.versions[idx])
	for i := 0; i < len(bc.fields.columns); i++ {
		for j := 0; j < len(b.fieldBuf.columns); j++ {
			if bc.fields.columns[i].name == b.fieldBuf.columns[j].name {
				b.fieldBuf.columns[j].values = append(b.fieldBuf.columns[j].values, bc.fields.columns[i].values[idx])
				break
			}
		}
	}
}

func (b *bucket) addRange(bc *blockCursor, idx int) {
	b.timestampsBuf.values = append(b.timestampsBuf.values, bc.timestamps[idx:]...)
	b.versionsBuf.values = append(b.versionsBuf.values, bc.versions[idx:]...)
	if b.count == 0 {
		b.fieldBuf.columns = make([]columnBuf, len(bc.fields.columns))
		for i := 0; i < len(bc.fields.columns); i++ {
			b.fieldBuf.columns[i].name = bc.fields.columns[i].name
			b.fieldBuf.columns[i].valueType = bc.fields.columns[i].valueType
			b.fieldBuf.columns[i].values = append(b.fieldBuf.columns[i].values, bc.fields.columns[i].values[idx:]...)
		}
		return
	}
	for i := 0; i < len(bc.fields.columns); i++ {
		for j := 0; j < len(b.fieldBuf.columns); j++ {
			if bc.fields.columns[i].name == b.fieldBuf.columns[j].name {
				b.fieldBuf.columns[j].values = append(b.fieldBuf.columns[j].values, bc.fields.columns[i].values[idx:]...)
				break
			}
		}
	}
}

func (b *bucket) merge() error {
	ts, vs := make([]int64, 0), make([]int64, 0)
	var err error
	if b.count > 0 {
		ts, err = encoding.BytesToInt64List(ts, b.timestampsBuf.buf, b.timestampsBuf.encodeType, b.timestampsBuf.minTimestamp, b.count)
		if err != nil {
			return err
		}
		vs, err = encoding.BytesToInt64List(vs, b.versionsBuf.buf, b.versionsBuf.encodeType, b.versionsBuf.firstVersion, b.count)
		if err != nil {
			return err
		}
	}

	mts, mvs, indexInfos := mergeTimestampsAndVersions(ts, b.timestampsBuf.values, vs, b.versionsBuf.values)
	b.timestampsBuf.minTimestamp, b.timestampsBuf.maxTimestamp = mts[0], mts[len(mts)-1]
	b.versionsBuf.firstVersion = mvs[0]
	b.timestampsBuf.buf, b.timestampsBuf.encodeType, _ = encoding.Int64ListToBytes(b.timestampsBuf.buf[:0], mts)
	b.versionsBuf.buf, b.versionsBuf.encodeType, _ = encoding.Int64ListToBytes(b.versionsBuf.buf[:0], mvs)
	b.timestampsBuf.values, b.versionsBuf.values = b.timestampsBuf.values[:0], b.versionsBuf.values[:0]

	decoder := encoding.NewBytesBlockDecoder()
	for i := 0; i < len(b.fieldBuf.columns); i++ {
		decoder.Reset()
		vals := make([][]byte, 0)
		if b.count > 0 {
			vals, err = decoder.Decode(vals, b.fieldBuf.columns[i].buf, uint64(b.count))
			if err != nil {
				return err
			}
		}
		mvals := mergeValues(vals, b.fieldBuf.columns[i].column.values, indexInfos)
		b.fieldBuf.columns[i].buf = encoding.EncodeBytesBlock(b.fieldBuf.columns[i].buf[:0], mvals)
		b.fieldBuf.columns[i].values = b.fieldBuf.columns[i].values[:0]
	}
	b.count, b.size = len(mts), unsafe.Sizeof(b)
	return nil
}

func (b *bucket) findRange(startTimestamp, endTimestamp int64) (*block, error) {
	if startTimestamp > b.timestampsBuf.maxTimestamp || endTimestamp < b.timestampsBuf.minTimestamp {
		return nil, nil
	}
	if startTimestamp < b.timestampsBuf.minTimestamp {
		startTimestamp = b.timestampsBuf.minTimestamp
	}
	if endTimestamp > b.timestampsBuf.maxTimestamp {
		endTimestamp = b.timestampsBuf.maxTimestamp
	}
	ts, vs := make([]int64, 0), make([]int64, 0)
	ts, err := encoding.BytesToInt64List(ts, b.timestampsBuf.buf, b.timestampsBuf.encodeType, b.timestampsBuf.minTimestamp, b.count)
	if err != nil {
		return nil, err
	}
	vs, err = encoding.BytesToInt64List(vs, b.versionsBuf.buf, b.versionsBuf.encodeType, b.versionsBuf.firstVersion, b.count)
	if err != nil {
		return nil, err
	}
	startIndex := sort.Search(len(ts), func(i int) bool {
		return ts[i] >= startTimestamp
	})
	endIndex := sort.Search(len(ts), func(i int) bool {
		return ts[i] > endTimestamp
	})
	if startIndex == endIndex {
		return nil, nil
	}
	block := &block{
		timestamps: make([]int64, endIndex-startIndex),
		versions:   make([]int64, endIndex-startIndex),
		field: columnFamily{
			name:    "",
			columns: make([]column, len(b.fieldBuf.columns)),
		},
	}
	copy(block.timestamps, ts[startIndex:endIndex])
	copy(block.versions, vs[startIndex:endIndex])

	decoder := encoding.NewBytesBlockDecoder()
	vals := make([][]byte, 0)
	for i := 0; i < len(b.fieldBuf.columns); i++ {
		decoder.Reset()
		vals, err := decoder.Decode(vals[:0], b.fieldBuf.columns[i].buf, uint64(b.count))
		if err != nil {
			return nil, err
		}
		block.field.columns[i].name = b.fieldBuf.columns[i].name
		block.field.columns[i].valueType = b.fieldBuf.columns[i].valueType
		for j := startIndex; j < endIndex; j++ {
			val := make([]byte, len(vals[j]))
			copy(val, vals[j])
			block.field.columns[i].values = append(block.field.columns[i].values, val)
		}
	}
	return block, nil
}

type indexInfo struct {
	arrayIndex    int
	positionIndex int
}

func mergeTimestampsAndVersions(ts1, ts2, vs1, vs2 []int64) ([]int64, []int64, []indexInfo) {
	ts, vs := make([]int64, 0), make([]int64, 0)
	indexInfos := make([]indexInfo, 0)
	i, j := 0, 0

	for i < len(ts1) && j < len(ts2) {
		if ts1[i] == ts2[j] {
			if vs1[i] < vs2[j] {
				ts, vs = append(ts, ts1[i]), append(vs, vs1[i])
				indexInfos = append(indexInfos, indexInfo{arrayIndex: 0, positionIndex: i})
			} else {
				ts, vs = append(ts, ts2[j]), append(vs, vs2[j])
				indexInfos = append(indexInfos, indexInfo{arrayIndex: 1, positionIndex: j})
			}
			continue
		}
		if ts1[i] < ts2[j] {
			ts, vs = append(ts, ts1[i]), append(vs, vs1[i])
			indexInfos = append(indexInfos, indexInfo{arrayIndex: 0, positionIndex: i})
			i++
		} else {
			ts, vs = append(ts, ts2[j]), append(vs, vs2[j])
			indexInfos = append(indexInfos, indexInfo{arrayIndex: 1, positionIndex: j})
			j++
		}
	}

	for i < len(ts1) {
		ts, vs = append(ts, ts1[i]), append(vs, vs1[i])
		indexInfos = append(indexInfos, indexInfo{arrayIndex: 0, positionIndex: i})
		i++
	}
	for j < len(ts2) {
		ts, vs = append(ts, ts2[j]), append(vs, vs2[j])
		indexInfos = append(indexInfos, indexInfo{arrayIndex: 1, positionIndex: j})
		j++
	}
	return ts, vs, indexInfos
}

func mergeValues(vals1, vals2 [][]byte, ii []indexInfo) [][]byte {
	vals := make([][]byte, 0)
	for i := 0; i < len(ii); i++ {
		if ii[i].arrayIndex == 0 {
			vals = append(vals, vals1[ii[i].positionIndex])
		} else {
			vals = append(vals, vals2[ii[i].positionIndex])
		}
	}
	return vals
}

var bucketPool = pool.Register[*bucket]("bucket")

func generateBucket(seriesID common.SeriesID) *bucket {
	v := bucketPool.Get()
	if v == nil {
		return &bucket{
			seriesID: seriesID,
			timestampsBuf: timestampsBuf{
				values: make([]int64, 0),
				buf:    make([]byte, 0),
			},
			versionsBuf: versionsBuf{
				values: make([]int64, 0),
				buf:    make([]byte, 0),
			},
			fieldBuf: columnFamilyBuf{
				name:    "",
				columns: make([]columnBuf, 0),
			},
		}
	}
	return v
}

func releaseBucket(b *bucket) {
	b.reset()
	bucketPool.Put(b)
}

type bucketHeap []*bucket

func (bh bucketHeap) Len() int { return len(bh) }

func (bh bucketHeap) Less(i, j int) bool { return bh[i].lastQueryTimestamp < bh[j].lastQueryTimestamp }

func (bh bucketHeap) Swap(i, j int) { bh[i], bh[j] = bh[j], bh[i] }

func (bh *bucketHeap) Push(x interface{}) {
	b := x.(*bucket)
	*bh = append(*bh, b)
}

func (bh *bucketHeap) Pop() interface{} {
	b := (*bh)[len(*bh)-1]
	*bh = (*bh)[:len(*bh)-1]
	return b
}
