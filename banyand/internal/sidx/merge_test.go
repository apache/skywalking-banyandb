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

package sidx

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var conventionalBlock = block{
	userKeys: []int64{1, 2},
	data:     [][]byte{[]byte("data1"), []byte("data2")},
	tags: map[string]*tagData{
		"service": {
			name:      "service",
			valueType: pbv1.ValueTypeStr,
			values: []tagRow{
				{value: []byte("service1")},
				{value: []byte("service2")},
			},
		},
	},
}

var mergedBlock = block{
	userKeys: []int64{1, 2, 3, 4},
	data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data3"), []byte("data4")},
	tags: map[string]*tagData{
		"arrTag": {
			name:      "arrTag",
			valueType: pbv1.ValueTypeStrArr,
			values: []tagRow{
				{valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
				{valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
				{valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
				{valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
			},
		},
	},
}

var duplicatedMergedBlock = block{
	userKeys: []int64{1, 2, 2, 3, 3, 4},
	data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data3"), []byte("data5"), []byte("data4"), []byte("data6")},
	tags: map[string]*tagData{
		"arrTag": {
			name:      "arrTag",
			valueType: pbv1.ValueTypeStrArr,
			values: []tagRow{
				{valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
				{valueArr: [][]byte{[]byte("duplicated1")}},
				{valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
				{valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
				{valueArr: [][]byte{[]byte("duplicated2")}},
				{valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
			},
		},
	},
}

func Test_mergeTwoBlocks(t *testing.T) {
	tests := []struct {
		left  *blockPointer
		right *blockPointer
		want  *blockPointer
		name  string
	}{
		{
			name:  "Merge two empty blocks",
			left:  &blockPointer{},
			right: &blockPointer{},
			want:  &blockPointer{},
		},
		{
			name:  "Merge left is non-empty right is empty",
			left:  &blockPointer{block: deepCopyBlock(&conventionalBlock)},
			right: &blockPointer{},
			want:  &blockPointer{block: deepCopyBlock(&conventionalBlock), bm: blockMetadata{minKey: 1, maxKey: 2}},
		},
		{
			name:  "Merge left is empty right is non-empty",
			left:  &blockPointer{},
			right: &blockPointer{block: deepCopyBlock(&conventionalBlock)},
			want:  &blockPointer{block: deepCopyBlock(&conventionalBlock), bm: blockMetadata{minKey: 1, maxKey: 2}},
		},
		{
			name: "Merge two non-empty blocks without overlap",
			left: &blockPointer{
				block: block{
					userKeys: []int64{1, 2},
					data:     [][]byte{[]byte("data1"), []byte("data2")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					userKeys: []int64{3, 4},
					data:     [][]byte{[]byte("data3"), []byte("data4")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
								{valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
							},
						},
					},
				},
			},
			want: &blockPointer{block: deepCopyBlock(&mergedBlock), bm: blockMetadata{minKey: 1, maxKey: 4}},
		},
		{
			name: "Merge two non-empty blocks without duplicated userKeys",
			left: &blockPointer{
				block: block{
					userKeys: []int64{1, 3},
					data:     [][]byte{[]byte("data1"), []byte("data3")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					userKeys: []int64{2, 4},
					data:     [][]byte{[]byte("data2"), []byte("data4")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
								{valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
							},
						},
					},
				},
			},
			want: &blockPointer{block: deepCopyBlock(&mergedBlock), bm: blockMetadata{minKey: 1, maxKey: 4}},
		},
		{
			name: "Merge two non-empty blocks with duplicated userKeys",
			left: &blockPointer{
				block: block{
					userKeys: []int64{1, 2, 3},
					data:     [][]byte{[]byte("data1"), []byte("data2"), []byte("data4")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
								{valueArr: [][]byte{[]byte("duplicated1")}},
								{valueArr: [][]byte{[]byte("duplicated2")}},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					userKeys: []int64{2, 3, 4},
					data:     [][]byte{[]byte("data3"), []byte("data5"), []byte("data6")},
					tags: map[string]*tagData{
						"arrTag": {
							name:      "arrTag",
							valueType: pbv1.ValueTypeStrArr,
							values: []tagRow{
								{valueArr: [][]byte{[]byte("value3"), []byte("value4")}},
								{valueArr: [][]byte{[]byte("value5"), []byte("value6")}},
								{valueArr: [][]byte{[]byte("value7"), []byte("value8")}},
							},
						},
					},
				},
			},
			want: &blockPointer{block: deepCopyBlock(&duplicatedMergedBlock), bm: blockMetadata{minKey: 1, maxKey: 4}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &blockPointer{}
			mergeTwoBlocks(target, tt.left, tt.right)
			if diff := cmp.Diff(target, tt.want,
				cmpopts.IgnoreFields(tagData{}, "uniqueValues", "tmpBytes"),
				cmpopts.IgnoreFields(blockMetadata{}, "tagsBlocks", "tagProjection", "dataBlock", "keysBlock", "seriesID", "uncompressedSize", "count", "keysEncodeType"),
				cmp.AllowUnexported(block{}, tagData{}, tagRow{}, blockPointer{}, blockMetadata{}),
			); diff != "" {
				t.Errorf("mergeTwoBlocks() mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func generateHugeElements(start, end int64, seriesIDBase common.SeriesID) *elements {
	es := generateElements()
	for i := start; i <= end; i++ {
		seriesID := seriesIDBase
		if i%1000 == 0 {
			seriesID = seriesIDBase + 1
		}
		if i%2000 == 0 {
			seriesID = seriesIDBase + 2
		}

		tags := []Tag{
			{Name: "service", Value: []byte("test-service"), ValueType: pbv1.ValueTypeStr},
		}
		data := make([]byte, 50)
		es.mustAppend(seriesID, i, int64(i)*1000000000, data, tags)
	}
	return es
}

var (
	es1 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 100, 1000000000, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 200, 2000000000, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 300, 3000000000, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()

	es2 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 150, 1500000000, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 250, 2500000000, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 350, 3500000000, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()

	esStrArr1 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 100, 1000000000, make([]byte, 100), []Tag{
			{Name: "arrTag", ValueArr: [][]byte{[]byte("value1"), []byte("value2")}, ValueType: pbv1.ValueTypeStrArr},
		})
		es.mustAppend(2, 200, 2000000000, make([]byte, 100), []Tag{
			{Name: "arrTag", ValueArr: [][]byte{[]byte("value3"), []byte("value4")}, ValueType: pbv1.ValueTypeStrArr},
		})
		return es
	}()

	esStrArr2 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 150, 1500000000, make([]byte, 100), []Tag{
			{Name: "arrTag", ValueArr: [][]byte{[]byte("value5"), []byte("value6")}, ValueType: pbv1.ValueTypeStrArr},
		})
		es.mustAppend(2, 250, 2500000000, make([]byte, 100), []Tag{
			{Name: "arrTag", ValueArr: [][]byte{[]byte("value7"), []byte("value8")}, ValueType: pbv1.ValueTypeStrArr},
		})
		return es
	}()

	esStrArrWithEmpty = func() *elements {
		es := generateElements()
		es.mustAppend(1, 300, 3000000000, make([]byte, 100), []Tag{
			{Name: "arrTag", ValueArr: [][]byte{[]byte("a"), []byte(""), []byte("b")}, ValueType: pbv1.ValueTypeStrArr},
		})
		return es
	}()
)

func Test_mergeParts(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		esList  []*elements
		want    []blockMetadata
	}{
		{
			name:    "Test with no element",
			esList:  []*elements{},
			wantErr: errNoPartToMerge,
		},
		{
			name:   "Test with single part",
			esList: []*elements{es1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSize: 1623},
				{seriesID: 2, count: 1, uncompressedSize: 55},
				{seriesID: 3, count: 1, uncompressedSize: 41},
			},
		},
		{
			name:   "Test with multiple parts with different userKeys",
			esList: []*elements{es1, es2, es2},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSize: 4869},
				{seriesID: 2, count: 3, uncompressedSize: 165},
				{seriesID: 3, count: 3, uncompressedSize: 123},
			},
		},
		{
			name:   "Test with multiple parts with same userKeys",
			esList: []*elements{es1, es1, es1},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSize: 4869},
				{seriesID: 2, count: 3, uncompressedSize: 165},
				{seriesID: 3, count: 3, uncompressedSize: 123},
			},
		},
		{
			name:   "Test with multiple parts with a large quantity of different userKeys",
			esList: []*elements{generateHugeElements(1, 1000, 1), generateHugeElements(1001, 2000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 999, uncompressedSize: 76923},
				{seriesID: 2, count: 1000, uncompressedSize: 77000},
				{seriesID: 4, count: 1, uncompressedSize: 77},
			},
		},
		{
			name:   "Test with string array value type",
			esList: []*elements{esStrArr1, esStrArr2},
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSize: 264},
				{seriesID: 2, count: 2, uncompressedSize: 264},
			},
		},
		{
			name:   "Test with string array containing empty strings",
			esList: []*elements{esStrArrWithEmpty},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSize: 128},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wantBlocks := elementsToBlocks(tt.esList)

			verify := func(t *testing.T, pp []*partWrapper, fileSystem fs.FileSystem, root string, partID uint64) {
				closeCh := make(chan struct{})
				defer close(closeCh)
				s := &sidx{pm: protector.Nop{}}
				p, err := s.mergeParts(fileSystem, closeCh, pp, partID, root)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("Unexpected error: got %v, want %v", err, tt.wantErr)
					}
					return
				}
				defer func() {
					if p != nil {
						p.release()
					}
				}()
				pmi := &partMergeIter{}
				pmi.mustInitFromPart(p.p)
				reader := &blockReader{}
				reader.init([]*partMergeIter{pmi})
				var got []blockMetadata
				var gotBlocks []block
				decoder := generateTagValuesDecoder()
				defer releaseTagValuesDecoder(decoder)

				for reader.nextBlockMetadata() {
					got = append(got, reader.block.bm)
					reader.loadBlockData(decoder)
					gotBlocks = append(gotBlocks, deepCopyBlock(&reader.block.block))
				}
				require.NoError(t, reader.error())

				if diff := cmp.Diff(got, tt.want,
					cmpopts.IgnoreFields(blockMetadata{}, "tagsBlocks"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagProjection"),
					cmpopts.IgnoreFields(blockMetadata{}, "dataBlock"),
					cmpopts.IgnoreFields(blockMetadata{}, "keysBlock"),
					cmpopts.IgnoreFields(blockMetadata{}, "minKey"),
					cmpopts.IgnoreFields(blockMetadata{}, "maxKey"),
					cmpopts.IgnoreFields(blockMetadata{}, "keysEncodeType"),
					cmp.AllowUnexported(blockMetadata{}),
				); diff != "" {
					t.Errorf("Unexpected blockMetadata (-got +want):\n%s", diff)
				}

				if diff := cmp.Diff(gotBlocks, wantBlocks,
					cmpopts.IgnoreFields(tagData{}, "uniqueValues", "tmpBytes"),
					cmp.AllowUnexported(block{}, tagData{}, tagRow{}),
				); diff != "" {
					t.Errorf("Unexpected blocks (-got +want):\n%s", diff)
				}
			}

			t.Run("memory parts", func(t *testing.T) {
				var pp []*partWrapper
				tmpPath, defFn := test.Space(require.New(t))
				defer func() {
					for _, pw := range pp {
						pw.release()
					}
					defFn()
				}()
				for _, es := range tt.esList {
					mp := GenerateMemPart()
					mp.mustInitFromElements(es)
					pp = append(pp, newPartWrapper(mp, openMemPart(mp)))
				}
				verify(t, pp, fs.NewLocalFileSystem(), tmpPath, 1)
			})

			t.Run("file parts", func(t *testing.T) {
				var fpp []*partWrapper
				tmpPath, defFn := test.Space(require.New(t))
				defer func() {
					for _, pw := range fpp {
						pw.release()
					}
					defFn()
				}()
				fileSystem := fs.NewLocalFileSystem()
				for i, es := range tt.esList {
					mp := GenerateMemPart()
					mp.mustInitFromElements(es)
					partPath := filepath.Join(tmpPath, "part_"+string(rune('0'+i)))
					mp.mustFlush(fileSystem, partPath)
					filePart := mustOpenPart(uint64(i), partPath, fileSystem)
					filePW := newPartWrapper(nil, filePart)
					fpp = append(fpp, filePW)
					ReleaseMemPart(mp)
				}
				verify(t, fpp, fileSystem, tmpPath, uint64(len(tt.esList)))
			})
		})
	}
}

func elementsToBlocks(esList []*elements) []block {
	merged := generateElements()
	defer releaseElements(merged)

	for _, es := range esList {
		for i := 0; i < len(es.seriesIDs); i++ {
			var tags []Tag
			for _, t := range es.tags[i] {
				tags = append(tags, Tag{
					Name:      t.name,
					Value:     t.value,
					ValueArr:  t.valueArr,
					ValueType: t.valueType,
				})
			}
			merged.mustAppend(es.seriesIDs[i], es.userKeys[i], es.timestamps[i], es.data[i], tags)
		}
	}

	sort.Sort(merged)

	var blocks []block
	if merged.Len() == 0 {
		return blocks
	}

	start := 0
	for i := 1; i <= merged.Len(); i++ {
		if i == merged.Len() || merged.seriesIDs[i] != merged.seriesIDs[start] {
			b := block{
				tags:     make(map[string]*tagData),
				userKeys: make([]int64, i-start),
				data:     make([][]byte, i-start),
			}
			copy(b.userKeys, merged.userKeys[start:i])
			for k := 0; k < i-start; k++ {
				b.data[k] = merged.data[start+k]
			}
			(&b).mustInitFromTags(merged.tags[start:i])
			blocks = append(blocks, b)
			start = i
		}
	}
	return blocks
}

func deepCopyBlock(b *block) block {
	newB := block{
		tags: make(map[string]*tagData),
	}
	newB.userKeys = append([]int64(nil), b.userKeys...)
	newB.data = make([][]byte, len(b.data))
	for i, d := range b.data {
		newB.data[i] = cloneBytes(d)
	}
	for k, v := range b.tags {
		newTd := &tagData{
			name:      v.name,
			valueType: v.valueType,
			values:    make([]tagRow, len(v.values)),
		}
		for i, row := range v.values {
			newRow := tagRow{}
			if row.value != nil {
				newRow.value = cloneBytes(row.value)
			}
			if row.valueArr != nil {
				newRow.valueArr = make([][]byte, len(row.valueArr))
				for j, arrVal := range row.valueArr {
					newRow.valueArr[j] = cloneBytes(arrVal)
				}
			}
			newTd.values[i] = newRow
		}
		newB.tags[k] = newTd
	}
	return newB
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func TestRecomputeTimestampRanges(t *testing.T) {
	tests := []struct {
		name     string
		parts    []*partWrapper
		expected *partMetadata
	}{
		{
			name: "single part with timestamps",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: intPtr(1000000000),
						MaxTimestamp: intPtr(2000000000),
					},
				}),
			},
			expected: &partMetadata{
				MinTimestamp: intPtr(1000000000),
				MaxTimestamp: intPtr(2000000000),
			},
		},
		{
			name: "multiple parts with timestamps",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: intPtr(1000000000),
						MaxTimestamp: intPtr(2000000000),
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          2,
						MinTimestamp: intPtr(500000000),
						MaxTimestamp: intPtr(1500000000),
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          3,
						MinTimestamp: intPtr(2500000000),
						MaxTimestamp: intPtr(3000000000),
					},
				}),
			},
			expected: &partMetadata{
				MinTimestamp: intPtr(500000000),
				MaxTimestamp: intPtr(3000000000),
			},
		},
		{
			name: "parts with some missing timestamps",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: intPtr(1000000000),
						MaxTimestamp: intPtr(2000000000),
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          2,
						MinTimestamp: nil,
						MaxTimestamp: nil,
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          3,
						MinTimestamp: intPtr(500000000),
						MaxTimestamp: intPtr(1500000000),
					},
				}),
			},
			expected: &partMetadata{
				MinTimestamp: intPtr(500000000),
				MaxTimestamp: intPtr(2000000000),
			},
		},
		{
			name: "all parts without timestamps",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: nil,
						MaxTimestamp: nil,
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          2,
						MinTimestamp: nil,
						MaxTimestamp: nil,
					},
				}),
			},
			expected: &partMetadata{
				MinTimestamp: nil,
				MaxTimestamp: nil,
			},
		},
		{
			name: "parts with nil part metadata",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: intPtr(1000000000),
						MaxTimestamp: intPtr(2000000000),
					},
				}),
				newPartWrapper(nil, &part{
					partMetadata: nil,
				}),
			},
			expected: &partMetadata{
				MinTimestamp: intPtr(1000000000),
				MaxTimestamp: intPtr(2000000000),
			},
		},
		{
			name: "parts with nil part",
			parts: []*partWrapper{
				newPartWrapper(nil, &part{
					partMetadata: &partMetadata{
						ID:          1,
						MinTimestamp: intPtr(1000000000),
						MaxTimestamp: intPtr(2000000000),
					},
				}),
				newPartWrapper(nil, nil),
			},
			expected: &partMetadata{
				MinTimestamp: intPtr(1000000000),
				MaxTimestamp: intPtr(2000000000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &partMetadata{}
			recomputeTimestampRanges(result, tt.parts)

			if tt.expected.MinTimestamp == nil {
				assert.Nil(t, result.MinTimestamp, "MinTimestamp should be nil")
			} else {
				require.NotNil(t, result.MinTimestamp, "MinTimestamp should not be nil")
				assert.Equal(t, *tt.expected.MinTimestamp, *result.MinTimestamp)
			}

			if tt.expected.MaxTimestamp == nil {
				assert.Nil(t, result.MaxTimestamp, "MaxTimestamp should be nil")
			} else {
				require.NotNil(t, result.MaxTimestamp, "MaxTimestamp should not be nil")
				assert.Equal(t, *tt.expected.MaxTimestamp, *result.MaxTimestamp)
			}

			// Cleanup
			for _, pw := range tt.parts {
				if pw != nil {
					pw.release()
				}
			}
		})
	}
}

func intPtr(v int64) *int64 {
	return &v
}
