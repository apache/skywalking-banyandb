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
	"encoding/binary"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

func marshalStrArr(strArr [][]byte) []byte {
	if len(strArr) == 0 {
		return []byte{}
	}
	var result []byte
	result = binary.LittleEndian.AppendUint32(result, uint32(len(strArr)))
	for _, str := range strArr {
		result = binary.LittleEndian.AppendUint32(result, uint32(len(str)))
		result = append(result, str...)
	}
	return result
}

var conventionalBlock = block{
	userKeys: []int64{1, 2},
	data:     [][]byte{[]byte("data1"), []byte("data2")},
	tags: map[string]*tagData{
		"service": {
			name:      "service",
			valueType: pbv1.ValueTypeStr,
			values:    [][]byte{[]byte("service1"), []byte("service2")},
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
			values: [][]byte{
				marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
				marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
				marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
				marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
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
			values: [][]byte{
				marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
				marshalStrArr([][]byte{[]byte("duplicated1")}),
				marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
				marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
				marshalStrArr([][]byte{[]byte("duplicated2")}),
				marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
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
			left:  &blockPointer{block: conventionalBlock},
			right: &blockPointer{},
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{minKey: 1, maxKey: 2}},
		},
		{
			name:  "Merge left is empty right is non-empty",
			left:  &blockPointer{},
			right: &blockPointer{block: conventionalBlock},
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{minKey: 1, maxKey: 2}},
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
								marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
								marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
							},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{minKey: 1, maxKey: 4}},
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
								marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
								marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
							},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{minKey: 1, maxKey: 4}},
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
								marshalStrArr([][]byte{[]byte("duplicated1")}),
								marshalStrArr([][]byte{[]byte("duplicated2")}),
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
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
								marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
								marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
							},
						},
					},
				},
			},
			want: &blockPointer{block: duplicatedMergedBlock, bm: blockMetadata{minKey: 1, maxKey: 4}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &blockPointer{}
			mergeTwoBlocks(target, tt.left, tt.right)
			if !reflect.DeepEqual(target, tt.want) {
				t.Errorf("mergeTwoBlocks() = %v, want %v", target, tt.want)
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
		es.mustAppend(seriesID, i, data, tags)
	}
	return es
}

var (
	es1 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 100, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 200, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 300, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
		})
		return es
	}()

	es2 = func() *elements {
		es := generateElements()
		es.mustAppend(1, 150, make([]byte, 1600), []Tag{
			{Name: "service", Value: []byte("service1"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(2, 250, make([]byte, 40), []Tag{
			{Name: "env", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr},
		})
		es.mustAppend(3, 350, make([]byte, 25), []Tag{
			{Name: "region", Value: []byte("us"), ValueType: pbv1.ValueTypeStr},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				for reader.nextBlockMetadata() {
					got = append(got, reader.block.bm)
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
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					ReleaseMemPart(mp)
				}
				verify(t, fpp, fileSystem, tmpPath, uint64(len(tt.esList)))
			})
		})
	}
}
