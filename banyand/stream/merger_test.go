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
	"errors"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// nopProtector is a dummy Protector implementation for tests.
type nopProtector struct{}

func (nopProtector) ShouldApplyFadvis(int64) bool { return false }

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
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 2}}},
		},
		{
			name:  "Merge left is empty right is non-empty",
			left:  &blockPointer{},
			right: &blockPointer{block: conventionalBlock},
			want:  &blockPointer{block: conventionalBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 2}}},
		},
		{
			name: "Merge two non-empty blocks without overlap",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})},
								},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{3, 4},
					elementIDs: []uint64{2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}}},
		},
		{
			name: "Merge two non-empty blocks without duplicated timestamps",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 3},
					elementIDs: []uint64{0, 2},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value5"), []byte("value6")})},
								},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{2, 4},
					elementIDs: []uint64{1, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}}},
		},
		{
			name: "Merge two non-empty blocks with duplicated timestamps",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 3},
					elementIDs: []uint64{0, 1, 2},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}),
										marshalStrArr([][]byte{[]byte("duplicated1")}),
										marshalStrArr([][]byte{[]byte("duplicated2")}),
									},
								},
							},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{2, 3, 4},
					elementIDs: []uint64{1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "arrTag",
							tags: []tag{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{
										marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}), marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}),
										marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
									},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{block: duplicatedMergedBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}}},
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

var mergedBlock = block{
	timestamps: []int64{1, 2, 3, 4},
	elementIDs: []uint64{0, 1, 2, 3},
	tagFamilies: []tagFamily{
		{
			name: "arrTag",
			tags: []tag{
				{
					name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
					values: [][]byte{
						marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}),
						marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")}),
					},
				},
			},
		},
	},
}

var duplicatedMergedBlock = block{
	timestamps: []int64{1, 2, 2, 3, 3, 4},
	elementIDs: []uint64{0, 1, 1, 2, 2, 3},
	tagFamilies: []tagFamily{
		{
			name: "arrTag",
			tags: []tag{
				{
					name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
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
		},
	},
}

func Test_mergeParts(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		esList  []*elements
		want    []blockMetadata
	}{
		{
			name:    "Test with no data points",
			esList:  []*elements{},
			wantErr: errNoPartToMerge,
		},
		{
			name:   "Test with single part",
			esList: []*elements{esTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 889},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
			},
		},
		{
			name:   "Test with multiple parts with different ts",
			esList: []*elements{esTS1, esTS2, esTS2},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSizeBytes: 2667},
				{seriesID: 2, count: 3, uncompressedSizeBytes: 189},
				{seriesID: 3, count: 3, uncompressedSizeBytes: 48},
			},
		},
		{
			name:   "Test with multiple parts with same ts",
			esList: []*elements{esTS1, esTS1, esTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSizeBytes: 2667},
				{seriesID: 2, count: 3, uncompressedSizeBytes: 189},
				{seriesID: 3, count: 3, uncompressedSizeBytes: 48},
			},
		},
		{
			name:   "Test with multiple parts with a large quantity of different ts",
			esList: []*elements{generateHugeEs(1, 5000, 1), generateHugeEs(5001, 10000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 2448, uncompressedSizeBytes: 2176272},
				{seriesID: 1, count: 2448, uncompressedSizeBytes: 2176272},
				{seriesID: 1, count: 2552, uncompressedSizeBytes: 2268728},
				{seriesID: 1, count: 2448, uncompressedSizeBytes: 2176272},
				{seriesID: 1, count: 104, uncompressedSizeBytes: 92456},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 126},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 32},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, pp []*partWrapper, fileSystem fs.FileSystem, root string, partID uint64) {
				closeCh := make(chan struct{})
				defer close(closeCh)
				tst := &tsTable{pm: nopProtector{}}
				p, err := tst.mergeParts(fileSystem, closeCh, pp, partID, root)
				if tt.wantErr != nil {
					if !errors.Is(err, tt.wantErr) {
						t.Fatalf("Unexpected error: got %v, want %v", err, tt.wantErr)
					}
					return
				}
				defer p.decRef()
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
					cmpopts.IgnoreFields(blockMetadata{}, "timestamps"),
					cmpopts.IgnoreFields(blockMetadata{}, "elementIDs"),
					cmpopts.IgnoreFields(blockMetadata{}, "tagFamilies"),
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
						pw.decRef()
					}
					defFn()
				}()
				for _, es := range tt.esList {
					mp := generateMemPart()
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
						pw.decRef()
					}
					defFn()
				}()
				fileSystem := fs.NewLocalFileSystem()
				for i, es := range tt.esList {
					mp := generateMemPart()
					mp.mustInitFromElements(es)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					releaseMemPart(mp)
				}
				verify(t, fpp, fileSystem, tmpPath, uint64(len(tt.esList)))
			})
		})
	}
}
