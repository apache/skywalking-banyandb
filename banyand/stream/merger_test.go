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

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

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
			name:    "Test with no element",
			esList:  []*elements{},
			wantErr: errNoPartToMerge,
		},
		{
			name:   "Test with single part",
			esList: []*elements{esTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 859},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 47},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 16},
			},
		},
		{
			name:   "Test with multiple parts with different ts",
			esList: []*elements{esTS1, esTS2, esTS2},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSizeBytes: 2451},
				{seriesID: 2, count: 3, uncompressedSizeBytes: 95},
				{seriesID: 3, count: 3, uncompressedSizeBytes: 48},
			},
		},
		{
			name:   "Test with multiple parts with same ts",
			esList: []*elements{esTS1, esTS1, esTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 3, uncompressedSizeBytes: 2451},
				{seriesID: 2, count: 3, uncompressedSizeBytes: 95},
				{seriesID: 3, count: 3, uncompressedSizeBytes: 48},
			},
		},
		{
			name:   "Test with multiple parts with a large quantity of different ts",
			esList: []*elements{generateHugeElements(1, 5000, 1), generateHugeElements(5001, 10000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 4896, uncompressedSizeBytes: 3897279},
				{seriesID: 1, count: 5000, uncompressedSizeBytes: 3980063},
				{seriesID: 1, count: 104, uncompressedSizeBytes: 82847},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 71},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 32},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, pp []*partWrapper, fileSystem fs.FileSystem, root string, partID uint64) {
				closeCh := make(chan struct{})
				defer close(closeCh)
				tst := &tsTable{pm: protector.Nop{}}
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

func Test_mergeTwoBlocksMixedType(t *testing.T) {
	tests := []struct {
		left  *blockPointer
		right *blockPointer
		want  *blockPointer
		name  string
	}{
		{
			name: "Merge blocks with different tag types",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeStr,
									values:    [][]byte{[]byte("str1"), []byte("str2")},
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
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeBinaryData,
									values:    [][]byte{[]byte("binary1"), []byte("binary2")},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 3, 4},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("str1"), []byte("str2"), []byte("binary1"), []byte("binary2")},
									types:     []pbv1.ValueType{pbv1.ValueTypeStr, pbv1.ValueTypeStr, pbv1.ValueTypeBinaryData, pbv1.ValueTypeBinaryData},
								},
							},
						},
					},
				},
				bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}},
			},
		},
		{
			name: "Merge mixed tag with non-mixed tag",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("str1"), []byte("int1")},
									types:     []pbv1.ValueType{pbv1.ValueTypeStr, pbv1.ValueTypeInt64},
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
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeBinaryData,
									values:    [][]byte{[]byte("binary1"), []byte("binary2")},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 3, 4},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("str1"), []byte("int1"), []byte("binary1"), []byte("binary2")},
									types:     []pbv1.ValueType{pbv1.ValueTypeStr, pbv1.ValueTypeInt64, pbv1.ValueTypeBinaryData, pbv1.ValueTypeBinaryData},
								},
							},
						},
					},
				},
				bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}},
			},
		},
		{
			name: "Merge two mixed tags",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("str1"), []byte("str2")},
									types:     []pbv1.ValueType{pbv1.ValueTypeStr, pbv1.ValueTypeStr},
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
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("int1"), []byte("int2")},
									types:     []pbv1.ValueType{pbv1.ValueTypeInt64, pbv1.ValueTypeInt64},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 3, 4},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "mixedTag",
									valueType: pbv1.ValueTypeMixed,
									values:    [][]byte{[]byte("str1"), []byte("str2"), []byte("int1"), []byte("int2")},
									types:     []pbv1.ValueType{pbv1.ValueTypeStr, pbv1.ValueTypeStr, pbv1.ValueTypeInt64, pbv1.ValueTypeInt64},
								},
							},
						},
					},
				},
				bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}},
			},
		},
		{
			name: "Merge blocks with same tag types",
			left: &blockPointer{
				block: block{
					timestamps: []int64{1, 2},
					elementIDs: []uint64{0, 1},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "strTag",
									valueType: pbv1.ValueTypeStr,
									values:    [][]byte{[]byte("str1"), []byte("str2")},
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
							name: "tf1",
							tags: []tag{
								{
									name:      "strTag",
									valueType: pbv1.ValueTypeStr,
									values:    [][]byte{[]byte("str3"), []byte("str4")},
								},
							},
						},
					},
				},
			},
			want: &blockPointer{
				block: block{
					timestamps: []int64{1, 2, 3, 4},
					elementIDs: []uint64{0, 1, 2, 3},
					tagFamilies: []tagFamily{
						{
							name: "tf1",
							tags: []tag{
								{
									name:      "strTag",
									valueType: pbv1.ValueTypeStr,
									values:    [][]byte{[]byte("str1"), []byte("str2"), []byte("str3"), []byte("str4")},
								},
							},
						},
					},
				},
				bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &blockPointer{}
			mergeTwoBlocks(target, tt.left, tt.right)

			if !reflect.DeepEqual(target.timestamps, tt.want.timestamps) {
				t.Errorf("timestamps mismatch: got %v, want %v", target.timestamps, tt.want.timestamps)
			}
			if !reflect.DeepEqual(target.elementIDs, tt.want.elementIDs) {
				t.Errorf("elementIDs mismatch: got %v, want %v", target.elementIDs, tt.want.elementIDs)
			}

			if len(target.tagFamilies) != len(tt.want.tagFamilies) {
				t.Fatalf("tagFamilies length mismatch: got %d, want %d", len(target.tagFamilies), len(tt.want.tagFamilies))
			}
			for i := range target.tagFamilies {
				if target.tagFamilies[i].name != tt.want.tagFamilies[i].name {
					t.Errorf("tagFamily[%d].name mismatch: got %q, want %q", i, target.tagFamilies[i].name, tt.want.tagFamilies[i].name)
				}
				if len(target.tagFamilies[i].tags) != len(tt.want.tagFamilies[i].tags) {
					t.Fatalf("tagFamily[%d].tags length mismatch: got %d, want %d", i, len(target.tagFamilies[i].tags), len(tt.want.tagFamilies[i].tags))
				}
				for j := range target.tagFamilies[i].tags {
					gotTag := &target.tagFamilies[i].tags[j]
					wantTag := &tt.want.tagFamilies[i].tags[j]

					if gotTag.name != wantTag.name {
						t.Errorf("tag[%d][%d].name mismatch: got %q, want %q", i, j, gotTag.name, wantTag.name)
					}
					if gotTag.valueType != wantTag.valueType {
						t.Errorf("tag[%d][%d].valueType mismatch: got %d, want %d", i, j, gotTag.valueType, wantTag.valueType)
					}
					if !reflect.DeepEqual(gotTag.values, wantTag.values) {
						t.Errorf("tag[%d][%d].values mismatch: got %v, want %v", i, j, gotTag.values, wantTag.values)
					}
					if !reflect.DeepEqual(gotTag.types, wantTag.types) {
						t.Errorf("tag[%d][%d].types mismatch: got %v, want %v", i, j, gotTag.types, wantTag.types)
					}
				}
			}

			if target.bm.timestamps.min != tt.want.bm.timestamps.min {
				t.Errorf("bm.timestamps.min mismatch: got %d, want %d", target.bm.timestamps.min, tt.want.bm.timestamps.min)
			}
			if target.bm.timestamps.max != tt.want.bm.timestamps.max {
				t.Errorf("bm.timestamps.max mismatch: got %d, want %d", target.bm.timestamps.max, tt.want.bm.timestamps.max)
			}
		})
	}
}
