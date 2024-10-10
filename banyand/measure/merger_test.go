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
					versions:   []int64{1, 4},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value3"), []byte("value4")})},
								},
							},
						},
					},
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field1"), []byte("field2")}},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{3, 4},
					versions:   []int64{5, 6},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value5"), []byte("value6")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
								},
							},
						},
					},
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field3"), []byte("field4")}},
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
					versions:   []int64{1, 5},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value1"), []byte("value2")}), marshalStrArr([][]byte{[]byte("value5"), []byte("value6")})},
								},
							},
						},
					},
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field1"), []byte("field3")}},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{2, 4},
					versions:   []int64{4, 6},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
								{
									name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
									values: [][]byte{marshalStrArr([][]byte{[]byte("value3"), []byte("value4")}), marshalStrArr([][]byte{[]byte("value7"), []byte("value8")})},
								},
							},
						},
					},
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field2"), []byte("field4")}},
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
					versions:   []int64{1, 2, 3},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
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
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field1"), []byte("duplicated1"), []byte("duplicated2")}},
						},
					},
				},
			},
			right: &blockPointer{
				block: block{
					timestamps: []int64{2, 3, 4},
					versions:   []int64{4, 5, 6},
					tagFamilies: []columnFamily{
						{
							name: "arrTag",
							columns: []column{
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
					field: columnFamily{
						columns: []column{
							{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field2"), []byte("field3"), []byte("field4")}},
						},
					},
				},
			},
			want: &blockPointer{block: mergedBlock, bm: blockMetadata{timestamps: timestampsMetadata{min: 1, max: 4}}},
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
	versions:   []int64{1, 4, 5, 6},
	tagFamilies: []columnFamily{
		{
			name: "arrTag",
			columns: []column{
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
	field: columnFamily{
		columns: []column{
			{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field1"), []byte("field2"), []byte("field3"), []byte("field4")}},
		},
	},
}

func Test_mergeParts(t *testing.T) {
	tests := []struct {
		wantErr error
		name    string
		dpsList []*dataPoints
		want    []blockMetadata
	}{
		{
			name:    "Test with no data points",
			dpsList: []*dataPoints{},
			wantErr: errNoPartToMerge,
		},
		{
			name:    "Test with single part",
			dpsList: []*dataPoints{dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
			},
		},
		{
			name:    "Test with multiple parts with different ts",
			dpsList: []*dataPoints{dpsTS1, dpsTS2, dpsTS2},
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3368},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 126},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 64},
			},
		},
		{
			name:    "Test with multiple parts with same ts",
			dpsList: []*dataPoints{dpsTS11, dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1684},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 63},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
			},
		},
		{
			name:    "Test with multiple parts with a large quantity of different ts",
			dpsList: []*dataPoints{generateHugeDps(1, 5000, 1), generateHugeDps(5001, 10000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 1265, uncompressedSizeBytes: 2130260},
				{seriesID: 1, count: 1265, uncompressedSizeBytes: 2130260},
				{seriesID: 1, count: 1265, uncompressedSizeBytes: 2130260},
				{seriesID: 1, count: 2470, uncompressedSizeBytes: 4159480},
				{seriesID: 1, count: 1265, uncompressedSizeBytes: 2130260},
				{seriesID: 1, count: 1265, uncompressedSizeBytes: 2130260},
				{seriesID: 1, count: 1205, uncompressedSizeBytes: 2029220},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 126},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 64},
			},
		},
		{
			name:    "Test with multiple parts with a large small quantity of different ts",
			dpsList: []*dataPoints{generateHugeSmallDps(1, 5000, 1), generateHugeSmallDps(5001, 10000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 8192, uncompressedSizeBytes: 262144},
				{seriesID: 1, count: 1808, uncompressedSizeBytes: 57856},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 32},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 64},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, pp []*partWrapper, fileSystem fs.FileSystem, root string, partID uint64) {
				closeCh := make(chan struct{})
				defer close(closeCh)
				p, err := mergeParts(fileSystem, closeCh, pp, partID, root)
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
					cmpopts.IgnoreFields(blockMetadata{}, "field"),
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
				for _, dps := range tt.dpsList {
					mp := generateMemPart()
					mp.mustInitFromDataPoints(dps)
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
				for i, dps := range tt.dpsList {
					mp := generateMemPart()
					mp.mustInitFromDataPoints(dps)
					mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i)))
					filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i), tmpPath, fileSystem))
					filePW.p.partMetadata.ID = uint64(i)
					fpp = append(fpp, filePW)
					releaseMemPart(mp)
				}
				verify(t, fpp, fileSystem, tmpPath, uint64(len(tt.dpsList)))
			})
		})
	}
}
