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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/convert"
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

// Test_mergeTwoBlocks_edgeCase tests the edge case that previously caused panic:
// runtime error: index out of range [-1] at merger.go:394.
// This occurs when left.idx = 0 and left.timestamps[0] > right.timestamps[right.idx].
// The fix includes:
// 1. Check i > left.idx before accessing left.timestamps[i-1] to prevent panic.
// 2. Optimization to swap blocks upfront if right starts with smaller timestamp.
func Test_mergeTwoBlocks_edgeCase(t *testing.T) {
	left := &blockPointer{
		block: block{
			timestamps: []int64{20, 30, 40},
			versions:   []int64{1, 2, 3},
			tagFamilies: []columnFamily{
				{
					name: "arrTag",
					columns: []column{
						{
							name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("left1")}),
								marshalStrArr([][]byte{[]byte("left2")}),
								marshalStrArr([][]byte{[]byte("left3")}),
							},
						},
					},
				},
			},
			field: columnFamily{
				columns: []column{
					{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field20"), []byte("field30"), []byte("field40")}},
				},
			},
		},
		bm: blockMetadata{
			timestamps: timestampsMetadata{min: 20, max: 40},
		},
		idx: 0, // Start at the beginning
	}

	right := &blockPointer{
		block: block{
			timestamps: []int64{5, 10, 15, 25},
			versions:   []int64{4, 5, 6, 7},
			tagFamilies: []columnFamily{
				{
					name: "arrTag",
					columns: []column{
						{
							name: "strArrTag", valueType: pbv1.ValueTypeStrArr,
							values: [][]byte{
								marshalStrArr([][]byte{[]byte("right1")}),
								marshalStrArr([][]byte{[]byte("right2")}),
								marshalStrArr([][]byte{[]byte("right3")}),
								marshalStrArr([][]byte{[]byte("right4")}),
							},
						},
					},
				},
			},
			field: columnFamily{
				columns: []column{
					{name: "strField", valueType: pbv1.ValueTypeStr, values: [][]byte{[]byte("field5"), []byte("field10"), []byte("field15"), []byte("field25")}},
				},
			},
		},
		bm: blockMetadata{
			timestamps: timestampsMetadata{min: 5, max: 25}, // Overlaps with left (20 <= 25 and 5 <= 40)
		},
		idx: 2, // Point to timestamp 15, which is less than left.timestamps[0] = 20
	}

	// After the fix, this should NOT panic and should merge correctly
	target := &blockPointer{}
	mergeTwoBlocks(target, left, right)

	// Verify the merge result: should have timestamps [15, 20, 25, 30, 40]
	// Note: right.idx = 2, so merge starts from timestamp 15 (not 5 or 10)
	expectedTimestamps := []int64{15, 20, 25, 30, 40}
	require.Equal(t, expectedTimestamps, target.timestamps, "merged timestamps should be correct")
	require.Equal(t, int64(15), target.bm.timestamps.min, "min timestamp should be 15")
	require.Equal(t, int64(40), target.bm.timestamps.max, "max timestamp should be 40")
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
			name:    "Test with no data point",
			dpsList: []*dataPoints{},
			wantErr: errNoPartToMerge,
		},
		{
			name:    "Test with single part",
			dpsList: []*dataPoints{dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1654},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 47},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
			},
		},
		{
			name:    "Test with multiple parts with different ts",
			dpsList: []*dataPoints{dpsTS1, dpsTS2, dpsTS2},
			want: []blockMetadata{
				{seriesID: 1, count: 2, uncompressedSizeBytes: 3245},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 71},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 64},
			},
		},
		{
			name:    "Test with multiple parts with same ts",
			dpsList: []*dataPoints{dpsTS11, dpsTS1},
			want: []blockMetadata{
				{seriesID: 1, count: 1, uncompressedSizeBytes: 1654},
				{seriesID: 2, count: 1, uncompressedSizeBytes: 47},
				{seriesID: 3, count: 1, uncompressedSizeBytes: 32},
			},
		},
		{
			name:    "Test with multiple parts with a large quantity of different ts",
			dpsList: []*dataPoints{generateHugeDatapoints(1, 5000, 1), generateHugeDatapoints(5001, 10000, 2)},
			want: []blockMetadata{
				{seriesID: 1, count: 2530, uncompressedSizeBytes: 4025293},
				{seriesID: 1, count: 2470, uncompressedSizeBytes: 3929833},
				{seriesID: 1, count: 2530, uncompressedSizeBytes: 4025293},
				{seriesID: 1, count: 2470, uncompressedSizeBytes: 3929833},
				{seriesID: 2, count: 2, uncompressedSizeBytes: 71},
				{seriesID: 3, count: 2, uncompressedSizeBytes: 64},
			},
		},
		{
			name:    "Test with multiple parts with a large small quantity of different ts",
			dpsList: []*dataPoints{generateSmallDatapoints(1, 5000, 1), generateSmallDatapoints(5001, 10000, 2)},
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

// generateDatapointsWithMultipleBlocks generates datapoints that will create multiple blocks
// by using different seriesIDs. Each seriesID creates a separate block.
func generateDatapointsWithMultipleBlocks(startTimestamp, countPerBlock, numBlocks int64) *dataPoints {
	dataPoints := &dataPoints{
		seriesIDs:   []common.SeriesID{},
		timestamps:  []int64{},
		versions:    []int64{},
		tagFamilies: [][]nameValues{},
		fields:      []nameValues{},
	}
	now := time.Now().UnixNano()
	ts := startTimestamp
	for blockIdx := int64(0); blockIdx < numBlocks; blockIdx++ {
		seriesID := common.SeriesID(blockIdx + 1) // Different seriesID for each block
		for i := int64(0); i < countPerBlock; i++ {
			dataPoints.seriesIDs = append(dataPoints.seriesIDs, seriesID)
			dataPoints.timestamps = append(dataPoints.timestamps, ts)
			dataPoints.versions = append(dataPoints.versions, now+ts)
			dataPoints.tagFamilies = append(dataPoints.tagFamilies, []nameValues{
				{
					name: "arrTag", values: []*nameValue{
						{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
					},
				},
			})
			dataPoints.fields = append(dataPoints.fields, nameValues{
				name: "skipped", values: []*nameValue{
					{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1000 + ts), valueArr: nil},
				},
			})
			ts++
		}
	}
	return dataPoints
}

// generateDatapointsForBlockSplit generates datapoints that will create blocks
// that when merged will exceed maxBlockLength, triggering the split logic at lines 338-348.
func generateDatapointsForBlockSplit(startTimestamp, count int64) *dataPoints {
	dataPoints := &dataPoints{
		seriesIDs:   []common.SeriesID{},
		timestamps:  []int64{},
		versions:    []int64{},
		tagFamilies: [][]nameValues{},
		fields:      []nameValues{},
	}
	now := time.Now().UnixNano()
	for i := int64(0); i < count; i++ {
		dataPoints.seriesIDs = append(dataPoints.seriesIDs, 1)
		dataPoints.timestamps = append(dataPoints.timestamps, startTimestamp+i)
		dataPoints.versions = append(dataPoints.versions, now+i)
		dataPoints.tagFamilies = append(dataPoints.tagFamilies, []nameValues{
			{
				name: "arrTag", values: []*nameValue{
					{name: "strArrTag", valueType: pbv1.ValueTypeStrArr, value: nil, valueArr: [][]byte{[]byte("value1"), []byte("value2")}},
				},
			},
		})
		dataPoints.fields = append(dataPoints.fields, nameValues{
			name: "skipped", values: []*nameValue{
				{name: "intField", valueType: pbv1.ValueTypeInt64, value: convert.Int64ToBytes(1000 + i), valueArr: nil},
			},
		})
	}
	return dataPoints
}

// Test_mergeParts_fileBased tests file-based merging with various scenarios:
// 1. Single seriesID exceeding maxBlockLength (triggers split at lines 338-348)
// 2. Multiple blocks per part with overlapping timestamps.
// 3. Multiple blocks per part exceeding maxBlockLength when merged.
// 4. Multiple parts with multiple blocks.
func Test_mergeParts_fileBased(t *testing.T) {
	t.Run("exceeds maxBlockLength with single seriesID", func(t *testing.T) {
		// maxBlockLength is 8192, so we create parts that when merged will exceed this
		part1Count := int64(5000)
		part2Count := int64(5000)
		expectedTotalCount := part1Count + part2Count

		var fpp []*partWrapper
		tmpPath, defFn := test.Space(require.New(t))
		defer func() {
			for _, pw := range fpp {
				pw.decRef()
			}
			defFn()
		}()

		fileSystem := fs.NewLocalFileSystem()
		dps1 := generateDatapointsForBlockSplit(1, part1Count)
		dps2 := generateDatapointsForBlockSplit(part1Count+1, part2Count)

		mp1 := generateMemPart()
		mp1.mustInitFromDataPoints(dps1)
		mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
		filePW1 := newPartWrapper(nil, mustOpenFilePart(1, tmpPath, fileSystem))
		filePW1.p.partMetadata.ID = 1
		fpp = append(fpp, filePW1)
		releaseMemPart(mp1)

		mp2 := generateMemPart()
		mp2.mustInitFromDataPoints(dps2)
		mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
		filePW2 := newPartWrapper(nil, mustOpenFilePart(2, tmpPath, fileSystem))
		filePW2.p.partMetadata.ID = 2
		fpp = append(fpp, filePW2)
		releaseMemPart(mp2)

		closeCh := make(chan struct{})
		defer close(closeCh)
		tst := &tsTable{pm: protector.Nop{}}
		p, err := tst.mergeParts(fileSystem, closeCh, fpp, 3, tmpPath)
		require.NoError(t, err)
		defer p.decRef()

		pmi := &partMergeIter{}
		pmi.mustInitFromPart(p.p)
		reader := &blockReader{}
		reader.init([]*partMergeIter{pmi})
		var totalCount uint64
		var blockCount int
		for reader.nextBlockMetadata() {
			blockCount++
			totalCount += reader.block.bm.count
			require.LessOrEqual(t, reader.block.bm.count, uint64(maxBlockLength),
				"block count should not exceed maxBlockLength")
		}
		require.NoError(t, reader.error())
		require.Equal(t, uint64(expectedTotalCount), totalCount,
			"total count should match sum of input parts")
		require.Greater(t, blockCount, 1, "should have multiple blocks due to splitting")
	})

	t.Run("exceeds maxBlockLength with overlapping timestamps", func(t *testing.T) {
		part1Count := int64(6000)
		part2Count := int64(6000)
		overlapStart := part1Count / 2

		var fpp []*partWrapper
		tmpPath, defFn := test.Space(require.New(t))
		defer func() {
			for _, pw := range fpp {
				pw.decRef()
			}
			defFn()
		}()

		fileSystem := fs.NewLocalFileSystem()
		dps1 := generateDatapointsForBlockSplit(1, part1Count)
		dps2 := generateDatapointsForBlockSplit(overlapStart, part2Count)

		mp1 := generateMemPart()
		mp1.mustInitFromDataPoints(dps1)
		mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
		filePW1 := newPartWrapper(nil, mustOpenFilePart(1, tmpPath, fileSystem))
		filePW1.p.partMetadata.ID = 1
		fpp = append(fpp, filePW1)
		releaseMemPart(mp1)

		mp2 := generateMemPart()
		mp2.mustInitFromDataPoints(dps2)
		mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
		filePW2 := newPartWrapper(nil, mustOpenFilePart(2, tmpPath, fileSystem))
		filePW2.p.partMetadata.ID = 2
		fpp = append(fpp, filePW2)
		releaseMemPart(mp2)

		closeCh := make(chan struct{})
		defer close(closeCh)
		tst := &tsTable{pm: protector.Nop{}}
		p, err := tst.mergeParts(fileSystem, closeCh, fpp, 3, tmpPath)
		require.NoError(t, err)
		defer p.decRef()

		pmi := &partMergeIter{}
		pmi.mustInitFromPart(p.p)
		reader := &blockReader{}
		reader.init([]*partMergeIter{pmi})
		var totalCount uint64
		var blockCount int
		for reader.nextBlockMetadata() {
			blockCount++
			totalCount += reader.block.bm.count
			require.LessOrEqual(t, reader.block.bm.count, uint64(maxBlockLength),
				"block count should not exceed maxBlockLength")
		}
		require.NoError(t, reader.error())
		overlapSize := (part1Count - overlapStart) + 1
		expectedUniqueCount := part1Count + part2Count - overlapSize
		require.Equal(t, uint64(expectedUniqueCount), totalCount,
			"total count should match unique count after deduplication")
		require.Greater(t, blockCount, 1, "should have multiple blocks due to splitting")
	})

	t.Run("multiple blocks per part", func(t *testing.T) {
		blocksPerPart := int64(5)
		countPerBlock := int64(2000)
		totalPerPart := blocksPerPart * countPerBlock

		var fpp []*partWrapper
		tmpPath, defFn := test.Space(require.New(t))
		defer func() {
			for _, pw := range fpp {
				pw.decRef()
			}
			defFn()
		}()

		fileSystem := fs.NewLocalFileSystem()
		dps1 := generateDatapointsWithMultipleBlocks(1, countPerBlock, blocksPerPart)
		dps2 := generateDatapointsWithMultipleBlocks(totalPerPart+1, countPerBlock, blocksPerPart)

		mp1 := generateMemPart()
		mp1.mustInitFromDataPoints(dps1)
		mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
		filePW1 := newPartWrapper(nil, mustOpenFilePart(1, tmpPath, fileSystem))
		filePW1.p.partMetadata.ID = 1
		fpp = append(fpp, filePW1)
		releaseMemPart(mp1)

		mp2 := generateMemPart()
		mp2.mustInitFromDataPoints(dps2)
		mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
		filePW2 := newPartWrapper(nil, mustOpenFilePart(2, tmpPath, fileSystem))
		filePW2.p.partMetadata.ID = 2
		fpp = append(fpp, filePW2)
		releaseMemPart(mp2)

		closeCh := make(chan struct{})
		defer close(closeCh)
		tst := &tsTable{pm: protector.Nop{}}
		p, err := tst.mergeParts(fileSystem, closeCh, fpp, 3, tmpPath)
		require.NoError(t, err)
		defer p.decRef()

		pmi := &partMergeIter{}
		pmi.mustInitFromPart(p.p)
		reader := &blockReader{}
		reader.init([]*partMergeIter{pmi})
		var totalCount uint64
		var blockCount int
		seriesIDCounts := make(map[common.SeriesID]uint64)
		for reader.nextBlockMetadata() {
			blockCount++
			seriesID := reader.block.bm.seriesID
			count := reader.block.bm.count
			totalCount += count
			seriesIDCounts[seriesID] += count
			require.LessOrEqual(t, count, uint64(maxBlockLength),
				"block count should not exceed maxBlockLength")
		}
		require.NoError(t, reader.error())
		expectedTotalCount := uint64(totalPerPart * int64(len(fpp)))
		require.Equal(t, expectedTotalCount, totalCount,
			"total count should match sum of all parts")
		require.GreaterOrEqual(t, blockCount, int(blocksPerPart),
			"should have at least one block per seriesID")
		for sid := common.SeriesID(1); sid <= common.SeriesID(blocksPerPart); sid++ {
			expectedCount := uint64(countPerBlock * int64(len(fpp)))
			require.Equal(t, expectedCount, seriesIDCounts[sid],
				"seriesID %d should have merged count from all parts", sid)
		}
	})

	t.Run("multiple blocks exceeding maxBlockLength when merged", func(t *testing.T) {
		blocksPerPart := int64(3)
		countPerBlock := int64(5000) // 5000 * 2 parts = 10000 per seriesID (exceeds 8192)
		totalPerPart := blocksPerPart * countPerBlock

		var fpp []*partWrapper
		tmpPath, defFn := test.Space(require.New(t))
		defer func() {
			for _, pw := range fpp {
				pw.decRef()
			}
			defFn()
		}()

		fileSystem := fs.NewLocalFileSystem()
		dps1 := generateDatapointsWithMultipleBlocks(1, countPerBlock, blocksPerPart)
		dps2 := generateDatapointsWithMultipleBlocks(totalPerPart+1, countPerBlock, blocksPerPart)

		mp1 := generateMemPart()
		mp1.mustInitFromDataPoints(dps1)
		mp1.mustFlush(fileSystem, partPath(tmpPath, 1))
		filePW1 := newPartWrapper(nil, mustOpenFilePart(1, tmpPath, fileSystem))
		filePW1.p.partMetadata.ID = 1
		fpp = append(fpp, filePW1)
		releaseMemPart(mp1)

		mp2 := generateMemPart()
		mp2.mustInitFromDataPoints(dps2)
		mp2.mustFlush(fileSystem, partPath(tmpPath, 2))
		filePW2 := newPartWrapper(nil, mustOpenFilePart(2, tmpPath, fileSystem))
		filePW2.p.partMetadata.ID = 2
		fpp = append(fpp, filePW2)
		releaseMemPart(mp2)

		closeCh := make(chan struct{})
		defer close(closeCh)
		tst := &tsTable{pm: protector.Nop{}}
		p, err := tst.mergeParts(fileSystem, closeCh, fpp, 3, tmpPath)
		require.NoError(t, err)
		defer p.decRef()

		pmi := &partMergeIter{}
		pmi.mustInitFromPart(p.p)
		reader := &blockReader{}
		reader.init([]*partMergeIter{pmi})
		var totalCount uint64
		var blockCount int
		seriesIDCounts := make(map[common.SeriesID]uint64)
		for reader.nextBlockMetadata() {
			blockCount++
			seriesID := reader.block.bm.seriesID
			count := reader.block.bm.count
			totalCount += count
			seriesIDCounts[seriesID] += count
			require.LessOrEqual(t, count, uint64(maxBlockLength),
				"block count should not exceed maxBlockLength")
		}
		require.NoError(t, reader.error())
		expectedTotalCount := uint64(totalPerPart * int64(len(fpp)))
		require.Equal(t, expectedTotalCount, totalCount,
			"total count should match sum of all parts")
		expectedCountPerSeriesID := uint64(countPerBlock * int64(len(fpp)))
		for sid := common.SeriesID(1); sid <= common.SeriesID(blocksPerPart); sid++ {
			require.Equal(t, expectedCountPerSeriesID, seriesIDCounts[sid],
				"seriesID %d should have merged count from all parts", sid)
		}
		require.Greater(t, blockCount, int(blocksPerPart),
			"should have more blocks due to splitting when exceeding maxBlockLength")
	})

	t.Run("multiple parts with multiple blocks", func(t *testing.T) {
		numParts := 3
		blocksPerPart := int64(4)
		countPerBlock := int64(1500)
		totalPerPart := blocksPerPart * countPerBlock

		var fpp []*partWrapper
		tmpPath, defFn := test.Space(require.New(t))
		defer func() {
			for _, pw := range fpp {
				pw.decRef()
			}
			defFn()
		}()

		fileSystem := fs.NewLocalFileSystem()
		for i := 0; i < numParts; i++ {
			startTS := int64(i)*totalPerPart + 1
			dps := generateDatapointsWithMultipleBlocks(startTS, countPerBlock, blocksPerPart)
			mp := generateMemPart()
			mp.mustInitFromDataPoints(dps)
			mp.mustFlush(fileSystem, partPath(tmpPath, uint64(i+1)))
			filePW := newPartWrapper(nil, mustOpenFilePart(uint64(i+1), tmpPath, fileSystem))
			filePW.p.partMetadata.ID = uint64(i + 1)
			fpp = append(fpp, filePW)
			releaseMemPart(mp)
		}

		closeCh := make(chan struct{})
		defer close(closeCh)
		tst := &tsTable{pm: protector.Nop{}}
		p, err := tst.mergeParts(fileSystem, closeCh, fpp, uint64(numParts+1), tmpPath)
		require.NoError(t, err)
		defer p.decRef()

		pmi := &partMergeIter{}
		pmi.mustInitFromPart(p.p)
		reader := &blockReader{}
		reader.init([]*partMergeIter{pmi})
		var totalCount uint64
		var blockCount int
		seriesIDCounts := make(map[common.SeriesID]uint64)
		for reader.nextBlockMetadata() {
			blockCount++
			seriesID := reader.block.bm.seriesID
			count := reader.block.bm.count
			totalCount += count
			seriesIDCounts[seriesID] += count
			require.LessOrEqual(t, count, uint64(maxBlockLength),
				"block count should not exceed maxBlockLength")
		}
		require.NoError(t, reader.error())
		expectedTotalCount := uint64(totalPerPart * int64(len(fpp)))
		require.Equal(t, expectedTotalCount, totalCount,
			"total count should match sum of all parts")
		for sid := common.SeriesID(1); sid <= common.SeriesID(blocksPerPart); sid++ {
			expectedCount := uint64(countPerBlock * int64(len(fpp)))
			require.Equal(t, expectedCount, seriesIDCounts[sid],
				"seriesID %d should have merged count from all parts", sid)
		}
		require.GreaterOrEqual(t, blockCount, int(blocksPerPart),
			"should have at least one block per seriesID")
	})
}
