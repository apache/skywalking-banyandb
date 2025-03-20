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
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestInitBlockCursorFromParts(t *testing.T) {
	tests := []struct {
		wantErr       error
		name          string
		dpsList       []*dataPoints
		sids          []common.SeriesID
		want          []*blockCursor
		minTimestamp  int64
		maxTimestamp  int64
		orderBySeries bool
		ascTS         bool
	}{
		{
			name:         "Test intialize block cursor from parts",
			dpsList:      []*dataPoints{dpsTS0},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 3,
			want: []*blockCursor{
				{
					timestamps: []int64{1},
					versions:   []int64{1},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field1")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
				{
					timestamps: []int64{2},
					versions:   []int64{2},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field2")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
				{
					timestamps: []int64{3},
					versions:   []int64{3},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field3")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
			},
		},
	}

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, tst *tsTable) {
				defer tst.Close()
				queryOpts := queryOptions{
					minTimestamp: tt.minTimestamp,
					maxTimestamp: tt.maxTimestamp,
				}
				s := tst.currentSnapshot()
				require.NotNil(t, s)
				defer s.decRef()
				pp, _ := s.getParts(nil, queryOpts.minTimestamp, queryOpts.maxTimestamp)
				sids := make([]common.SeriesID, len(tt.sids))
				copy(sids, tt.sids)
				sort.Slice(sids, func(i, j int) bool {
					return sids[i] < tt.sids[j]
				})
				ti := &tstIter{}
				ti.init(bma, pp, sids, tt.minTimestamp, tt.maxTimestamp)

				schema := databasev1.Measure{
					Fields: []*databasev1.FieldSpec{{Name: "strField"}, {Name: "intField"}, {Name: "floatField"}, {Name: "binaryField"}},
				}

				bcs := initBlockCursorsFromPart(pp[0], sids, &schema)

				for i := 0; i < len(tt.want); i++ {
					if !cmp.Equal(bcs[i].timestamps, tt.want[i].timestamps) || !cmp.Equal(bcs[i].versions, tt.want[i].versions) {
						t.Error("Unexpected timestamps or versions")
					}
					for j := 0; j < len(tt.want[i].fields.columns); j++ {
						if bcs[i].fields.columns[j].name != tt.want[i].fields.columns[j].name {
							t.Error("Unexpected field name")
						}
						if !cmp.Equal(bcs[i].fields.columns[j].values, tt.want[i].fields.columns[j].values) {
							t.Error("Unexpected field values")
						}
					}
				}
			}

			t.Run("test initialize block cursor from parts", func(t *testing.T) {
				// Initialize a tstIter object.
				tmpPath, defFn := test.Space(require.New(t))
				fileSystem := fs.NewLocalFileSystem()
				defer defFn()
				tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
				require.NoError(t, err)
				for _, dps := range tt.dpsList {
					tst.mustAddDataPoints(dps)
					time.Sleep(100 * time.Millisecond)
				}
				// wait until the introducer is done
				if len(tt.dpsList) > 0 {
					for {
						snp := tst.currentSnapshot()
						if snp == nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if snp.creator == snapshotCreatorMemPart {
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
							continue
						}
						snp.decRef()
						tst.Close()
						break
					}
				}

				// reopen the table
				tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
				require.NoError(t, err)

				verify(t, tst)
			})
		})
	}
}

func TestWriteAndReadCache(t *testing.T) {
	tests := []struct {
		wantErr       error
		name          string
		dpsList       []*dataPoints
		sids          []common.SeriesID
		want          []*blockCursor
		minTimestamp  int64
		maxTimestamp  int64
		orderBySeries bool
		ascTS         bool
	}{
		{
			name:         "Test write and read cache",
			dpsList:      []*dataPoints{duplicatedDpsTS0},
			sids:         []common.SeriesID{1, 2, 3},
			minTimestamp: 1,
			maxTimestamp: 6,
			want: []*blockCursor{
				{
					timestamps: []int64{1, 2},
					versions:   []int64{1, 1},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field11"), []byte("field12")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110), convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343), convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText, longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
				{
					timestamps: []int64{3, 4},
					versions:   []int64{1, 1},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field21"), []byte("field22")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110), convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343), convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText, longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
				{
					timestamps: []int64{5, 6},
					versions:   []int64{1, 1},
					fields: columnFamily{
						name: "",
						columns: []column{
							{name: "strField", values: [][]byte{[]byte("field31"), []byte("field32")}, valueType: pbv1.ValueTypeStr},
							{name: "intField", values: [][]byte{convert.Int64ToBytes(1110), convert.Int64ToBytes(1110)}, valueType: pbv1.ValueTypeInt64},
							{name: "floatField", values: [][]byte{convert.Float64ToBytes(1221233.343), convert.Float64ToBytes(1221233.343)}, valueType: pbv1.ValueTypeFloat64},
							{name: "binaryField", values: [][]byte{longText, longText}, valueType: pbv1.ValueTypeBinaryData},
						},
					},
				},
			},
		},
	}

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verify := func(t *testing.T, tst *tsTable) {
				defer tst.Close()
				queryOpts := queryOptions{
					minTimestamp: tt.minTimestamp,
					maxTimestamp: tt.maxTimestamp,
				}
				s := tst.currentSnapshot()
				require.NotNil(t, s)
				defer s.decRef()
				pp, _ := s.getParts(nil, queryOpts.minTimestamp, queryOpts.maxTimestamp)
				sids := make([]common.SeriesID, len(tt.sids))
				copy(sids, tt.sids)
				sort.Slice(sids, func(i, j int) bool {
					return sids[i] < tt.sids[j]
				})
				ti := &tstIter{}
				ti.init(bma, pp, sids, tt.minTimestamp, tt.maxTimestamp)

				schema := databasev1.Measure{
					Fields: []*databasev1.FieldSpec{{Name: "strField"}, {Name: "intField"}, {Name: "floatField"}, {Name: "binaryField"}},
				}

				c := newCache()
				c.put(pp[0], sids, &schema)
				bcs := make([]*blockCursor, 0)
				for i := 0; i < len(tt.want); i++ {
					bc := generateBlockCursor()
					mqo := model.MeasureQueryOptions{
						FieldProjection: []string{},
					}
					for _, field := range schema.Fields {
						mqo.FieldProjection = append(mqo.FieldProjection, field.Name)
					}
					qo := queryOptions{
						minTimestamp:        0,
						maxTimestamp:        math.MaxInt64,
						MeasureQueryOptions: mqo,
					}
					bm := generateBlockMetadata()
					defer releaseBlockMetadata(bm)
					bm.seriesID = common.SeriesID(i + 1)
					bc.init(nil, bm, qo)
					tmpBlock := generateBlock()
					defer releaseBlock(tmpBlock)
					val, _ := c.data.Load(common.SeriesID(i + 1))
					buckets := val.([]*bucket)
					bc.loadDataFromBuckets(buckets, tmpBlock)
					bcs = append(bcs, bc)
				}

				for i := 0; i < len(tt.want); i++ {
					if !cmp.Equal(bcs[i].timestamps, tt.want[i].timestamps) || !cmp.Equal(bcs[i].versions, tt.want[i].versions) {
						t.Error("Unexpected timestamps or versions")
					}
					for j := 0; j < len(tt.want[i].fields.columns); j++ {
						if bcs[i].fields.columns[j].name != tt.want[i].fields.columns[j].name {
							t.Error("Unexpected field name")
						}
						if !cmp.Equal(bcs[i].fields.columns[j].values, tt.want[i].fields.columns[j].values) {
							t.Error("Unexpected field values")
						}
					}
				}
			}

			t.Run("test write and read cache", func(t *testing.T) {
				// Initialize a tstIter object.
				tmpPath, defFn := test.Space(require.New(t))
				fileSystem := fs.NewLocalFileSystem()
				defer defFn()
				tst, err := newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
				require.NoError(t, err)
				for _, dps := range tt.dpsList {
					tst.mustAddDataPoints(dps)
					time.Sleep(100 * time.Millisecond)
				}
				// wait until the introducer is done
				if len(tt.dpsList) > 0 {
					for {
						snp := tst.currentSnapshot()
						if snp == nil {
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if snp.creator == snapshotCreatorMemPart {
							snp.decRef()
							time.Sleep(100 * time.Millisecond)
							continue
						}
						snp.decRef()
						tst.Close()
						break
					}
				}

				// reopen the table
				tst, err = newTSTable(fileSystem, tmpPath, common.Position{},
					logger.GetLogger("test"), timestamp.TimeRange{}, option{flushTimeout: defaultFlushTimeout, mergePolicy: newDefaultMergePolicyForTesting()}, nil)
				require.NoError(t, err)

				verify(t, tst)
			})
		})
	}
}
