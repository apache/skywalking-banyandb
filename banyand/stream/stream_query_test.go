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
	"embed"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type shardStruct struct {
	id       common.ShardID
	location []string
	elements []string
}

type shardsForTest []shardStruct

func Test_Stream_SelectShard(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(t)
	defer deferFunc()
	_ = setupQueryData(t, "multiple_shards.json", s)
	tests := []struct {
		name         string
		entity       tsdb.Entity
		wantShardNum int
		wantErr      bool
	}{
		{
			name:         "all shards",
			wantShardNum: 2,
		},
		{
			name:         "select a shard",
			entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Int64ToBytes(0)},
			wantShardNum: 1,
		},
		{
			name:         "select shards",
			entity:       tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.AnyEntry, convert.Int64ToBytes(0)},
			wantShardNum: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards, err := s.Shards(tt.entity)
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
			tester.Equal(tt.wantShardNum, len(shards))
		})
	}

}

func Test_Stream_Series(t *testing.T) {
	s, deferFunc := setup(t)
	defer deferFunc()
	baseTime := setupQueryData(t, "multiple_shards.json", s)
	tests := []struct {
		name    string
		args    queryOpts
		want    shardsForTest
		wantErr bool
	}{
		{
			name: "all",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
					elements: []string{"4"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
					elements: []string{"2"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},

		{
			name: "time range",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime.Add(1500*time.Millisecond), 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
					elements: []string{"4"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"5"},
				},
			},
		},
		{
			name: "find series by service_id and instance_id",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},
		{
			name: "find a series",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Int64ToBytes(0)},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
			},
			want: shardsForTest{
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},
		{
			name: "filter",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					builder.Filter(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "endpoint_id",
							Group: "default",
							Id:    4,
						},
						Tags:     []string{"endpoint_id"},
						Type:     databasev2.IndexRule_TYPE_INVERTED,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, tsdb.Condition{
						"endpoint_id": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_EQ,
								Values: [][]byte{[]byte("/home_id")},
							},
						},
					})
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3"},
				},
			},
		},
		{
			name: "order by duration",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					builder.OrderByIndex(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
							Id:    3,
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, modelv2.QueryOrder_SORT_ASC)
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
					elements: []string{"4"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
					elements: []string{"2"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
					elements: []string{"1"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},
		{
			name: "filter by duration",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					rule := &databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
							Id:    3,
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}
					builder.Filter(rule, tsdb.Condition{
						"duration": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_LT,
								Values: [][]byte{convert.Int64ToBytes(500)},
							},
						},
					})
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
					elements: []string{"4"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},
		{
			name: "filter and sort by duration",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					rule := &databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
							Id:    3,
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}
					builder.Filter(rule, tsdb.Condition{
						"duration": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_LT,
								Values: [][]byte{convert.Int64ToBytes(500)},
							},
						},
					})
					builder.OrderByIndex(rule, modelv2.QueryOrder_SORT_ASC)
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
					elements: []string{"4"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3", "5"},
				},
			},
		},
		{
			name: "filter by several conditions",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					rule := &databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
							Id:    3,
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}
					builder.Filter(rule, tsdb.Condition{
						"duration": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_LT,
								Values: [][]byte{convert.Int64ToBytes(500)},
							},
						},
					})
					builder.Filter(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "endpoint_id",
							Group: "default",
							Id:    4,
						},
						Tags:     []string{"endpoint_id"},
						Type:     databasev2.IndexRule_TYPE_INVERTED,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, tsdb.Condition{
						"endpoint_id": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_EQ,
								Values: [][]byte{[]byte("/home_id")},
							},
						},
					})
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3"},
				},
			},
		},
		{
			name: "filter by several conditions, sort by duration",
			args: queryOpts{
				entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
				timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
				buildFn: func(builder tsdb.SeekerBuilder) {
					rule := &databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "duration",
							Group: "default",
							Id:    3,
						},
						Tags:     []string{"duration"},
						Type:     databasev2.IndexRule_TYPE_TREE,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}
					builder.Filter(rule, tsdb.Condition{
						"duration": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_LT,
								Values: [][]byte{convert.Int64ToBytes(500)},
							},
						},
					})
					builder.OrderByIndex(rule, modelv2.QueryOrder_SORT_ASC)
					builder.Filter(&databasev2.IndexRule{
						Metadata: &commonv2.Metadata{
							Name:  "endpoint_id",
							Group: "default",
							Id:    4,
						},
						Tags:     []string{"endpoint_id"},
						Type:     databasev2.IndexRule_TYPE_INVERTED,
						Location: databasev2.IndexRule_LOCATION_SERIES,
					}, tsdb.Condition{
						"endpoint_id": []index.ConditionValue{
							{
								Op:     modelv2.Condition_BINARY_OP_EQ,
								Values: [][]byte{[]byte("/home_id")},
							},
						},
					})
				},
			},
			want: shardsForTest{
				{
					id:       0,
					location: []string{"series_16283518706331625322", "data_flow_0"},
				},
				{
					id:       0,
					location: []string{"series_4862694201852929188", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_13343478452567673284", "data_flow_0"},
				},
				{
					id:       1,
					location: []string{"series_7898679171060804990", "data_flow_0"},
					elements: []string{"3"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast := assert.New(t)
			got, err := queryData(ast, s, tt.args)
			if tt.wantErr {
				ast.Error(err)
				return
			}
			ast.NoError(err)
			sort.SliceStable(got, func(i, j int) bool {
				a := got[i]
				b := got[j]
				if a.id > b.id {
					return false
				}
				for i, al := range a.location {
					bl := b.location[i]
					if bytes.Compare([]byte(al), []byte(bl)) > 0 {
						return false
					}
				}
				return true
			})
			ast.Equal(tt.want, got)
		})
	}

}

func Test_Stream_Global_Index(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(t)
	defer deferFunc()
	_ = setupQueryData(t, "global_index.json", s)
	tests := []struct {
		name                string
		traceID             string
		wantTraceSegmentNum int
		wantErr             bool
	}{
		{
			name:                "trace id is 1",
			traceID:             "1",
			wantTraceSegmentNum: 2,
		},
		{
			name:                "trace id is 2",
			traceID:             "2",
			wantTraceSegmentNum: 3,
		},
		{
			name:    "unknown trace id",
			traceID: "foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards, errShards := s.Shards(nil)
			tester.NoError(errShards)
			err := func() error {
				for _, shard := range shards {
					itemIDs, err := shard.Index().Seek(index.Field{
						Key: index.FieldKey{
							//trace_id
							IndexRuleID: 10,
						},
						Term: []byte(tt.traceID),
					})
					if err != nil {
						return errors.WithStack(err)
					}
					if len(itemIDs) < 1 {
						continue
					}
					if err != nil {
						return errors.WithStack(err)
					}
					tester.Equal(tt.wantTraceSegmentNum, len(itemIDs))
					for _, itemID := range itemIDs {
						segShard, err := s.Shard(itemID.ShardID)
						if err != nil {
							return errors.WithStack(err)
						}
						series, err := segShard.Series().GetByID(itemID.SeriesID)
						if err != nil {
							return errors.WithStack(err)
						}
						err = func() error {
							item, closer, errInner := series.Get(itemID)
							defer func(closer io.Closer) {
								_ = closer.Close()
							}(closer)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							tagFamily, errInner := s.ParseTagFamily("searchable", item)
							if errInner != nil {
								return errors.WithStack(errInner)
							}
							for _, tag := range tagFamily.GetTags() {
								if tag.GetKey() == "trace_id" {
									tester.Equal(tt.traceID, tag.GetValue().GetStr().GetValue())
								}
							}
							return nil
						}()
						if err != nil {
							return errors.WithStack(err)
						}

					}
				}
				return nil
			}()
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
		})
	}

}

type queryOpts struct {
	entity    tsdb.Entity
	timeRange tsdb.TimeRange
	buildFn   func(builder tsdb.SeekerBuilder)
}

func queryData(tester *assert.Assertions, s *stream, opts queryOpts) (shardsForTest, error) {
	shards, err := s.Shards(opts.entity)
	tester.NoError(err)
	got := shardsForTest{}
	for _, shard := range shards {
		seriesList, err := shard.Series().List(tsdb.NewPath(opts.entity))
		if err != nil {
			return nil, err
		}
		for _, series := range seriesList {
			got, err = func(g shardsForTest) (shardsForTest, error) {
				sp, errInner := series.Span(opts.timeRange)
				defer func(sp tsdb.SeriesSpan) {
					_ = sp.Close()
				}(sp)
				if errInner != nil {
					return nil, errInner
				}
				builder := sp.SeekerBuilder()
				if opts.buildFn != nil {
					opts.buildFn(builder)
				}
				seeker, errInner := builder.Build()
				if errInner != nil {
					return nil, errInner
				}
				iter, errInner := seeker.Seek()
				if errInner != nil {
					return nil, errInner
				}
				for dataFlowID, iterator := range iter {
					var elements []string
					for iterator.Next() {
						tagFamily, errInner := s.ParseTagFamily("searchable", iterator.Val())
						if errInner != nil {
							return nil, errInner
						}
						for _, tag := range tagFamily.GetTags() {
							if tag.GetKey() == "trace_id" {
								elements = append(elements, tag.GetValue().GetStr().GetValue())
							}
						}
						eleID, errInner := s.ParseElementID(iterator.Val())
						if errInner != nil {
							return nil, errInner
						}
						tester.NotEmpty(eleID)
					}
					_ = iterator.Close()
					g = append(g, shardStruct{
						id: shard.ID(),
						location: []string{
							fmt.Sprintf("series_%v", series.ID()),
							"data_flow_" + strconv.Itoa(dataFlowID),
						},
						elements: elements,
					})
				}

				return g, nil
			}(got)
			if err != nil {
				return nil, err
			}
		}
	}
	return got, nil
}

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(testing *testing.T, dataFile string, stream *stream) (baseTime time.Time) {
	t := assert.New(testing)
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		t.NoError(errMarshal)
		searchTagFamily := &modelv2.TagFamilyForWrite{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
		e := &streamv2.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
			TagFamilies: []*modelv2.TagFamilyForWrite{
				{
					Tags: []*modelv2.TagValue{
						{
							Value: &modelv2.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		errInner := stream.Write(e)
		t.NoError(errInner)
	}
	return baseTime
}
