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
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

func Test_Stream_SelectShard(t *testing.T) {
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()
	_ = setupQueryData(tester, "multiple_shards.json", s)
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
	tester := assert.New(t)
	s, deferFunc := setup(tester)
	defer deferFunc()
	baseTime := setupQueryData(tester, "multiple_shards.json", s)
	type args struct {
		entity tsdb.Entity
	}
	type shardStruct struct {
		id       common.ShardID
		location []string
		elements []string
	}
	type want struct {
		shards []shardStruct
	}

	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "all",
			args: args{
				entity: tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
			},
			want: want{
				shards: []shardStruct{
					{
						id:       0,
						location: []string{"series_12243341348514563931", "data_flow_0"},
						elements: []string{"1"},
					},
					{
						id:       0,
						location: []string{"series_1671844747554927007", "data_flow_0"},
						elements: []string{"2"},
					},
					{
						id:       1,
						location: []string{"series_2374367181827824198", "data_flow_0"},
						elements: []string{"5", "3"},
					},
					{
						id:       1,
						location: []string{"series_8429137420168685297", "data_flow_0"},
						elements: []string{"4"},
					},
				},
			},
		},
		{
			name: "find series by service_id and instance_id",
			args: args{
				entity: tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), tsdb.AnyEntry},
			},
			want: want{
				shards: []shardStruct{
					{
						id:       0,
						location: []string{"series_12243341348514563931", "data_flow_0"},
						elements: []string{"1"},
					},
					{
						id:       1,
						location: []string{"series_2374367181827824198", "data_flow_0"},
						elements: []string{"5", "3"},
					},
				},
			},
		},
		{
			name: "find a series",
			args: args{
				entity: tsdb.Entity{tsdb.Entry("webapp_id"), tsdb.Entry("10.0.0.1_id"), convert.Uint64ToBytes(1)},
			},
			want: want{
				shards: []shardStruct{
					{
						id:       1,
						location: []string{"series_2374367181827824198", "data_flow_0"},
						elements: []string{"5", "3"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shards, err := s.Shards(tt.args.entity)
			tester.NoError(err)
			got := want{
				shards: []shardStruct{},
			}

			for _, shard := range shards {
				seriesList, err := shard.Series().List(tsdb.NewPath(tt.args.entity))
				tester.NoError(err)
				for _, series := range seriesList {
					func(g *want) {
						sp, err := series.Span(tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour))
						defer func(sp tsdb.SeriesSpan) {
							_ = sp.Close()
						}(sp)
						tester.NoError(err)
						seeker, err := sp.SeekerBuilder().Build()
						tester.NoError(err)
						iter, err := seeker.Seek()
						tester.NoError(err)
						for dataFlowID, iterator := range iter {
							var elements []string
							for iterator.Next() {
								tagFamily, err := s.ParseTagFamily("searchable", iterator.Val())
								tester.NoError(err)
								for _, tag := range tagFamily.GetTags() {
									if tag.GetKey() == "trace_id" {
										elements = append(elements, tag.GetValue().GetStr().GetValue())
									}
								}
							}
							_ = iterator.Close()
							g.shards = append(g.shards, shardStruct{
								id: shard.ID(),
								location: []string{
									fmt.Sprintf("series_%v", series.ID()),
									"data_flow_" + strconv.Itoa(dataFlowID),
								},
								elements: elements,
							})
						}

					}(&got)
				}
			}
			if tt.wantErr {
				tester.Error(err)
				return
			}
			tester.NoError(err)
			sort.SliceStable(got.shards, func(i, j int) bool {
				a := got.shards[i]
				b := got.shards[j]
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
			tester.Equal(tt.want, got)
		})
	}

}

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(t *assert.Assertions, dataFile string, stream *stream) (baseTime time.Time) {
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, err := json.Marshal(template)
		t.NoError(err)
		searchTagFamily := &streamv2.ElementValue_TagFamily{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
		e := &streamv2.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(time.Millisecond * time.Duration(i))),
			TagFamilies: []*streamv2.ElementValue_TagFamily{
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
		entity, err := stream.buildEntity(e)
		t.NoError(err)
		shardID, err := partition.ShardID(entity.Marshal(), stream.schema.GetShardNum())
		t.NoError(err)
		itemID, err := stream.write(common.ShardID(shardID), e)
		t.NoError(err)
		sa, err := stream.Shards(entity)
		for _, shard := range sa {
			se, err := shard.Series().Get(entity)
			t.NoError(err)
			for {
				item, closer, _ := se.Get(*itemID)
				rawTagFamily, _ := item.Val("searchable")
				if len(rawTagFamily) > 0 {
					_ = closer.Close()
					break
				}
				_ = closer.Close()
			}

		}
	}
	return baseTime
}
