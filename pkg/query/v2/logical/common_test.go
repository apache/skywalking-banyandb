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

package logical_test

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(testingT *testing.T, dataFile string, streamT stream.StreamT) (baseTime time.Time) {
	t := assert.New(testingT)
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		t.NoError(errMarshal)
		searchTagFamily := &streamv2.ElementValue_TagFamily{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
		e := &streamv2.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
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
		entity, errInner := streamT.BuildEntity(e)
		t.NoError(errInner)
		shardID, errInner := partition.ShardID(entity.Marshal(), streamT.GetShardNum())
		t.NoError(errInner)
		_, errInner = streamT.Write(common.ShardID(shardID), e)
		t.NoError(errInner)
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	err = ready(ctx, t, streamT, queryOpts{
		entity:    tsdb.Entity{tsdb.AnyEntry, tsdb.AnyEntry, tsdb.AnyEntry},
		timeRange: tsdb.NewTimeRangeDuration(baseTime, 1*time.Hour),
	})
	require.NoError(testingT, err)
	return baseTime
}

func ready(ctx context.Context, t *assert.Assertions, stream stream.StreamT, options queryOpts) error {
	for {
	loop:
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, err := queryData(t, stream, options)
			if err != nil {
				return err
			}
			for _, d := range data {
				if len(d.elements) < 1 {
					time.Sleep(300 * time.Millisecond)
					break loop
				}
			}
			return nil
		}
	}
}

type queryOpts struct {
	entity    tsdb.Entity
	timeRange tsdb.TimeRange
	buildFn   func(builder tsdb.SeekerBuilder)
}

type shardStruct struct {
	id       common.ShardID
	location []string
	elements []string
}

type shardsForTest []shardStruct

func queryData(tester *assert.Assertions, s stream.StreamT, opts queryOpts) (shardsForTest, error) {
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

func setup(t *assert.Assertions) (stream.StreamT, func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "trace",
	}))
	tempDir, deferFunc := test.Space(t)
	streamRepo, err := schema.NewStream()
	t.NoError(err)
	sa, err := streamRepo.Get(context.TODO(), &commonv2.Metadata{
		Name:  "sw",
		Group: "default",
	})
	t.NoError(err)
	mService, err := metadata.NewService(context.TODO())
	t.NoError(err)
	iRules, err := mService.IndexRules(context.TODO(), sa.Metadata)
	t.NoError(err)

	s, err := stream.OpenStreamT(tempDir, sa, iRules, logger.GetLogger("test"))

	t.NoError(err)
	return s, func() {
		_ = s.Close()
		deferFunc()
	}
}
