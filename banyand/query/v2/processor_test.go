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

package v2

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v2"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v2"
	"github.com/apache/skywalking-banyandb/pkg/query/v2/logical"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

var (
	withoutDataBinaryChecker = func(elements []*streamv2.Element) bool {
		for _, elem := range elements {
			for _, tagFamily := range elem.GetTagFamilies() {
				if tagFamily.GetName() == "data" {
					return false
				}
			}
		}
		return true
	}
	withDataBinaryChecker = func(elements []*streamv2.Element) bool {
		for _, elem := range elements {
			for _, tagFamily := range elem.GetTagFamilies() {
				if tagFamily.GetName() == "data" {
					return true
				}
			}
		}
		return false
	}
)

func setupServices(tester *assert.Assertions) (stream.Service, queue.Queue, func()) {
	// Bootstrap logger system
	tester.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "trace",
	}))

	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	tester.NoError(err)
	tester.NotNil(repo)
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	tester.NoError(err)

	// Create a random directory
	rootPath, deferFunc := test.Space(tester)

	// Init `Metadata` module
	metadataSvc, err := metadata.NewService(context.TODO())
	tester.NoError(err)

	streamSvc, err := stream.NewService(context.TODO(), metadataSvc, pipeline)
	tester.NoError(err)

	err = streamSvc.FlagSet().Parse([]string{"--root-path=" + rootPath})
	tester.NoError(err)

	// Init `Query` module
	executor, err := NewExecutor(context.TODO(), streamSvc, repo, pipeline)
	tester.NoError(err)

	// :PreRun:
	// 1) stream
	// 2) query
	// 3) liaison
	err = streamSvc.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	return streamSvc, pipeline, func() {
		deferFunc()
		_ = os.RemoveAll(rootPath)
	}
}

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

func TestQueryProcessor(t *testing.T) {
	assertT := assert.New(t)
	streamSvc, pipeline, deferFunc := setupServices(assertT)
	stm, err := streamSvc.StreamT(&commonv2.Metadata{Name: "sw", Group: "default"})
	defer func() {
		_ = stm.Close()
		deferFunc()
	}()
	assertT.NoError(err)
	baseTs := setupQueryData(t, "multiple_shards.json", stm)

	sT, eT := baseTs, baseTs.Add(1*time.Hour)

	tests := []struct {
		// name of the test case
		name string
		// queryGenerator is used to generate a Query
		queryGenerator func(baseTs time.Time) *streamv2.QueryRequest
		// wantLen is the length of entities expected to return
		wantLen int
		// checker is the customized checker for extra checks
		checker func([]*streamv2.Element) bool
	}{
		{
			name: "query given timeRange is out of the time range of data",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(time.Unix(0, 0), time.Unix(0, 1)).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 0,
		},
		{
			name: "query given timeRange which covers all the segments with data binary projection",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Projection("data", "data_binary").
					Build()
			},
			wantLen: 5,
			checker: withDataBinaryChecker,
		},
		{
			name: "query given timeRange which covers all the segments and sort by duration DESC",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(sT, eT).
					OrderBy("duration", modelv2.QueryOrder_SORT_DESC).
					Projection("searchable", "trace_id", "duration").
					Build()
			},
			wantLen: 5,
			checker: func(elements []*streamv2.Element) bool {
				return logical.SortedByIndex(elements, 0, 1, modelv2.QueryOrder_SORT_DESC)
			},
		},
		{
			name: "query TraceID given timeRange includes the time range of data",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "trace_id", "=", "1").
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 1,
			checker: withoutDataBinaryChecker,
		},
		{
			name: "query TraceID given timeRange includes the time range of data with dataBinary projection",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "trace_id", "=", "1").
					TimeRange(sT, eT).
					Projection("data", "data_binary").
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 1,
			checker: withDataBinaryChecker,
		},
		{
			name: "Numerical Index - query duration < 500",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "duration", "<", 500).
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 3,
		},
		{
			name: "Numerical Index - query duration <= 500",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "duration", "<=", 500).
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 4,
		},
		{
			name: "Textual Index - http.method == GET",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "http.method", "=", "GET").
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 2,
			checker: withoutDataBinaryChecker,
		},
		{
			name: "Textual Index - http.method == GET with dataBinary projection",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "http.method", "=", "GET").
					TimeRange(sT, eT).
					Projection("data", "data_binary").
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 3,
			checker: withDataBinaryChecker,
		},
		{
			name: "Mixed Index - status_code == 500 AND duration <= 100",
			queryGenerator: func(baseTs time.Time) *streamv2.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "status_code", "=", "500", "duration", "<=", 100).
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			singleTester := require.New(t)
			now := time.Now()
			m := bus.NewMessage(bus.MessageID(now.UnixNano()), tt.queryGenerator(baseTs))
			f, err := pipeline.Publish(data.TopicQueryEvent, m)
			singleTester.NoError(err)
			singleTester.NotNil(f)
			msg, err := f.Get()
			singleTester.NoError(err)
			singleTester.NotNil(msg)
			// TODO: better error response
			singleTester.NotNil(msg.Data())
			singleTester.Len(msg.Data(), tt.wantLen)
			if tt.checker != nil {
				singleTester.True(tt.checker(msg.Data().([]*streamv2.Element)))
			}
		})
	}
}
