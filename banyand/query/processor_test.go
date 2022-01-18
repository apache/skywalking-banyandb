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

package query

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pb "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/test"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

var (
	withoutDataBinaryChecker = func(elements []*streamv1.Element) bool {
		for _, elem := range elements {
			for _, tagFamily := range elem.GetTagFamilies() {
				if tagFamily.GetName() == "data" {
					return false
				}
			}
		}
		return true
	}
	withDataBinaryChecker = func(elements []*streamv1.Element) bool {
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

func setupServices(require *require.Assertions) (stream.Service, queue.Queue, func()) {
	// Bootstrap logger system
	require.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	}))

	// Create a random directory
	rootPath, deferFunc := test.Space(require)

	var repo discovery.ServiceRepo
	var pipeline queue.Queue
	var metadataSvc metadata.Service
	var streamSvc stream.Service
	var etcdRootDir string
	var executor Executor

	flow := test.NewTestFlow().PushErrorHandler(func() {
		deferFunc()
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		// Init `Discovery` module
		repo, err = discovery.NewServiceRepo(context.Background())
		return
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		// Init `Queue` module
		pipeline, err = queue.NewQueue(context.TODO(), repo)
		return
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		// Init `Metadata` module
		metadataSvc, err = metadata.NewService(context.TODO())
		return
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		// Init `Stream` module
		streamSvc, err = stream.NewService(context.TODO(), metadataSvc, repo, pipeline)
		return
	}).Run(context.TODO(), func() error {
		etcdRootDir = teststream.RandomTempDir()
		return metadataSvc.FlagSet().Parse([]string{"--metadata-root-path=" + etcdRootDir})
	}, func() {
		if len(etcdRootDir) > 0 {
			_ = os.RemoveAll(etcdRootDir)
		}
	}).RunWithoutSideEffect(context.TODO(), func() error {
		return streamSvc.FlagSet().Parse([]string{"--root-path=" + rootPath})
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		// Init `Query` module
		executor, err = NewExecutor(context.TODO(), streamSvc, metadataSvc, repo, pipeline)
		return err
	}).Run(context.TODO(), func() error {
		// :PreRun:
		// 1) metadata
		// 2) measure
		// 3) query
		// 4) liaison
		if err := metadataSvc.PreRun(); err != nil {
			return err
		}

		if err := teststream.PreloadSchema(metadataSvc.SchemaRegistry()); err != nil {
			return err
		}

		if err := streamSvc.PreRun(); err != nil {
			return err
		}

		if err := executor.PreRun(); err != nil {
			return err
		}

		return nil
	}, func() {
		if metadataSvc != nil {
			metadataSvc.GracefulStop()
		}
	})

	require.NoError(flow.Error())

	return streamSvc, pipeline, flow.Shutdown()
}

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(testing *testing.T, dataFile string, stream stream.Stream) (baseTime time.Time) {
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
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_BinaryData{
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

func TestQueryProcessor(t *testing.T) {
	assertT := assert.New(t)
	streamSvc, pipeline, deferFunc := setupServices(require.New(t))
	stm, err := streamSvc.Stream(&commonv1.Metadata{Name: "sw", Group: "default"})
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
		queryGenerator func(baseTs time.Time) *streamv1.QueryRequest
		// wantLen is the length of entities expected to return
		wantLen int
		// checker is the customized checker for extra checks
		checker func([]*streamv1.Element) bool
	}{
		{
			name: "query given timeRange is out of the time range of data",
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(sT, eT).
					OrderBy("duration", modelv1.Sort_SORT_DESC).
					Projection("searchable", "trace_id", "duration").
					Build()
			},
			wantLen: 5,
			checker: func(elements []*streamv1.Element) bool {
				return logical.SortedByIndex(elements, 0, 1, modelv1.Sort_SORT_DESC)
			},
		},
		{
			name: "query TraceID given timeRange includes the time range of data",
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					FieldsInTagFamily("searchable", "http.method", "=", "GET").
					TimeRange(sT, eT).
					Projection("searchable", "trace_id").
					Build()
			},
			wantLen: 3,
			checker: withoutDataBinaryChecker,
		},
		{
			name: "Textual Index - http.method == GET with dataBinary projection",
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			queryGenerator: func(baseTs time.Time) *streamv1.QueryRequest {
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
			f, err := pipeline.Publish(data.TopicStreamQuery, m)
			singleTester.NoError(err)
			singleTester.NotNil(f)
			msg, err := f.Get()
			singleTester.NoError(err)
			singleTester.NotNil(msg)
			// TODO: better error response
			singleTester.NotNil(msg.Data())
			singleTester.Len(msg.Data(), tt.wantLen)
			if tt.checker != nil {
				singleTester.True(tt.checker(msg.Data().([]*streamv1.Element)))
			}
		})
	}
}
