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
	"os"
	"path"
	"testing"
	"time"

	googleUUID "github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/banyand/series/trace"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pb"
)

var interval = time.Millisecond * 500

type entityValue struct {
	seriesID   string
	entityID   string
	dataBinary []byte
	ts         time.Time
	items      []interface{}
}

func setupServices(t *testing.T, tester *require.Assertions) (discovery.ServiceRepo, series.Service, func()) {
	// Bootstrap logger system
	tester.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	}))

	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	tester.NoError(err)
	tester.NotNil(repo)
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	tester.NoError(err)

	// Init `Index` module
	indexSvc, err := index.NewService(context.TODO(), repo)
	tester.NoError(err)

	// Init `Database` module
	db, err := storage.NewDB(context.TODO(), repo)
	tester.NoError(err)
	uuid, err := googleUUID.NewUUID()
	tester.NoError(err)
	rootPath := path.Join(os.TempDir(), "banyandb-"+uuid.String())
	tester.NoError(db.FlagSet().Parse([]string{"--root-path=" + rootPath}))

	// Init `Trace` module
	traceSvc, err := trace.NewService(context.TODO(), db, repo, indexSvc, pipeline)
	tester.NoError(err)

	// Init `Query` module
	executor, err := NewExecutor(context.TODO(), repo, indexSvc, traceSvc, traceSvc)
	tester.NoError(err)

	// Init `Liaison` module
	liaison := grpc.NewServer(context.TODO(), pipeline, repo)

	// :PreRun:
	// 1) TraceSeries,
	// 2) Database
	// 3) Index
	err = traceSvc.PreRun()
	tester.NoError(err)

	err = db.PreRun()
	tester.NoError(err)

	err = indexSvc.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	err = liaison.PreRun()
	tester.NoError(err)

	// :Serve:
	go func() {
		err = traceSvc.Serve()
		tester.NoError(err)
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()
	tester.True(indexSvc.Ready(ctx, index.MetaExists("default", "sw")))

	return repo, traceSvc, func() {
		db.GracefulStop()
		_ = os.RemoveAll(rootPath)
	}
}

func setupData(tester *require.Assertions, baseTs time.Time, svc series.Service) {
	metadata := common.Metadata{
		Spec: &v1.Metadata{
			Name:  "sw",
			Group: "default",
		},
	}

	entityValues := []entityValue{
		{
			ts:         baseTs,
			seriesID:   "webapp_10.0.0.1",
			entityID:   "1",
			dataBinary: []byte{11},
			items: []interface{}{
				"trace_id-xxfff.111323",
				0,
				"webapp_id",
				"10.0.0.1_id",
				"/home_id",
				"webapp",
				"10.0.0.1",
				"/home",
				300,
				1622933202000000000,
			},
		},
		{
			ts:         baseTs.Add(interval),
			seriesID:   "gateway_10.0.0.2",
			entityID:   "2",
			dataBinary: []byte{12},
			items: []interface{}{
				"trace_id-xxfff.111323a",
				1,
			},
		},
		{
			ts:         baseTs.Add(interval * 2),
			seriesID:   "httpserver_10.0.0.3",
			entityID:   "3",
			dataBinary: []byte{13},
			items: []interface{}{
				"trace_id-xxfff.111323",
				1,
				"httpserver_id",
				"10.0.0.3_id",
				"/home_id",
				"httpserver",
				"10.0.0.3",
				"/home",
				300,
				1622933202000000000,
				"GET",
				"200",
			},
		},
		{
			ts:         baseTs.Add(interval * 3),
			seriesID:   "database_10.0.0.4",
			entityID:   "4",
			dataBinary: []byte{14},
			items: []interface{}{
				"trace_id-xxfff.111323",
				0,
				"database_id",
				"10.0.0.4_id",
				"/home_id",
				"database",
				"10.0.0.4",
				"/home",
				300,
				1622933202000000000,
				nil,
				nil,
				"MySQL",
				"10.1.1.4",
			},
		},
		{
			ts:         baseTs.Add(interval * 4),
			seriesID:   "mq_10.0.0.5",
			entityID:   "5",
			dataBinary: []byte{15},
			items: []interface{}{
				"trace_id-zzpp.111323",
				0,
				"mq_id",
				"10.0.0.5_id",
				"/home_id",
				"mq",
				"10.0.0.5",
				"/home",
				300,
				1622933202000000000,
				nil,
				nil,
				nil,
				nil,
				"test_topic",
				"10.0.0.5",
			},
		},
		{
			ts:         baseTs.Add(interval * 5),
			seriesID:   "database_10.0.0.6",
			entityID:   "6",
			dataBinary: []byte{16},
			items: []interface{}{
				"trace_id-zzpp.111323",
				1,
				"database_id",
				"10.0.0.6_id",
				"/home_id",
				"database",
				"10.0.0.6",
				"/home",
				300,
				1622933202000000000,
				nil,
				nil,
				"MySQL",
				"10.1.1.6",
			},
		},
		{
			ts:         baseTs.Add(interval * 6),
			seriesID:   "mq_10.0.0.7",
			entityID:   "7",
			dataBinary: []byte{17},
			items: []interface{}{
				"trace_id-zzpp.111323",
				0,
				"nq_id",
				"10.0.0.7_id",
				"/home_id",
				"mq",
				"10.0.0.7",
				"/home",
				300,
				1622933202000000000,
				nil,
				nil,
				nil,
				nil,
				"test_topic",
				"10.0.0.7",
			},
		},
	}

	for _, ev := range entityValues {
		ok, err := svc.Write(metadata, ev.ts, ev.seriesID, ev.entityID, ev.dataBinary, ev.items...)
		tester.True(ok)
		tester.NoError(err)
	}
}

func TestQueryProcessor(t *testing.T) {
	tester := require.New(t)

	// setup services
	repo, traceSvc, gracefulStop := setupServices(t, tester)
	defer gracefulStop()

	baseTs := time.Now()
	setupData(tester, baseTs, traceSvc)

	tests := []struct {
		// name of the test case
		name string
		// queryGenerator is used to generate a Query
		queryGenerator func(baseTs time.Time) *v1.QueryRequest
		// wantLen is the length of entities expected to return
		wantLen int
	}{
		{
			name: "query given timeRange is out of the time range of data",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(time.Unix(0, 0), time.Unix(0, 1)).
					Projection("trace_id").
					Build()
			},
			wantLen: 0,
		},
		{
			name: "query given timeRange which slightly covers the first three segments",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					TimeRange(baseTs.Add(-1*time.Nanosecond), baseTs.Add(2*interval).Add(1*time.Nanosecond)).
					Projection("trace_id").
					Build()
			},
			wantLen: 3,
		},
		{
			name: "query TraceID given timeRange includes the time range of data",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("trace_id", "=", "trace_id-zzpp.111323").
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 3,
		},
		{
			name: "query TraceID given timeRange includes the time range of data but limit to 1",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(1).
					Offset(0).
					Metadata("default", "sw").
					Fields("trace_id", "=", "trace_id-zzpp.111323").
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 1,
		},
		{
			name: "Numerical Index - query duration < 200",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(1).
					Offset(0).
					Metadata("default", "sw").
					Fields("duration", "<", 200).
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 0,
		},
		{
			name: "Numerical Index - query duration < 400",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("duration", "<", 400).
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 6,
		},
		{
			name: "Textual Index - db.type == MySQL",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("db.type", "=", "MySQL").
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 2,
		},
		{
			name: "Mixed Index - db.type == MySQL AND duration <= 300",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("db.type", "=", "MySQL", "duration", "<=", 300).
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			singleTester := require.New(t)
			now := time.Now()
			m := bus.NewMessage(bus.MessageID(now.UnixNano()), tt.queryGenerator(baseTs))
			f, err := repo.Publish(event.TopicQueryEvent, m)
			singleTester.NoError(err)
			singleTester.NotNil(f)
			msg, err := f.Get()
			singleTester.NoError(err)
			singleTester.NotNil(msg)
			// TODO: better error response
			singleTester.NotNil(msg.Data())
			singleTester.Len(msg.Data(), tt.wantLen)
		})
	}
}
