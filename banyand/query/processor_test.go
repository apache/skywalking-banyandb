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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/event"
	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/series/trace"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pb"
)

func setupServices(tester *require.Assertions) (discovery.ServiceRepo, series.Service) {
	// Bootstrap logger system
	err := logger.Bootstrap()
	tester.NoError(err)

	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	tester.NoError(err)
	tester.NotNil(repo)

	// Init `Database` module
	db, err := storage.NewDB(context.TODO(), repo)
	tester.NoError(err)
	tester.NoError(db.FlagSet().Parse(nil))

	// Init `Trace` module
	traceSvc, err := trace.NewService(context.TODO(), db, repo)
	tester.NoError(err)

	// Init `Query` module
	executor, err := NewExecutor(context.TODO(), repo, nil, traceSvc, traceSvc)
	tester.NoError(err)

	// :PreRun:
	// 1) TraceSeries,
	// 2) Database
	err = traceSvc.PreRun()
	tester.NoError(err)

	err = db.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	return repo, traceSvc
}

func TestQueryProcessor(t *testing.T) {
	tester := require.New(t)

	// setup services
	repo, traceSvc := setupServices(tester)
	
	tracesMetadata := common.Metadata{
		KindVersion: common.KindVersion{},
		Spec:        ,
	}
	now := time.Now()
	traceSvc.Write()

	tests := []struct {
		// name of the test case
		name string
		// queryGenerator is used to generate a Query
		queryGenerator func(baseTs time.Time) *v1.QueryRequest
		// wantLen is the length of entities expected to return
		wantLen int
	}{
		{
			name: "Query Trace ID when no initial data is given",
			queryGenerator: func(baseTs time.Time) *v1.QueryRequest {
				return pb.NewQueryRequestBuilder().
					Limit(5).
					Offset(10).
					Metadata("default", "sw").
					Fields("trace_id", "=", "123").
					Build()
			},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			singleTester := require.New(t)
			now := time.Now()
			m := bus.NewMessage(bus.MessageID(now.UnixNano()), tt.queryGenerator(now))
			f, err := repo.Publish(event.TopicQueryEvent, m)
			singleTester.NoError(err)
			singleTester.NotNil(f)
			msg, err := f.Get()
			singleTester.NoError(err)
			singleTester.NotNil(msg)
			singleTester.Len(msg.Data(), tt.wantLen)
		})
	}
}
