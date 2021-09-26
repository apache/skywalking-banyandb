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

package grpc_test

import (
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	googleUUID "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	v12 "github.com/apache/skywalking-banyandb/banyand/query/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/series/trace"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	v1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

type testData struct {
	certFile           string
	serverHostOverride string
	TLS                bool
	addr               string
}

func setup(tester *require.Assertions) (*grpc.Server, *grpc.Server, func()) {
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
	// Init `Database` module
	db, err := storage.NewDB(context.TODO(), repo)
	tester.NoError(err)
	uuid, err := googleUUID.NewUUID()
	tester.NoError(err)
	rootPath := path.Join(os.TempDir(), "banyandb-"+uuid.String())
	tester.NoError(db.FlagSet().Parse([]string{"--root-path=" + rootPath}))
	// Init `Index` module
	indexSvc, err := index.NewService(context.TODO(), repo)
	tester.NoError(err)
	// Init `Trace` module
	traceSvc, err := trace.NewService(context.TODO(), db, repo, indexSvc, pipeline)
	tester.NoError(err)
	// Init `Query` module
	executor, err := v12.NewExecutor(context.TODO(), repo, indexSvc, traceSvc, traceSvc, pipeline)
	tester.NoError(err)
	// Init `liaison` module
	tcp := grpc.NewServer(context.TODO(), pipeline, repo)
	tester.NoError(tcp.FlagSet().Parse([]string{"--tls=false", "--addr=:17912"}))
	tcpTLS := grpc.NewServer(context.TODO(), pipeline, repo)
	tester.NoError(tcpTLS.FlagSet().Parse([]string{"--tls=true", "--addr=:17913", "--cert-file=testdata/server_cert.pem", "--key-file=testdata/server_key.pem"}))

	err = indexSvc.PreRun()
	tester.NoError(err)

	err = traceSvc.PreRun()
	tester.NoError(err)

	err = db.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	err = tcp.PreRun()
	tester.NoError(err)
	tester.NoError(tcpTLS.PreRun())

	go func() {
		tester.NoError(traceSvc.Serve())
	}()

	go func() {
		tester.NoError(tcpTLS.Serve())
	}()

	go func() {
		tester.NoError(tcp.Serve())
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	tester.True(indexSvc.Ready(ctx, index.MetaExists("default", "sw")))

	return tcp, tcpTLS, func() {
		db.GracefulStop()
		_ = os.RemoveAll(rootPath)
	}
}

type caseData struct {
	name           string
	queryGenerator func(baseTs time.Time) *tracev1.QueryRequest
	writeGenerator func() *tracev1.WriteRequest
	args           testData
	wantLen        int
}

func TestTraceService(t *testing.T) {
	tester := require.New(t)
	tcp, tcpTLS, gracefulStop := setup(tester)
	defer gracefulStop()
	_, currentFile, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(currentFile)
	certFile := filepath.Join(basePath, "testdata/server_cert.pem")
	testCases := []caseData{
		{
			name: "isTLS",
			queryGenerator: func(baseTs time.Time) *tracev1.QueryRequest {
				return v1.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("trace_id", "=", "trace_id-xxfff.111").
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			writeGenerator: func() *tracev1.WriteRequest {
				entityValue := v1.NewEntityValueBuilder().
					EntityID("entityId123").
					DataBinary([]byte{12}).
					Fields("trace_id-xxfff.111",
						0,
						"webapp_id",
						"10.0.0.1_id",
						"/home_id",
						300,
						1622933202000000000).
					Timestamp(time.Now()).
					Build()
				criteria := v1.NewWriteEntityBuilder().
					EntityValue(entityValue).
					Metadata("default", "sw").
					Build()
				return criteria
			},
			args: testData{
				TLS:                true,
				certFile:           certFile,
				serverHostOverride: "localhost",
				addr:               "localhost:17913",
			},
			wantLen: 1,
		},
		{
			name: "noTLS",
			queryGenerator: func(baseTs time.Time) *tracev1.QueryRequest {
				return v1.NewQueryRequestBuilder().
					Limit(10).
					Offset(0).
					Metadata("default", "sw").
					Fields("trace_id", "=", "trace_id-xxfff.111323").
					TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
					Projection("trace_id").
					Build()
			},
			writeGenerator: func() *tracev1.WriteRequest {
				entityValue := v1.NewEntityValueBuilder().
					EntityID("entityId123").
					DataBinary([]byte{12}).
					Fields("trace_id-xxfff.111323",
						0,
						"webapp_id",
						"10.0.0.1_id",
						"/home_id",
						300,
						1622933202000000000).
					Timestamp(time.Now()).
					Build()
				criteria := v1.NewWriteEntityBuilder().
					EntityValue(entityValue).
					Metadata("default", "sw").
					Build()
				return criteria
			},
			args: testData{
				TLS:  false,
				addr: "localhost:17912",
			},
			wantLen: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.args.TLS {
				errValidate := tcpTLS.Validate()
				assert.NoError(t, errValidate)
				var opts []grpclib.DialOption
				creds, err := credentials.NewClientTLSFromFile(tc.args.certFile, tc.args.serverHostOverride)
				assert.NoError(t, err)
				opts = append(opts, grpclib.WithTransportCredentials(creds))
				dialService(t, tc, opts)
			} else {
				errValidate := tcp.Validate()
				assert.NoError(t, errValidate)
				var opts []grpclib.DialOption
				opts = append(opts, grpclib.WithInsecure())
				dialService(t, tc, opts)
			}
		})
	}
}

func dialService(t *testing.T, tc caseData, opts []grpclib.DialOption) {
	conn, err := grpclib.Dial(tc.args.addr, opts...)
	assert.NoError(t, err)
	defer conn.Close()
	traceWrite(t, tc, conn)
	//requireTester := require.New(t)
	//retry := 10
	//for retry > 0 {
	//	now := time.Now()
	//	resp := traceQuery(requireTester, conn, tc.queryGenerator(now))
	//	if assert.Len(t, resp.GetEntities(), tc.wantLen) {
	//		break
	//	} else {
	//		time.Sleep(1 * time.Second)
	//		retry--
	//	}
	//}
	//if retry == 0 {
	//	requireTester.FailNow("retry fail")
	//}
}

func traceWrite(t *testing.T, tc caseData, conn *grpclib.ClientConn) {
	client := tracev1.NewTraceServiceClient(conn)
	ctx := context.Background()
	stream, errorWrite := client.Write(ctx)
	if errorWrite != nil {
		t.Errorf("%v.write(_) = _, %v", client, errorWrite)
	}
	waitc := make(chan struct{})
	go func() {
		for {
			writeResponse, errRecv := stream.Recv()
			if errRecv == io.EOF {
				// read done.
				close(waitc)
				return
			}
			assert.NoError(t, errRecv)
			assert.NotNil(t, writeResponse)
		}
	}()
	if errSend := stream.Send(tc.writeGenerator()); errSend != nil {
		t.Errorf("Failed to send a note: %v", errSend)
	}
	if errorSend := stream.CloseSend(); errorSend != nil {
		t.Errorf("Failed to send a note: %v", errorSend)
	}
	<-waitc
}

func traceQuery(tester *require.Assertions, conn *grpclib.ClientConn, request *tracev1.QueryRequest) *tracev1.QueryResponse {
	client := tracev1.NewTraceServiceClient(conn)
	ctx := context.Background()
	stream, errRev := client.Query(ctx, request)
	if errRev != nil {
		tester.Errorf(errRev, "Retrieve client failed: %v")
	}
	tester.NotNil(stream)
	return stream
}
