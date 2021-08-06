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
	"log"
	"os"
	"path"
	"testing"
	"time"

	googleUUID "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"

	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/index"
	"github.com/apache/skywalking-banyandb/banyand/liaison"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/series/trace"
	"github.com/apache/skywalking-banyandb/banyand/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pb"
)

var (
	serverAddr = "localhost:17912"
)

func setupService(t *testing.T, tester *require.Assertions) func() {
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
	executor, err := query.NewExecutor(context.TODO(), repo, indexSvc, traceSvc, traceSvc)
	tester.NoError(err)
	// Init `liaison` module
	tcp, err := liaison.NewEndpoint(context.TODO(), pipeline, repo)
	tester.NoError(err)

	err = db.PreRun()
	tester.NoError(err)

	err = indexSvc.PreRun()
	tester.NoError(err)

	err = traceSvc.PreRun()
	tester.NoError(err)

	err = executor.PreRun()
	tester.NoError(err)

	tcp.FlagSet()
	err = tcp.PreRun()
	tester.NoError(err)

	go func() {
		tester.NoError(traceSvc.Serve())
		tester.NoError(err)
	}()

	go func() {
		tester.NoError(tcp.Serve())
		tester.NoError(err)
	}()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFunc()

	tester.True(indexSvc.Ready(ctx, index.MetaExists("default", "sw")))

	return func() {
		db.GracefulStop()
		_ = os.RemoveAll(rootPath)
	}
}

func TestTraceWrite(t *testing.T) {
	tester := require.New(t)
	gracefulStop := setupService(t, tester)
	defer gracefulStop()
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure())
	assert.NoError(t, err)
	defer conn.Close()

	client := v1.NewTraceServiceClient(conn)
	ctx := context.Background()
	entityValue := pb.NewEntityValueBuilder().
		EntityID("entityId").
		DataBinary([]byte{12}).
		Fields("trace_id-xxfff.111323",
			0,
			"webapp_id",
			"10.0.0.1_id",
			"/home_id",
			"webapp",
			"10.0.0.1",
			"/home",
			300,
			1622933202000000000).
		Timestamp(time.Now()).
		Build()
	criteria := pb.NewWriteEntityBuilder().
		EntityValue(entityValue).
		Metadata("default", "sw").
		Build()
	stream, errorWrite := client.Write(ctx)
	if errorWrite != nil {
		log.Fatalf("%v.runWrite(_) = _, %v", client, errorWrite)
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
			if errRecv != nil {
				log.Fatalf("Failed to receive data : %v", errRecv)
			}
			log.Println("writeResponse: ", writeResponse)
		}
	}()
	if errSend := stream.Send(criteria); errSend != nil {
		log.Fatalf("Failed to send a note: %v", errSend)
	}
	if errorSend := stream.CloseSend(); errorSend != nil {
		log.Fatalf("Failed to send a note: %v", errorSend)
	}
	<-waitc
}
func TestTraceQuery(t *testing.T) {
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure(), grpclib.WithDefaultCallOptions())
	assert.NoError(t, err)
	defer conn.Close()

	client := v1.NewTraceServiceClient(conn)
	ctx := context.Background()
	sT, eT := time.Now().Add(-3*time.Hour), time.Now()
	criteria := pb.NewQueryRequestBuilder().
		Limit(5).
		Offset(10).
		OrderBy("service_instance_id", v1.QueryOrder_SORT_DESC).
		Metadata("default", "trace").
		Projection("http.method", "service_id", "service_instance_id").
		Fields("service_id", "=", "my_app", "http.method", "=", "GET").
		TimeRange(sT, eT).
		Build()
	stream, errRev := client.Query(ctx, criteria)
	if errRev != nil {
		log.Fatalf("Retrieve client failed: %v", errRev)
	}

	log.Println("QueryResponse: ", stream)
}
