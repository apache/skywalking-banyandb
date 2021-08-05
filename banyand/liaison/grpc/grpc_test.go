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
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"testing"
	"time"

	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pb"
	grpclib "google.golang.org/grpc"
)

var (
	serverAddr = "localhost:17912"
)
func setup(t *testing.T) {
	_ = logger.Bootstrap()
	repo, err := discovery.NewServiceRepo(context.TODO())
	if err != nil {
		log.Fatalf("failed to initiate service repository")
	}
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	if err != nil {
		log.Fatal("failed to initiate data pipeline")
	}
	tcp, err := liaison.NewEndpoint(context.TODO(), pipeline, repo)
	if err != nil {
		log.Fatal("failed to initiate Endpoint transport layer")
	}
	tcp.FlagSet()
	go func() {
		assert.NoError(t, err)
		assert.NoError(t,tcp.PreRun())
		assert.NoError(t, tcp.Serve())
	}()
}
func Test_trace_write(t *testing.T) {
	setup(t)
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
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
func Test_trace_query(t *testing.T) {
	setup(t)
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure(), grpclib.WithDefaultCallOptions())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
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
