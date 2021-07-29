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
	"github.com/apache/skywalking-banyandb/banyand/liaison/data"
	"google.golang.org/grpc/credentials"
	"io"
	"log"
	"net"
	"testing"
	"time"

	grpclib "google.golang.org/grpc"

	v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/pb"
)
var (
	serverAddr = "localhost:17912"
)

func Test_server_start(t *testing.T) {
	go func() {
		lis, err := net.Listen("tcp", serverAddr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		var opts []grpclib.ServerOption
		if *grpc.Tls {
			if *grpc.CertFile == "" {
				*grpc.CertFile = data.Path("x509/server_cert.pem")
			}
			if *grpc.KeyFile == "" {
				*grpc.KeyFile = data.Path("x509/server_key.pem")
			}
			creds, err := credentials.NewServerTLSFromFile(*grpc.CertFile, *grpc.KeyFile)
			if err != nil {
				log.Fatalf("Failed to generate credentials %v", err)
			}
			opts = []grpclib.ServerOption{grpclib.Creds(creds)}
		}
		ser := grpclib.NewServer(opts...)
		v1.RegisterTraceServiceServer(ser, &grpc.TraceServiceWriteServer{})
		if err := ser.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
}

func Test_trace_write(t *testing.T) {
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := v1.NewTraceServiceClient(conn)
	ctx := context.Background()
	binary := byte(12)
	entityValue := pb.NewEntityValueBuilder().
		EntityID("entityId").
		DataBinary([]byte{binary}).
		Fields("service_instance_id", "service_id_1234", "service_instance_id_43543").
		Build()
	criteria := pb.NewWriteEntityBuilder().
		EntityValue(entityValue).
		Metadata("default", "trace").
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
