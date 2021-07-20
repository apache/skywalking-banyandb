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
	"net"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	grpclib "google.golang.org/grpc"

	v1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/pkg/fb"
)

var serverAddr = "localhost:17912"

func Test_server_start(t *testing.T) {
	go func() {
		lis, err := net.Listen("tcp", serverAddr)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		ser := grpclib.NewServer(grpclib.CustomCodec(flatbuffers.FlatbuffersCodec{}))
		v1.RegisterTraceServer(ser, &grpc.TraceServer{})
		if err := ser.Serve(lis); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
}

func Test_trace_write(t *testing.T) {
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure(), grpclib.WithDefaultCallOptions(grpclib.CustomCodecCallOption{Codec: flatbuffers.FlatbuffersCodec{}}))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := v1.NewTraceClient(conn)
	ctx := context.Background()
	b := fb.NewWriteEntityBuilder()
	binary := byte(12)
	criteria, e := b.BuildWriteEntity(
		b.BuildEntity("entityId", []byte{binary}, "service_instance_id", "service_id_1234", "service_instance_id_43543"),
		b.BuildMetaData("default", "trace"),
	)
	if e != nil {
		log.Fatalf("Failed to connect: %v", e)
	}
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
	conn, err := grpclib.Dial(serverAddr, grpclib.WithInsecure(), grpclib.WithDefaultCallOptions(grpclib.CustomCodecCallOption{Codec: flatbuffers.FlatbuffersCodec{}}))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := v1.NewTraceClient(conn)
	ctx := context.Background()
	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	b := fb.NewCriteriaBuilder()
	builder, e := b.BuildQueryEntity(
		fb.AddLimit(5),
		fb.AddOffset(10),
		b.BuildMetaData("default", "trace"),
		b.BuildTimeStampNanoSeconds(sT, eT),
		b.BuildFields("service_id", "=", "my_app", "http.method", "=", "GET"),
		b.BuildProjection("http.method", "service_id", "service_instance_id"),
		b.BuildOrderBy("service_instance_id", v1.SortDESC),
	)
	if e != nil {
		log.Fatalf("Failed to connect: %v", e)
	}
	stream, errRev := client.Query(ctx, builder)
	if errRev != nil {
		log.Fatalf("Retrieve client failed: %v", errRev)
	}

	log.Println("QueryResponse: ", stream)
}
