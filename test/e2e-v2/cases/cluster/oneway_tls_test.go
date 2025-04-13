//go:build integration
// +build integration

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

package cluster

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measure "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	model "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// TestWriteMeasureViaLiaison validates gRPC communication via TLS.
//
// Note for contributors:
// - This test requires a TLS-enabled liaison node running at localhost:17914.
// - To run this test, you must generate test certs based on test/testdata/certs/cert.conf:
//
//     openssl req -x509 -newkey rsa:4096 -nodes -keyout test/testdata/certs/server.key \
//       -out test/testdata/certs/server.crt -days 365 -config test/testdata/certs/cert.conf \
//       -extensions v3_req
//
// - Ensure the server is started with TLS flags:
//     --tls=true --cert-file=test/testdata/certs/server.crt --key-file=test/testdata/certs/server.key

func TestWriteMeasureViaLiaison(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("Failed to get current file path")
	}
	certPath := filepath.Join(filepath.Dir(currentFile), "../../../testdata/certs/server.crt")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Load TLS credentials from test certs.
	creds, err := credentials.NewClientTLSFromFile(certPath, "")
	if err != nil {
		t.Fatalf("❌ Failed to load TLS credentials: %v", err)
	}

	// Connect to the Liaison node over TLS.
	conn, err := grpc.DialContext(ctx, "localhost:17914", grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("❌ Failed to create gRPC client: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("⚠️ Failed to close gRPC connection: %v", err)
		}
	}()

	client := measure.NewMeasureServiceClient(conn)

	// Open a gRPC stream to send a Measure WriteRequest.
	stream, err := client.Write(context.Background())
	if err != nil {
		t.Fatalf("❌ Failed to open stream: %v", err)
	}

	// Construct the request.
	req := &measure.WriteRequest{
		Metadata: &common.Metadata{
			Group: "default",
			Name:  "test_metric",
		},
		DataPoint: &measure.DataPointValue{
			Timestamp: timestamppb.Now(),
			TagFamilies: []*model.TagFamilyForWrite{
				{
					Tags: []*model.TagValue{
						{
							Value: &model.TagValue_Str{
								Str: &model.Str{Value: "demo-tag-value"},
							},
						},
					},
				},
			},
			Fields: []*model.FieldValue{
				{
					Value: &model.FieldValue_Int{
						Int: &model.Int{Value: 42},
					},
				},
			},
			Type: measure.DataPointValue_TYPE_DELTA,
		},
		MessageId: uint64(time.Now().UnixNano()),
	}

	// Send the request and close the stream.
	if err := stream.Send(req); err != nil {
		t.Fatalf("❌ Failed to send WriteRequest: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("❌ Failed to close stream: %v", err)
	}

	t.Log("✅ WriteRequest sent via streaming successfully")
}
