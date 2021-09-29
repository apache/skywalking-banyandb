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

package grpc

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	streamv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v2"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	query "github.com/apache/skywalking-banyandb/banyand/query/v2"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	v2 "github.com/apache/skywalking-banyandb/pkg/pb/v2"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

type testData struct {
	certFile           string
	serverHostOverride string
	TLS                bool
	addr               string
	basePath           string
}

func setup(req *require.Assertions, testData testData) func() {
	req.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	}))
	g := run.Group{Name: "standalone"}
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	req.NoError(err)
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	req.NoError(err)
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	req.NoError(err)
	streamSvc, err := stream.NewService(context.TODO(), metaSvc, repo, pipeline)
	req.NoError(err)
	q, err := query.NewExecutor(context.TODO(), streamSvc, repo, pipeline)
	req.NoError(err)

	tcp := NewServer(context.TODO(), pipeline, repo)

	closer := run.NewTester("closer")
	startListener := run.NewTester("started-listener")
	g.Register(
		closer,
		repo,
		pipeline,
		metaSvc,
		streamSvc,
		q,
		tcp,
		startListener,
	)
	// Create a random directory
	rootPath, deferFunc := test.Space(req)
	flags := []string{"--root-path=" + rootPath}
	if testData.TLS {
		flags = append(flags, "--tls=true")
		certFile := filepath.Join(testData.basePath, "testdata/server_cert.pem")
		keyFile := filepath.Join(testData.basePath, "testdata/server_key.pem")
		flags = append(flags, "--cert-file="+certFile)
		flags = append(flags, "--key-file="+keyFile)
		flags = append(flags, "--addr="+testData.addr)
	}
	err = g.RegisterFlags().Parse(flags)
	req.NoError(err)

	go func() {
		errRun := g.Run()
		if errRun != nil {
			startListener.GracefulStop()
			req.NoError(errRun)
		}
		deferFunc()
	}()
	req.NoError(startListener.WaitUntilStarted())
	return func() {
		closer.GracefulStop()
	}
}

type caseData struct {
	name           string
	queryGenerator func(baseTs time.Time) *streamv2.QueryRequest
	writeGenerator func() *streamv2.WriteRequest
	args           testData
	wantLen        int
}

func TestStreamService(t *testing.T) {
	req := require.New(t)
	_, currentFile, _, _ := runtime.Caller(0)
	basePath := filepath.Dir(currentFile)
	certFile := filepath.Join(basePath, "testdata/server_cert.pem")
	testCases := []caseData{
		{
			name:           "isTLS",
			queryGenerator: queryCriteria,
			writeGenerator: writeData,
			args: testData{
				TLS:                true,
				certFile:           certFile,
				serverHostOverride: "localhost",
				addr:               "localhost:17913",
				basePath:           basePath,
			},
			wantLen: 1,
		},
		{
			name:           "noTLS",
			queryGenerator: queryCriteria,
			writeGenerator: writeData,
			args: testData{
				TLS:  false,
				addr: "localhost:17912",
			},
			wantLen: 1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gracefulStop := setup(req, tc.args)
			defer gracefulStop()
			if tc.args.TLS {
				var opts []grpclib.DialOption
				creds, err := credentials.NewClientTLSFromFile(tc.args.certFile, tc.args.serverHostOverride)
				assert.NoError(t, err)
				opts = append(opts, grpclib.WithTransportCredentials(creds))
				dialService(t, tc, opts)
			} else {
				var opts []grpclib.DialOption
				opts = append(opts, grpclib.WithInsecure())
				dialService(t, tc, opts)
			}
		})
	}
}

func writeData() *streamv2.WriteRequest {
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	return v2.NewStreamWriteRequestBuilder().
		ID("1").
		Metadata("default", "sw").
		Timestamp(time.Now()).
		TagFamily(bb).
		TagFamily(
			"trace_id-xxfff.111",
			0,
			"webapp_id",
			"10.0.0.1_id",
			"/home_id",
			300,
			1622933202000000000,
		).
		Build()
}

func queryCriteria(baseTs time.Time) *streamv2.QueryRequest {
	return v2.NewQueryRequestBuilder().
		Limit(10).
		Offset(0).
		Metadata("default", "sw").
		TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
		Projection("searchable", "trace_id").
		Build()
}

func dialService(t *testing.T, tc caseData, opts []grpclib.DialOption) {
	conn, err := grpclib.Dial(tc.args.addr, opts...)
	assert.NoError(t, err)
	defer func(conn *grpclib.ClientConn) {
		_ = conn.Close()
	}(conn)
	streamWrite(t, tc, conn)
	requireTester := require.New(t)
	assert.NoError(t, test.Retry(10, 100*time.Millisecond, func() error {
		now := time.Now()
		resp := streamQuery(requireTester, conn, tc.queryGenerator(now))
		if len(resp.GetElements()) == tc.wantLen {
			return nil
		}
		return fmt.Errorf("expected elements number: %d got: %d", tc.wantLen, len(resp.GetElements()))
	}))
}

func streamWrite(t *testing.T, tc caseData, conn *grpclib.ClientConn) {
	client := streamv2.NewStreamServiceClient(conn)
	ctx := context.Background()
	writeClient, errorWrite := client.Write(ctx)
	if errorWrite != nil {
		t.Errorf("%v.write(_) = _, %v", client, errorWrite)
	}
	waitc := make(chan struct{})
	go func() {
		for {
			writeResponse, errRecv := writeClient.Recv()
			if errRecv == io.EOF {
				// read done.
				close(waitc)
				return
			}
			assert.NoError(t, errRecv)
			assert.NotNil(t, writeResponse)
		}
	}()
	if errSend := writeClient.Send(tc.writeGenerator()); errSend != nil {
		t.Errorf("Failed to send a note: %v", errSend)
	}
	if errorSend := writeClient.CloseSend(); errorSend != nil {
		t.Errorf("Failed to send a note: %v", errorSend)
	}
	<-waitc
}

func streamQuery(tester *require.Assertions, conn *grpclib.ClientConn, request *streamv2.QueryRequest) *streamv2.QueryResponse {
	client := streamv2.NewStreamServiceClient(conn)
	ctx := context.Background()
	queryResponse, errRev := client.Query(ctx, request)
	if errRev != nil {
		tester.Errorf(errRev, "Retrieve client failed: %v")
	}
	tester.NotNil(queryResponse)
	return queryResponse
}
