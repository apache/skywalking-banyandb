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
	"encoding/base64"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

var _ = Describe("Stream", func() {
	var rootPath, metadataPath string
	var gracefulStop, deferRootFunc, deferMetadataFunc func()
	var conn *grpclib.ClientConn
	BeforeEach(func() {
		var err error
		rootPath, deferRootFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		metadataPath, deferMetadataFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
	})
	It("is a plain server", func() {
		By("Verifying an empty server")
		flags := []string{"--root-path=" + rootPath, "--metadata-root-path=" + metadataPath}
		gracefulStop = setup(flags)
		var err error
		conn, err = grpclib.Dial("localhost:17912", grpclib.WithInsecure())
		Expect(err).NotTo(HaveOccurred())
		streamWrite(conn)
		Eventually(func() (int, error) {
			return streamQuery(conn)
		}).Should(Equal(1))
		_ = conn.Close()
		gracefulStop()
		By("Verifying an existing server")
		gracefulStop = setup(flags)
		conn, err = grpclib.Dial("localhost:17912", grpclib.WithInsecure())
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int {
			num, err := streamQuery(conn)
			if err != nil {
				GinkgoWriter.Printf("stream query err: %v \n", err)
				return 0
			}
			return num
		}).Should(Equal(1))
	})
	It("is a TLS server", func() {
		flags := []string{"--tls=true", "--root-path=" + rootPath, "--metadata-root-path=" + metadataPath}
		_, currentFile, _, _ := runtime.Caller(0)
		basePath := filepath.Dir(currentFile)
		certFile := filepath.Join(basePath, "testdata/server_cert.pem")
		keyFile := filepath.Join(basePath, "testdata/server_key.pem")
		flags = append(flags, "--cert-file="+certFile)
		flags = append(flags, "--key-file="+keyFile)
		addr := "localhost:17913"
		flags = append(flags, "--addr="+addr)
		gracefulStop = setup(flags)
		creds, err := credentials.NewClientTLSFromFile(certFile, "localhost")
		Expect(err).NotTo(HaveOccurred())
		conn, err = grpclib.Dial(addr, grpclib.WithTransportCredentials(creds))
		Expect(err).NotTo(HaveOccurred())
		streamWrite(conn)
		Eventually(func() (int, error) {
			return streamQuery(conn)
		}).Should(Equal(1))
	})
	AfterEach(func() {
		_ = conn.Close()
		gracefulStop()
		deferMetadataFunc()
		deferRootFunc()
	})
})

func setup(flags []string) func() {

	g := run.Group{Name: "standalone"}
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	Expect(err).NotTo(HaveOccurred())
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).NotTo(HaveOccurred())
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())
	// Init `Stream` module
	streamSvc, err := stream.NewService(context.TODO(), metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	// Init `Query` module
	q, err := query.NewExecutor(context.TODO(), streamSvc, metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())

	tcp := grpc.NewServer(context.TODO(), pipeline, repo, metaSvc)

	closer := run.NewTester("closer")
	startListener := run.NewTester("started-listener")

	preloadStreamSvc := &preloadStreamService{metaSvc: metaSvc}
	g.Register(
		closer,
		repo,
		pipeline,
		metaSvc,
		preloadStreamSvc,
		streamSvc,
		q,
		tcp,
		startListener,
	)

	err = g.RegisterFlags().Parse(flags)
	Expect(err).NotTo(HaveOccurred())

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		// we have to wait for this goroutine to safely shutdown
		defer wg.Done()
		errRun := g.Run()
		if errRun != nil {
			startListener.GracefulStop()
			Expect(errRun).NotTo(HaveOccurred())
		}
	}()
	Expect(startListener.WaitUntilStarted()).Should(Succeed())
	return func() {
		closer.GracefulStop()
		wg.Wait()
	}
}

type preloadStreamService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-measure"
}

func (p *preloadStreamService) PreRun() error {
	return teststream.PreloadSchema(p.metaSvc.SchemaRegistry())
}

func writeData() *streamv1.WriteRequest {
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	return pbv1.NewStreamWriteRequestBuilder().
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

func queryCriteria(baseTs time.Time) *streamv1.QueryRequest {
	return pbv1.NewQueryRequestBuilder().
		Limit(10).
		Offset(0).
		Metadata("default", "sw").
		TimeRange(baseTs.Add(-1*time.Minute), baseTs.Add(1*time.Minute)).
		Projection("searchable", "trace_id").
		Build()
}

func streamWrite(conn *grpclib.ClientConn) {
	client := streamv1.NewStreamServiceClient(conn)
	ctx := context.Background()
	var writeClient streamv1.StreamService_WriteClient
	Eventually(func(g Gomega) {
		var err error
		writeClient, err = client.Write(ctx)
		g.Expect(err).NotTo(HaveOccurred())
	}).Should(Succeed())
	Eventually(func() error {
		return writeClient.Send(writeData())
	}).ShouldNot(HaveOccurred())
	Expect(writeClient.CloseSend()).Should(Succeed())
	Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}).Should(Equal(io.EOF))
}

func streamQuery(conn *grpclib.ClientConn) (int, error) {
	client := streamv1.NewStreamServiceClient(conn)
	ctx := context.Background()
	resp, err := client.Query(ctx, queryCriteria(time.Now()))

	return len(resp.GetElements()), err
}
