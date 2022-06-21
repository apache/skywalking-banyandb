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
	"path/filepath"
	"runtime"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var _ = Describe("Measure", func() {
	var path string
	var gracefulStop, deferFunc func()
	var conn *grpclib.ClientConn
	var listenClientURL, listenPeerURL string
	BeforeEach(func() {
		var err error
		path, deferFunc, err = test.NewSpace()
		Expect(err).NotTo(HaveOccurred())
		listenClientURL, listenPeerURL, err = test.NewEtcdListenUrls()
		Expect(err).NotTo(HaveOccurred())
	})
	It("is a plain server", func() {
		By("Verifying an empty server")
		flags := []string{
			"--stream-root-path=" + path, "--measure-root-path=" + path, "--metadata-root-path=" + path,
			"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
		}
		gracefulStop = setup(true, flags)
		credentials := insecure.NewCredentials()
		var err error
		conn, err = grpclib.Dial("localhost:17912", grpclib.WithTransportCredentials(credentials))
		Expect(err).NotTo(HaveOccurred())
		measureWrite(conn)
		Eventually(func() (int, error) {
			return measureQuery(conn)
		}).Should(Equal(1))
		_ = conn.Close()
		gracefulStop()
		By("Verifying an existing server")
		gracefulStop = setup(false, flags)
		conn, err = grpclib.Dial("localhost:17912", grpclib.WithTransportCredentials(credentials))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int {
			num, err := measureQuery(conn)
			if err != nil {
				GinkgoWriter.Printf("measure query err: %v \n", err)
				return 0
			}
			return num
		}, defaultEventallyTimeout).Should(Equal(1))
	})
	It("is a TLS server", func() {
		flags := []string{
			"--tls=true", "--stream-root-path=" + path, "--measure-root-path=" + path,
			"--metadata-root-path=" + path, "--stream-root-path=" + path, "--measure-root-path=" + path,
			"--metadata-root-path=" + path, "--etcd-listen-client-url=" + listenClientURL,
			"--etcd-listen-peer-url=" + listenPeerURL,
		}
		_, currentFile, _, _ := runtime.Caller(0)
		basePath := filepath.Dir(currentFile)
		certFile := filepath.Join(basePath, "testdata/server_cert.pem")
		keyFile := filepath.Join(basePath, "testdata/server_key.pem")
		flags = append(flags, "--cert-file="+certFile)
		flags = append(flags, "--key-file="+keyFile)
		addr := "localhost:17913"
		flags = append(flags, "--addr="+addr)
		gracefulStop = setup(true, flags)
		creds, err := credentials.NewClientTLSFromFile(certFile, "localhost")
		Expect(err).NotTo(HaveOccurred())
		conn, err = grpclib.Dial(addr, grpclib.WithTransportCredentials(creds))
		Expect(err).NotTo(HaveOccurred())
		measureWrite(conn)
		Eventually(func() (int, error) {
			return measureQuery(conn)
		}, defaultEventallyTimeout).Should(Equal(1))
	})
	AfterEach(func() {
		_ = conn.Close()
		gracefulStop()
		deferFunc()
	})
})

func writeMeasureData() *measurev1.WriteRequest {
	return pbv1.NewMeasureWriteRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		Timestamp(timestamp.NowMilli()).
		TagFamily(
			pbv1.ID("1"),
			"entity_1",
		).Fields(100, 1).
		Build()
}

type preloadMeasureService struct {
	metaSvc metadata.Service
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}

func (p *preloadMeasureService) PreRun() error {
	return testmeasure.PreloadSchema(p.metaSvc.SchemaRegistry())
}

func queryMeasureCriteria(baseTs time.Time) *measurev1.QueryRequest {
	return pbv1.NewMeasureQueryRequestBuilder().
		Metadata("sw_metric", "service_cpm_minute").
		TimeRange(baseTs.Add(-10*time.Minute), baseTs.Add(10*time.Minute)).
		TagProjection("default", "id").
		Build()
}

func measureWrite(conn *grpclib.ClientConn) {
	c := measurev1.NewMeasureServiceClient(conn)
	ctx := context.Background()
	var writeClient measurev1.MeasureService_WriteClient
	Eventually(func(g Gomega) {
		var err error
		writeClient, err = c.Write(ctx)
		g.Expect(err).NotTo(HaveOccurred())
	}).Should(Succeed())
	Eventually(func() error {
		return writeClient.Send(writeMeasureData())
	}).ShouldNot(HaveOccurred())
	Expect(writeClient.CloseSend()).Should(Succeed())
	Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}).Should(Equal(io.EOF))
}

func measureQuery(conn *grpclib.ClientConn) (int, error) {
	c := measurev1.NewMeasureServiceClient(conn)
	ctx := context.Background()
	resp, err := c.Query(ctx, queryMeasureCriteria(timestamp.NowMilli()))

	return len(resp.GetDataPoints()), err
}
