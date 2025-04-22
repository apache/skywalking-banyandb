// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build unit
// +build unit

package pub

import (
	"context"
	"crypto/tls"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	health "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"net"
	"path/filepath"
	"testing"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func TestPubTLS(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "queue‑pub TLS dial‑out Suite")
}

type mockService struct {
	clusterv1.UnimplementedServiceServer
}

func (m *mockService) Send(stream clusterv1.Service_SendServer) error {
	_, err := stream.Recv()
	if err != nil {
		return err
	}
	anyBody, err := anypb.New(&streamv1.QueryResponse{})
	if err != nil {
		return err
	}
	if err := stream.Send(&clusterv1.SendResponse{Body: anyBody}); err != nil {
		return err
	}
	return io.EOF
}

func tlsServer(addr string) func() {
	crtDir := filepath.Join("testdata", "certs")
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(crtDir, "server.crt"),
		filepath.Join(crtDir, "server.key"),
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	creds := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
	lis, err := net.Listen("tcp", addr)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	srv := grpc.NewServer(grpc.Creds(creds))
	clusterv1.RegisterServiceServer(srv, &mockService{})

	hs := health.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(srv, hs)

	go func() { _ = srv.Serve(lis) }()
	return func() { srv.Stop() }
}

func newTLSPub() *pub {
	p := NewWithoutMetadata().(*pub)
	p.tlsEnabled = true
	p.caCertPath = filepath.Join("testdata", "certs", "ca.crt")
	gomega.Expect(p.PreRun(context.Background())).ShouldNot(gomega.HaveOccurred())
	return p
}

var _ = ginkgo.Describe("Broadcast over one‑way TLS", func() {
	var before []gleak.Goroutine

	ginkgo.BeforeEach(func() {
		before = gleak.Goroutines()
	})
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).
			ShouldNot(gleak.HaveLeaked(before))
	})

	ginkgo.It("establishes TLS and broadcasts a QueryRequest", func() {
		addr := getAddress()
		stop := tlsServer(addr)
		defer stop()

		p := newTLSPub()
		defer p.GracefulStop()

		node := getDataNode("node-tls", addr)
		p.OnAddOrUpdate(node)

		gomega.Eventually(func() int {
			p.mu.RLock()
			defer p.mu.RUnlock()
			return len(p.active)
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))

		futures, err := p.Broadcast(
			flags.EventuallyTimeout,
			data.TopicStreamQuery,
			bus.NewMessage(bus.MessageID(1), &streamv1.QueryRequest{}),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(futures).Should(gomega.HaveLen(1))

		msgs, err := futures[0].GetAll()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		gomega.Expect(msgs).Should(gomega.HaveLen(1))

		_, ok := msgs[0].Data().(*streamv1.QueryResponse)
		gomega.Expect(ok).To(gomega.BeTrue())
	})
})
