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

package dns_test

import (
	"context"
	"net"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/discovery/dns"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/schemaserver"
	"github.com/apache/skywalking-banyandb/banyand/observability"
)

// These specs cover the metadata PreRun phase: discovery has not been started,
// so ListNode re-fetches unreachable peers on every call. A peer that can never
// answer during PreRun (typically the node's own gRPC address published through
// the headless service) must not hide the peers that did answer.
var _ = Describe("ListNode before Start", func() {
	var (
		ctx          context.Context
		cancel       context.CancelFunc
		svc          *dns.Service
		mockResolver *mockDNSResolver
		mockServer   *mockNodeQueryServer
		metaAddr     string
		refusedAddr  string
		stopServer   func()
	)

	BeforeEach(func() {
		ctx, cancel = context.WithCancel(context.Background())
		mockResolver = newMockDNSResolver()

		var listener net.Listener
		var grpcServer *grpc.Server
		listener, grpcServer, mockServer = setupMockGRPCServer()
		metaAddr = listener.Addr().String()
		mockServer.node = createTestNode("meta-node", metaAddr, databasev1.Role_ROLE_META)
		stopServer = func() {
			grpcServer.Stop()
			_ = listener.Close()
		}

		refusedAddr = reserveRefusedAddr()
	})

	AfterEach(func() {
		if svc != nil {
			Expect(svc.Close()).To(Succeed())
			svc = nil
		}
		stopServer()
		cancel()
	})

	It("should return reachable nodes when another SRV address refuses connections", func() {
		mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(refusedAddr), addrToSRV(metaAddr)})

		var err error
		svc, err = dns.NewServiceWithResolver(createDefaultConfig(), mockResolver)
		Expect(err).NotTo(HaveOccurred())

		nodes, listErr := svc.ListNode(ctx, databasev1.Role_ROLE_META)
		Expect(listErr).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(1))
		Expect(nodes[0].GetMetadata().GetName()).To(Equal("meta-node"))
		Expect(svc.RetryManager.IsInRetry(refusedAddr)).To(BeTrue(), "unreachable address should be parked for retry")

		// Every pre-Start ListNode re-dials the parked address; the repeated
		// failure must keep being tolerated rather than poisoning the result.
		nodes, listErr = svc.ListNode(ctx, databasev1.Role_ROLE_META)
		Expect(listErr).NotTo(HaveOccurred())
		Expect(nodes).To(HaveLen(1))
		Expect(svc.RetryManager.IsInRetry(refusedAddr)).To(BeTrue())
	})

	It("should still report an error when no SRV address is reachable", func() {
		mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(refusedAddr)})

		var err error
		svc, err = dns.NewServiceWithResolver(createDefaultConfig(), mockResolver)
		Expect(err).NotTo(HaveOccurred())

		nodes, listErr := svc.ListNode(ctx, databasev1.Role_ROLE_META)
		Expect(listErr).To(HaveOccurred())
		Expect(listErr.Error()).To(ContainSubstring(refusedAddr))
		Expect(nodes).To(BeEmpty())
	})

	It("should bootstrap the property schema registry while the node's own address is unreachable", func() {
		schemaAddr, stopSchema := startSchemaServer()
		defer stopSchema()

		// The meta node advertises the schema server; the refused address plays
		// the role of the booting node itself, whose gRPC port only opens after
		// PreRun completes.
		mockServer.node.PropertySchemaGrpcAddress = schemaAddr
		mockResolver.setResponse("_grpc._tcp.test.local", []*net.SRV{addrToSRV(refusedAddr), addrToSRV(metaAddr)})

		var err error
		svc, err = dns.NewServiceWithResolver(createDefaultConfig(), mockResolver)
		Expect(err).NotTo(HaveOccurred())

		reg, regErr := property.NewSchemaRegistryClient(&property.ClientConfig{
			GRPCTimeout:  5 * time.Second,
			InitWaitTime: 10 * time.Second,
			NodeRegistry: svc,
		})
		Expect(regErr).NotTo(HaveOccurred())
		Expect(reg.Close()).To(Succeed())
	})
})

// reserveRefusedAddr returns a loopback address that nothing listens on, so
// dialing it fails immediately with connection refused.
func reserveRefusedAddr() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	addr := listener.Addr().String()
	Expect(listener.Close()).To(Succeed())
	return addr
}

func startSchemaServer() (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())
	port := listener.Addr().(*net.TCPAddr).Port
	Expect(listener.Close()).To(Succeed())

	srv := schemaserver.NewServer(observability.BypassRegistry)
	Expect(srv.FlagSet().Parse([]string{
		"--schema-server-root-path", GinkgoT().TempDir(),
		"--schema-server-grpc-host", "127.0.0.1",
		"--schema-server-grpc-port", strconv.Itoa(port),
	})).To(Succeed())
	Expect(srv.Validate()).To(Succeed())
	Expect(srv.PreRun(context.Background())).To(Succeed())
	srv.Serve()

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	Eventually(func() error {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr != nil {
			return dialErr
		}
		return conn.Close()
	}, 5*time.Second, 50*time.Millisecond).Should(Succeed())
	return addr, srv.GracefulStop
}
