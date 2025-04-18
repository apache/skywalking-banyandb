//go:build unit
// +build unit

// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// ASF licenses this file to you under the Apache License, Version 2.0.

package pub

import (
	"context"
	"crypto/tls"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"io"
	"net"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

/* -------------------------------------------------------------------------- */
/*                               test helpers                                 */
/* -------------------------------------------------------------------------- */

// mockService implements clusterv1.ServiceServer and just drains the stream.
type mockService struct {
	clusterv1.UnimplementedServiceServer
}

func (m *mockService) Send(stream clusterv1.Service_SendServer) error {
	for {
		if _, err := stream.Recv(); err != nil {
			// client will see io.EOF when we close normally
			return err
		}
	}
}

// tlsServer spins a gRPC server that uses the repo’s test certs and
// returns a close func().
func tlsServer(addr string) func() {
	crtDir := filepath.Join("..", "..", "..", "test", "testdata", "certs") // repo‑relative
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(crtDir, "server.crt"),
		filepath.Join(crtDir, "server.key"),
	)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
	})

	lis, err := net.Listen("tcp", addr)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

	s := grpc.NewServer(grpc.Creds(creds))
	clusterv1.RegisterServiceServer(s, &mockService{})

	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s, hs)

	go func() { _ = s.Serve(lis) }()

	return func() { s.Stop() }
}

// newTLSPub returns a *pub with internal‑tls enabled and the CA bundle set.
func newTLSPub() *pub {
	p := NewWithoutMetadata().(*pub)
	p.tlsEnabled = true
	p.caCertPath = filepath.Join("..", "..", "..", "test", "testdata", "certs", "ca.crt")

	// we don’t use metadata in this test, so PreRun only initialises logging
	gomega.Expect(p.PreRun(context.Background())).ShouldNot(gomega.HaveOccurred())
	return p
}

/* -------------------------------------------------------------------------- */
/*                                    Test                                    */
/* -------------------------------------------------------------------------- */

var _ = ginkgo.Describe("queue‑pub TLS dial‑out", func() {
	var goods []gleak.Goroutine

	ginkgo.BeforeEach(func() { goods = gleak.Goroutines() })
	ginkgo.AfterEach(func() {
		gomega.Eventually(gleak.Goroutines, flags.EventuallyTimeout).
			ShouldNot(gleak.HaveLeaked(goods))
	})

	ginkgo.It("establishes a TLS connection and publishes", func() {
		addr := getAddress()       // helper from pub_test.go
		closeFn := tlsServer(addr) // mock Data‑Node (sub) with TLS
		defer closeFn()

		p := newTLSPub()
		defer p.GracefulStop()

		node := getDataNode("node‑tls", addr)
		p.OnAddOrUpdate(node)

		// wait until the node is moved to the active map by the health‑check
		gomega.Eventually(func() int {
			p.mu.RLock()
			defer p.mu.RUnlock()
			return len(p.active)
		}, flags.EventuallyTimeout).Should(gomega.Equal(1))

		/* ----------  exercise Publish path (hits getClientTransportCredentials) ---------- */

		ctx := context.Background()
		fut, err := p.Publish(ctx, data.TopicStreamWrite,
			bus.NewMessageWithNode(bus.MessageID(1), node.Metadata.Name,
				&streamv1.InternalWriteRequest{}),
		)
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

		// no response expected from mock, so Get() will return io.EOF – that’s OK.
		_, err = fut.Get()
		gomega.Expect(err).Should(gomega.MatchError(io.EOF))
	})
})
