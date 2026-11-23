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

package pub

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

// selfSignedServer is a TLS server for localhost with a certificate made for
// the test, and the file a client trusts it by.
type selfSignedServer struct {
	cert      tls.Certificate
	trustFile string
}

func newSelfSignedServer(t *testing.T) selfSignedServer {
	t.Helper()
	certPEM, keyPEM, err := pkgtls.GenerateSelfSignedCert("localhost", nil)
	require.NoError(t, err)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	trustFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(trustFile, certPEM, 0o600))
	return selfSignedServer{cert: cert, trustFile: trustFile}
}

// serve answers health checks over TLS on lis until the test ends.
func (s selfSignedServer) serve(t *testing.T, lis net.Listener) {
	t.Helper()
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{s.cert}, MinVersion: tls.VersionTLS12})))
	hs := health.NewServer()
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
}

func localhostListener(t *testing.T) (net.Listener, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	return lis, net.JoinHostPort("localhost", port)
}

// TestClientWithoutMetadataDialsWithTheTLSItWasGiven covers WithClientTLS: a
// client made without metadata registers no flag set, so the option is the
// only way it reaches a TLS server. Without it, the same client does not.
func TestClientWithoutMetadataDialsWithTheTLSItWasGiven(t *testing.T) {
	server := newSelfSignedServer(t)
	lis, addr := localhostListener(t)
	server.serve(t, lis)

	plain := NewWithoutMetadata(nil)
	t.Cleanup(plain.GracefulStop)
	plain.OnAddOrUpdate(DataNode("node-plain", addr))
	require.Empty(t, plain.HealthyNodes(), "a client without TLS reached a TLS server")

	client := NewWithoutMetadata(nil, WithClientTLS(server.trustFile, false))
	t.Cleanup(client.GracefulStop)
	client.OnAddOrUpdate(DataNode("node-tls", addr))
	require.Eventually(t, func() bool { return len(client.HealthyNodes()) == 1 },
		10*time.Second, 20*time.Millisecond, "the client given TLS did not reach the TLS server")
}

// TestKeeperConnectsANodeThatComesUpLater covers KeepNode: the first dial
// fails because the node is not up yet, and a client made without metadata
// would never dial again; the keeper does.
func TestKeeperConnectsANodeThatComesUpLater(t *testing.T) {
	server := newSelfSignedServer(t)
	lis, addr := localhostListener(t)
	// The port stays reserved but closed until the node starts.
	require.NoError(t, lis.Close())

	client := NewWithoutMetadata(nil, WithClientTLS(server.trustFile, false))
	t.Cleanup(client.GracefulStop)
	stop := KeepNode(client, DataNode("node-late", addr), 50*time.Millisecond, logger.GetLogger("keeper-test"))
	t.Cleanup(stop)
	require.Empty(t, client.HealthyNodes(), "connected to a node that is not up")

	lis, err := net.Listen("tcp", lis.Addr().String())
	if err != nil {
		t.Skipf("the reserved port was taken meanwhile: %v", err)
	}
	server.serve(t, lis)
	require.Eventually(t, func() bool { return len(client.HealthyNodes()) == 1 },
		10*time.Second, 20*time.Millisecond, "the keeper did not connect the node once it was up")

	// Stop returns once the keeper has exited, and a second call is harmless.
	stop()
	stop()
}
