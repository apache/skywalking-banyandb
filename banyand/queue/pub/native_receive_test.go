// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pub

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

type nativeOversizeServer struct {
	clusterv1.UnimplementedServiceServer
	bodyBytes int
}

func (s *nativeOversizeServer) Send(stream grpc.BidiStreamingServer[clusterv1.SendRequest, clusterv1.SendResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	size := s.bodyBytes
	if size == 0 {
		size = 2048
	}
	return stream.Send(&clusterv1.SendResponse{Body: make([]byte, size)})
}

func TestNativeReceiveCallCapRejectsOversizeRPCBeforeCodec(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	clusterv1.RegisterServiceServer(server, &nativeOversizeServer{})
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-served) })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, nativeReceiveLimitKey{}, 1024)
	stream, err := clusterv1.NewServiceClient(conn).Send(ctx, nativeReceiveCallOptions(ctx)...)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&clusterv1.SendRequest{}))
	retryCtx, retryCancel := context.WithCancel(t.Context())
	defer retryCancel()
	pending := &future{
		receiveGuarded: true,
		log:            logger.GetLogger("receive-cap-test"), clients: []clusterv1.Service_SendClient{stream},
		cancelFn: []func(){retryCancel, cancel}, topics: []bus.Topic{data.TopicInternalMeasureQuery}, nodes: []string{"test"},
		groups: []string{"test"}, roles: []string{"data"}, tiers: []string{"hot"}, starts: []time.Time{time.Now()},
	}
	_, err = pending.GetAll()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Empty(t, pending.clients)
	require.Empty(t, pending.cancelFn)
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, retryCtx.Err(), context.Canceled)
}

type closeCountingSendClient struct {
	clusterv1.Service_SendClient
	closed atomic.Int64
}

func (c *closeCountingSendClient) CloseSend() error { c.closed.Add(1); return nil }

func TestNativeFutureCloseCancelsUnreadAndFailedSendStreams(t *testing.T) {
	readCtx, readCancel := context.WithCancel(t.Context())
	failedCtx, failedCancel := context.WithCancel(t.Context())
	client := &closeCountingSendClient{}
	pending := &future{clients: []clusterv1.Service_SendClient{client}, cancelFn: []func(){readCancel, failedCancel}}
	require.NoError(t, pending.Close())
	require.ErrorIs(t, readCtx.Err(), context.Canceled)
	require.ErrorIs(t, failedCtx.Err(), context.Canceled)
	require.Equal(t, int64(1), client.closed.Load())
	require.NoError(t, pending.Close())
	require.Equal(t, int64(1), client.closed.Load())
}

func TestGuardedNativeReceiveErrorPreservesStatusesAndUnrelatedErrors(t *testing.T) {
	for _, message := range []string{
		"memory budget exceeded", "memory budget exceeded: used 12 + requested 8 > limit 16",
		"vec raw-frame emit: pipeline next: memory budget exceeded: storage envelope",
	} {
		require.Equal(t, codes.ResourceExhausted, status.Code(guardedNativeReceiveError(errors.New(message))))
	}
	unrelated := errors.New("unknown field memory budget exceeded allocation")
	require.Same(t, unrelated, guardedNativeReceiveError(unrelated))
	denied := status.Error(codes.PermissionDenied, "memory budget exceeded: quoted peer detail")
	require.Same(t, denied, guardedNativeReceiveError(denied))
	typed := fmt.Errorf("native execution: %w", vectorized.ErrMemoryBudgetExceeded)
	require.Equal(t, codes.ResourceExhausted, status.Code(guardedNativeReceiveError(typed)))
}

func TestNativeQueryConnectionCachesTLSAndFollowsNodeLifecycle(t *testing.T) {
	certPEM, keyPEM, err := pkgtls.GenerateSelfSignedCert("localhost", nil)
	require.NoError(t, err)
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	trustFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(trustFile, certPEM, 0o600))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})))
	clusterv1.RegisterServiceServer(server, &nativeOversizeServer{})
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-served) })
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	primary, err := grpc.NewClient(net.JoinHostPort("localhost", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, primary.Close()) })
	p := &pub{log: logger.GetLogger("native-query-tls-test"), tlsEnabled: true, caCertPath: trustFile}
	p.caCertReloader, err = pkgtls.NewClientCertReloader(trustFile, p.log)
	require.NoError(t, err)
	t.Cleanup(p.caCertReloader.Stop)
	t.Cleanup(func() { require.NoError(t, p.closeNativeQueryClients()) })
	node := &client{conn: primary}
	first, err := p.nativeQueryClient(node)
	require.NoError(t, err)
	repeated, err := p.nativeQueryClient(node)
	require.NoError(t, err)
	require.Same(t, first, repeated)
	cached := p.nativeQueries[node]
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := first.Send(ctx, grpc.MaxCallRecvMsgSize(4096))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&clusterv1.SendRequest{}))
	response, err := stream.Recv()
	require.NoError(t, err, "native query connection must use the configured TLS trust")
	require.Len(t, response.GetBody(), 2048)
	require.NoError(t, stream.CloseSend())
	p.OnInactive("node", node)
	require.Equal(t, connectivity.Shutdown, cached.conn.GetState())
	require.Empty(t, p.nativeQueries)
	_, err = p.nativeQueryClient(node)
	require.ErrorContains(t, err, "closed")
	// A new node generation obtains current reloader credentials, and failure
	// to load changed trust must not install a partially initialized connection.
	next := &client{conn: primary}
	require.NoError(t, os.WriteFile(trustFile, []byte("invalid trust"), 0o600))
	_, err = p.nativeQueryClient(next)
	require.ErrorContains(t, err, "failed to parse PEM")
	require.Empty(t, p.nativeQueries)
	require.NoError(t, os.WriteFile(trustFile, certPEM, 0o600))
	afterReload, err := p.nativeQueryClient(next)
	require.NoError(t, err)
	require.NotSame(t, first, afterReload)
	cached = p.nativeQueries[next]
	require.NoError(t, p.closeNativeQueryClients())
	require.Equal(t, connectivity.Shutdown, cached.conn.GetState())
	_, err = p.nativeQueryClient(&client{conn: primary})
	require.ErrorContains(t, err, "closed")
}

func TestNativeReceiveDefaultBudgetAllowsFrameLargerThan32KiB(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	clusterv1.RegisterServiceServer(server, &nativeOversizeServer{bodyBytes: 64 << 10})
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-served) })
	primary, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, primary.Close()) })
	publisher := &pub{log: logger.GetLogger("native-default-frame-test")}
	t.Cleanup(func() { require.NoError(t, publisher.closeNativeQueryClients()) })
	client, err := publisher.nativeQueryClient(&client{conn: primary})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tracker := vectorized.NewMemoryTracker(8 << 20)
	_, scope := vectorized.WithNativeReceiveScope(ctx, tracker)
	defer scope.Close()
	capBytes, err := scope.ReserveFanout(2, nativeQueryReceiveWindow, maxReceiveMessageSize)
	require.NoError(t, err)
	require.Greater(t, capBytes, 64<<10)
	ctx = context.WithValue(ctx, nativeReceiveLimitKey{}, capBytes)
	stream, err := client.Send(ctx, nativeReceiveCallOptions(ctx)...)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&clusterv1.SendRequest{}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, response.GetBody(), 64<<10)
	require.Positive(t, tracker.Used(), "guard remains charged until the receive owner closes")
	require.NoError(t, stream.CloseSend())
	scope.Close()
	require.Zero(t, tracker.Used())
}
