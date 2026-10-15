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

package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/banyand/queue/sub"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

type scanListener struct {
	health *common.Error
	fn     func(ctx context.Context, message bus.Message, sink bus.Sink) error
}

func (l *scanListener) CheckHealth() *common.Error { return l.health }

func (l *scanListener) RevStream(ctx context.Context, message bus.Message, sink bus.Sink) error {
	return l.fn(ctx, message, sink)
}

type scanSetup struct {
	client   queue.Client
	stopNode func()
	node     string
}

// startScanNode runs a real data-node queue server whose trace query topic is served by fn,
// and a real publisher connected to it.
func startScanNode(t *testing.T, name, heartbeat string, listener bus.StreamListener) *scanSetup {
	t.Helper()
	ports, err := test.AllocateFreePorts(2)
	require.NoError(t, err)
	grpcPort, httpPort := uint32(ports[0]), uint32(ports[1])

	server := sub.NewServerWithPorts(observability.BypassRegistry, name, grpcPort, httpPort)
	require.NoError(t, server.SubscribeStream(data.TopicTraceQuery, listener))
	group := run.NewGroup(name)
	closer, deferFn := run.NewTester(name + "-closer")
	group.Register(closer, server)
	cmd := &cobra.Command{
		Use:                name,
		FParseErrWhitelist: cobra.FParseErrWhitelist{UnknownFlags: true},
		Run: func(_ *cobra.Command, _ []string) {
			if runErr := group.Run(context.Background()); runErr != nil {
				t.Logf("server group failed: %v", runErr)
			}
		},
	}
	cmd.Flags().AddFlagSet(group.RegisterFlags().FlagSet)
	cmd.SetArgs([]string{fmt.Sprintf("--%s-scan-heartbeat-interval=%s", name, heartbeat)})
	go func() { _ = cmd.Execute() }()
	var stopOnce atomic.Bool
	stop := func() {
		if stopOnce.CompareAndSwap(false, true) {
			deferFn()
		}
	}
	t.Cleanup(stop)

	addr := fmt.Sprintf("localhost:%d", grpcPort)
	require.Eventually(t, func() bool {
		return helpers.HealthCheck(addr, 10*time.Second, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))() == nil
	}, flags.EventuallyTimeout, 100*time.Millisecond)

	client := pub.NewWithoutMetadata(nil)
	t.Cleanup(client.GracefulStop)
	nodeName := name + "-node"
	client.OnAddOrUpdate(schema.Metadata{
		TypeMeta: schema.TypeMeta{Name: nodeName, Kind: schema.KindNode},
		Spec: &databasev1.Node{
			Metadata:    &commonv1.Metadata{Name: nodeName},
			Roles:       []databasev1.Role{databasev1.Role_ROLE_DATA},
			GrpcAddress: addr,
		},
	})
	return &scanSetup{client: client, node: nodeName, stopNode: stop}
}

func (s *scanSetup) scatter(ctx context.Context, t *testing.T, opts queue.ScatterOptions) queue.NodeStream {
	t.Helper()
	streams, err := s.client.Scatter(ctx, data.TopicTraceQuery, bus.NewMessage(1, &tracev1.QueryRequest{Groups: []string{"g"}}), opts)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	t.Cleanup(streams[0].Close)
	return streams[0]
}

func traceChunk(id string) bus.Message {
	return bus.NewMessage(1, &tracev1.InternalQueryResponse{InternalTraces: []*tracev1.InternalTrace{{TraceId: id}}})
}

func chunkID(t *testing.T, m bus.Message) string {
	t.Helper()
	resp, ok := m.Data().(*tracev1.InternalQueryResponse)
	require.True(t, ok, "unexpected chunk type %T", m.Data())
	require.Len(t, resp.InternalTraces, 1)
	return resp.InternalTraces[0].TraceId
}

func TestScanMultiChunkInOrderThenEOF(t *testing.T) {
	var gotGroups atomic.Value
	setup := startScanNode(t, "scan-order", "100ms", &scanListener{fn: func(ctx context.Context, m bus.Message, sink bus.Sink) error {
		gotGroups.Store(m.Data().(*tracev1.QueryRequest).Groups)
		for i := 0; i < 20; i++ {
			if err := sink.Send(ctx, traceChunk(fmt.Sprintf("t%02d", i))); err != nil {
				return err
			}
		}
		return nil
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{Prefetch: 2})
	for i := 0; i < 20; i++ {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("t%02d", i), chunkID(t, m))
	}
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"g"}, gotGroups.Load())
}

func TestScanHeartbeatsKeepASlowScanAlive(t *testing.T) {
	// The scan is silent for 4x the idle timeout; only the heartbeats keep the stream open.
	setup := startScanNode(t, "scan-heartbeat", "50ms", &scanListener{fn: func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		select {
		case <-time.After(800 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		return sink.Send(ctx, traceChunk("slow"))
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{IdleTimeout: 200 * time.Millisecond})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "slow", chunkID(t, m))
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
}

func TestScanIdleTimeoutFiresWithoutHeartbeats(t *testing.T) {
	listenerCtxDone := make(chan struct{})
	// A heartbeat interval far beyond the idle timeout makes the node effectively silent.
	setup := startScanNode(t, "scan-silent", "1h", &scanListener{fn: func(ctx context.Context, _ bus.Message, _ bus.Sink) error {
		<-ctx.Done()
		close(listenerCtxDone)
		return ctx.Err()
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{IdleTimeout: 200 * time.Millisecond})
	_, err := ns.Next(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, queue.ErrScanIdleTimeout)
	<-listenerCtxDone
}

func TestScanCallerCancellationReachesTheListener(t *testing.T) {
	started := make(chan struct{})
	listenerErr := make(chan error, 1)
	setup := startScanNode(t, "scan-cancel", "100ms", &scanListener{fn: func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, traceChunk("first")); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		listenerErr <- ctx.Err()
		return ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	ns := setup.scatter(ctx, t, queue.ScatterOptions{})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "first", chunkID(t, m))
	<-started
	cancel()
	assert.ErrorIs(t, <-listenerErr, context.Canceled)
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
}

func TestScanCloseReachesTheListener(t *testing.T) {
	started := make(chan struct{})
	listenerCtxDone := make(chan struct{})
	setup := startScanNode(t, "scan-close", "100ms", &scanListener{fn: func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, traceChunk("first")); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		close(listenerCtxDone)
		return ctx.Err()
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{})
	<-started
	ns.Close()
	<-listenerCtxDone
}

func TestScanListenerErrorIsDeliveredAfterTheChunks(t *testing.T) {
	setup := startScanNode(t, "scan-error", "100ms", &scanListener{fn: func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, traceChunk("ok")); err != nil {
			return err
		}
		return errors.New("disk on fire")
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ok", chunkID(t, m))
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.Contains(t, err.Error(), "disk on fire")
}

func TestScanTransportErrorWhenTheNodeDiesIsNotEOF(t *testing.T) {
	started := make(chan struct{})
	setup := startScanNode(t, "scan-die", "100ms", &scanListener{fn: func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, traceChunk("first")); err != nil {
			return err
		}
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{})
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "first", chunkID(t, m))
	<-started
	setup.stopNode()
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
}

func TestScanUnhealthyListenerRejectsTheScan(t *testing.T) {
	var ran atomic.Bool
	setup := startScanNode(t, "scan-unhealthy", "100ms", &scanListener{
		health: common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "not ready"),
		fn: func(context.Context, bus.Message, bus.Sink) error {
			ran.Store(true)
			return nil
		},
	})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{})
	_, err := ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.Contains(t, err.Error(), "not ready")
	assert.False(t, ran.Load())
}

func TestScanForwardsChunkLimitsToTheListener(t *testing.T) {
	var got atomic.Value
	setup := startScanNode(t, "scanlimits", "1s", &scanListener{fn: func(ctx context.Context, _ bus.Message, _ bus.Sink) error {
		got.Store(bus.ChunkLimitsFrom(ctx))
		return nil
	}})
	ns := setup.scatter(context.Background(), t, queue.ScatterOptions{MaxChunkRows: 3, MaxChunkBytes: 1024})
	_, err := ns.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, bus.ChunkLimits{MaxRows: 3, MaxBytes: 1024}, got.Load())
}
