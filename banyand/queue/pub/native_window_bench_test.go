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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type delayedNativeWrite struct {
	at   time.Time
	data []byte
}

// delayedNativeConn models propagation delay without serially sleeping on every
// frame. Writes are bounded to 128 queued chunks; one worker delivers them in order
// at their original scheduled times. Both ends apply 500us one-way delay.
type delayedNativeConn struct {
	net.Conn
	writes chan delayedNativeWrite
	done   chan struct{}
	once   sync.Once
}

func newDelayedNativeConn(conn net.Conn) *delayedNativeConn {
	c := &delayedNativeConn{Conn: conn, writes: make(chan delayedNativeWrite, 128), done: make(chan struct{})}
	go c.deliver()
	return c
}

func (c *delayedNativeConn) deliver() {
	for {
		select {
		case <-c.done:
			return
		case write := <-c.writes:
			timer := time.NewTimer(time.Until(write.at))
			select {
			case <-c.done:
				timer.Stop()
				return
			case <-timer.C:
			}
			if _, err := c.Conn.Write(write.data); err != nil {
				_ = c.Close()
				return
			}
		}
	}
}

func (c *delayedNativeConn) Write(data []byte) (int, error) {
	// Match grpc's 32KiB write buffer even if its writer coalesces more input.
	written := 0
	for len(data) > 0 {
		chunk := min(len(data), 32<<10)
		write := delayedNativeWrite{data: append([]byte(nil), data[:chunk]...), at: time.Now().Add(500 * time.Microsecond)}
		select {
		case <-c.done:
			return written, net.ErrClosed
		case c.writes <- write:
			written += chunk
			data = data[chunk:]
		}
	}
	return written, nil
}

func (c *delayedNativeConn) Close() error {
	var err error
	c.once.Do(func() { close(c.done); err = c.Conn.Close() })
	return err
}

type delayedNativeListener struct{ net.Listener }

func (l delayedNativeListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newDelayedNativeConn(conn), nil
}

func BenchmarkNativeConnectionWindowDelayedConcurrent(b *testing.B) {
	for _, window := range []int32{nativeQueryReceiveWindow, nativeQueryConnectionReceiveWindow} {
		b.Run(fmt.Sprintf("connection_%dKiB", window>>10), func(b *testing.B) {
			benchmarkNativeConnectionWindow(b, window)
		})
	}
}

func benchmarkNativeConnectionWindow(b *testing.B, window int32) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	server := grpc.NewServer()
	clusterv1.RegisterServiceServer(server, &nativeOversizeServer{bodyBytes: 64 << 10})
	served := make(chan error, 1)
	go func() { served <- server.Serve(delayedNativeListener{listener}) }()
	b.Cleanup(func() { server.Stop(); <-served })
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStaticStreamWindowSize(nativeQueryReceiveWindow), grpc.WithStaticConnWindowSize(window),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			underlying, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if dialErr != nil {
				return nil, dialErr
			}
			return newDelayedNativeConn(underlying), nil
		}))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	client := clusterv1.NewServiceClient(conn)
	// Warm the connection outside timing. Every iteration then issues 8 parallel
	// independent queries, each pre-reserving its own finite 8MiB receive guard.
	if err = receiveDelayedNativeBatch(b.Context(), client); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(8 * 64 << 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = receiveDelayedNativeBatch(b.Context(), client); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(1, "configured_RTT_ms")
	b.ReportMetric(8, "parallel_streams")
	b.ReportMetric(3<<20, "reservation_limit_B/query")
}

func receiveDelayedNativeBatch(ctx context.Context, client clusterv1.ServiceClient) error {
	var workers sync.WaitGroup
	errors := make(chan error, 8)
	for i := 0; i < 8; i++ {
		workers.Go(func() { errors <- receiveDelayedNativeResponse(ctx, client) })
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func receiveDelayedNativeResponse(ctx context.Context, client clusterv1.ServiceClient) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tracker := vectorized.NewMemoryTracker(8 << 20)
	_, scope := vectorized.WithNativeReceiveScope(ctx, tracker)
	defer scope.Close()
	capBytes, err := scope.ReserveFanout(1, nativeQueryReceiveWindow, maxReceiveMessageSize)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, nativeReceiveLimitKey{}, capBytes)
	stream, err := client.Send(ctx, nativeReceiveCallOptions(ctx)...)
	if err != nil {
		return err
	}
	defer func() { _ = stream.CloseSend() }()
	if err = stream.Send(&clusterv1.SendRequest{}); err != nil {
		return err
	}
	response, err := stream.Recv()
	if err != nil {
		return err
	}
	if len(response.Body) != 64<<10 || tracker.Used() <= 0 || tracker.Used() > 3<<20 {
		return fmt.Errorf("invalid bounded receive: body=%d reserved=%d", len(response.Body), tracker.Used())
	}
	scope.Close()
	if tracker.Used() != 0 {
		return fmt.Errorf("receive reservation leaked: %d", tracker.Used())
	}
	return nil
}
