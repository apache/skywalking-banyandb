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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
)

func TestRecordWriteStreamErr(t *testing.T) {
	tests := []struct {
		err       error
		finishErr error
		name      string
		counted   bool
	}{
		{name: "nil", err: nil},
		{name: "context canceled", err: context.Canceled},
		{name: "context deadline exceeded", err: context.DeadlineExceeded},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "client went away")},
		{name: "grpc deadline exceeded", err: status.Error(codes.DeadlineExceeded, "too slow")},
		{name: "wrapped context canceled", err: fmt.Errorf("x: %w", context.Canceled)},
		{name: "plain error", err: errors.New("metadata is required for the first request of gRPC stream"), counted: true},
		{name: "deadline exceeded with a session failure", err: context.DeadlineExceeded, finishErr: status.Error(codes.ResourceExhausted, "busy"), counted: true},
		{name: "deadline exceeded with a canceled session", err: context.DeadlineExceeded, finishErr: context.Canceled},
		{name: "plain error with a session failure", err: errors.New("x"), finishErr: status.Error(codes.ResourceExhausted, "busy"), counted: true},
		{name: "grpc unavailable", err: status.Error(codes.Unavailable, "transport closed"), counted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil))
			recordWriteStreamErr(m, "measure", tt.err, tt.finishErr)
			want := float64(0)
			if tt.counted {
				want = 1
			}
			require.Equal(t, want, counterSeries(t, reg, "total_stream_err")["measure/write"])
		})
	}
}

// writeErrClient is a write stream client of one service, reduced to what the stream_err scenarios need.
type writeErrClient struct {
	// send writes a row; the first row of a stream carries the metadata and the spec, the others carry neither.
	send      func(id uint64, first bool) error
	closeSend func() error
	recv      func() error
}

// recvUntilEnd reads replies until the stream ends.
func (c writeErrClient) recvUntilEnd() {
	err := c.recv()
	for err == nil {
		err = c.recv()
	}
}

// writeErrService starts the real write handler of one service against fake data nodes, with its metrics replaced by m,
// and returns the fake data nodes and a function that opens a client stream.
type writeErrService struct {
	start func(t *testing.T, cfg writeSessionConfig, m *metrics) (*fakePublishers, func(ctx context.Context) writeErrClient)
	name  string
}

func writeErrServices() []writeErrService {
	return []writeErrService{
		{name: "measure", start: func(t *testing.T, cfg writeSessionConfig, m *metrics) (*fakePublishers, func(context.Context) writeErrClient) {
			h := newMeasureWriteHarness(t, cfg)
			h.service.metrics = m
			return h.fake, func(ctx context.Context) writeErrClient {
				stream, err := h.client.Write(ctx)
				require.NoError(t, err)
				return writeErrClient{
					send: func(id uint64, first bool) error {
						var spec *measurev1.DataPointSpec
						if first {
							spec = measureSpec("svc")
						}
						return stream.Send(measureWriteRow(id, first, spec))
					},
					closeSend: stream.CloseSend,
					recv:      func() error { _, recvErr := stream.Recv(); return recvErr },
				}
			}
		}},
		{name: "stream", start: func(t *testing.T, cfg writeSessionConfig, m *metrics) (*fakePublishers, func(context.Context) writeErrClient) {
			h := newStreamWriteHarness(t, cfg)
			h.service.metrics = m
			return h.fake, func(ctx context.Context) writeErrClient {
				stream, err := h.client.Write(ctx)
				require.NoError(t, err)
				return writeErrClient{
					send: func(id uint64, first bool) error {
						var spec []*streamv1.TagFamilySpec
						if first {
							spec = streamSpec("svc")
						}
						return stream.Send(streamRow(id, first, spec))
					},
					closeSend: stream.CloseSend,
					recv:      func() error { _, recvErr := stream.Recv(); return recvErr },
				}
			}
		}},
		{name: "trace", start: func(t *testing.T, cfg writeSessionConfig, m *metrics) (*fakePublishers, func(context.Context) writeErrClient) {
			h := newTraceWriteHarness(t, cfg)
			h.service.metrics = m
			return h.fake, func(ctx context.Context) writeErrClient {
				stream, err := h.client.Write(ctx)
				require.NoError(t, err)
				return writeErrClient{
					send: func(id uint64, first bool) error {
						var spec *tracev1.TagSpec
						if first {
							spec = &tracev1.TagSpec{TagNames: []string{"trace_id", "ts"}}
						}
						return stream.Send(traceRow(id, first, spec))
					},
					closeSend: stream.CloseSend,
					recv:      func() error { _, recvErr := stream.Recv(); return recvErr },
				}
			}
		}},
	}
}

// TestNewMetricsCreatesEventCountersAtZero asserts that the panic and write-stream error counters exist at zero before
// any event, so that increase() sees their first event.
func TestNewMetricsCreatesEventCountersAtZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil))

	require.Equal(t, map[string]float64{"measure/write": 0, "stream/write": 0, "trace/write": 0}, counterSeries(t, reg, "total_stream_err"))
	require.Equal(t, map[string]float64{"/": 0}, counterSeries(t, reg, "total_panic"))
}

// counterSeries returns the value of every series of a total_stream_* counter, keyed by "service/method".
func counterSeries(t *testing.T, reg *prometheus.Registry, name string) map[string]float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	series := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != "banyandb_liaison_grpc_"+name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string)
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			series[labels["service"]+"/"+labels["method"]] += metric.GetCounter().GetValue()
		}
	}
	return series
}

// TestWriteStreamErrByService drives the real Write handler of every service and checks that exactly the streams
// that ended with an error are counted, under the right service and method labels.
func TestWriteStreamErrByService(t *testing.T) {
	scenarios := []struct {
		run     func(t *testing.T, fake *fakePublishers, open func(context.Context) writeErrClient)
		name    string
		cfg     writeSessionConfig
		counted bool
	}{
		{
			name: "missing metadata", counted: true,
			run: func(t *testing.T, _ *fakePublishers, open func(context.Context) writeErrClient) {
				client := open(context.Background())
				require.NoError(t, client.send(1, false))
				require.Error(t, client.recv())
			},
		},
		{
			// The receive loop ends with io.EOF, so only the error of Finish reports the failure.
			name: "half-close with a failing Finish", counted: true,
			cfg: writeSessionConfig{
				sealInterval: time.Hour, pressureTimeout: 100 * time.Millisecond,
				retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond,
			},
			run: func(t *testing.T, fake *fakePublishers, open func(context.Context) writeErrClient) {
				fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
					return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
				}
				client := open(context.Background())
				require.NoError(t, client.send(1, true))
				require.NoError(t, client.closeSend())
				var err error
				for err == nil {
					err = client.recv()
				}
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			},
		},
		{
			// pressureTimeout is an hour, so only the client deadline can fail the session, while the receive loop ends
			// with DeadlineExceeded and the data node is still busy.
			name: "client deadline while the data node stays busy", counted: true,
			cfg: writeSessionConfig{
				sealInterval: 10 * time.Millisecond, pressureTimeout: time.Hour,
				retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond,
			},
			run: func(t *testing.T, fake *fakePublishers, open func(context.Context) writeErrClient) {
				fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
					return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				defer cancel()
				client := open(ctx)
				require.NoError(t, client.send(1, true))
				client.recvUntilEnd()
			},
		},
		{
			name: "client deadline while the data node is healthy",
			run: func(t *testing.T, _ *fakePublishers, open func(context.Context) writeErrClient) {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				client := open(ctx)
				require.NoError(t, client.send(1, true))
				client.recvUntilEnd()
			},
		},
		{
			name: "client cancellation while the data node is busy",
			cfg:  writeSessionConfig{sealInterval: 10 * time.Millisecond, retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond},
			run: func(t *testing.T, fake *fakePublishers, open func(context.Context) writeErrClient) {
				fake.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
					return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := open(ctx)
				require.NoError(t, client.send(1, true))
				require.Eventually(t, func() bool { return len(fake.attempts()) >= 2 }, 5*time.Second, 5*time.Millisecond)
				cancel()
			},
		},
		{
			name: "client half-close",
			run: func(t *testing.T, _ *fakePublishers, open func(context.Context) writeErrClient) {
				client := open(context.Background())
				require.NoError(t, client.send(1, true))
				require.NoError(t, client.closeSend())
				var err error
				for err == nil {
					err = client.recv()
				}
				require.ErrorIs(t, err, io.EOF)
			},
		},
	}
	for _, service := range writeErrServices() {
		for _, scenario := range scenarios {
			t.Run(service.name+"/"+scenario.name, func(t *testing.T) {
				reg := prometheus.NewRegistry()
				m := newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil))
				fake, open := service.start(t, scenario.cfg, m)
				scenario.run(t, fake, open)
				require.Eventually(t, func() bool { return counterSeries(t, reg, "total_stream_finished")[service.name+"/write"] == 1 },
					5*time.Second, 10*time.Millisecond)
				want := map[string]float64{"measure/write": 0, "stream/write": 0, "trace/write": 0}
				if scenario.counted {
					want[service.name+"/write"] = 1
				}
				require.Equal(t, want, counterSeries(t, reg, "total_stream_err"))
			})
		}
	}
}

// deadlineCtx reports a fixed deadline while it is ended by a plain cancellation, which is what the server sees when the
// client's RST_STREAM arrives right at the deadline: Err() is Canceled although the deadline has passed.
type deadlineCtx struct {
	context.Context
	deadline time.Time
}

func (c deadlineCtx) Deadline() (time.Time, bool) { return c.deadline, true }

// TestWriteSessionBusyUntilClientDeadline checks that a session whose data node stays busy fails with
// ResourceExhausted, and so is counted, when its context ends at or after the client deadline, whatever the context
// error is and whichever step of the retry loop the end interrupts. A cancellation before the deadline is not a failure.
func TestWriteSessionBusyUntilClientDeadline(t *testing.T) {
	tests := []struct {
		name          string
		deadlineAfter time.Duration
		blockPublish  bool
		counted       bool
	}{
		{name: "canceled after the deadline while waiting to retry", deadlineAfter: -time.Second, counted: true},
		{name: "canceled after the deadline while publishing", deadlineAfter: -time.Second, blockPublish: true, counted: true},
		{name: "canceled before the deadline while waiting to retry", deadlineAfter: time.Hour},
		{name: "canceled before the deadline while publishing", deadlineAfter: time.Hour, blockPublish: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := deadlineCtx{Context: parent, deadline: time.Now().Add(tt.deadlineAfter)}
			f := &fakePublishers{}
			f.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
				if tt.blockPublish && len(f.attempts()) >= 3 {
					<-parent.Done()
					return nil, parent.Err()
				}
				return map[string]*common.Error{b.node: common.NewErrorWithStatus(modelv1.Status_STATUS_SERVER_BUSY, "busy")}, nil
			}
			c := &replyCollector{}
			s := newWriteSession(ctx, writeSessionConfig{
				sealInterval: time.Hour, pressureTimeout: time.Hour,
				retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond,
			}, writeSessionDeps{
				newPublisher: f.newPublisher,
				send:         c.send,
				log:          logger.GetLogger("test"),
				metrics:      newBypassMetrics(),
				service:      "trace",
				topic:        data.TopicTraceWrite,
			})
			require.NoError(t, appendRow(s, "n1", 1, 10))
			go func() {
				for len(f.attempts()) < 3 {
					time.Sleep(time.Millisecond)
				}
				cancel()
			}()
			err := s.Finish()
			if tt.counted {
				require.Equal(t, codes.ResourceExhausted, status.Code(err))
			} else {
				require.NoError(t, err)
			}
			reg := prometheus.NewRegistry()
			m := newMetrics(services.NewFactory(prom.NewProvider(liaisonGrpcScope, reg), nil, nil))
			recordWriteStreamErr(m, "trace", context.Canceled, err)
			want := float64(0)
			if tt.counted {
				want = 1
			}
			require.Equal(t, want, counterSeries(t, reg, "total_stream_err")["trace/write"])
		})
	}
}

// TestWriteSessionBusyThenErrorBeforeContextEnds checks that a non-busy failure near the client deadline does not fail
// the session while the context is still alive.
func TestWriteSessionBusyThenErrorBeforeContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	f := &fakePublishers{}
	f.onClose = func(b *fakeBatch) (map[string]*common.Error, error) {
		status := modelv1.Status_STATUS_SERVER_BUSY
		if len(f.attempts()) >= 3 {
			status = modelv1.Status_STATUS_INTERNAL_ERROR
		}
		return map[string]*common.Error{b.node: common.NewErrorWithStatus(status, "failed")}, nil
	}
	s := newWriteSession(deadlineCtx{Context: ctx, deadline: time.Now().Add(time.Millisecond)}, writeSessionConfig{
		sealInterval: time.Hour, pressureTimeout: time.Hour,
		retryBackoff: 10 * time.Millisecond, retryBackoffMax: 10 * time.Millisecond,
	}, writeSessionDeps{
		newPublisher: f.newPublisher,
		send:         (&replyCollector{}).send,
		log:          logger.GetLogger("test"),
		metrics:      newBypassMetrics(),
		service:      "trace",
		topic:        data.TopicTraceWrite,
	})
	require.NoError(t, appendRow(s, "n1", 1, 10))
	require.NoError(t, s.Finish())
}
