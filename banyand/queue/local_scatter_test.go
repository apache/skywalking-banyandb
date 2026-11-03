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

package queue

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type funcStreamListener struct {
	health *common.Error
	fn     func(ctx context.Context, message bus.Message, sink bus.Sink) error
}

func (f *funcStreamListener) CheckHealth() *common.Error { return f.health }

func (f *funcStreamListener) RevStream(ctx context.Context, message bus.Message, sink bus.Sink) error {
	return f.fn(ctx, message, sink)
}

type releasable struct {
	released *atomic.Int32
	id       int
}

func (r releasable) Release() { r.released.Add(1) }

func scatterLocal(ctx context.Context, t *testing.T, fn func(context.Context, bus.Message, bus.Sink) error, opts ScatterOptions) NodeStream {
	t.Helper()
	q := Local()
	require.NoError(t, q.SubscribeStream(data.TopicTraceQuery, &funcStreamListener{fn: fn}))
	streams, err := q.Scatter(ctx, data.TopicTraceQuery, bus.NewMessage(1, "req"), opts)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	return streams[0]
}

func TestLocalScatterDeliversChunksInOrderThenEOF(t *testing.T) {
	var gotRequest atomic.Value
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, m bus.Message, sink bus.Sink) error {
		gotRequest.Store(m.Data())
		for i := 0; i < 5; i++ {
			if err := sink.Send(ctx, bus.NewMessage(bus.MessageID(i), i)); err != nil {
				return err
			}
		}
		return nil
	}, ScatterOptions{Prefetch: 2})
	defer ns.Close()
	for i := 0; i < 5; i++ {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, i, m.Data(), "Go values pass through unencoded")
	}
	_, err := ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF)
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, io.EOF, "EOF is sticky")
	assert.Equal(t, "req", gotRequest.Load())
}

func TestLocalScatterListenerErrorIsNotEOF(t *testing.T) {
	boom := errors.New("boom")
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, bus.NewMessage(1, "a")); err != nil {
			return err
		}
		return boom
	}, ScatterOptions{})
	defer ns.Close()
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "a", m.Data())
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, io.EOF)
}

func TestLocalScatterPrefetchBoundsTheProducer(t *testing.T) {
	const prefetch = 2
	var sent atomic.Int32
	attempts := make(chan struct{}, 16)
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		for i := 0; i < 6; i++ {
			attempts <- struct{}{}
			if err := sink.Send(ctx, bus.NewMessage(bus.MessageID(i), i)); err != nil {
				return err
			}
			sent.Add(1)
		}
		return nil
	}, ScatterOptions{Prefetch: prefetch})
	defer ns.Close()
	// The producer attempts prefetch+1 sends: the last one blocks on the full buffer.
	for i := 0; i < prefetch+1; i++ {
		<-attempts
	}
	require.Never(t, func() bool { return sent.Load() > prefetch }, 100*time.Millisecond, 10*time.Millisecond)
	for i := 0; i < 6; i++ {
		m, err := ns.Next(context.Background())
		require.NoError(t, err)
		assert.Equal(t, i, m.Data())
	}
}

func TestLocalScatterCloseStopsProducerAndReleasesQueuedValues(t *testing.T) {
	var released atomic.Int32
	queued := make(chan struct{})
	listenerCtxDone := make(chan struct{})
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		for i := 0; i < 3; i++ {
			if err := sink.Send(ctx, bus.NewMessage(bus.MessageID(i), releasable{released: &released, id: i})); err != nil {
				return err
			}
		}
		close(queued)
		<-ctx.Done()
		close(listenerCtxDone)
		return ctx.Err()
	}, ScatterOptions{Prefetch: 4})
	<-queued
	// One value is consumed by the caller, which then owns its release.
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, m.Data().(releasable).id)

	ns.Close()
	<-listenerCtxDone
	assert.EqualValues(t, 2, released.Load(), "the two values still queued are released")
	_, err = ns.Next(context.Background())
	assert.ErrorIs(t, err, ErrStreamClosed)
	ns.Close() // idempotent
	assert.EqualValues(t, 2, released.Load())
}

func TestLocalScatterCloseUnblocksAProducerBlockedOnAFullBuffer(t *testing.T) {
	var released atomic.Int32
	producerDone := make(chan error, 1)
	attempts := make(chan struct{}, 8)
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		var err error
		for i := 0; i < 8 && err == nil; i++ {
			attempts <- struct{}{}
			err = sink.Send(ctx, bus.NewMessage(bus.MessageID(i), releasable{released: &released, id: i}))
		}
		producerDone <- err
		return err
	}, ScatterOptions{Prefetch: 1})
	<-attempts
	<-attempts // the second send is blocked: the buffer holds one value
	ns.Close()
	assert.ErrorIs(t, <-producerDone, context.Canceled)
	assert.EqualValues(t, 1, released.Load())
}

func TestLocalScatterCallerCancellationReachesTheListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	listenerErr := make(chan error, 1)
	ns := scatterLocal(ctx, t, func(lctx context.Context, _ bus.Message, _ bus.Sink) error {
		<-lctx.Done()
		listenerErr <- lctx.Err()
		return lctx.Err()
	}, ScatterOptions{})
	defer ns.Close()
	cancel()
	assert.ErrorIs(t, <-listenerErr, context.Canceled)
	nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second)
	defer nextCancel()
	_, err := ns.Next(nextCtx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLocalScatterNextHonoursItsContext(t *testing.T) {
	release := make(chan struct{})
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, _ bus.Sink) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}, ScatterOptions{})
	defer ns.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ns.Next(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLocalScatterRejectsUnknownTopicAndUnhealthyListener(t *testing.T) {
	q := Local()
	_, err := q.Scatter(context.Background(), data.TopicTraceQuery, bus.NewMessage(1, nil), ScatterOptions{})
	require.Error(t, err)

	unhealthy := &funcStreamListener{
		health: common.NewErrorWithStatus(modelv1.Status_STATUS_INTERNAL_ERROR, "not ready"),
		fn: func(context.Context, bus.Message, bus.Sink) error {
			t.Error("an unhealthy listener must not run")
			return nil
		},
	}
	require.NoError(t, q.SubscribeStream(data.TopicTraceQuery, unhealthy))
	_, err = q.Scatter(context.Background(), data.TopicTraceQuery, bus.NewMessage(1, nil), ScatterOptions{})
	require.Error(t, err)
	assert.Error(t, q.SubscribeStream(data.TopicTraceQuery, unhealthy), "a topic has one stream listener")
}

func TestLocalScatterListenerPanicBecomesAnError(t *testing.T) {
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, sink bus.Sink) error {
		if err := sink.Send(ctx, bus.NewMessage(1, "a")); err != nil {
			return err
		}
		panic("boom")
	}, ScatterOptions{})
	defer ns.Close()
	m, err := ns.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "a", m.Data())
	_, err = ns.Next(context.Background())
	require.Error(t, err)
	assert.NotErrorIs(t, err, io.EOF)
	assert.Contains(t, err.Error(), "panicked")
}

func TestLocalScatterForwardsChunkLimits(t *testing.T) {
	var got atomic.Value
	ns := scatterLocal(context.Background(), t, func(ctx context.Context, _ bus.Message, _ bus.Sink) error {
		got.Store(bus.ChunkLimitsFrom(ctx))
		return nil
	}, ScatterOptions{MaxChunkRows: 7, MaxChunkBytes: 4096})
	defer ns.Close()
	_, err := ns.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, bus.ChunkLimits{MaxRows: 7, MaxBytes: 4096}, got.Load())
}
