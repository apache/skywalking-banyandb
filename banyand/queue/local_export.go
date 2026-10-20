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
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// localExportClient is the standalone pipeline's transferv1.ExportServiceClient: like
// localChunkedSyncClient it stays in-process, running the handler SetExportServer
// registered on a goroutine and handing its frames to the caller over a channel.
type localExportClient struct {
	srv transferv1.ExportServiceServer
}

func (c localExportClient) Plan(ctx context.Context, in *transferv1.PlanRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[transferv1.PlanResponse], error) {
	return startLocalStream(ctx, func(s grpc.ServerStreamingServer[transferv1.PlanResponse]) error {
		return c.srv.Plan(in, s)
	}), nil
}

func (c localExportClient) Sessions(ctx context.Context, in *transferv1.SessionsRequest,
	_ ...grpc.CallOption,
) (grpc.ServerStreamingClient[transferv1.SessionsResponse], error) {
	return startLocalStream(ctx, func(s grpc.ServerStreamingServer[transferv1.SessionsResponse]) error {
		return c.srv.Sessions(in, s)
	}), nil
}

// localStream is both ends of one in-process server-streaming call. Send blocks until the
// caller receives the frame or the call's context ends, which gives the handler the same
// back-pressure a gRPC stream does.
type localStream[T any] struct {
	ctx    context.Context
	err    error // the handler's result, set before done is closed
	cancel context.CancelFunc
	frames chan *T
	done   chan struct{}
}

func startLocalStream[T any](ctx context.Context, handler func(grpc.ServerStreamingServer[T]) error) *localStream[T] {
	callCtx, cancel := context.WithCancel(ctx)
	s := &localStream[T]{ctx: callCtx, cancel: cancel, frames: make(chan *T), done: make(chan struct{})}
	//panicdiag:allow-rawgo the goroutine recovers a handler panic into the call's status
	go func() {
		defer close(s.done)
		// Recover before done closes, as the cluster's recovery interceptor does, so a panic
		// fails the call instead of the process.
		defer func() {
			if r := recover(); r != nil {
				s.err = status.Errorf(codes.Internal, "export handler panic: %v", r)
			}
		}()
		s.err = handler(s)
	}()
	return s
}

// Send implements grpc.ServerStreamingServer.
func (s *localStream[T]) Send(m *T) error {
	select {
	case s.frames <- m:
		return nil
	case <-s.ctx.Done():
		return status.FromContextError(s.ctx.Err()).Err()
	}
}

// Recv implements grpc.ServerStreamingClient: the next frame, io.EOF once the handler
// returned nil, or its error as a status the way a gRPC client sees it.
func (s *localStream[T]) Recv() (*T, error) {
	select {
	case m := <-s.frames:
		return m, nil
	case <-s.done:
		s.cancel()
		if s.err == nil {
			return nil, io.EOF
		}
		if _, ok := status.FromError(s.err); ok {
			return nil, s.err
		}
		if ctxErr := status.FromContextError(s.err); ctxErr.Code() != codes.Unknown {
			return nil, ctxErr.Err()
		}
		return nil, status.Error(codes.Unknown, s.err.Error())
	case <-s.ctx.Done():
		return nil, status.FromContextError(s.ctx.Err()).Err()
	}
}

// Context implements grpc.ServerStream and grpc.ClientStream.
func (s *localStream[T]) Context() context.Context { return s.ctx }

// Header implements grpc.ClientStream.
func (*localStream[T]) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream.
func (*localStream[T]) Trailer() metadata.MD { return nil }

// CloseSend implements grpc.ClientStream.
func (*localStream[T]) CloseSend() error { return nil }

// SetHeader implements grpc.ServerStream.
func (*localStream[T]) SetHeader(metadata.MD) error { return nil }

// SendHeader implements grpc.ServerStream.
func (*localStream[T]) SendHeader(metadata.MD) error { return nil }

// SetTrailer implements grpc.ServerStream.
func (*localStream[T]) SetTrailer(metadata.MD) {}

// SendMsg implements grpc.ServerStream and grpc.ClientStream; the typed Send is the only
// way frames travel.
func (*localStream[T]) SendMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}

// RecvMsg implements grpc.ServerStream and grpc.ClientStream; the typed Recv is the only
// way frames travel.
func (*localStream[T]) RecvMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}
