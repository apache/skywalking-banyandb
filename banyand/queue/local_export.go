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
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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

func (c localExportClient) Export(ctx context.Context, _ ...grpc.CallOption,
) (grpc.BidiStreamingClient[transferv1.ExportRequest, transferv1.ExportResponse], error) {
	return startLocalBidiStream(ctx, c.srv.Export), nil
}

// localAddr is the peer address of an in-process call, so handlers that log their caller
// print "local" instead of "unknown".
type localAddr struct{}

func (localAddr) Network() string { return "local" }

func (localAddr) String() string { return "local" }

// localCallContext derives the context of one in-process call, carrying the local peer.
func localCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(peer.NewContext(ctx, &peer.Peer{Addr: localAddr{}}))
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
	callCtx, cancel := localCallContext(ctx)
	s := &localStream[T]{ctx: callCtx, cancel: cancel, frames: make(chan *T), done: make(chan struct{})}
	runLocalHandler(s.done, &s.err, func() error { return handler(s) })
	return s
}

// runLocalHandler runs handler on a goroutine, stores its result in *result and then closes
// done.
func runLocalHandler(done chan struct{}, result *error, handler func() error) {
	//panicdiag:allow-rawgo the goroutine recovers a handler panic into the call's status
	go func() {
		defer close(done)
		// Recover before done closes, as the cluster's recovery interceptor does, so a panic
		// fails the call instead of the process.
		defer func() {
			if r := recover(); r != nil {
				*result = status.Errorf(codes.Internal, "export handler panic: %v", r)
			}
		}()
		*result = handler()
	}()
}

// handlerStatus is the error a gRPC client sees for a handler's non-nil result.
func handlerStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}
	if ctxErr := status.FromContextError(err); ctxErr.Code() != codes.Unknown {
		return ctxErr.Err()
	}
	return status.Error(codes.Unknown, err.Error())
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
		return nil, handlerStatus(s.err)
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

// localBidiStream is the client end of one in-process bidirectional call; localBidiServer is
// its server end. Both directions hand frames over unbuffered channels, so a Send blocks until
// the peer receives the frame or the call ends: one frame in flight per direction, the
// back-pressure a gRPC stream gives.
type localBidiStream[Req, Resp any] struct {
	ctx        context.Context
	err        error // the handler's result, set before done is closed
	cancel     context.CancelFunc
	reqs       chan *Req
	resps      chan *Resp
	sendClosed chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
}

func startLocalBidiStream[Req, Resp any](ctx context.Context,
	handler func(grpc.BidiStreamingServer[Req, Resp]) error,
) *localBidiStream[Req, Resp] {
	callCtx, cancel := localCallContext(ctx)
	s := &localBidiStream[Req, Resp]{
		ctx: callCtx, cancel: cancel,
		reqs: make(chan *Req), resps: make(chan *Resp),
		sendClosed: make(chan struct{}), done: make(chan struct{}),
	}
	runLocalHandler(s.done, &s.err, func() error { return handler(&localBidiServer[Req, Resp]{s: s}) })
	return s
}

// Send implements grpc.BidiStreamingClient. Once the handler has returned it answers io.EOF,
// as a gRPC client stream does; Recv then reports the handler's result.
func (s *localBidiStream[Req, Resp]) Send(m *Req) error {
	select {
	case <-s.sendClosed:
		return status.Error(codes.Internal, "send after CloseSend")
	default:
	}
	select {
	case s.reqs <- m:
		return nil
	case <-s.done:
		return io.EOF
	case <-s.ctx.Done():
		return status.FromContextError(s.ctx.Err()).Err()
	}
}

// Recv implements grpc.BidiStreamingClient: the next frame, io.EOF once the handler returned
// nil, or its error as a status the way a gRPC client sees it.
func (s *localBidiStream[Req, Resp]) Recv() (*Resp, error) {
	select {
	case m := <-s.resps:
		return m, nil
	case <-s.done:
		s.cancel()
		if s.err == nil {
			return nil, io.EOF
		}
		return nil, handlerStatus(s.err)
	case <-s.ctx.Done():
		return nil, status.FromContextError(s.ctx.Err()).Err()
	}
}

// CloseSend implements grpc.ClientStream: the handler's next Recv answers io.EOF.
func (s *localBidiStream[Req, Resp]) CloseSend() error {
	s.closeOnce.Do(func() { close(s.sendClosed) })
	return nil
}

// Context implements grpc.ClientStream.
func (s *localBidiStream[Req, Resp]) Context() context.Context { return s.ctx }

// Header implements grpc.ClientStream.
func (*localBidiStream[Req, Resp]) Header() (metadata.MD, error) { return nil, nil }

// Trailer implements grpc.ClientStream.
func (*localBidiStream[Req, Resp]) Trailer() metadata.MD { return nil }

// SendMsg implements grpc.ClientStream; the typed Send is the only way frames travel.
func (*localBidiStream[Req, Resp]) SendMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}

// RecvMsg implements grpc.ClientStream; the typed Recv is the only way frames travel.
func (*localBidiStream[Req, Resp]) RecvMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}

// localBidiServer is the handler's end of a localBidiStream.
type localBidiServer[Req, Resp any] struct {
	s *localBidiStream[Req, Resp]
}

// Recv implements grpc.BidiStreamingServer: the client's next frame, or io.EOF once the
// client called CloseSend. A frame sent before CloseSend is always received first, because
// Send returns only after the handler took it.
func (b *localBidiServer[Req, Resp]) Recv() (*Req, error) {
	select {
	case m := <-b.s.reqs:
		return m, nil
	case <-b.s.sendClosed:
		return nil, io.EOF
	case <-b.s.ctx.Done():
		return nil, status.FromContextError(b.s.ctx.Err()).Err()
	}
}

// Send implements grpc.BidiStreamingServer.
func (b *localBidiServer[Req, Resp]) Send(m *Resp) error {
	select {
	case b.s.resps <- m:
		return nil
	case <-b.s.ctx.Done():
		return status.FromContextError(b.s.ctx.Err()).Err()
	}
}

// Context implements grpc.ServerStream.
func (b *localBidiServer[Req, Resp]) Context() context.Context { return b.s.ctx }

// SetHeader implements grpc.ServerStream.
func (*localBidiServer[Req, Resp]) SetHeader(metadata.MD) error { return nil }

// SendHeader implements grpc.ServerStream.
func (*localBidiServer[Req, Resp]) SendHeader(metadata.MD) error { return nil }

// SetTrailer implements grpc.ServerStream.
func (*localBidiServer[Req, Resp]) SetTrailer(metadata.MD) {}

// SendMsg implements grpc.ServerStream; the typed Send is the only way frames travel.
func (*localBidiServer[Req, Resp]) SendMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}

// RecvMsg implements grpc.ServerStream; the typed Recv is the only way frames travel.
func (*localBidiServer[Req, Resp]) RecvMsg(any) error {
	return status.Error(codes.Unimplemented, "local export stream carries typed frames only")
}
