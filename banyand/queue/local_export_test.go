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
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

type fakeExportServer struct {
	transferv1.UnimplementedExportServiceServer
	planErr  error
	exited   chan struct{}
	lastPlan *transferv1.PlanRequest
	frames   int
	block    bool // send frames forever, until Send fails
}

func (f *fakeExportServer) Plan(req *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	f.lastPlan = req
	if f.exited != nil {
		defer close(f.exited)
	}
	for i := 0; f.block || i < f.frames; i++ {
		frame := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Units{Units: &transferv1.UnitFrame{Stage: "hot"}}}
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	return f.planErr
}

func (f *fakeExportServer) Sessions(_ *transferv1.SessionsRequest, stream transferv1.ExportService_SessionsServer) error {
	return stream.Send(&transferv1.SessionsResponse{Outcome: &transferv1.SessionsResponse_Done{Done: &transferv1.Ack{}}})
}

func exportClient(t *testing.T, srv transferv1.ExportServiceServer) transferv1.ExportServiceClient {
	t.Helper()
	q := Local()
	q.SetExportServer(srv)
	client, err := q.NewExportClient("any-node")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestLocalExportClient_WithoutServerIsNotImplemented(t *testing.T) {
	if _, err := Local().NewExportClient("self"); !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("got %v, want ErrNotImplemented", err)
	}
}

func TestLocalExportClient_PlanStreamsEveryFrameThenEOF(t *testing.T) {
	srv := &fakeExportServer{frames: 3}
	req := &transferv1.PlanRequest{Selectors: []*transferv1.Selector{{Groups: []string{"g"}}}}
	stream, err := exportClient(t, srv).Plan(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var got int
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if frame.GetUnits().GetStage() != "hot" {
			t.Fatalf("unexpected frame %+v", frame)
		}
		got++
	}
	if got != 3 {
		t.Fatalf("got %d frames, want 3", got)
	}
	if srv.lastPlan != req {
		t.Fatal("the handler must receive the caller's request")
	}
}

func TestLocalExportClient_PlanErrorsArriveAsStatus(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want codes.Code
	}{
		{name: "status", err: status.Error(codes.FailedPrecondition, "expired"), want: codes.FailedPrecondition},
		{name: "context", err: context.Canceled, want: codes.Canceled},
		{name: "plain", err: errors.New("boom"), want: codes.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, err := exportClient(t, &fakeExportServer{frames: 1, planErr: tt.err}).Plan(context.Background(), &transferv1.PlanRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = stream.Recv(); err != nil {
				t.Fatalf("the frame before the error must arrive: %v", err)
			}
			_, err = stream.Recv()
			if status.Code(err) != tt.want {
				t.Fatalf("got %v, want %s", err, tt.want)
			}
		})
	}
}

func TestLocalExportClient_CancelUnblocksTheHandler(t *testing.T) {
	srv := &fakeExportServer{block: true, exited: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := exportClient(t, srv).Plan(ctx, &transferv1.PlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-srv.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler must stop once the caller cancels")
	}
	for {
		if _, err = stream.Recv(); err != nil {
			break
		}
	}
	if status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", err)
	}
}

func TestLocalExportClient_SessionsAnswersOneFrame(t *testing.T) {
	stream, err := exportClient(t, &fakeExportServer{}).Sessions(context.Background(), &transferv1.SessionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Recv()
	if err != nil || frame.GetDone() == nil {
		t.Fatalf("got %+v, %v", frame, err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

type panickingExportServer struct {
	transferv1.UnimplementedExportServiceServer
}

func (panickingExportServer) Plan(*transferv1.PlanRequest, transferv1.ExportService_PlanServer) error {
	panic("boom")
}

// A panicking handler fails the call as Internal instead of crashing the process.
func TestLocalExportClient_HandlerPanicIsInternal(t *testing.T) {
	stream, err := exportClient(t, panickingExportServer{}).Plan(context.Background(), &transferv1.PlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "export handler panic: boom" {
		t.Fatalf("got %v, want Internal naming the panic", err)
	}
}

// echoExportServer answers every client frame with one response frame carrying the
// received unit's group, and returns exportErr once the client half-closes.
type echoExportServer struct {
	transferv1.UnimplementedExportServiceServer
	exportErr error
	exited    chan struct{}
	received  []*transferv1.ExportRequest
	panicking bool
}

func (f *echoExportServer) Export(stream transferv1.ExportService_ExportServer) error {
	if f.exited != nil {
		defer close(f.exited)
	}
	if f.panicking {
		panic("boom")
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return f.exportErr
		}
		if err != nil {
			return err
		}
		f.received = append(f.received, req)
		resp := &transferv1.ExportResponse{Frame: &transferv1.ExportResponse_FileStarted{
			FileStarted: &transferv1.FileStarted{RelativePath: req.GetInit().GetSessionId()},
		}}
		if err = stream.Send(resp); err != nil {
			return err
		}
	}
}

func initFrame(id string) *transferv1.ExportRequest {
	return &transferv1.ExportRequest{Frame: &transferv1.ExportRequest_Init_{Init: &transferv1.ExportRequest_Init{SessionId: id}}}
}

func waitExited(t *testing.T, exited chan struct{}) {
	t.Helper()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler goroutine must exit")
	}
}

func TestLocalExportClient_ExportRoundTripAndHalfClose(t *testing.T) {
	srv := &echoExportServer{exited: make(chan struct{})}
	stream, err := exportClient(t, srv).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sent := []*transferv1.ExportRequest{initFrame("a"), initFrame("b"), initFrame("c")}
	for _, req := range sent {
		if err = stream.Send(req); err != nil {
			t.Fatal(err)
		}
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if resp.GetFileStarted().GetRelativePath() != req.GetInit().GetSessionId() {
			t.Fatalf("got %+v for %+v", resp, req)
		}
	}
	if err = stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF after the handler returned nil", err)
	}
	waitExited(t, srv.exited)
	if len(srv.received) != len(sent) {
		t.Fatalf("handler received %d frames, want %d", len(srv.received), len(sent))
	}
	for i := range sent {
		if srv.received[i] != sent[i] {
			t.Fatalf("frame %d must reach the handler unchanged", i)
		}
	}
}

func TestLocalExportClient_ExportSendBlocksUntilReceived(t *testing.T) {
	block := make(chan struct{})
	srv := &blockingExportServer{release: block}
	stream, err := exportClient(t, srv).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sendDone := make(chan error, 1)
	//panicdiag:allow-rawgo test sender
	go func() { sendDone <- stream.Send(initFrame("a")) }()
	select {
	case <-sendDone:
		t.Fatal("Send must block until the handler receives the frame")
	case <-time.After(50 * time.Millisecond):
	}
	close(block)
	if err = <-sendDone; err != nil {
		t.Fatal(err)
	}
	if err = stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

// blockingExportServer receives only after release is closed, then drains to io.EOF.
type blockingExportServer struct {
	transferv1.UnimplementedExportServiceServer
	release chan struct{}
}

func (f *blockingExportServer) Export(stream transferv1.ExportService_ExportServer) error {
	<-f.release
	for {
		if _, err := stream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func TestLocalExportClient_ExportHandlerErrorEndsRecvAndSend(t *testing.T) {
	srv := &echoExportServer{exportErr: status.Error(codes.FailedPrecondition, "expired"), exited: make(chan struct{})}
	stream, err := exportClient(t, srv).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
	waitExited(t, srv.exited)

	stream, err = exportClient(t, &failingExportServer{err: errors.New("boom")}).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.Unknown {
		t.Fatalf("got %v, want Unknown for a plain error", err)
	}
}

// failingExportServer returns err without reading anything.
type failingExportServer struct {
	transferv1.UnimplementedExportServiceServer
	err error
}

func (f *failingExportServer) Export(transferv1.ExportService_ExportServer) error { return f.err }

func TestLocalExportClient_ExportSendAfterHandlerReturnedIsEOF(t *testing.T) {
	stream, err := exportClient(t, &failingExportServer{err: status.Error(codes.Internal, "gone")}).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want Internal", err)
	}
	if err = stream.Send(initFrame("a")); err == nil {
		t.Fatal("Send after the handler returned must fail")
	}
}

func TestLocalExportClient_ExportCancelUnblocksBothSides(t *testing.T) {
	srv := &echoExportServer{exited: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := exportClient(t, srv).Export(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The handler blocks in Send of its answer because nobody receives it.
	if err = stream.Send(initFrame("a")); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitExited(t, srv.exited)
	if _, err = stream.Recv(); status.Code(err) != codes.Canceled {
		t.Fatalf("got %v, want Canceled", err)
	}
	if err = stream.Send(initFrame("b")); err == nil {
		t.Fatal("Send after cancel must fail")
	}

	// A handler blocked in Recv is unblocked too.
	idle := &echoExportServer{exited: make(chan struct{})}
	ctx, cancel = context.WithCancel(context.Background())
	if _, err = exportClient(t, idle).Export(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitExited(t, idle.exited)
}

func TestLocalExportClient_ExportHandlerPanicIsInternal(t *testing.T) {
	srv := &echoExportServer{panicking: true, exited: make(chan struct{})}
	stream, err := exportClient(t, srv).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "export handler panic: boom" {
		t.Fatalf("got %v, want Internal naming the panic", err)
	}
	waitExited(t, srv.exited)
}

func TestLocalExportClient_ExportLeavesNoGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := exportClient(t, &echoExportServer{}).Export(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.Send(initFrame("a")); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			cancel()
			continue
		}
		if _, err = stream.Recv(); err != nil {
			t.Fatal(err)
		}
		if err = stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if _, err = stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("got %v, want io.EOF", err)
		}
		cancel()
	}
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

// peerExportServer records the peer address each handler sees.
type peerExportServer struct {
	transferv1.UnimplementedExportServiceServer
	addrs chan string
}

func (p *peerExportServer) record(ctx context.Context) {
	addr := "unknown"
	if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
		addr = pr.Addr.String()
	}
	p.addrs <- addr
}

func (p *peerExportServer) Plan(_ *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	p.record(stream.Context())
	return nil
}

func (p *peerExportServer) Export(stream transferv1.ExportService_ExportServer) error {
	p.record(stream.Context())
	return nil
}

// In-process calls carry a "local" peer so the data node's audit log names the caller.
func TestLocalExportClient_HandlersSeeTheLocalPeer(t *testing.T) {
	srv := &peerExportServer{addrs: make(chan string, 2)}
	client := exportClient(t, srv)
	plan, err := client.Plan(context.Background(), &transferv1.PlanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = plan.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
	export, err := client.Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = export.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
	for i := 0; i < 2; i++ {
		if got := <-srv.addrs; got != "local" {
			t.Fatalf("handler saw peer %q, want local", got)
		}
	}
}
