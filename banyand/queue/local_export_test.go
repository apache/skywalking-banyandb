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
	"testing"
	"time"

	"google.golang.org/grpc/codes"
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
