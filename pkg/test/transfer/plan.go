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

// Package transfer holds helpers the data-export integration suites share.
package transfer

import (
	"context"
	"errors"
	"io"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

// CreateRequest builds the CreateSession PlanRequest a client sends to the liaison: no id,
// the given selectors and preempt flag.
func CreateRequest(selectors []*transferv1.Selector, preempt bool) *transferv1.PlanRequest {
	return &transferv1.PlanRequest{
		Selectors: selectors,
		Session:   &transferv1.PlanRequest_Create{Create: &transferv1.CreateSession{Preempt: preempt}},
	}
}

// ReadRequest builds the ReadSession PlanRequest that inventories (and renews) the
// existing session id.
func ReadRequest(id string) *transferv1.PlanRequest {
	return &transferv1.PlanRequest{Session: &transferv1.PlanRequest_Read{Read: &transferv1.ReadSession{Id: id}}}
}

// callCtx bounds one ExportService call by flags.EventuallyTimeout.
func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), flags.EventuallyTimeout)
}

// Plan runs one ExportService.Plan call and drains every frame it streams.
func Plan(conn *grpc.ClientConn, req *transferv1.PlanRequest) ([]*transferv1.PlanResponse, error) {
	ctx, cancel := callCtx()
	defer cancel()
	stream, err := transferv1.NewExportServiceClient(conn).Plan(ctx, req)
	if err != nil {
		return nil, err
	}
	return drain(stream.Recv)
}

// MustPlan is Plan asserting success and at least one frame.
func MustPlan(conn *grpc.ClientConn, req *transferv1.PlanRequest) []*transferv1.PlanResponse {
	frames, err := Plan(conn, req)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, frames).NotTo(gomega.BeEmpty())
	return frames
}

// Summary returns the final frame of a Plan stream, asserting it is the `summary` frame.
func Summary(frames []*transferv1.PlanResponse) *transferv1.PlanSummary {
	last := frames[len(frames)-1]
	gomega.ExpectWithOffset(1, last.GetSummary()).NotTo(gomega.BeNil(), "the last frame must be the summary: %+v", last)
	return last.GetSummary()
}

// UnitFrames keeps the `units` frames of a Plan stream, dropping the `created` and `summary` frames.
func UnitFrames(frames []*transferv1.PlanResponse) []*transferv1.UnitFrame {
	var out []*transferv1.UnitFrame
	for _, f := range frames {
		if u := f.GetUnits(); u != nil {
			out = append(out, u)
		}
	}
	return out
}

// Sessions runs one ExportService.Sessions call and drains every frame it streams: one per
// data node through a liaison, a single one straight from a data node.
func Sessions(conn *grpc.ClientConn, req *transferv1.SessionsRequest) ([]*transferv1.SessionsResponse, error) {
	ctx, cancel := callCtx()
	defer cancel()
	stream, err := transferv1.NewExportServiceClient(conn).Sessions(ctx, req)
	if err != nil {
		return nil, err
	}
	return drain(stream.Recv)
}

// List runs ACTION_LIST and returns every frame.
func List(conn *grpc.ClientConn) []*transferv1.SessionsResponse {
	frames, err := Sessions(conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return frames
}

// Heartbeat runs ACTION_HEARTBEAT for id and returns every frame.
func Heartbeat(conn *grpc.ClientConn, id string) []*transferv1.SessionsResponse {
	frames, err := Sessions(conn, &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: id})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return frames
}

// Release runs ACTION_RELEASE for id and returns the nodes whose frame carries the `error` outcome.
func Release(conn *grpc.ClientConn, id string) []string {
	var failed []string
	for _, f := range ReleaseOutcome(conn, id).Failed {
		failed = append(failed, f.GetNodeId())
	}
	return failed
}

// ReleaseOutcome runs ACTION_RELEASE for id and returns every node's answer, sorted into the
// nodes that released it, the nodes that did not hold it and the failed ones.
func ReleaseOutcome(conn *grpc.ClientConn, id string) *exporter.ReleaseResult {
	ctx, cancel := callCtx()
	defer cancel()
	result, err := exporter.ReleaseSession(ctx, conn, id)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return result
}

// drain collects frames from recv until EOF; the frames read so far are returned with any
// other error.
func drain[T any](recv func() (T, error)) ([]T, error) {
	var frames []T
	for {
		frame, err := recv()
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, frame)
	}
}
