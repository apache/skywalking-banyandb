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

package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

// ReleaseResult sorts the data nodes by their answer to a release.
type ReleaseResult struct {
	// Released are the nodes that held the session and deleted it.
	Released []string
	// NotHeld are the nodes that did not hold the session; nothing was deleted there.
	NotHeld []string
	// Failed are the error frames, one per node that could not release; an unexpected outcome becomes one.
	Failed []*transferv1.SessionsResponse
}

// ReleaseSession asks every data node to drop the session snapshot and sorts their answers.
func ReleaseSession(ctx context.Context, conn *grpc.ClientConn, sessionID string) (*ReleaseResult, error) {
	req := &transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: sessionID}
	stream, err := transferv1.NewExportServiceClient(conn).Sessions(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("release session %s: %w", sessionID, unsupported(err))
	}
	result := &ReleaseResult{}
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return result, nil
		}
		if recvErr != nil {
			return nil, fmt.Errorf("release session %s: %w", sessionID, unsupported(recvErr))
		}
		switch outcome := frame.GetOutcome().(type) {
		case *transferv1.SessionsResponse_Done:
			result.Released = append(result.Released, frame.GetNodeId())
		case *transferv1.SessionsResponse_None:
			result.NotHeld = append(result.NotHeld, frame.GetNodeId())
		case *transferv1.SessionsResponse_Error:
			result.Failed = append(result.Failed, frame)
		default:
			// A release answers done, none or error; anything else is a protocol error for that node.
			result.Failed = append(result.Failed, &transferv1.SessionsResponse{
				NodeId:  frame.GetNodeId(),
				Outcome: &transferv1.SessionsResponse_Error{Error: fmt.Sprintf("unexpected release outcome %T", outcome)},
			})
		}
	}
}
