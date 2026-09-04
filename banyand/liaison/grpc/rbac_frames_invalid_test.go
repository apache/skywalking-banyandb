// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package grpc_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

type frameDecisionCall struct {
	fullMethod string
	permission string
	reason     liaisongrpc.DecisionReason
	decision   liaisongrpc.Decision
}

type frameDecisionObserver struct {
	calls []frameDecisionCall
}

func (o *frameDecisionObserver) ObserveDecision(
	fullMethod, permission string,
	decision liaisongrpc.Decision,
	reason liaisongrpc.DecisionReason,
) {
	o.calls = append(o.calls, frameDecisionCall{
		fullMethod: fullMethod,
		permission: permission,
		reason:     reason,
		decision:   decision,
	})
}

func TestFrameAuthorizer_FirstMetadataLessFrameIsInvalid(t *testing.T) {
	snapshot := dataSnapshot(t, dataPolicyYAML)
	actors := dataActors(t, snapshot)
	policy, classified := policyTable(t).Policy(mMeasureWrite)
	if !classified {
		t.Fatalf("GlobalMethodPolicies() does not classify %s", mMeasureWrite)
	}
	observer := &frameDecisionObserver{}
	stream := liaisongrpc.NewFrameAuthorizer(&scriptedStream{
		ctx:    context.Background(),
		frames: []proto.Message{&measurev1.WriteRequest{MessageId: 1}},
	}, liaisongrpc.FrameAuthorization{
		Snapshots: &scriptedSnapshots{revisions: []auth.Snapshot{snapshot}},
		Observer:  observer,
		Principal: actors["writer-alpha"],
		Policy:    policy,
	})

	received := &measurev1.WriteRequest{}
	recvErr := stream.RecvMsg(received)

	if status.Code(recvErr) != codes.InvalidArgument {
		t.Errorf("RecvMsg(first metadata-less frame) = %v, want codes.InvalidArgument", recvErr)
	}
	if received.GetMessageId() != 0 || received.GetMetadata() != nil {
		t.Errorf("RecvMsg(first metadata-less frame) delivered %#v, want the withheld zero value", received)
	}
	if len(observer.calls) != 1 {
		t.Fatalf("RecvMsg(first metadata-less frame) recorded %d decisions, want 1", len(observer.calls))
	}
	got := observer.calls[0]
	if got.fullMethod != mMeasureWrite || got.permission != "data:write" ||
		got.decision != liaisongrpc.DecisionInvalidRequest || got.reason != liaisongrpc.DecisionReasonInvalidRequest {
		t.Errorf("RecvMsg(first metadata-less frame) recorded %#v, want the bounded invalid-request decision", got)
	}
}
