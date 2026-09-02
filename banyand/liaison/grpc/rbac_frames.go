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
	"errors"
	"fmt"
	"strings"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
)

// ErrFrameGroupUnresolvable reports a write frame with no resolvable group.
var ErrFrameGroupUnresolvable = errors.New("write frame: frame carries no resolvable group")

// SnapshotSource supplies the current security snapshot.
type SnapshotSource interface {
	// CurrentSnapshot returns the security snapshot in force.
	CurrentSnapshot() auth.Snapshot
}

// FrameAuthorization configures authorization for a write stream.
type FrameAuthorization struct {
	// Snapshots supplies the snapshot each frame is decided against.
	Snapshots SnapshotSource
	// Observer records one bounded decision per resource-bearing frame.
	Observer DecisionObserver
	// Principal is the trusted identity the stream was opened by.
	Principal auth.Principal
	// Policy classifies the stream's write method.
	Policy MethodPolicy
}

// FrameGroup returns the group addressed by a write frame. A frame without metadata
// continues lastGroup. Unresolvable frames return an error wrapping ErrFrameGroupUnresolvable.
func FrameGroup(frame any, lastGroup string) (string, error) {
	switch typedFrame := frame.(type) {
	case *measurev1.WriteRequest:
		if typedFrame != nil {
			return frameMetadataGroup(frame, typedFrame.GetMetadata(), lastGroup)
		}
	case *streamv1.WriteRequest:
		if typedFrame != nil {
			return frameMetadataGroup(frame, typedFrame.GetMetadata(), lastGroup)
		}
	case *tracev1.WriteRequest:
		if typedFrame != nil {
			return frameMetadataGroup(frame, typedFrame.GetMetadata(), lastGroup)
		}
	}
	return "", fmt.Errorf("%w: %T continuing %q", ErrFrameGroupUnresolvable, frame, lastGroup)
}

func frameMetadataGroup(frame any, metadata *commonv1.Metadata, lastGroup string) (string, error) {
	if metadata != nil {
		group := strings.TrimSpace(metadata.GetGroup())
		if group != "" {
			return group, nil
		}
		return "", fmt.Errorf("%w: %T names an empty group", ErrFrameGroupUnresolvable, frame)
	}
	continuedGroup := strings.TrimSpace(lastGroup)
	if continuedGroup != "" {
		return continuedGroup, nil
	}
	return "", fmt.Errorf("%w: %T has no established group", ErrFrameGroupUnresolvable, frame)
}

// NewFrameAuthorizer returns a server stream that authorizes each write frame against
// the current snapshot before delivering it to the handler.
func NewFrameAuthorizer(stream grpclib.ServerStream, authorization FrameAuthorization) grpclib.ServerStream {
	return &frameAuthorizer{ServerStream: stream, authorization: authorization}
}

type frameAuthorizer struct {
	grpclib.ServerStream
	lastGroup     string
	authorization FrameAuthorization
}

func (a *frameAuthorizer) RecvMsg(frame any) error {
	if recvErr := a.ServerStream.RecvMsg(frame); recvErr != nil {
		return fmt.Errorf("failed to receive write frame: %w", recvErr)
	}
	snapshot := a.currentSnapshot()
	if snapshot == nil || !snapshot.RBACEnabled() {
		return nil
	}
	group, groupErr := FrameGroup(frame, a.lastGroup)
	if groupErr != nil {
		withholdFrame(frame)
		a.observe(snapshot, DecisionInvalidRequest, DecisionReasonInvalidRequest)
		return status.Error(codes.InvalidArgument, "invalid request")
	}
	if !snapshot.Allows(a.authorization.Principal, a.authorization.Policy.Permission, group) {
		withholdFrame(frame)
		a.observe(snapshot, DecisionDeny, DecisionReasonPermissionMissing)
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	a.lastGroup = group
	a.observe(snapshot, DecisionAllow, DecisionReasonGranted)
	return nil
}

func (a *frameAuthorizer) currentSnapshot() auth.Snapshot {
	if a.authorization.Snapshots == nil {
		return nil
	}
	return a.authorization.Snapshots.CurrentSnapshot()
}

func (a *frameAuthorizer) observe(snapshot auth.Snapshot, decision Decision, reason DecisionReason) {
	observeDecision(a.authorization.Observer, snapshot, a.authorization.Policy, decision, reason)
}

func withholdFrame(frame any) {
	if message, isProto := frame.(proto.Message); isProto {
		proto.Reset(message)
	}
}
