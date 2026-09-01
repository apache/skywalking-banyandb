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

// ErrFrameGroupUnresolvable reports a write-stream frame the group it addresses cannot be
// read from: a frame of a type no write service sends, a first frame carrying no metadata at
// all, or a frame naming an empty or whitespace-only group. The liaison reports it to the
// caller as codes.InvalidArgument, so a malformed frame from an authenticated writer is
// answered as malformed rather than as an authorization outcome.
var ErrFrameGroupUnresolvable = errors.New("write frame: frame carries no resolvable group")

// SnapshotSource supplies the security snapshot in force at the moment it is asked for.
// *auth.Reloader is the production implementation. A write stream holds the source rather
// than a snapshot because a stream outlives a policy revision: asking again for every frame
// is what makes revoking a binding effective on the next frame of a stream already open.
type SnapshotSource interface {
	// CurrentSnapshot returns the security snapshot in force.
	CurrentSnapshot() auth.Snapshot
}

// FrameAuthorization carries everything one write stream's per-frame decisions need. It is
// built once when the stream interceptor admits the stream, and the principal in it is the
// one authentication established at that moment: a caller cannot change identity mid-stream,
// only lose the grants that identity holds.
type FrameAuthorization struct {
	// Snapshots supplies the snapshot each frame is decided against.
	Snapshots SnapshotSource
	// Observer records one bounded decision per resource-bearing frame.
	Observer DecisionObserver
	// Principal is the trusted identity the stream was opened by.
	Principal auth.Principal
	// Policy is the classification of the stream's write method, whose Permission every
	// resource-bearing frame must be held for in the group that frame resolves to.
	Policy MethodPolicy
}

// FrameGroup returns the BanyanDB group a resource-bearing write frame addresses.
//
// The three write services share one metadata contract: a frame carrying metadata of its own
// establishes the group, and a frame carrying none continues in the group the most recent
// frame that did carry it established, which the caller passes as lastGroup. Resolving a
// frame the same way its handler does is what keeps a legal continuation frame from being
// decided against the wrong group, or against no group at all.
//
// A frame of a type no write service sends, a first frame with no metadata — lastGroup empty
// — and a frame naming an empty or whitespace-only group all return an error wrapping
// ErrFrameGroupUnresolvable.
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

// NewFrameAuthorizer returns a grpclib.ServerStream that authorizes every resource-bearing
// frame before the write handler receives it.
//
// It is the liaison's only per-frame decision point. Each received frame resolves to a group
// through FrameGroup, is decided against the snapshot the source reports at that moment, and
// reaches the handler only when the stream's principal holds the method's permission in that
// group. A denied frame is answered codes.PermissionDenied and never returned to the handler,
// so it cannot be published, indexed or written to storage; a frame no group can be read from
// is answered codes.InvalidArgument. Every resource-bearing frame reports exactly one bounded
// decision to the observer.
//
// The returned stream wraps rather than replaces the one passed in, so the frame it hands the
// handler is the frame the interceptor chain below it produced — already validated, since the
// request validator sits between this wrapper and the transport.
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
		return recvErr
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
