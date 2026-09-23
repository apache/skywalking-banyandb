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

package grpc_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	liaisongrpc "github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/liaison/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const streamReloadUsersOnlyYAML = `
users:
  - username: "alice"
    password: "secret"
`

const streamReloadRBACEnabledYAML = `
users:
  - username: "alice"
    password: "secret"
rbac:
  enabled: true
  roles:
    writer_alpha:
      permissions: ["data:write", "data:read", "schema:read", "schema:write"]
  bindings:
    - principal: "alice"
      role: "writer_alpha"
      groups: ["alpha"]
`

type reloadScriptedStream struct {
	grpclib.ServerStream
	ctx    context.Context
	frames []proto.Message
	idx    int
}

func (s *reloadScriptedStream) Context() context.Context { return s.ctx }

func (s *reloadScriptedStream) RecvMsg(m any) error {
	if s.idx >= len(s.frames) {
		return status.Error(codes.OutOfRange, "no more frames")
	}
	proto.Merge(m.(proto.Message), s.frames[s.idx])
	s.idx++
	return nil
}

// TestStreamOpenedWithoutRBACEnforcesAfterHotEnable proves that a Measure write
// stream accepted while RBAC was off still authorizes frames after a reload turns
// RBAC on. Without FrameAuthorizer wrapping at accept time, enabling RBAC left
// long-lived write streams unchecked.
func TestStreamOpenedWithoutRBACEnforcesAfterHotEnable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.yaml")
	if writeErr := os.WriteFile(path, []byte(streamReloadUsersOnlyYAML), 0o600); writeErr != nil {
		t.Fatalf("writing users-only policy: %v", writeErr)
	}

	reloader := auth.InitAuthReloader()
	if configErr := reloader.ConfigAuthReloader(path, false, logger.GetLogger("rbac-stream-reload-test")); configErr != nil {
		t.Fatalf("ConfigAuthReloader: %v", configErr)
	}
	if startErr := reloader.Start(); startErr != nil {
		t.Fatalf("Start: %v", startErr)
	}
	defer reloader.Stop()
	if reloader.CurrentSnapshot().RBACEnabled() {
		t.Fatal("RBACEnabled() = true, want false for users-only policy")
	}

	interceptor := liaisongrpc.NewAuthorizationStreamInterceptor(reloader, liaisongrpc.GlobalMethodPolicies(), nil)
	info := &grpclib.StreamServerInfo{FullMethod: "/banyandb.measure.v1.MeasureService/Write"}
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs("username", "alice", "password", "secret"))

	base := &reloadScriptedStream{
		ctx: incoming,
		frames: []proto.Message{
			&measurev1.WriteRequest{Metadata: &commonv1.Metadata{Group: "beta", Name: "m"}, MessageId: 1},
		},
	}
	callErr := interceptor(nil, base, info, func(_ any, stream grpclib.ServerStream) error {
		if writeErr := os.WriteFile(path, []byte(streamReloadRBACEnabledYAML), 0o600); writeErr != nil {
			return writeErr
		}
		select {
		case <-reloader.GetUpdateChannel():
		case <-time.After(3 * time.Second):
			return status.Error(codes.DeadlineExceeded, "reload did not land")
		}
		if !reloader.CurrentSnapshot().RBACEnabled() {
			return status.Error(codes.FailedPrecondition, "RBAC still disabled after reload")
		}
		return stream.RecvMsg(&measurev1.WriteRequest{})
	})
	if status.Code(callErr) != codes.PermissionDenied {
		t.Fatalf("beta write after hot-enable RBAC = %v (%s), want PermissionDenied", callErr, status.Code(callErr))
	}
}
