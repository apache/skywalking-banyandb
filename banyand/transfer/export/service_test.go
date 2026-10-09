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

package export

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// fakePipeline records the ExportService registration; nothing else of queue.Server is used.
type fakePipeline struct {
	queue.Server
	registered transferv1.ExportServiceServer
}

func (p *fakePipeline) SetExportServer(srv transferv1.ExportServiceServer) { p.registered = srv }

// fakeServerStream is the server side of a streaming call that records every sent frame.
type fakeServerStream[T any] struct {
	grpc.ServerStream
	frames []*T
}

func (s *fakeServerStream[T]) Context() context.Context { return context.Background() }

func (s *fakeServerStream[T]) Send(frame *T) error {
	s.frames = append(s.frames, frame)
	return nil
}

func TestService_RejectsInvalidSessionIDs(t *testing.T) {
	// No planner: a request that got past validation would panic.
	s := &Service{}
	for _, req := range []*transferv1.PlanRequest{
		createReq("ABC", false),
		readReq("../data"),
	} {
		stream := &fakeServerStream[transferv1.PlanResponse]{}
		if err := s.Plan(req, stream); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Plan(%+v) = %v, want InvalidArgument", req, err)
		}
		if len(stream.frames) != 0 {
			t.Fatalf("a rejected Plan must send nothing, got %+v", stream.frames)
		}
	}
	for _, req := range []*transferv1.SessionsRequest{
		{Action: transferv1.SessionsRequest_ACTION_HEARTBEAT, SessionId: "ABC"},
		{Action: transferv1.SessionsRequest_ACTION_RELEASE, SessionId: "../data"},
	} {
		stream := &fakeServerStream[transferv1.SessionsResponse]{}
		if err := s.Sessions(req, stream); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Sessions(%+v) = %v, want InvalidArgument", req, err)
		}
		if len(stream.frames) != 0 {
			t.Fatalf("a rejected Sessions call must send nothing, got %+v", stream.frames)
		}
	}
}

func TestService_SessionsSendsExactlyOneFrame(t *testing.T) {
	fx := newTestPlanner(t, nil, defaultGroups(), nil)
	s := &Service{planner: fx.planner}
	stream := &fakeServerStream[transferv1.SessionsResponse]{}
	if err := s.Sessions(&transferv1.SessionsRequest{Action: transferv1.SessionsRequest_ACTION_LIST}, stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.frames) != 1 || stream.frames[0].GetNone() == nil {
		t.Fatalf("a data node answers exactly one frame, got %+v", stream.frames)
	}
}

func TestService_ServeSweepsExpiredAndKeepsLiveSessions(t *testing.T) {
	backends, _ := newTestBackends(t)
	exportDir := backends[commonv1.Catalog_CATALOG_STREAM].GetExportSnapshotDir()
	now := time.Now()
	plant := func(id string, expiresAt time.Time) string {
		dir := filepath.Join(exportDir, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeLease(dir, Lease{SessionID: id, StartedAt: now.Add(-time.Hour).UnixNano(), ExpiresAt: expiresAt.UnixNano()}); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	expired := plant("dead", now.Add(-time.Second))
	live := plant("beef", now.Add(time.Hour))

	pipeline := &fakePipeline{}
	s := NewService(nil, pipeline, backends)
	if err := s.PreRun(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pipeline.registered != s {
		t.Fatal("PreRun must register the service on the data-node pipeline")
	}
	s.Serve()
	defer s.GracefulStop()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(expired); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the startup sweep must remove an expired session")
		}
	}
	if _, err := readLease(live); err != nil {
		t.Fatalf("the startup sweep must keep a live session: %v", err)
	}
}

func TestService_PreRunRejectsCollidingExportDirs(t *testing.T) {
	stream := commonv1.Catalog_CATALOG_STREAM
	measure := commonv1.Catalog_CATALOG_MEASURE
	for name, tamper := range map[string]func(bs Backends){
		"shared export dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = bs[stream].GetExportSnapshotDir()
		},
		"nested export dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = filepath.Join(bs[stream].GetExportSnapshotDir(), "measure")
		},
		"export dir is its own snapshot dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = bs[measure].GetSnapshotDir()
		},
		"export dir inside another catalog's snapshot dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = filepath.Join(bs[stream].GetSnapshotDir(), "export")
		},
		"export dir is another catalog's data dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = bs[stream].GetDataPath()
		},
		"export dir inside its own data dir": func(bs Backends) {
			bs[measure].(*fakeBackend).exportSnapshotDir = filepath.Join(bs[measure].GetDataPath(), "export")
		},
		"data dir inside the export dir": func(bs Backends) {
			bs[stream].(*fakeBackend).dataDir = filepath.Join(bs[measure].GetExportSnapshotDir(), "data")
		},
	} {
		t.Run(name, func(t *testing.T) {
			backends, _ := newTestBackends(t)
			tamper(backends)
			pipeline := &fakePipeline{}
			if err := NewService(nil, pipeline, backends).PreRun(context.Background()); err == nil {
				t.Fatal("PreRun must reject colliding export snapshot directories")
			}
			if pipeline.registered != nil {
				t.Fatal("a rejected PreRun must not register the service")
			}
		})
	}
	backends, _ := newTestBackends(t)
	if err := backends.validateDirs(); err != nil {
		t.Fatalf("distinct per-catalog directories must pass: %v", err)
	}
}

// onlyBackend hides every method but Backend's, so it cannot read part metadata.
type onlyBackend struct{ Backend }

func TestService_PreRunRejectsABackendThatCannotReadParts(t *testing.T) {
	backends, _ := newTestBackends(t)
	backends[commonv1.Catalog_CATALOG_TRACE] = onlyBackend{backends[commonv1.Catalog_CATALOG_TRACE]}
	pipeline := &fakePipeline{}
	err := NewService(nil, pipeline, backends).PreRun(context.Background())
	if err == nil || !strings.Contains(err.Error(), "CATALOG_TRACE") {
		t.Fatalf("PreRun must name the backend that cannot read parts, got %v", err)
	}
	if pipeline.registered != nil {
		t.Fatal("a rejected PreRun must not register the service")
	}
	backends[commonv1.Catalog_CATALOG_PROPERTY] = onlyBackend{backends[commonv1.Catalog_CATALOG_PROPERTY]}
	delete(backends, commonv1.Catalog_CATALOG_TRACE)
	if err = backends.validatePartReaders(); err != nil {
		t.Fatalf("property holds no parts and needs no reader: %v", err)
	}
}
