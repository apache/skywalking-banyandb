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
	"context"
	"io"
	"sync"
	"time"

	grpcmd "google.golang.org/grpc/metadata"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
)

// mockBidiServer captures Send calls emitted by service handlers under test.
// It satisfies grpc.BidiStreamingServer[Req, Res] (= grpc.ServerStream + Recv + Send).
type mockBidiServer[Req any, Res any] struct {
	replies []*Res
	mu      sync.Mutex
}

func (s *mockBidiServer[Req, Res]) Send(resp *Res) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies = append(s.replies, resp)
	return nil
}

func (s *mockBidiServer[Req, Res]) Recv() (*Req, error)        { return nil, io.EOF }
func (s *mockBidiServer[Req, Res]) Context() context.Context   { return context.Background() }
func (s *mockBidiServer[Req, Res]) SetHeader(grpcmd.MD) error  { return nil }
func (s *mockBidiServer[Req, Res]) SendHeader(grpcmd.MD) error { return nil }
func (s *mockBidiServer[Req, Res]) SetTrailer(grpcmd.MD)       {}
func (s *mockBidiServer[Req, Res]) SendMsg(any) error          { return nil }
func (s *mockBidiServer[Req, Res]) RecvMsg(any) error          { return nil }

// newBypassMetrics returns a metrics instance backed by a no-op registry.
func newBypassMetrics() *metrics {
	factory := observability.NewBypassRegistry().With(liaisonGrpcScope)
	return newMetrics(factory)
}

// newEmptyEntityRepo constructs an entityRepo with all maps initialized but empty.
func newEmptyEntityRepo() *entityRepo {
	return &entityRepo{
		log:             logger.GetLogger("test"),
		entitiesMap:     make(map[identity]partition.Locator),
		measureMap:      make(map[identity]*databasev1.Measure),
		streamMap:       make(map[identity]*databasev1.Stream),
		traceMap:        make(map[identity]*databasev1.Trace),
		traceIDIndexMap: make(map[identity]int),
	}
}

// seededLocatorRepoRev is the baseline ModRevision every test uses for the seeded
// locator. Picking 100 leaves headroom for tests that simulate stale (<100) and
// future (>100) client revisions against this baseline.
const seededLocatorRepoRev int64 = 100

// seededLocatorRepo returns an entityRepo with a single locator entry at the
// fixed seededLocatorRepoRev baseline. Tests that need a different revision
// should mutate er.entitiesMap directly.
func seededLocatorRepo(id identity) *entityRepo {
	er := newEmptyEntityRepo()
	er.entitiesMap[id] = partition.Locator{ModRevision: seededLocatorRepoRev}
	return er
}

// seededTraceRepo returns an entityRepo with a single trace entry at rev.
func seededTraceRepo(id identity, rev int64) *entityRepo {
	er := newEmptyEntityRepo()
	er.traceMap[id] = &databasev1.Trace{
		Metadata: &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: rev},
	}
	return er
}

// advanceLocatorAfter advances the locator revision for id in er to newRev after delay.
func advanceLocatorAfter(er *entityRepo, id identity, newRev int64, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		er.RWMutex.Lock()
		er.entitiesMap[id] = partition.Locator{ModRevision: newRev}
		er.RWMutex.Unlock()
	}()
}

// advanceTraceRevAfter replaces the trace entry for id with one at newRev after delay.
func advanceTraceRevAfter(er *entityRepo, id identity, newRev int64, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		er.RWMutex.Lock()
		er.traceMap[id] = &databasev1.Trace{
			Metadata: &commonv1.Metadata{Group: id.group, Name: id.name, ModRevision: newRev},
		}
		er.RWMutex.Unlock()
	}()
}
