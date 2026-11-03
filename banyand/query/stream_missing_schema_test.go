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

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type missingSchemaStreamService struct {
	stream.Service
	streamForGroup map[string]stream.Stream
	errForGroup    map[string]error
}

func (s missingSchemaStreamService) Stream(meta *commonv1.Metadata) (stream.Stream, error) {
	return s.streamForGroup[meta.GetGroup()], s.errForGroup[meta.GetGroup()]
}

func newMissingSchemaProcessor(service stream.Service, distributed bool) *streamQueryProcessor {
	return &streamQueryProcessor{
		streamService: service,
		queryService:  &queryService{log: logger.GetLogger(moduleName)},
		distributed:   distributed,
	}
}

func TestDistributedStreamQueryReturnsEmptyWhenNodeLacksSchema(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}}, true)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*streamv1.QueryResponse); !ok {
		t.Fatalf("distributed query response type = %T, want empty QueryResponse", response.Data())
	}
	if got := len(response.Data().(*streamv1.QueryResponse).GetElements()); got != 0 {
		t.Fatalf("distributed query returned %d elements for a missing stream", got)
	}
}

func TestStandaloneStreamQueryStillReturnsMissingSchemaError(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}}, false)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*common.Error); !ok {
		t.Fatalf("standalone query response type = %T, want schema error", response.Data())
	}
}

func TestDistributedStreamQueryPreservesOtherSchemaErrors(t *testing.T) {
	wantErr := errors.New("metadata backend unavailable")
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"monitoring": wantErr}}, true)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*common.Error); !ok {
		t.Fatalf("distributed query response type = %T, want metadata error", response.Data())
	}
}

func TestResolveStreamGroupsKeepsAvailableGroups(t *testing.T) {
	processor := newMissingSchemaProcessor(missingSchemaStreamService{
		streamForGroup: map[string]stream.Stream{"available": availableTestStream{}},
		errForGroup:    map[string]error{"missing": stream.ErrStreamNotExist},
	}, true)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"missing", "available"}}

	groups, err := processor.resolveStreamGroups(request, true)
	if err != nil {
		t.Fatalf("resolveStreamGroups returned error: %v", err)
	}
	if len(groups.request.GetGroups()) != 1 || groups.request.GetGroups()[0] != "available" {
		t.Fatalf("scoped groups = %v, want [available]", groups.request.GetGroups())
	}
	if len(groups.metadata) != 1 || len(groups.schemas) != 1 || len(groups.contexts) != 1 {
		t.Fatalf("resolved metadata/schemas/contexts lengths = %d/%d/%d, want 1/1/1",
			len(groups.metadata), len(groups.schemas), len(groups.contexts))
	}
	if groups.metadata[0].GetGroup() != "available" {
		t.Fatalf("resolved metadata group = %s, want available", groups.metadata[0].GetGroup())
	}
	if len(request.GetGroups()) != 2 {
		t.Fatalf("the original request groups = %v, want them untouched", request.GetGroups())
	}
}

func TestResolveStreamGroupsDoesNotIgnoreOtherErrors(t *testing.T) {
	wantErr := errors.New("metadata backend unavailable")
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"broken": wantErr}}, true)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"broken"}}

	groups, err := processor.resolveStreamGroups(request, true)
	if !errors.Is(err, wantErr) {
		t.Fatalf("resolveStreamGroups error = %v, want wrapped %v", err, wantErr)
	}
	if len(groups.request.GetGroups()) != 0 {
		t.Fatalf("scoped groups = %v, want none", groups.request.GetGroups())
	}
}

func TestResolveStreamGroupsDoesNotIgnoreMissingSchemaWhenNotDistributed(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"missing": stream.ErrStreamNotExist}}, false)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"missing"}}

	groups, err := processor.resolveStreamGroups(request, false)
	if !errors.Is(err, stream.ErrStreamNotExist) {
		t.Fatalf("resolveStreamGroups error = %v, want wrapped ErrStreamNotExist", err)
	}
	if len(groups.request.GetGroups()) != 0 {
		t.Fatalf("scoped groups = %v, want none", groups.request.GetGroups())
	}
}

func TestBuildStreamPlanReturnsNoPlanWhenNoGroupIsAvailable(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"missing": stream.ErrStreamNotExist}}, true)
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"missing"}}

	scoped, plan, err := processor.buildStreamPlan(request)
	if err != nil {
		t.Fatalf("buildStreamPlan returned error: %v", err)
	}
	if plan != nil {
		t.Fatalf("buildStreamPlan returned a plan for a missing stream")
	}
	if len(scoped.GetGroups()) != 0 {
		t.Fatalf("scoped groups = %v, want none", scoped.GetGroups())
	}
}

func TestDistributedStreamChunkQueryReturnsEmptyWhenNodeLacksSchema(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}}, true)
	sink := &responseRecordingSink{}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}

	if err := processor.RevStream(context.Background(), bus.NewMessage(bus.MessageID(1), request), sink); err != nil {
		t.Fatalf("distributed chunk query returned error: %v", err)
	}
	if len(sink.responses) != 1 {
		t.Fatalf("distributed chunk query sent %d responses, want 1", len(sink.responses))
	}
	if got := len(sink.responses[0].GetElements()); got != 0 {
		t.Fatalf("distributed chunk query returned %d elements for a missing stream", got)
	}
}

func TestDistributedStreamChunkQueryCarriesTraceWhenNodeLacksSchema(t *testing.T) {
	processor := newMissingSchemaProcessor(
		missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}}, true)
	sink := &responseRecordingSink{}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}, Trace: true}

	if err := processor.RevStream(context.Background(), bus.NewMessage(bus.MessageID(1), request), sink); err != nil {
		t.Fatalf("distributed chunk query returned error: %v", err)
	}
	if len(sink.responses) != 1 {
		t.Fatalf("distributed chunk query sent %d responses, want 1", len(sink.responses))
	}
	if sink.responses[0].GetTrace() == nil {
		t.Fatal("distributed chunk query response carries no trace")
	}
}

// responseRecordingSink records the query responses a chunked query sends.
type responseRecordingSink struct {
	responses []*streamv1.QueryResponse
}

func (s *responseRecordingSink) Send(_ context.Context, msg bus.Message) error {
	if resp, ok := msg.Data().(*streamv1.QueryResponse); ok {
		s.responses = append(s.responses, resp)
	}
	return nil
}

type availableTestStream struct {
	stream.Stream
}

func (availableTestStream) GetSchema() *databasev1.Stream {
	return &databasev1.Stream{}
}

func (availableTestStream) GetIndexRules() []*databasev1.IndexRule {
	return nil
}

func TestBuildStreamPlanReportsAnalyzeFailure(t *testing.T) {
	processor := newMissingSchemaProcessor(missingSchemaStreamService{
		streamForGroup: map[string]stream.Stream{"available": availableTestStream{}},
	}, true)
	request := &streamv1.QueryRequest{
		Name:       "logs",
		Groups:     []string{"available"},
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "f", Tags: []string{"unknown"}}}},
	}

	_, _, err := processor.buildStreamPlan(request)
	if err == nil {
		t.Fatal("buildStreamPlan returned no error for an unknown projection tag")
	}
}
