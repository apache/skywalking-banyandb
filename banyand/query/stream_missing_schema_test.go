// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package query

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
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

func TestDistributedStreamQueryReturnsEmptyWhenNodeLacksSchema(t *testing.T) {
	processor := &streamQueryProcessor{
		streamService: missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}},
		queryService:  &queryService{log: logger.GetLogger(moduleName)},
		distributed:   true,
	}
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
	processor := &streamQueryProcessor{
		streamService: missingSchemaStreamService{errForGroup: map[string]error{"monitoring": stream.ErrStreamNotExist}},
		queryService:  &queryService{log: logger.GetLogger(moduleName)},
	}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*common.Error); !ok {
		t.Fatalf("standalone query response type = %T, want schema error", response.Data())
	}
}

func TestDistributedStreamQueryPreservesOtherSchemaErrors(t *testing.T) {
	wantErr := errors.New("metadata backend unavailable")
	processor := &streamQueryProcessor{
		streamService: missingSchemaStreamService{errForGroup: map[string]error{"monitoring": wantErr}},
		queryService:  &queryService{log: logger.GetLogger(moduleName)},
		distributed:   true,
	}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*common.Error); !ok {
		t.Fatalf("distributed query response type = %T, want metadata error", response.Data())
	}
}

func TestResolveStreamQuerySchemasKeepsAvailableGroups(t *testing.T) {
	service := missingSchemaStreamService{
		streamForGroup: map[string]stream.Stream{"available": availableTestStream{}},
		errForGroup:    map[string]error{"missing": stream.ErrStreamNotExist},
	}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"missing", "available"}}

	filtered, metadata, schemas, contexts, err := resolveStreamQuerySchemas(service, request, true)
	if err != nil {
		t.Fatalf("resolveStreamQuerySchemas returned error: %v", err)
	}
	if len(filtered.GetGroups()) != 1 || filtered.GetGroups()[0] != "available" {
		t.Fatalf("filtered groups = %v, want [available]", filtered.GetGroups())
	}
	if len(metadata) != 1 || len(schemas) != 1 || len(contexts) != 1 {
		t.Fatalf("resolved metadata/schemas/contexts lengths = %d/%d/%d, want 1/1/1",
			len(metadata), len(schemas), len(contexts))
	}
}

func TestResolveStreamQuerySchemasDoesNotIgnoreOtherErrors(t *testing.T) {
	wantErr := errors.New("metadata backend unavailable")
	service := missingSchemaStreamService{errForGroup: map[string]error{"broken": wantErr}}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"broken"}}

	_, _, _, _, err := resolveStreamQuerySchemas(service, request, true)
	if !errors.Is(err, wantErr) {
		t.Fatalf("resolveStreamQuerySchemas error = %v, want wrapped %v", err, wantErr)
	}
}

func TestResolveStreamQuerySchemasDoesNotIgnoreMissingSchemaWhenNotDistributed(t *testing.T) {
	service := missingSchemaStreamService{errForGroup: map[string]error{"missing": stream.ErrStreamNotExist}}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"missing"}}

	_, _, _, _, err := resolveStreamQuerySchemas(service, request, false)
	if !errors.Is(err, stream.ErrStreamNotExist) {
		t.Fatalf("resolveStreamQuerySchemas error = %v, want wrapped ErrStreamNotExist", err)
	}
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
