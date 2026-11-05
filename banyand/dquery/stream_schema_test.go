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

package dquery

import (
	"context"
	"testing"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type missingSchemaStreamService struct {
	stream.Service
}

func (missingSchemaStreamService) Stream(_ *commonv1.Metadata) (stream.Stream, error) {
	return nil, stream.ErrStreamNotExist
}

func TestLiaisonStreamQueryStillRejectsUnknownSchema(t *testing.T) {
	processor := &streamQueryProcessor{
		streamService: missingSchemaStreamService{},
		queryService:  &queryService{log: logger.GetLogger("dquery")},
	}
	request := &streamv1.QueryRequest{Name: "logs", Groups: []string{"monitoring"}}
	response := processor.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), request))

	if _, ok := response.Data().(*common.Error); !ok {
		t.Fatalf("liaison query response type = %T, want schema error", response.Data())
	}
}
