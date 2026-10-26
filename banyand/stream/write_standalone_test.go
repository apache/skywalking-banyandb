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

package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// TestWriteCallback_RevReportsUnstoredEvents verifies that a batch in which no
// element could be stored is answered with an error instead of an empty reply,
// which the caller would take as success.
func TestWriteCallback_RevReportsUnstoredEvents(t *testing.T) {
	cb := &writeCallback{
		l:          logger.GetLogger("test"),
		schemaRepo: newTestSchemaRepo(nil, "other-group"),
	}
	event := &streamv1.InternalWriteRequest{
		Request: &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{Group: "missing-group", Name: "log"},
			Element:  &streamv1.ElementValue{Timestamp: timestamppb.New(time.UnixMilli(time.Now().UnixMilli()))},
		},
	}

	resp := cb.Rev(context.Background(), bus.NewMessage(bus.MessageID(7), []any{event}))

	ce, ok := resp.Data().(*common.Error)
	require.True(t, ok, "the reply must carry the error, got %T", resp.Data())
	assert.Equal(t, modelv1.Status_STATUS_INTERNAL_ERROR, ce.Status())
	assert.Contains(t, ce.Error(), "missing-group")
	assert.Equal(t, bus.MessageID(7), resp.ID())
}

// TestWriteCallback_RevReportsUndecodableEvents covers the other way an element is dropped:
// bytes that are not a write request.
func TestWriteCallback_RevReportsUndecodableEvents(t *testing.T) {
	cb := &writeCallback{
		l:          logger.GetLogger("test"),
		schemaRepo: newTestSchemaRepo(nil, "other-group"),
	}

	resp := cb.Rev(context.Background(), bus.NewMessage(bus.MessageID(1), []any{[]byte{0xff, 0xff, 0xff}}))

	_, ok := resp.Data().(*common.Error)
	assert.True(t, ok, "the reply must carry the error, got %T", resp.Data())
}
