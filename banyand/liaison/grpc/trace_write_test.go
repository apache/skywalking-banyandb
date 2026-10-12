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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func newTestTraceService(er *entityRepo, maxWait time.Duration) *traceService {
	return &traceService{
		discoveryService: &discoveryService{entityRepo: er},
		maxWaitDuration:  maxWait,
		l:                logger.GetLogger("test"),
		metrics:          newBypassMetrics(),
	}
}

// validTraceWriteRequest returns a WriteRequest whose tags include one timestamp tag,
// satisfying the foundTimestamp check in validateWriteRequest.
func validTraceWriteRequest() *tracev1.WriteRequest {
	return &tracev1.WriteRequest{
		Tags: []*modelv1.TagValue{{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
			},
		}},
	}
}

// TestValidateWriteRequest_Trace_ClientLower_ReturnsExpiredSchema verifies that
// a write with client ModRevision below the cached revision is rejected immediately
// with STATUS_EXPIRED_SCHEMA.
func TestValidateWriteRequest_Trace_ClientLower_ReturnsExpiredSchema(t *testing.T) {
	id := identity{group: "g", name: "t"}
	er := seededTraceRepo(id, 100)
	svc := newTestTraceService(er, 50*time.Millisecond)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "t", ModRevision: 50}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, st)
	require.Len(t, mock.replies, 1)
	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA.String(), mock.replies[0].Status)
}

// TestValidateWriteRequest_Trace_ClientHigher_WaitsAndReturnsNotApplied_OnTimeout
// verifies that a write with client ModRevision above the cached revision waits up to
// maxWaitDuration and then returns STATUS_SCHEMA_NOT_APPLIED when the cache stays stale.
func TestValidateWriteRequest_Trace_ClientHigher_WaitsAndReturnsNotApplied_OnTimeout(t *testing.T) {
	id := identity{group: "g", name: "t"}
	er := seededTraceRepo(id, 100)
	svc := newTestTraceService(er, 50*time.Millisecond)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "t", ModRevision: 200}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)
	require.Len(t, mock.replies, 1)
	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED.String(), mock.replies[0].Status)
}

// TestValidateWriteRequest_Trace_ClientHigher_SucceedsWhenCacheCatchesUp verifies
// that a write with client ModRevision ahead of the trace cache returns STATUS_SUCCEED
// once the trace entry advances to the required revision within maxWaitDuration.
func TestValidateWriteRequest_Trace_ClientHigher_SucceedsWhenCacheCatchesUp(t *testing.T) {
	id := identity{group: "g", name: "t"}
	er := seededTraceRepo(id, 100)
	svc := newTestTraceService(er, 500*time.Millisecond)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	advanceTraceRevAfter(er, id, 200, 20*time.Millisecond)

	meta := &commonv1.Metadata{Group: "g", Name: "t", ModRevision: 200}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies, "no error reply should be sent when validation succeeds")
}

// TestValidateWriteRequest_Trace_Equal_ReturnsSucceed verifies that a write with
// client ModRevision equal to the cached trace revision is accepted immediately.
func TestValidateWriteRequest_Trace_Equal_ReturnsSucceed(t *testing.T) {
	id := identity{group: "g", name: "t"}
	er := seededTraceRepo(id, 100)
	svc := newTestTraceService(er, 50*time.Millisecond)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "t", ModRevision: 100}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}

// TestValidateWriteRequest_Trace_ZeroRevision_SkipsCheck verifies that a write
// with ModRevision == 0 skips the revision gate and returns STATUS_SUCCEED.
// The trace entity must still be present because the trace path always resolves the
// entity before the revision check.
func TestValidateWriteRequest_Trace_ZeroRevision_SkipsCheck(t *testing.T) {
	id := identity{group: "g", name: "t"}
	// Seed the trace entity so getTrace(id) finds it; the revision value does not matter.
	er := seededTraceRepo(id, 999)
	svc := newTestTraceService(er, 50*time.Millisecond)
	mock := &mockBidiServer[tracev1.WriteRequest, tracev1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "t", ModRevision: 0}
	st := svc.validateWriteRequest(validTraceWriteRequest(), meta, nil, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}
