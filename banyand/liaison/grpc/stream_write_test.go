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
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func newTestStreamService(er *entityRepo, maxWait time.Duration) *streamService {
	return &streamService{
		discoveryService: &discoveryService{entityRepo: er},
		maxWaitDuration:  maxWait,
		l:                logger.GetLogger("test"),
		metrics:          newBypassMetrics(),
	}
}

func validStreamWriteRequest() *streamv1.WriteRequest {
	return &streamv1.WriteRequest{
		Element: &streamv1.ElementValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
		},
	}
}

// TestValidateWriteRequest_Stream_ClientLower_ReturnsExpiredSchema verifies that
// a write with client ModRevision below the cached revision is rejected immediately
// with STATUS_EXPIRED_SCHEMA.
func TestValidateWriteRequest_Stream_ClientLower_ReturnsExpiredSchema(t *testing.T) {
	id := identity{group: "g", name: "s"}
	er := seededLocatorRepo(id)
	svc := newTestStreamService(er, 50*time.Millisecond)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "s", ModRevision: 50}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA, st)
	require.Len(t, mock.replies, 1)
	assert.Equal(t, modelv1.Status_STATUS_EXPIRED_SCHEMA.String(), mock.replies[0].Status)
}

// TestValidateWriteRequest_Stream_ClientHigher_WaitsAndReturnsNotApplied_OnTimeout
// verifies that a write with client ModRevision above the cached revision waits up to
// maxWaitDuration and then returns STATUS_SCHEMA_NOT_APPLIED when the cache stays stale.
func TestValidateWriteRequest_Stream_ClientHigher_WaitsAndReturnsNotApplied_OnTimeout(t *testing.T) {
	id := identity{group: "g", name: "s"}
	er := seededLocatorRepo(id)
	svc := newTestStreamService(er, 50*time.Millisecond)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "s", ModRevision: 200}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED, st)
	require.Len(t, mock.replies, 1)
	assert.Equal(t, modelv1.Status_STATUS_SCHEMA_NOT_APPLIED.String(), mock.replies[0].Status)
}

// TestValidateWriteRequest_Stream_ClientHigher_SucceedsWhenCacheCatchesUp verifies
// that a write with client ModRevision ahead of the cache returns STATUS_SUCCEED
// once the cache advances to the required revision within maxWaitDuration.
func TestValidateWriteRequest_Stream_ClientHigher_SucceedsWhenCacheCatchesUp(t *testing.T) {
	id := identity{group: "g", name: "s"}
	er := seededLocatorRepo(id)
	svc := newTestStreamService(er, 500*time.Millisecond)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	advanceLocatorAfter(er, id, 200, 20*time.Millisecond)

	meta := &commonv1.Metadata{Group: "g", Name: "s", ModRevision: 200}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies, "no error reply should be sent when validation succeeds")
}

// TestValidateWriteRequest_Stream_Equal_ReturnsSucceed verifies that a write with
// client ModRevision equal to the cached revision is accepted immediately.
func TestValidateWriteRequest_Stream_Equal_ReturnsSucceed(t *testing.T) {
	id := identity{group: "g", name: "s"}
	er := seededLocatorRepo(id)
	svc := newTestStreamService(er, 50*time.Millisecond)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "s", ModRevision: 100}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}

// TestValidateWriteRequest_Stream_ZeroRevision_SkipsCheck verifies that a write
// with ModRevision == 0 skips the revision gate entirely and returns STATUS_SUCCEED,
// even when no locator is registered for the schema.
func TestValidateWriteRequest_Stream_ZeroRevision_SkipsCheck(t *testing.T) {
	// Empty repo — if the gate ran it would return STATUS_NOT_FOUND.
	er := newEmptyEntityRepo()
	svc := newTestStreamService(er, 50*time.Millisecond)
	mock := &mockBidiServer[streamv1.WriteRequest, streamv1.WriteResponse]{}

	meta := &commonv1.Metadata{Group: "g", Name: "s", ModRevision: 0}
	st := svc.validateWriteRequest(validStreamWriteRequest(), meta, mock)

	assert.Equal(t, modelv1.Status_STATUS_SUCCEED, st)
	assert.Empty(t, mock.replies)
}
