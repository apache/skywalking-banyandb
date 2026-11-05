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

package importer

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

func TestRetryableFirst(t *testing.T) {
	tests := []struct {
		err  error
		name string
		sent bool
		want bool
	}{
		{name: "liaison unavailable", err: status.Error(codes.Unavailable, "connection refused"), want: true},
		{name: "liaison unavailable after a stream batch was sent", err: status.Error(codes.Unavailable, "transport is closing"), sent: true, want: false},
		{name: "load refusal after a stream batch was sent", err: status.Error(codes.ResourceExhausted, "under memory pressure"), sent: true, want: true},
		{
			name: "schema not live yet after a stream batch was sent", err: importStatus(codes.NotFound, transfer.ImportReasonSchemaNotFound, nil, "group g not found"),
			sent: true, want: true,
		},
		{
			name: "liaison refused before writing after a stream batch was sent",
			err:  importStatus(codes.Unavailable, transfer.ImportReasonNotWritten, nil, "get group g: no active schema server"),
			sent: true, want: true,
		},
		{name: "node unavailable after fan-out", err: nodeFailed(codes.Unavailable, "0", "hot-b", "failed to get client"), want: false},
		{name: "schema not live yet", err: importStatus(codes.NotFound, transfer.ImportReasonSchemaNotFound, nil, "group g not found"), want: true},
		{name: "older liaison: not found", err: status.Error(codes.NotFound, "group g not found"), want: true},
		{name: "load refusal", err: status.Error(codes.ResourceExhausted, "too many in-flight requests"), want: true},
		{name: "node disk full", err: nodeFailed(codes.ResourceExhausted, "0", "hot-b", "disk full"), want: false},
		{name: "batch too large", err: importStatus(codes.ResourceExhausted, transfer.ImportReasonMessageTooLarge, nil, "message larger than max (5 vs. 4)"), want: false},
		{name: "invalid row", err: importStatus(codes.InvalidArgument, transfer.ImportReasonRowInvalid, nil, "row 0: bad"), want: false},
		{name: "node internal error", err: nodeFailed(codes.Internal, "0", "hot-b", "boom"), want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, reason, _ := failureOf(tc.err)
			if got := retryableFirst(code, reason, tc.sent); got != tc.want {
				t.Fatalf("retryableFirst(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
