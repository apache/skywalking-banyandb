// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package measure

import (
	"context"
	"errors"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/tracelabels"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func TestBatchLimitTracingTags(t *testing.T) {
	tracer, ctx := query.NewTracer(context.Background(), "limit-trace")
	limit := NewBatchLimit(&vectorized.BatchSchema{}, 1, 2)
	if initErr := limit.Init(ctx); initErr != nil {
		t.Fatalf("Init returned error: %v", initErr)
	}
	batch := &vectorized.RecordBatch{Len: 5}
	processErr := limit.Process(ctx, batch)
	if !errors.Is(processErr, vectorized.ErrLimitExhausted) {
		t.Fatalf("Process error = %v, want ErrLimitExhausted", processErr)
	}
	if closeErr := limit.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
	if closeErr := limit.Close(); closeErr != nil {
		t.Fatalf("second Close returned error: %v", closeErr)
	}
	trace := tracer.ToProto()
	if len(trace.GetSpans()) != 1 {
		t.Fatalf("span count = %d, want 1", len(trace.GetSpans()))
	}
	span := trace.GetSpans()[0]
	if span.GetMessage() != "limit" {
		t.Fatalf("span message = %q, want limit", span.GetMessage())
	}
	assertTraceTag(t, span.GetTags(), tracelabels.TagRowsIn, "5")
	assertTraceTag(t, span.GetTags(), tracelabels.TagRowsOut, "2")
	assertTraceTag(t, span.GetTags(), tracelabels.TagDroppedRows, "3")
	assertTraceTag(t, span.GetTags(), tracelabels.TagDropReason, "limit")
	if span.GetDuration() <= 0 {
		t.Fatalf("span duration = %d, want > 0", span.GetDuration())
	}
}

func assertTraceTag(t *testing.T, tags []*commonv1.Tag, key, want string) {
	t.Helper()
	for _, tag := range tags {
		if tag.GetKey() == key {
			if tag.GetValue() != want {
				t.Fatalf("tag %s = %q, want %q", key, tag.GetValue(), want)
			}
			return
		}
	}
	t.Fatalf("missing tag %s", key)
}
