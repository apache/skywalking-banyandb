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

package plan

import (
	"bytes"
	"errors"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

type rawFrameFuture struct {
	err     error
	message bus.Message
}

func (f rawFrameFuture) Get() (bus.Message, error) {
	return f.message, f.err
}

func (f rawFrameFuture) GetAll() ([]bus.Message, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []bus.Message{f.message}, nil
}

func TestCollectRawFrameResponsesTraceEnvelope(t *testing.T) {
	rawBody := []byte{0x00, 0x01, 0x02}
	trace := &commonv1.Trace{TraceId: "trace-1"}
	frames, traces, collectErr := collectRawFrameResponses([]bus.Future{
		rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{RawFrameBody: rawBody, Trace: trace})},
	})
	if collectErr != nil {
		t.Fatalf("collectRawFrameResponses returned error: %v", collectErr)
	}
	if len(frames) != 1 || !bytes.Equal(frames[0], rawBody) {
		t.Fatalf("frames = %v, want one raw body %v", frames, rawBody)
	}
	if len(traces) != 1 || traces[0].GetTraceId() != "trace-1" {
		t.Fatalf("traces = %v, want trace-1", traces)
	}
}

func TestCollectRawFrameResponsesRejectsProtoDataPointsUnderRawMode(t *testing.T) {
	_, _, collectErr := collectRawFrameResponses([]bus.Future{
		rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{DataPoints: []*measurev1.InternalDataPoint{{}}})},
	})
	if collectErr == nil {
		t.Fatal("collectRawFrameResponses returned nil error for proto data_points response under raw mode")
	}
}

func TestCollectRawFrameResponsesTraceOnlyErrorEnvelope(t *testing.T) {
	_, traces, collectErr := collectRawFrameResponses([]bus.Future{
		rawFrameFuture{message: bus.NewMessage(1, &measurev1.InternalQueryResponse{Trace: &commonv1.Trace{TraceId: "trace-error", Error: true}})},
	})
	if collectErr == nil {
		t.Fatal("collectRawFrameResponses returned nil error for trace-only error envelope")
	}
	if len(traces) != 1 || traces[0].GetTraceId() != "trace-error" {
		t.Fatalf("traces = %v, want trace-error", traces)
	}
}

func TestCollectRawFrameResponsesFutureError(t *testing.T) {
	_, _, collectErr := collectRawFrameResponses([]bus.Future{rawFrameFuture{err: errors.New("boom")}})
	if collectErr == nil {
		t.Fatal("collectRawFrameResponses returned nil error for future error")
	}
}
