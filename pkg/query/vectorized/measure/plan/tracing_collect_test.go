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

func TestFrameOfTraceEnvelope(t *testing.T) {
	rawBody := []byte{0x00, 0x01, 0x02}
	trace := &commonv1.Trace{TraceId: "trace-1"}
	body, gotTrace, err := frameOf(&measurev1.InternalQueryResponse{RawFrameBody: rawBody, Trace: trace})
	if err != nil {
		t.Fatalf("frameOf returned error: %v", err)
	}
	if !bytes.Equal(body, rawBody) {
		t.Fatalf("body = %v, want %v", body, rawBody)
	}
	if gotTrace.GetTraceId() != "trace-1" {
		t.Fatalf("trace = %v, want trace-1", gotTrace)
	}
}

func TestFrameOfRawBodyAndEmptyResult(t *testing.T) {
	body, trace, err := frameOf([]byte{0x00, 0x01})
	if err != nil || len(body) != 2 || trace != nil {
		t.Fatalf("raw frame: body=%v trace=%v err=%v", body, trace, err)
	}
	body, trace, err = frameOf(nil)
	if err != nil || body != nil || trace != nil {
		t.Fatalf("empty result: body=%v trace=%v err=%v", body, trace, err)
	}
}

func TestFrameOfRejectsProtoDataPointsUnderRawMode(t *testing.T) {
	if _, _, err := frameOf(&measurev1.InternalQueryResponse{DataPoints: []*measurev1.InternalDataPoint{{}}}); err == nil {
		t.Fatal("frameOf returned nil error for proto data_points response under raw mode")
	}
}

func TestFrameOfTraceOnlyEnvelopeCarriesNoFrame(t *testing.T) {
	body, trace, err := frameOf(&measurev1.InternalQueryResponse{Trace: &commonv1.Trace{TraceId: "trace-error", Error: true}})
	if err != nil || len(body) != 0 || trace.GetTraceId() != "trace-error" {
		t.Fatalf("trace-only envelope: body=%v trace=%v err=%v", body, trace, err)
	}
}

func TestFrameOfRejectsUnexpectedPayload(t *testing.T) {
	if _, _, err := frameOf(errors.New("boom")); err == nil {
		t.Fatal("frameOf returned nil error for an unexpected payload")
	}
}
