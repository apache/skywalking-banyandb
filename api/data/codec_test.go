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

package data

import (
	"bytes"
	"testing"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"google.golang.org/protobuf/proto"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
)

// TestRawFrameMagic_ProtoUnmarshalFailsLoud is the G9f spec's load-bearing
// proof test (Principle 3, G9f.2 codec contract): a raw vec columnar frame's
// leading 0x00 byte deterministically forces a flag-off node's
// proto.Unmarshal of the frame body into *measurev1.InternalQueryResponse{}
// to return a non-nil error, collapsing the dangerous "garbage-but-parsed
// silently-empty" outcome into an unmistakable hard decode error. Verified
// against google.golang.org/protobuf@v1.36.11: 0x00 decodes as field number 0
// which protowire.ConsumeTag rejects with errCodeFieldNumber, and
// proto/decode.go converts the resulting negative tag length into a hard
// errDecode *before* any unknown-field skip — independent of every byte
// that follows.
func TestRawFrameMagic_ProtoUnmarshalFailsLoud(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{name: "magic-only", body: []byte{RawFrameMagicLeadingByte}},
		{name: "magic-with-version", body: []byte{RawFrameMagicLeadingByte, 0x01}},
		{name: "magic-with-columnar-payload", body: []byte{RawFrameMagicLeadingByte, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var m measurev1.InternalQueryResponse
			if err := proto.Unmarshal(tc.body, &m); err == nil {
				t.Fatalf("proto.Unmarshal of 0x00-leading raw frame %x into *InternalQueryResponse{} returned nil error; want fail-loud (silently-parsed message: %+v)", tc.body, &m)
			}
		})
	}
}

// TestRawFrameCodec_NilEmptyBody_NotMagicValidated covers the spec carve-out
// (G9f.0 exit, sub.go:195-205 / pub.go:519-521 audit): an empty/nil response
// body is a legitimate empty distributed result and MUST NOT be magic-validated,
// else legitimate empties would fail loud incorrectly.
func TestRawFrameCodec_NilEmptyBody_NotMagicValidated(t *testing.T) {
	codec := NewRawFrameCodec()
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{name: "nil", body: nil},
		{name: "empty", body: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, err := codec.Unmarshal(tc.body)
			if err != nil {
				t.Fatalf("RawFrameCodec.Unmarshal(%v) returned err=%v; want nil (empty body must NOT be magic-validated)", tc.body, err)
			}
			b, _ := v.([]byte)
			if len(b) != 0 {
				t.Fatalf("RawFrameCodec.Unmarshal(%v) returned non-empty body %v; want empty", tc.body, b)
			}
		})
	}
}

// TestRawFrameCodec_WrongMagic_FailsLoud covers the positive side of the
// G9f.0 codec contract: a non-empty body whose leading byte is not 0x00
// must be rejected loudly.
func TestRawFrameCodec_WrongMagic_FailsLoud(t *testing.T) {
	codec := NewRawFrameCodec()
	for _, body := range [][]byte{{0x01}, {0xff, 0x00}, {0x08, 0x01, 0x02}} {
		if _, err := codec.Unmarshal(body); err == nil {
			t.Fatalf("RawFrameCodec.Unmarshal(%x) returned nil error; want fail-loud on non-0x00 leading byte", body)
		}
	}
}

// TestTopicResponseMap_ProtoCodec_RoundTripsByteIdentical is the G9f.0
// blast-radius regression: every proto-bodied topic in TopicResponseMap
// (all ~18 stream/measure/topn/property/trace/collect-info topics, excluding
// TopicInternalMeasureQuery which uses the wire-mode-dispatching codec) must
// marshal/unmarshal byte-identically to the pre-G9f.0
// proto.Marshal/proto.Unmarshal path. Empty round-trip is sufficient to prove
// the wrapper is a pure passthrough.
func TestTopicResponseMap_ProtoCodec_RoundTripsByteIdentical(t *testing.T) {
	var protoTopics int
	for topic, codec := range TopicResponseMap {
		if topic == TopicInternalMeasureQuery {
			// Wire-mode-dispatching codec; covered separately below.
			continue
		}
		pc, ok := codec.(*ProtoCodec)
		if !ok {
			t.Errorf("topic %s: expected *ProtoCodec, got %T", topic, codec)
			continue
		}
		protoTopics++
		msg := pc.newMessage()
		wantBytes, err := proto.Marshal(msg)
		if err != nil {
			t.Errorf("topic %s: baseline proto.Marshal failed: %v", topic, err)
			continue
		}
		gotBytes, err := pc.Marshal(msg)
		if err != nil {
			t.Errorf("topic %s: ProtoCodec.Marshal failed: %v", topic, err)
			continue
		}
		if !bytes.Equal(gotBytes, wantBytes) {
			t.Errorf("topic %s: marshal mismatch: got %x want %x", topic, gotBytes, wantBytes)
			continue
		}
		decoded, err := pc.Unmarshal(gotBytes)
		if err != nil {
			t.Errorf("topic %s: ProtoCodec.Unmarshal failed: %v", topic, err)
			continue
		}
		if _, ok := decoded.(proto.Message); !ok {
			t.Errorf("topic %s: decoded type %T does not implement proto.Message", topic, decoded)
		}
	}
	if protoTopics == 0 {
		t.Fatal("no ProtoCodec topics found in TopicResponseMap — blast-radius regression test would be a no-op")
	}
}

// TestMeasureQueryResponseCodec_DispatchesOnWireMode covers the G9f.0
// topic-AND-process-wire-mode guard: TopicInternalMeasureQuery is served by a
// single static supplier in the map but the codec selected per-encode/decode
// must depend on the per-process wire-mode flag set via SetMeasureWireModeRaw
// (flag-on → RawFrameCodec; flag-off → ProtoCodec).
func TestMeasureQueryResponseCodec_DispatchesOnWireMode(t *testing.T) {
	codec, ok := TopicResponseMap[TopicInternalMeasureQuery]
	if !ok {
		t.Fatal("TopicInternalMeasureQuery is not registered in TopicResponseMap")
	}

	// The wire mode is a process-wide atomic. Save and restore so the test
	// is hermetic and the suite can run in any order.
	prev := MeasureWireModeRaw()
	t.Cleanup(func() { SetMeasureWireModeRaw(prev) })

	// Flag OFF: proto behavior, byte-identical to pre-G9f.0 path for the
	// real measurev1.InternalQueryResponse{} body.
	SetMeasureWireModeRaw(false)
	msg := &measurev1.InternalQueryResponse{}
	body, err := codec.Marshal(msg)
	if err != nil {
		t.Fatalf("flag-off Marshal failed: %v", err)
	}
	decoded, err := codec.Unmarshal(body)
	if err != nil {
		t.Fatalf("flag-off Unmarshal failed: %v", err)
	}
	if _, ok := decoded.(*measurev1.InternalQueryResponse); !ok {
		t.Fatalf("flag-off Unmarshal returned %T; want *measurev1.InternalQueryResponse", decoded)
	}

	// Flag ON: raw passthrough; bytes in == bytes out, 0x00-leading.
	SetMeasureWireModeRaw(true)
	rawBody := []byte{RawFrameMagicLeadingByte, 0x01, 0xAA, 0xBB}
	encoded, err := codec.Marshal(rawBody)
	if err != nil {
		t.Fatalf("flag-on Marshal failed: %v", err)
	}
	if !bytes.Equal(encoded, rawBody) {
		t.Fatalf("flag-on Marshal is not passthrough: got %x want %x", encoded, rawBody)
	}
	out, err := codec.Unmarshal(rawBody)
	if err != nil {
		t.Fatalf("flag-on Unmarshal of 0x00-leading body failed: %v", err)
	}
	outBytes, _ := out.([]byte)
	if !bytes.Equal(outBytes, rawBody) {
		t.Fatalf("flag-on Unmarshal is not passthrough: got %x want %x", outBytes, rawBody)
	}

	// Flag ON + body with the wrong magic now routes to the proto envelope decoder.
	if _, err := codec.Unmarshal([]byte{0xff, 0x02}); err == nil {
		t.Fatal("flag-on Unmarshal of invalid proto envelope returned nil error; want fail-loud")
	}
}

func TestMeasureQueryResponseCodec_RawModeTraceEnvelopeRoundTrip(t *testing.T) {
	codec, ok := TopicResponseMap[TopicInternalMeasureQuery]
	if !ok {
		t.Fatal("TopicInternalMeasureQuery is not registered in TopicResponseMap")
	}
	prev := MeasureWireModeRaw()
	t.Cleanup(func() { SetMeasureWireModeRaw(prev) })
	SetMeasureWireModeRaw(true)

	rawBody := []byte{RawFrameMagicLeadingByte, 0x01, 0xAA, 0xBB}
	envelope := &measurev1.InternalQueryResponse{
		RawFrameBody: rawBody,
		Trace:        &commonv1.Trace{TraceId: "trace-1"},
	}
	encoded, err := codec.Marshal(envelope)
	if err != nil {
		t.Fatalf("raw-mode envelope Marshal failed: %v", err)
	}
	if len(encoded) == 0 || encoded[0] == RawFrameMagicLeadingByte {
		t.Fatalf("raw-mode envelope encoded with raw-frame prefix: %x", encoded)
	}
	decoded, err := codec.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("raw-mode envelope Unmarshal failed: %v", err)
	}
	got, ok := decoded.(*measurev1.InternalQueryResponse)
	if !ok {
		t.Fatalf("raw-mode envelope Unmarshal returned %T; want *measurev1.InternalQueryResponse", decoded)
	}
	if !bytes.Equal(got.GetRawFrameBody(), rawBody) {
		t.Fatalf("RawFrameBody mismatch: got %x want %x", got.GetRawFrameBody(), rawBody)
	}
	if got.GetTrace().GetTraceId() != "trace-1" {
		t.Fatalf("TraceId mismatch: got %q", got.GetTrace().GetTraceId())
	}
}

func TestMeasureQueryResponseCodec_RawModeTraceEmptyEnvelopeRoundTrip(t *testing.T) {
	codec, ok := TopicResponseMap[TopicInternalMeasureQuery]
	if !ok {
		t.Fatal("TopicInternalMeasureQuery is not registered in TopicResponseMap")
	}
	prev := MeasureWireModeRaw()
	t.Cleanup(func() { SetMeasureWireModeRaw(prev) })
	SetMeasureWireModeRaw(true)

	envelope := &measurev1.InternalQueryResponse{Trace: &commonv1.Trace{TraceId: "empty"}}
	encoded, err := codec.Marshal(envelope)
	if err != nil {
		t.Fatalf("raw-mode empty envelope Marshal failed: %v", err)
	}
	decoded, err := codec.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("raw-mode empty envelope Unmarshal failed: %v", err)
	}
	got, ok := decoded.(*measurev1.InternalQueryResponse)
	if !ok {
		t.Fatalf("raw-mode empty envelope Unmarshal returned %T; want *measurev1.InternalQueryResponse", decoded)
	}
	if len(got.GetRawFrameBody()) != 0 {
		t.Fatalf("RawFrameBody length = %d, want 0", len(got.GetRawFrameBody()))
	}
	if got.GetTrace().GetTraceId() != "empty" {
		t.Fatalf("TraceId mismatch: got %q", got.GetTrace().GetTraceId())
	}
}
