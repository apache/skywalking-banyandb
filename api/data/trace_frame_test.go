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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

func newTraceCodec() *traceQueryResponseCodec {
	return &traceQueryResponseCodec{
		proto: NewProtoCodec(func() proto.Message { return &tracev1.InternalQueryResponse{} }),
		raw:   NewRawFrameCodec(),
	}
}

// TestTraceCodec_RawPassthrough verifies that flag-on the codec passes a
// columnar frame ([]byte) through unchanged on both encode and decode.
func TestTraceCodec_RawPassthrough(t *testing.T) {
	SetTraceWireModeRaw(true)
	defer SetTraceWireModeRaw(false)
	c := newTraceCodec()

	frame := []byte{RawFrameMagicLeadingByte, 0x01, 0x02, 0x03}
	body, err := c.Marshal(frame)
	require.NoError(t, err)
	require.Equal(t, frame, body, "raw frame must pass through Marshal unchanged")

	got, err := c.Unmarshal(body)
	require.NoError(t, err)
	decoded, ok := got.([]byte)
	require.True(t, ok, "raw-mode Unmarshal of a magic-prefixed body must return []byte")
	require.Equal(t, frame, decoded, "raw frame must pass through Unmarshal unchanged")
}

// TestTraceCodec_RawEmptyBody verifies that flag-on, a nil/empty body (a
// legitimate empty distributed result) falls through to the proto path, yielding
// an empty *tracev1.InternalQueryResponse rather than being magic-validated.
func TestTraceCodec_RawEmptyBody(t *testing.T) {
	SetTraceWireModeRaw(true)
	defer SetTraceWireModeRaw(false)
	c := newTraceCodec()

	got, err := c.Unmarshal(nil)
	require.NoError(t, err)
	decoded, ok := got.(*tracev1.InternalQueryResponse)
	require.True(t, ok)
	require.Empty(t, decoded.GetInternalTraces())
}

// TestTraceCodec_FlagOffIsProto verifies flag-off encodes byte-identically to
// plain proto.Marshal and round-trips through the proto path.
func TestTraceCodec_FlagOffIsProto(t *testing.T) {
	SetTraceWireModeRaw(false)
	c := newTraceCodec()
	orig := &tracev1.InternalQueryResponse{
		InternalTraces: []*tracev1.InternalTrace{
			{TraceId: "trace-1", Key: -42, Spans: []*tracev1.Span{{SpanId: "span-1", Span: []byte("payload-1")}}},
		},
	}

	body, err := c.Marshal(orig)
	require.NoError(t, err)
	want, err := proto.Marshal(orig)
	require.NoError(t, err)
	require.Equal(t, want, body, "flag-off must encode byte-identically to proto.Marshal")

	got, err := c.Unmarshal(body)
	require.NoError(t, err)
	require.True(t, proto.Equal(orig, got.(*tracev1.InternalQueryResponse)))
}

// TestTraceCodec_FlagOnProtoFallback verifies that flag-on, a proto.Message
// value (e.g. the tracing path) still goes through the proto path.
func TestTraceCodec_FlagOnProtoFallback(t *testing.T) {
	SetTraceWireModeRaw(true)
	defer SetTraceWireModeRaw(false)
	c := newTraceCodec()
	orig := &tracev1.InternalQueryResponse{}

	body, err := c.Marshal(orig)
	require.NoError(t, err)
	want, err := proto.Marshal(orig)
	require.NoError(t, err)
	require.Equal(t, want, body)

	got, err := c.Unmarshal(body)
	require.NoError(t, err)
	require.True(t, proto.Equal(orig, got.(*tracev1.InternalQueryResponse)))
}
