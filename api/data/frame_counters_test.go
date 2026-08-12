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

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
)

// The counters exist to let a soak or an operator assert that the native columnar
// frame is genuinely carrying traffic. Their whole value is that they are zero when
// it is not, so both directions are pinned here: a frame body must move them, and a
// flag-off process must leave them alone.
//
// Encode and decode are counted at different layers because the wire path is not
// symmetric. The liaison decodes through ResponseCodec.Unmarshal, so the decode
// count lives in the codec; the data node hands its already-encoded frame straight
// to the transport and never calls ResponseCodec.Marshal, so the encode count lives
// in the queue's marshalResponse and is exercised via IncrFrameEncoded. Counting
// encode at the codec looked right and reported a flat zero on a cluster that was
// demonstrably emitting frames.

func TestStreamFrameCounters_CountRawTrafficOnly(t *testing.T) {
	c := newStreamCodec()
	frame := []byte{RawFrameMagicLeadingByte, 0x01, 0x02, 0x03}

	// Flag OFF: a proto response must not be counted as frame traffic. This is the
	// standalone case, where the data node never emits a frame at all.
	SetStreamWireModeRaw(false)
	encBefore, decBefore := StreamFrameEncodedCount(), StreamFrameDecodedCount()
	body, err := c.Marshal(&streamv1.QueryResponse{})
	require.NoError(t, err)
	_, err = c.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, encBefore, StreamFrameEncodedCount(), "proto encode must not count as a frame")
	require.Equal(t, decBefore, StreamFrameDecodedCount(), "proto decode must not count as a frame")

	// Flag ON with a frame body: the decode counts, and the send path's
	// IncrFrameEncoded counts the matching encode.
	SetStreamWireModeRaw(true)
	defer SetStreamWireModeRaw(false)
	encBefore, decBefore = StreamFrameEncodedCount(), StreamFrameDecodedCount()
	body, err = c.Marshal(frame)
	require.NoError(t, err)
	_, err = c.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, decBefore+1, StreamFrameDecodedCount())
	IncrFrameEncoded(TopicStreamQuery)
	require.Equal(t, encBefore+1, StreamFrameEncodedCount())

	// Flag ON but a proto body still travels the proto path (the fallback a traced
	// query or an unsupported shape takes), so it must not be counted either.
	encBefore, decBefore = StreamFrameEncodedCount(), StreamFrameDecodedCount()
	body, err = c.Marshal(&streamv1.QueryResponse{})
	require.NoError(t, err)
	_, err = c.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, encBefore, StreamFrameEncodedCount(), "proto fallback must not count as a frame")
	require.Equal(t, decBefore, StreamFrameDecodedCount(), "proto fallback must not count as a frame")
}

func TestTraceFrameCounters_CountRawTrafficOnly(t *testing.T) {
	c := &traceQueryResponseCodec{
		proto: NewProtoCodec(func() proto.Message { return &tracev1.InternalQueryResponse{} }),
		raw:   NewRawFrameCodec(),
	}
	frame := []byte{RawFrameMagicLeadingByte, 0x0a, 0x0b}

	SetTraceWireModeRaw(false)
	encBefore, decBefore := TraceFrameEncodedCount(), TraceFrameDecodedCount()
	body, err := c.Marshal(&tracev1.InternalQueryResponse{})
	require.NoError(t, err)
	_, err = c.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, encBefore, TraceFrameEncodedCount(), "proto encode must not count as a frame")
	require.Equal(t, decBefore, TraceFrameDecodedCount(), "proto decode must not count as a frame")

	SetTraceWireModeRaw(true)
	defer SetTraceWireModeRaw(false)
	encBefore, decBefore = TraceFrameEncodedCount(), TraceFrameDecodedCount()
	body, err = c.Marshal(frame)
	require.NoError(t, err)
	_, err = c.Unmarshal(body)
	require.NoError(t, err)
	require.Equal(t, decBefore+1, TraceFrameDecodedCount())
	IncrFrameEncoded(TopicTraceQuery)
	require.Equal(t, encBefore+1, TraceFrameEncodedCount())
}

// TestIncrFrameEncoded_IgnoresNonFrameTopic guards the default branch: only the
// three topics that actually carry a columnar frame may move a counter.
func TestIncrFrameEncoded_IgnoresNonFrameTopic(t *testing.T) {
	before := MeasureFrameEncodedCount() + StreamFrameEncodedCount() + TraceFrameEncodedCount()
	IncrFrameEncoded(TopicStreamWrite)
	after := MeasureFrameEncodedCount() + StreamFrameEncodedCount() + TraceFrameEncodedCount()
	require.Equal(t, before, after, "a non-frame topic must not be counted as frame traffic")
}
