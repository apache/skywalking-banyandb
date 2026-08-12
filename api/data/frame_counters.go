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
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// Which wire format a query response actually traveled on is otherwise
// invisible from outside the process: the mode is a per-process atomic set at
// startup, and the frame branches are plain code paths that emit no signal. That
// gap is not academic — a 48h soak reported green while never exercising the frame
// at all, because it ran a standalone server, where the data node always falls back
// to protobuf. Nothing in the run could have revealed that.
//
// These counters make the frame path assert its own presence. They are incremented
// at the single choke point per direction (the codec Marshal/Unmarshal raw
// branches), so a non-zero encode count on a data node and a non-zero decode count
// on a liaison together prove the columnar frame is carrying real traffic.
// banyand/observability/services publishes them; a test or soak can also read them
// in-process through the getters below.
//
// They are cumulative and monotonic for the life of the process.
var (
	measureFrameEncoded atomic.Int64
	measureFrameDecoded atomic.Int64
	streamFrameEncoded  atomic.Int64
	streamFrameDecoded  atomic.Int64
	traceFrameEncoded   atomic.Int64
	traceFrameDecoded   atomic.Int64
)

// MeasureFrameEncodedCount returns how many measure responses this process has
// emitted as a native columnar frame rather than protobuf.
func MeasureFrameEncodedCount() int64 { return measureFrameEncoded.Load() }

// MeasureFrameDecodedCount returns how many measure responses this process has
// decoded from a native columnar frame rather than protobuf.
func MeasureFrameDecodedCount() int64 { return measureFrameDecoded.Load() }

// StreamFrameEncodedCount returns how many stream responses this process has
// emitted as a native columnar frame rather than protobuf.
func StreamFrameEncodedCount() int64 { return streamFrameEncoded.Load() }

// StreamFrameDecodedCount returns how many stream responses this process has
// decoded from a native columnar frame rather than protobuf.
func StreamFrameDecodedCount() int64 { return streamFrameDecoded.Load() }

// TraceFrameEncodedCount returns how many trace responses this process has emitted
// as a native columnar frame rather than protobuf.
func TraceFrameEncodedCount() int64 { return traceFrameEncoded.Load() }

// TraceFrameDecodedCount returns how many trace responses this process has decoded
// from a native columnar frame rather than protobuf.
func TraceFrameDecodedCount() int64 { return traceFrameDecoded.Load() }

// IncrFrameEncoded records that a native columnar frame body for this topic was
// put on the wire. It is called from the queue's response-marshal path rather
// than from a codec: the per-topic ResponseCodec.Marshal methods are only reached
// by in-process callers, while the real data-node send path hands the already
// encoded []byte straight to the transport, so counting at the codec would report
// zero frames on a cluster that is in fact emitting them.
func IncrFrameEncoded(t bus.Topic) {
	switch t {
	case TopicInternalMeasureQuery:
		measureFrameEncoded.Add(1)
	case TopicStreamQuery:
		streamFrameEncoded.Add(1)
	case TopicTraceQuery:
		traceFrameEncoded.Add(1)
	default:
	}
}
