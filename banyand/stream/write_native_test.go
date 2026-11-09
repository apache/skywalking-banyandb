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

package stream

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// TestWriteCallbackKeepsNativeWritesOffNativeStorage is the falsifying
// assertion for the feedback protection on the write path. The callback logs
// a refused write: for a native log write that line must reach the console
// and not native storage, where storing it would refuse and log again. The
// same refusal for any other write is stored as usual.
func TestWriteCallbackKeepsNativeWritesOffNativeStorage(t *testing.T) {
	printed := len(console.String())
	sink := test.NativeLoggingAtWarn(t)

	w := &writeCallback{l: logger.GetLogger("stream")}
	malformed := bus.NewMessage(1, "not a batch")

	w.Rev(logger.WithNativeWrite(context.Background()), malformed)
	require.Eventually(t, func() bool { return strings.Contains(console.String()[printed:], "invalid event data type") },
		5*time.Second, 10*time.Millisecond, "the refused native write was not reported on the console")
	if n := sink.Count(); n != 0 {
		t.Fatalf("the refused native write was stored natively %d times", n)
	}

	w.Rev(context.Background(), malformed)
	if n := sink.Count(); n != 1 {
		t.Fatalf("an ordinary refused write was stored %d times, want 1", n)
	}
}

// refusedEvent is a write the callbacks refuse before they read any schema or
// queue: a timestamp finer than a millisecond. Each logs "cannot handle write
// event" about it.
func refusedEvent(group string) bus.Message {
	return bus.NewMessage(1, []any{&streamv1.InternalWriteRequest{Request: &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: group, Name: "log"},
		Element:  &streamv1.ElementValue{Timestamp: timestamppb.New(time.Unix(1, 1)), TagFamilies: []*modelv1.TagFamilyForWrite{}},
	}}})
}

// TestWriteCallbackKeepsReceivedLogEventsOffNativeStorage covers a data node
// that receives the log events of another process, such as backup or
// lifecycle, over its cluster server. They arrive without the mark the
// producer's context carried. What the callback logs about one must still
// reach the console and not native storage; the same refusal for an event of
// another group is stored as usual.
func TestWriteCallbackKeepsReceivedLogEventsOffNativeStorage(t *testing.T) {
	printed := len(console.String())
	sink := test.NativeLoggingAtWarn(t)

	w := &writeCallback{l: logger.GetLogger("stream")}

	w.Rev(context.Background(), refusedEvent(logger.NativeLogGroup))
	require.Eventually(t, func() bool { return strings.Contains(console.String()[printed:], "cannot handle write event") },
		5*time.Second, 10*time.Millisecond, "the refused log event was not reported on the console")
	if n := sink.Count(); n != 0 {
		t.Fatalf("the refusal of a received log event was stored natively %d times", n)
	}

	w.Rev(context.Background(), refusedEvent("sw_metric"))
	if n := sink.Count(); n != 1 {
		t.Fatalf("the refusal of an ordinary event was stored %d times, want 1", n)
	}
}

// TestWriteQueueCallbackKeepsLogEventsOffNativeStorage covers a liaison's
// write queue. A log event arrives over the cluster server without the mark
// its context carried on the sending liaison. What the callback logs about it
// must still reach the console and not native storage, where it would be
// written, and refused, again; the same refusal for an event of another group
// is stored as usual.
func TestWriteQueueCallbackKeepsLogEventsOffNativeStorage(t *testing.T) {
	printed := len(console.String())
	sink := test.NativeLoggingAtWarn(t)

	w := &writeQueueCallback{l: logger.GetLogger("stream")}

	w.Rev(context.Background(), refusedEvent(logger.NativeLogGroup))
	require.Eventually(t, func() bool { return strings.Contains(console.String()[printed:], "cannot handle write event") },
		5*time.Second, 10*time.Millisecond, "the refused log event was not reported on the console")
	if n := sink.Count(); n != 0 {
		t.Fatalf("the refusal of a received log event was stored natively %d times", n)
	}

	w.Rev(context.Background(), refusedEvent("sw_metric"))
	if n := sink.Count(); n != 1 {
		t.Fatalf("the refusal of an ordinary event was stored %d times, want 1", n)
	}
}
