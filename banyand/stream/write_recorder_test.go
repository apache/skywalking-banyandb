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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

func TestWriteCallbackRecordsTheRowsItCannotWrite(t *testing.T) {
	// A timestamp finer than a millisecond fails before the write reaches any storage.
	badTime := &streamv1.InternalWriteRequest{Request: &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: "default", Name: "sw"},
		Element:  &streamv1.ElementValue{Timestamp: timestamppb.New(time.Unix(1_800_000_000, 1))},
	}}
	events := []any{badTime, []byte{0xff}, 42}
	w := &writeCallback{l: logger.GetLogger("test-stream-write")}

	recorder := common.NewWriteRecorder(time.Time{})
	reply := w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "the reply stays as it was; the failures go to the recorder")
	require.Empty(t, recorder.Failures(), "no row was stored, so nothing failed after storing")
	failures := recorder.RowFailures()
	require.Len(t, failures, 3)
	for i, want := range []string{"stream default/sw: invalid timestamp", "stream: cannot decode write event", "stream: invalid event data type int"} {
		require.Equal(t, i, failures[i].Row, "the failure is recorded by the row's index in the message")
		require.ErrorContains(t, failures[i].Err, want)
	}

	reply = w.Rev(context.Background(), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "a write without a recorder only logs the rows it drops")
}

func TestImportedBatchIsAnsweredWithTheRowsTheWriteDropped(t *testing.T) {
	badTime := &streamv1.InternalWriteRequest{Request: &streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Group: "default", Name: "sw"},
		Element:  &streamv1.ElementValue{Timestamp: timestamppb.New(time.Unix(1_800_000_000, 1))},
	}}
	frames := []any{
		&transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{}},
		&transferv1.ImportWrite{Row: &transferv1.ImportWrite_Stream{Stream: badTime}},
	}
	l := transfer.ImportListener("test-node", &writeCallback{l: logger.GetLogger("test-stream-import")})
	// The row is written again until the stream ends; a short stream keeps the test fast.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ce, ok := l.Rev(ctx, bus.NewMessage(1, frames)).Data().(*common.Error)
	require.True(t, ok, "the import is answered the row the write path dropped")
	require.Contains(t, ce.Error(), "1 of 1 imported row(s) were not written after retries (rows 0), the first: stream default/sw: invalid timestamp")
}
