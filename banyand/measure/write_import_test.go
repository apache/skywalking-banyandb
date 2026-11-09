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

package measure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
)

func TestDecodeWriteEventSkipsTopNOnlyForImportedWrites(t *testing.T) {
	iwr := &measurev1.InternalWriteRequest{ShardId: 3}
	raw, err := proto.Marshal(iwr)
	require.NoError(t, err)

	got, skip, err := decodeWriteEvent(&data.ImportedMeasureWrite{InternalWriteRequest: iwr})
	require.NoError(t, err)
	require.Same(t, iwr, got)
	require.True(t, skip, "an imported write never reaches the Top-N processors")

	got, skip, err = decodeWriteEvent(iwr)
	require.NoError(t, err)
	require.Same(t, iwr, got)
	require.False(t, skip, "a regular write reaches them")

	got, skip, err = decodeWriteEvent(raw)
	require.NoError(t, err)
	require.True(t, proto.Equal(iwr, got))
	require.False(t, skip, "a write from another node reaches them")

	_, _, err = decodeWriteEvent(42)
	require.Error(t, err)
}

func TestWriteCallbackRecordsTheRowsItCannotWrite(t *testing.T) {
	// A timestamp finer than a millisecond fails before the write reaches any storage.
	badTime := &measurev1.InternalWriteRequest{Request: &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: "sw_metric", Name: "cpm"},
		DataPoint: &measurev1.DataPointValue{Timestamp: timestamppb.New(time.Unix(1_800_000_000, 1))},
	}}
	events := []any{&data.ImportedMeasureWrite{InternalWriteRequest: badTime}, []byte{0xff}, 42}
	w := &writeCallback{l: logger.GetLogger("test-measure-write")}

	recorder := common.NewWriteRecorder(time.Time{})
	reply := w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "the reply stays as it was; the failures go to the recorder")
	require.Empty(t, recorder.Failures(), "no row was stored, so nothing failed after storing")
	failures := recorder.RowFailures()
	require.Len(t, failures, 3)
	for i, want := range []string{"measure sw_metric/cpm: invalid timestamp", "measure: cannot decode write event", "invalid event data type int"} {
		require.Equal(t, i, failures[i].Row, "the failure is recorded by the row's index in the message")
		require.ErrorContains(t, failures[i].Err, want)
	}

	reply = w.Rev(context.Background(), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "a write without a recorder only logs the rows it drops")
}

func TestImportedBatchIsAnsweredWithTheRowsTheWriteDropped(t *testing.T) {
	badTime := &measurev1.InternalWriteRequest{Request: &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: "sw_metric", Name: "cpm"},
		DataPoint: &measurev1.DataPointValue{Timestamp: timestamppb.New(time.Unix(1_800_000_000, 1))},
	}}
	frames := []any{
		&transferv1.ImportWrite{Init: &transferv1.ImportWriteInit{}},
		&transferv1.ImportWrite{Row: &transferv1.ImportWrite_Measure{Measure: badTime}},
	}
	l := transfer.ImportListener("test-node", &writeCallback{l: logger.GetLogger("test-measure-import")})
	// The row is written again until the stream ends; a short stream keeps the test fast.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ce, ok := l.Rev(ctx, bus.NewMessage(1, frames)).Data().(*common.Error)
	require.True(t, ok, "the import is answered the row the write path dropped")
	require.Contains(t, ce.Error(), "1 of 1 imported row(s) were not written after retries (rows 0), the first: measure sw_metric/cpm: invalid timestamp")
}
