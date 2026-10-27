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

package trace

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestWriteCallbackRecordsTheRowsItCannotWrite(t *testing.T) {
	// Rows that cannot be decoded fail before the write reaches any storage.
	events := []any{[]byte{0xff}, 42}
	w := &writeCallback{l: logger.GetLogger("test-trace-write")}

	recorder := common.NewWriteRecorder(time.Time{})
	reply := w.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "the reply stays as it was; the failures go to the recorder")
	require.Empty(t, recorder.Failures(), "no row was stored, so nothing failed after storing")
	failures := recorder.RowFailures()
	require.Len(t, failures, 2)
	for i, want := range []string{"trace: cannot decode write event", "trace: invalid event data type int"} {
		require.Equal(t, i, failures[i].Row, "the failure is recorded by the row's index in the message")
		require.ErrorContains(t, failures[i].Err, want)
	}

	reply = w.Rev(context.Background(), bus.NewMessage(1, events))
	require.Nil(t, reply.Data(), "a write without a recorder only logs the rows it drops")
}
