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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// highMem is a fake protector.Memory that always reports a configurable state.
type highMem struct {
	protector.Nop
	state protector.State
}

// State returns the fixed scripted state.
func (h highMem) State() protector.State {
	return h.state
}

// TestSyncSeriesCallback_HandleFileChunkBusy verifies that the series chunk
// handler sheds load by returning queue.ErrServerBusy when memory is high,
// before touching any segment or streamer state.
func TestSyncSeriesCallback_HandleFileChunkBusy(t *testing.T) {
	cb := &syncSeriesCallback{l: logger.GetLogger("test")}
	// seriesCtx has a nil segment; the BUSY check must fire first so the
	// nil-segment path is never reached.
	seriesCtx := &syncSeriesContext{
		l:  logger.GetLogger("test"),
		pm: highMem{state: protector.StateHigh},
	}
	ctx := &queue.ChunkedSyncPartContext{Handler: seriesCtx, FileName: "series.idx"}
	err := cb.HandleFileChunk(ctx, []byte("payload"))
	require.Error(t, err)
	assert.ErrorIs(t, err, queue.ErrServerBusy)
}

// TestSyncSeriesCallback_HandleFileChunkNilHandler verifies the early nil
// handler guard still applies.
func TestSyncSeriesCallback_HandleFileChunkNilHandler(t *testing.T) {
	cb := &syncSeriesCallback{l: logger.GetLogger("test")}
	ctx := &queue.ChunkedSyncPartContext{}
	err := cb.HandleFileChunk(ctx, []byte("payload"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, queue.ErrServerBusy)
}
