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

package sidx

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFlushStopsWhenClosed pins that Flush writes no part once its close channel is closed, so a table that is
// shutting down does not wait for a flush it will never introduce.
func TestFlushStopsWhenClosed(t *testing.T) {
	raw := createTestSIDX(t).(*sidx)
	defer func() {
		assert.NoError(t, raw.Close())
	}()
	writeTestDataWithTimeRange(t, raw, []WriteRequest{createTestWriteRequest(1, 100, "data1")}, 1, 1, nil, nil)

	closed := make(chan struct{})
	close(closed)
	flushIntro, err := raw.Flush(closed, map[uint64]struct{}{1: {}})
	require.ErrorIs(t, err, ErrFlushInterrupted)
	require.Nil(t, flushIntro)
	_, statErr := os.Stat(partPath(raw.root, 1))
	require.True(t, os.IsNotExist(statErr), "an interrupted flush must not write the part")
}
