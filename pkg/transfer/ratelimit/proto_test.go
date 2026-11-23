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

package ratelimit_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/ratelimit"
)

// A configured rate goes on the wire rounded down, so the node never runs above it: 90/min is
// 1.5 rows per second and allows 1, not 2 (120/min).
func TestRateLimitProto_RoundsDown(t *testing.T) {
	for _, tc := range []struct {
		rows, bytes string
		wantRows    uint64
		wantBytes   uint64
	}{
		{rows: "90/min", bytes: "1.5KiB/s", wantRows: 1, wantBytes: 1536},
		{rows: "1/s", bytes: "1000/min", wantRows: 1, wantBytes: 16},
		{rows: "7199/h", bytes: "0", wantRows: 1, wantBytes: 0},
		{rows: "0", bytes: "1MiB", wantRows: 0, wantBytes: 1 << 20},
	} {
		rows, err := config.ParseRate(tc.rows, config.RateRows)
		require.NoError(t, err)
		bytesPerSecond, err := config.ParseRate(tc.bytes, config.RateBytes)
		require.NoError(t, err)
		got := ratelimit.Proto(bytesPerSecond, rows)
		require.Equal(t, tc.wantRows, got.GetRowsPerSecond(), tc.rows)
		require.Equal(t, tc.wantBytes, got.GetBytesPerSecond(), tc.bytes)
	}
}
