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

	"github.com/stretchr/testify/require"
)

func TestStaticTraceIDSourceEmitsLookupRows(t *testing.T) {
	source := NewStaticTraceIDSource([]string{"trace-a", "trace-b"}, map[string]int64{"trace-b": 2}, 1)
	require.NoError(t, source.Init(context.Background()))

	first, err := source.NextBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"trace-a"}, phase1TraceIDs(first).Data())
	require.Equal(t, []int64{0}, phase1Keys(first).Data())
	require.Equal(t, [][]byte{encodeTraceIDPayload("trace-a")}, phase1Payloads(first).Data())

	second, err := source.NextBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"trace-b"}, phase1TraceIDs(second).Data())
	require.Equal(t, []int64{2}, phase1Keys(second).Data())
	require.Equal(t, [][]byte{encodeTraceIDPayload("trace-b")}, phase1Payloads(second).Data())

	done, err := source.NextBatch(context.Background())
	require.NoError(t, err)
	require.Nil(t, done)
	require.NoError(t, source.Close())
}
