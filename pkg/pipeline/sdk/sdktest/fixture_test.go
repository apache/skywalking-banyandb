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

package sdktest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

func TestTraceBuilder_SingleRowBroadcast(t *testing.T) {
	block, err := sdktest.NewTrace("t1").
		Tag("duration", int64(120)).
		Tag("status", "success").
		TS(100, 200).
		Build()
	require.NoError(t, err)

	require.Equal(t, "t1", block.TraceID)
	assert.Equal(t, int64(100), block.MinTS)
	assert.Equal(t, int64(200), block.MaxTS)

	durCol := block.Tag("duration")
	require.NotNil(t, durCol)
	durVal, decErr := durCol.At(0)
	require.NoError(t, decErr)
	assert.Equal(t, int64(120), durVal.Int64())

	statusCol := block.Tag("status")
	require.NotNil(t, statusCol)
	statusVal, decErr := statusCol.At(0)
	require.NoError(t, decErr)
	assert.Equal(t, "success", statusVal.Str())
}

func TestTraceBuilder_MultiRowViaSpanID(t *testing.T) {
	block, err := sdktest.NewTrace("t1").
		SpanID("s1").
		SpanID("s2").
		SpanID("s3").
		Tag("duration", int64(50)). // broadcast to all 3 rows
		Build()
	require.NoError(t, err)

	require.Equal(t, []string{"s1", "s2", "s3"}, block.SpanIDs)
	durCol := block.Tag("duration")
	require.NotNil(t, durCol)
	require.Len(t, durCol.Values, 3)
	for i := range 3 {
		v, decErr := durCol.At(i)
		require.NoError(t, decErr)
		assert.Equal(t, int64(50), v.Int64())
	}
}

func TestTraceBuilder_MultiRowPerRowTagValues(t *testing.T) {
	block, err := sdktest.NewTrace("t1").
		SpanID("s1").
		SpanID("s2").
		Tag("status", "ok").
		Tag("status", "error").
		Build()
	require.NoError(t, err)

	statusCol := block.Tag("status")
	require.NotNil(t, statusCol)
	v0, decErr := statusCol.At(0)
	require.NoError(t, decErr)
	assert.Equal(t, "ok", v0.Str())
	v1, decErr := statusCol.At(1)
	require.NoError(t, decErr)
	assert.Equal(t, "error", v1.Str())
}

func TestTraceBuilder_TagAsDisambiguatesTimestamp(t *testing.T) {
	block, err := sdktest.NewTrace("t1").
		TagAs("startedAt", valuetype.ValueTypeTimestamp, int64(1_700_000_000_000_000_000)).
		Build()
	require.NoError(t, err)

	col := block.Tag("startedAt")
	require.NotNil(t, col)
	assert.Equal(t, valuetype.ValueTypeTimestamp, col.ValueType)
	v, decErr := col.At(0)
	require.NoError(t, decErr)
	assert.Equal(t, int64(1_700_000_000_000_000_000), v.Int64())
}

func TestTraceBuilder_InferenceErrorSurfacesAtBuild(t *testing.T) {
	_, err := sdktest.NewTrace("t1").Tag("bad", nil).Build()
	require.Error(t, err, "nil value cannot be inferred; TagAs is required")
}

func TestTraceBuilder_TypeMismatchSurfacesAtBuild(t *testing.T) {
	_, err := sdktest.NewTrace("t1").
		TagAs("duration", valuetype.ValueTypeInt64, "not an int64").
		Build()
	require.Error(t, err)
}

func TestTraceBuilder_Span(t *testing.T) {
	block, err := sdktest.NewTrace("t1").
		Span([]byte("span-body-1")).
		Span([]byte("span-body-2")).
		Build()
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("span-body-1"), []byte("span-body-2")}, block.Spans)
}

func TestBatch(t *testing.T) {
	b1, err := sdktest.NewTrace("a").Build()
	require.NoError(t, err)
	b2, err := sdktest.NewTrace("b").Build()
	require.NoError(t, err)

	batch := sdktest.Batch(b1, b2)
	require.Len(t, batch.Traces, 2)
	assert.Equal(t, "a", batch.Traces[0].TraceID)
	assert.Equal(t, "b", batch.Traces[1].TraceID)
}
