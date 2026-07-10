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

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

// honestSampler projects "duration" and reads only "duration" — its verdict
// must be identical whether or not "status" is also present in the batch.
type honestSampler struct{}

func (honestSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (honestSampler) Project() sdk.Projection { return sdk.Projection{Tags: []string{"duration"}} }
func (honestSampler) Close() error            { return nil }
func (honestSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = true
		col := batch.Traces[i].Tag("duration")
		if col == nil {
			continue
		}
		v, err := col.At(0)
		if err == nil && !v.IsNull() && v.Int64() < 100 {
			keep[i] = false
		}
	}
	return sdk.Verdict{Keep: keep}, nil
}

// dishonestSampler projects ONLY "duration" but its Decide also reads
// "status" — a column it never declared. In production the engine would not
// materialize "status" for this sampler, so its real-world decision would
// differ from what this fixture (which carries "status") produces.
type dishonestSampler struct{}

func (dishonestSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (dishonestSampler) Project() sdk.Projection { return sdk.Projection{Tags: []string{"duration"}} }
func (dishonestSampler) Close() error            { return nil }
func (dishonestSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = true
		// Reads "status" despite never projecting it.
		if col := batch.Traces[i].Tag("status"); col != nil {
			if v, err := col.At(0); err == nil && !v.IsNull() && v.Str() == "error" {
				keep[i] = false
			}
		}
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestRun_HonestSamplerNoDivergence(t *testing.T) {
	block, err := sdktest.NewTrace("t1").Tag("duration", int64(50)).Tag("status", "success").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(block)

	verdict, report := sdktest.Run(honestSampler{}, batch)
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	assert.Empty(t, report.ProjectionDivergedIDs, "a sampler that only reads its own projection must never diverge")
	assert.Equal(t, []bool{false}, verdict.Keep)
}

func TestRun_DishonestSamplerFlagged(t *testing.T) {
	block, err := sdktest.NewTrace("t1").Tag("duration", int64(500)).Tag("status", "error").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(block)

	verdict, report := sdktest.Run(dishonestSampler{}, batch)
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	require.Contains(t, report.ProjectionDivergedIDs, "t1",
		"a sampler that reads an unprojected column must be flagged by the differential guard")
	// The primary (all-columns) run sees "status"=="error" and drops the trace.
	assert.Equal(t, []bool{false}, verdict.Keep)
}

func TestRun_DecideErrorPropagates(t *testing.T) {
	block, err := sdktest.NewTrace("t1").Build()
	require.NoError(t, err)
	batch := sdktest.Batch(block)

	erroringSampler := erroringSamplerType{}
	_, report := sdktest.Run(erroringSampler, batch)
	require.Error(t, report.Err)
}

// escapedStrArrSampler is an HONEST sampler: it projects "labels" and reads
// only "labels". It drops a trace whose first label element equals "a|b".
type escapedStrArrSampler struct{}

func (escapedStrArrSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (escapedStrArrSampler) Project() sdk.Projection { return sdk.Projection{Tags: []string{"labels"}} }
func (escapedStrArrSampler) Close() error            { return nil }
func (escapedStrArrSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = true
		col := batch.Traces[i].Tag("labels")
		if col == nil {
			continue
		}
		v, decErr := col.At(0)
		if decErr != nil || v.IsNull() {
			continue
		}
		arr := v.StrArr()
		if len(arr) > 0 && arr[0] == "a|b" {
			keep[i] = false
		}
	}
	return sdk.Verdict{Keep: keep}, nil
}

// TestRun_EscapedStrArrayNoFalseDivergence is the regression guard for the
// shared-backing-array bug: DecodeTagValue's str-array path decodes IN PLACE
// (vararray.UnmarshalVarArray mutates its src), so if Run's two Decide calls
// shared a TagColumn's Values, the first run would corrupt the escaped
// str-array element the second run reads, producing a spurious "projection
// divergence" and a wrong verdict on the second run. With per-run deep copies
// the honest sampler decides identically both times and the guard reports no
// divergence. The elements deliberately contain the delimiter '|' and escape
// '\' bytes, which are exactly what UnmarshalVarArray rewrites in place.
func TestRun_EscapedStrArrayNoFalseDivergence(t *testing.T) {
	block, err := sdktest.NewTrace("t1").Tag("labels", []string{"a|b", `c\d`, "e|f"}).Build()
	require.NoError(t, err)
	batch := sdktest.Batch(block)

	verdict, report := sdktest.Run(escapedStrArrSampler{}, batch)
	require.NoError(t, report.Err)
	require.NoError(t, report.ProjectionErr)
	assert.Empty(t, report.ProjectionDivergedIDs,
		"an honest sampler reading an escaped str-array must not be flagged (no shared-backing corruption)")
	assert.Equal(t, []bool{false}, verdict.Keep, "labels[0]==\"a|b\" must drop the trace on both runs")

	// The caller's batch must be untouched by Run (both Decide runs use deep
	// copies): re-decoding labels[0] from the original still yields "a|b".
	col := batch.Traces[0].Tag("labels")
	require.NotNil(t, col)
	v, decErr := col.At(0)
	require.NoError(t, decErr)
	require.Equal(t, "a|b", v.StrArr()[0], "Run must not mutate the caller's batch backing arrays")
}

type erroringSamplerType struct{}

func (erroringSamplerType) Kind() sdk.Kind          { return sdk.KindSampler }
func (erroringSamplerType) Project() sdk.Projection { return sdk.Projection{} }
func (erroringSamplerType) Close() error            { return nil }
func (erroringSamplerType) Decide(*sdk.TraceBatch) (sdk.Verdict, error) {
	return sdk.Verdict{}, assertError{}
}

type assertError struct{}

func (assertError) Error() string { return "decide failed" }
