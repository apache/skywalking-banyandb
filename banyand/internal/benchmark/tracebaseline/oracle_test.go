// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type oracleTestSampler struct{}

func (oracleTestSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (oracleTestSampler) Project() sdk.Projection { return sdk.Projection{} }
func (oracleTestSampler) Close() error            { return nil }
func (oracleTestSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		keep[traceIdx] = batch.Traces[traceIdx].TraceID != "drop"
	}
	return sdk.Verdict{Keep: keep}, nil
}

func TestEvaluateSamplingOracleProducesStableVerdicts(t *testing.T) {
	blocks := []sdk.TraceBlock{{TraceID: "keep"}, {TraceID: "drop"}}
	artifact, dropped, evaluateErr := evaluateSamplingOracle(context.Background(), blocks, oracleTestSampler{})

	require.NoError(t, evaluateErr)
	require.Equal(t, uint64(2), artifact.Evaluated)
	require.Equal(t, uint64(1), artifact.Retained)
	require.Equal(t, uint64(1), artifact.Dropped)
	require.NotEmpty(t, artifact.VerdictSHA256)
	require.Equal(t, map[string]struct{}{"drop": {}}, dropped)
}
