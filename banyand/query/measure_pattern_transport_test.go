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

package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	basequery "github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

type patternMemoryService struct{ measure.Service }

func (*patternMemoryService) VectorizedConfig() vmeasure.VectorizedConfig {
	return vmeasure.VectorizedConfig{QueryMemoryMiB: 1}
}

func TestPatternRequestDecodeBudgetAndCleanup(t *testing.T) {
	processor := &measureInternalQueryProcessor{measureService: &patternMemoryService{}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 1)
	response := processor.Rev(ctx, bus.NewMessage(1, []byte("invalid envelope")))
	require.ErrorContains(t, response.Data().(error), "memory budget exceeded")
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used())
	ctx = vectorized.WithQueryMemoryTracker(t.Context(), 1<<20)
	response = processor.Rev(ctx, bus.NewMessage(2, []byte("invalid envelope")))
	require.ErrorContains(t, response.Data().(error), "invalid measure pattern envelope")
	require.Zero(t, vectorized.QueryMemoryTracker(ctx, 0).Used(), "direct callers release on return")
	ctx, scope := basequery.NewTransportScope(ctx)
	response = processor.Rev(ctx, bus.NewMessage(3, []byte("invalid envelope")))
	require.ErrorContains(t, response.Data().(error), "invalid measure pattern envelope")
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	require.Positive(t, tracker.Used(), "wire body remains charged through transport response completion")
	scope.Close()
	require.Zero(t, tracker.Used())
}

func TestPatternRequestWireCapDoesNotGuaranteeMemoryAdmission(t *testing.T) {
	processor := &measureInternalQueryProcessor{measureService: &patternMemoryService{}}
	ctx := vectorized.WithQueryMemoryTracker(t.Context(), 256<<20)
	tracker := vectorized.QueryMemoryTracker(ctx, 0)
	for _, size := range []int{vmeasure.MaxMeasurePatternRequestBytes, vmeasure.MaxMeasurePatternRequestBytes + 1} {
		body := make([]byte, size)
		response := processor.Rev(ctx, bus.NewMessage(1, body))
		if size > vmeasure.MaxMeasurePatternRequestBytes {
			require.ErrorContains(t, response.Data().(error), "message limit")
		} else {
			require.ErrorContains(t, response.Data().(error), "memory budget exceeded",
				"query policy rejects the decode allowance before parsing the invalid envelope")
		}
		require.Zero(t, tracker.Used())
	}
}
