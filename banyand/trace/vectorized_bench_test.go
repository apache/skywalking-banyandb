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

	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// BenchmarkRowBasedVsVectorized_Lookup compares the original row-based goroutine path
// (startBlockScanStage → queryResult) against the new vectorized pull path
// (buildVectorizedScanBatch → vectorizedTraceQueryResult) for traceID lookup queries.
// Run with: go test -bench=BenchmarkRowBasedVsVectorized_Lookup -benchmem ./banyand/trace/.
func BenchmarkRowBasedVsVectorized_Lookup(b *testing.B) {
	tst, cleanup := newParityTSTable(b, tsTS1, tsTS2)
	defer cleanup()

	tr := newParityTrace()
	tables := []*tsTable{tst}
	qo := queryOptions{
		TraceQueryOptions: model.TraceQueryOptions{TagProjection: allTagProjections},
		schemaTagTypes:    testSchemaTagTypes,
	}
	traceIDs := []string{"trace1", "trace2", "trace3"}

	b.Run("RowBased", func(b *testing.B) {
		b.ResetTimer()
		for benchIdx := 0; benchIdx < b.N; benchIdx++ {
			ctx := context.Background()
			pushRes := pushLookupResult(ctx, tr, tables, qo, traceIDs, 0)
			for {
				r := pushRes.Pull()
				if r == nil {
					break
				}
			}
			b.StopTimer()
			pushRes.Release()
			b.StartTimer()
		}
	})

	b.Run("Vectorized", func(b *testing.B) {
		b.ResetTimer()
		for benchIdx := 0; benchIdx < b.N; benchIdx++ {
			ctx := context.Background()
			pullRes, pullErr := pullLookupResult(ctx, tr, tables, qo, traceIDs, 0)
			require.NoError(b, pullErr)
			for {
				r := pullRes.Pull()
				if r == nil {
					break
				}
			}
			b.StopTimer()
			pullRes.Release()
			b.StartTimer()
		}
	})
}
