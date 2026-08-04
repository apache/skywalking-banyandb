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

package tracefixture

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/sourcecatalog"
)

func TestTraceOffsetsKeepsTightPublicationBoundaryHot(t *testing.T) {
	dayStart := time.Unix(1_700_000_000, 0).UTC()
	fragment := Fragment{SourcePartID: 1, MinTimestamp: 0, MaxTimestamp: 10, Rows: 1}
	plan := Plan{
		DayStart: dayStart, DayDuration: 24 * time.Hour,
		Instances: []Instance{{SourceID: "source", GeneratedID: "generated", Fragments: []Fragment{fragment}}},
		Writes: []Write{{
			Publication: dayStart.Add(23 * time.Hour),
			Fragments: []ScheduledFragment{{
				GeneratedTraceID: "generated", SourceTraceID: "source", InstanceOrdinal: 0, FragmentOrdinal: 0,
			}},
		}},
	}
	lookup := sourceLookup{"source": {SourceID: "source", Fragments: []LoadedFragment{{Fragment: fragment}}}}
	offsets, offsetErr := traceOffsets(plan, lookup, GenerateOptions{
		DayStart: dayStart, DayDuration: 24 * time.Hour, MergeGrace: 2 * time.Hour,
	})
	require.NoError(t, offsetErr)
	frontier := plan.Writes[0].Publication.Add(-2 * time.Hour).UnixNano()
	require.Equal(t, frontier+1, fragment.MaxTimestamp+offsets[0])
}

func TestValidateArtifactSizesUsesConsolidatedDensityAndCombinedIndexGate(t *testing.T) {
	source := Source{IndexCompressedBytes: map[string]uint64{"latency": 1_000, "start_time": 1_000}, Catalog: sourcecatalog.Catalog{
		Core: sourcecatalog.CoreCatalog{TraceCount: 10, RowCount: 100, CompressedBytes: 1_000},
		Indexes: map[string]sourcecatalog.IndexCatalog{
			"latency": {Bytes: 1_000}, "start_time": {Bytes: 1_000},
		},
	}}
	artifact := Artifact{
		TraceCount: 10, RowCount: 100, CoreCompressedBytes: 50_000, CoreConsolidatedBytes: 1_020,
		IndexCompressedBytes:   map[string]uint64{"latency": 50_000, "start_time": 50_000},
		IndexConsolidatedBytes: map[string]uint64{"latency": 920, "start_time": 1_080},
	}
	require.NoError(t, validateArtifactSizes(artifact, source))

	artifact.IndexConsolidatedBytes = map[string]uint64{"latency": 890, "start_time": 1_110}
	require.ErrorContains(t, validateArtifactSizes(artifact, source), "index \"latency\"")

	artifact.IndexConsolidatedBytes = map[string]uint64{"latency": 940, "start_time": 940}
	require.ErrorContains(t, validateArtifactSizes(artifact, source), "index total")
}
