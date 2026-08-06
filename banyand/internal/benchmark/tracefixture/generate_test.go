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
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/sourcecatalog"
)

func TestCanonicalLedgerChecksumIgnoresMapAndRowOrder(t *testing.T) {
	rowA := sha256.Sum256([]byte("a"))
	rowB := sha256.Sum256([]byte("b"))
	first := ledgerHashes{"trace-b": {rowB}, "trace-a": {rowB, rowA}}
	second := ledgerHashes{"trace-a": {rowA, rowB}, "trace-b": {rowB}}
	require.Equal(t, canonicalLedgerChecksum(first), canonicalLedgerChecksum(second))

	second["trace-b"] = append(second["trace-b"], rowA)
	require.NotEqual(t, canonicalLedgerChecksum(first), canonicalLedgerChecksum(second))
}

func TestTraceOffsetsAlignsLatestTimestampWithPublication(t *testing.T) {
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
	require.Equal(t, plan.Writes[0].Publication.UnixNano(), fragment.MaxTimestamp+offsets[0])
}

func TestTraceOffsetsSpreadsTimestampsAcrossPublicationDay(t *testing.T) {
	dayStart := time.Unix(1_700_000_000, 0).UTC()
	fragments := []Fragment{
		{SourcePartID: 1, MinTimestamp: 0, MaxTimestamp: 10, Rows: 1},
		{SourcePartID: 2, MinTimestamp: 20, MaxTimestamp: 30, Rows: 1},
		{SourcePartID: 3, MinTimestamp: 40, MaxTimestamp: 50, Rows: 1},
	}
	plan := Plan{
		DayStart: dayStart, DayDuration: 24 * time.Hour,
		Instances: []Instance{
			{SourceID: "source-a", GeneratedID: "generated-a", Fragments: fragments[:1]},
			{SourceID: "source-b", GeneratedID: "generated-b", Fragments: fragments[1:2]},
			{SourceID: "source-c", GeneratedID: "generated-c", Fragments: fragments[2:]},
		},
		Writes: []Write{
			fixtureWrite(dayStart, "generated-a", "source-a", 0),
			fixtureWrite(dayStart.Add(12*time.Hour), "generated-b", "source-b", 1),
			fixtureWrite(dayStart.Add(24*time.Hour-time.Second), "generated-c", "source-c", 2),
		},
	}
	lookup := sourceLookup{
		"source-a": {SourceID: "source-a", Fragments: []LoadedFragment{{Fragment: fragments[0]}}},
		"source-b": {SourceID: "source-b", Fragments: []LoadedFragment{{Fragment: fragments[1]}}},
		"source-c": {SourceID: "source-c", Fragments: []LoadedFragment{{Fragment: fragments[2]}}},
	}
	offsets, offsetErr := traceOffsets(plan, lookup, GenerateOptions{
		DayStart: dayStart, DayDuration: 24 * time.Hour, MergeGrace: 2 * time.Hour,
	})
	require.NoError(t, offsetErr)
	for instanceIdx := range plan.Instances {
		mappedMax := fragments[instanceIdx].MaxTimestamp + offsets[instanceIdx]
		require.Equal(t, plan.Writes[instanceIdx].Publication.UnixNano(), mappedMax)
	}
	require.Equal(t, 24*time.Hour-time.Second, time.Duration(
		fragments[2].MaxTimestamp+offsets[2]-(fragments[0].MaxTimestamp+offsets[0])))
}

func fixtureWrite(publication time.Time, generatedID, sourceID string, instanceOrdinal int) Write {
	return Write{Publication: publication, Fragments: []ScheduledFragment{{
		GeneratedTraceID: generatedID, SourceTraceID: sourceID, InstanceOrdinal: instanceOrdinal, FragmentOrdinal: 0,
	}}}
}

func TestValidateArtifactSizesUsesConsolidatedDensityAndCombinedIndexGate(t *testing.T) {
	source := Source{IndexCompressedBytes: map[string]uint64{"latency": 1_000, "start_time": 1_000}, Catalog: sourcecatalog.Catalog{
		Core: sourcecatalog.CoreCatalog{TraceCount: 10, RowCount: 100, CompressedBytes: 1_000},
		Indexes: map[string]sourcecatalog.IndexCatalog{
			"latency": {Bytes: 1_000}, "start_time": {Bytes: 1_000},
		},
	}}
	artifact := Artifact{
		WriteIntensity: 1,
		TraceCount:     10, RowCount: 100, CoreCompressedBytes: 50_000, CoreConsolidatedBytes: 1_020,
		IndexCompressedBytes:   map[string]uint64{"latency": 50_000, "start_time": 50_000},
		IndexConsolidatedBytes: map[string]uint64{"latency": 920, "start_time": 1_080},
	}
	require.NoError(t, validateArtifactSizes(artifact, source))

	artifact.IndexConsolidatedBytes = map[string]uint64{"latency": 890, "start_time": 1_110}
	require.ErrorContains(t, validateArtifactSizes(artifact, source), "index \"latency\"")

	artifact.IndexConsolidatedBytes = map[string]uint64{"latency": 940, "start_time": 940}
	require.ErrorContains(t, validateArtifactSizes(artifact, source), "index total")

	artifact = Artifact{
		WriteIntensity: 2,
		TraceCount:     20, RowCount: 200, CoreCompressedBytes: 100_000, CoreConsolidatedBytes: 2_040,
		IndexCompressedBytes:   map[string]uint64{"latency": 100_000, "start_time": 100_000},
		IndexConsolidatedBytes: map[string]uint64{"latency": 1_840, "start_time": 2_160},
	}
	require.NoError(t, validateArtifactSizes(artifact, source))
}
