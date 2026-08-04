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
)

func TestBuildPlanDeterministicPopulationAndWholeFragments(t *testing.T) {
	dayStart := time.Unix(1_700_000_000, 0).UTC()
	mature := []Trace{
		{SourceID: "mature-a", Fragments: []Fragment{{SourcePartID: 1, Rows: 20, MinTimestamp: 10, MaxTimestamp: 20}}},
		{SourceID: "mature-b", Fragments: []Fragment{
			{SourcePartID: 2, Rows: 20, MinTimestamp: 30, MaxTimestamp: 40},
			{SourcePartID: 3, Rows: 20, MinTimestamp: 50, MaxTimestamp: 60},
		}},
		{SourceID: "mature-c", Fragments: []Fragment{{SourcePartID: 4, Rows: 20, MinTimestamp: 70, MaxTimestamp: 80}}},
	}
	small := []Trace{{SourceID: "small-a", Fragments: []Fragment{{SourcePartID: 5, Rows: 20, MinTimestamp: 90, MaxTimestamp: 100}}}}
	options := Options{DayStart: dayStart, DayDuration: 24 * time.Hour, Shapes: DefaultShapes(), CopyCount: 2}

	first, firstErr := BuildPlan(mature, small, options)
	require.NoError(t, firstErr)
	second, secondErr := BuildPlan(mature, small, options)
	require.NoError(t, secondErr)
	require.Equal(t, first, second)
	require.Len(t, first.Instances, 6)

	generated := make(map[string]struct{}, len(first.Instances))
	classCounts := make(map[TraceClass]int)
	for instanceIdx := range first.Instances {
		instance := &first.Instances[instanceIdx]
		require.Len(t, instance.GeneratedID, 36)
		_, exists := generated[instance.GeneratedID]
		require.False(t, exists)
		generated[instance.GeneratedID] = struct{}{}
		classCounts[instance.Class]++
	}
	require.Equal(t, 3, classCounts[TraceClassMature])
	require.Equal(t, 1, classCounts[TraceClassSmall])
	require.Equal(t, 2, classCounts[TraceClassCopy])

	expectedFragments := 0
	for instanceIdx := range first.Instances {
		expectedFragments += len(first.Instances[instanceIdx].Fragments)
	}
	actualFragments := 0
	var previousMinTimestamp int64
	hasPreviousTimestamp := false
	for writeIdx := range first.Writes {
		write := &first.Writes[writeIdx]
		require.NotEmpty(t, write.Fragments)
		seenInWrite := make(map[string]struct{}, len(write.Fragments))
		for fragmentIdx := range write.Fragments {
			scheduled := &write.Fragments[fragmentIdx]
			traceID := scheduled.GeneratedTraceID
			_, exists := seenInWrite[traceID]
			require.False(t, exists, "trace fragment must be scheduled in a later write")
			seenInWrite[traceID] = struct{}{}
			fragment := &first.Instances[scheduled.InstanceOrdinal].Fragments[scheduled.FragmentOrdinal]
			if hasPreviousTimestamp {
				require.GreaterOrEqual(t, fragment.MinTimestamp, previousMinTimestamp)
			}
			previousMinTimestamp = fragment.MinTimestamp
			hasPreviousTimestamp = true
		}
		actualFragments += len(write.Fragments)
		if writeIdx < len(first.Writes)-1 {
			require.False(t, write.PartialTail)
		}
	}
	require.Equal(t, expectedFragments, actualFragments)
	require.True(t, first.Writes[len(first.Writes)-1].PartialTail)
}

func TestBuildPlanUsesIndependentStableHashForCopies(t *testing.T) {
	mature := make([]Trace, 0, 20)
	for traceIdx := 0; traceIdx < 20; traceIdx++ {
		mature = append(mature, Trace{
			SourceID:  string(rune('a' + traceIdx)),
			Fragments: []Fragment{{SourcePartID: uint64(traceIdx + 1), Rows: 100, MinTimestamp: int64(traceIdx), MaxTimestamp: int64(traceIdx)}},
		})
	}
	selected := selectCopyTemplates(mature, 5)
	shuffled := append([]Trace(nil), mature...)
	for leftIdx, rightIdx := 0, len(shuffled)-1; leftIdx < rightIdx; leftIdx, rightIdx = leftIdx+1, rightIdx-1 {
		shuffled[leftIdx], shuffled[rightIdx] = shuffled[rightIdx], shuffled[leftIdx]
	}
	selectedAfterShuffle := selectCopyTemplates(shuffled, 5)
	require.Equal(t, selected, selectedAfterShuffle)
}

func TestBuildPlanPublishesEvenlyAcrossHalfOpenDay(t *testing.T) {
	dayStart := time.Unix(1_700_000_000, 0).UTC()
	fragments := make([]Fragment, 12)
	for fragmentIdx := range fragments {
		fragments[fragmentIdx] = Fragment{Rows: 101, MinTimestamp: int64(fragmentIdx), MaxTimestamp: int64(fragmentIdx)}
	}
	fragments[len(fragments)-1].Rows += 2
	mature := []Trace{{SourceID: "a", Fragments: fragments}}
	plan, planErr := BuildPlan(mature, nil, Options{DayStart: dayStart, DayDuration: 24 * time.Hour, Shapes: DefaultShapes()[:1]})
	require.NoError(t, planErr)
	require.Len(t, plan.Writes, 12)
	for writeIdx := range plan.Writes {
		write := &plan.Writes[writeIdx]
		require.Equal(t, dayStart.Add(time.Duration(writeIdx)*2*time.Hour), write.Publication)
		require.True(t, write.Publication.Before(dayStart.Add(24*time.Hour)))
	}
}
