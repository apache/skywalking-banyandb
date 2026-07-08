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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
)

// TestBuildHotMergeFilter_MergeEventGate verifies that the hot merge filter is applied
// only when the MERGE event is enabled for the group — even though the sampler set is
// shared with FINALIZE (DD11). A FINALIZE-only group (samplers registered, MERGE event
// off) must NOT filter hot merges.
func TestBuildHotMergeFilter_MergeEventGate(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "merge-gate-group"
	replaceSamplersForGroup(group, []namedSampler{{name: "s", sampler: &dummySampler{}}})
	tst := &tsTable{group: group, option: option{nativePipelineEnabled: true, mergeGraceDefault: time.Second}}
	// An ancient (non-hot) part so hotness never masks the gate.
	parts := []*partWrapper{{p: &part{partMetadata: partMetadata{MaxTimestamp: 1, TotalCount: 1}}}}

	// FINALIZE-only: samplers registered, MERGE event disabled → no hot-merge filter.
	setMergeEventForGroup(group, false)
	assert.Nil(t, tst.buildHotMergeFilter(parts), "FINALIZE-only group must not filter hot merges")

	// MERGE enabled → the filter is built.
	setMergeEventForGroup(group, true)
	assert.NotNil(t, tst.buildHotMergeFilter(parts), "MERGE-enabled group must filter hot merges")
}

// TestFinalizeEventEnabled_Matrix verifies the finalize event gate. Unlike MERGE,
// an empty enabled_events list must NOT default finalize on.
func TestFinalizeEventEnabled_Matrix(t *testing.T) {
	tests := []struct {
		name   string
		events []commonv1.PipelineEvent
		want   bool
	}{
		{"empty defaults off", nil, false},
		{"merge only", []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE}, false},
		{"finalize only", []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE}, true},
		{"both", []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_MERGE, commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &commonv1.TracePipelineConfig{EnabledEvents: tt.events}
			assert.Equal(t, tt.want, finalizeEventEnabled(cfg))
		})
	}
	// mergeEventEnabled semantics unchanged: empty defaults on.
	assert.True(t, mergeEventEnabled(&commonv1.TracePipelineConfig{}), "empty events must default MERGE on")
}

// TestSetFinalizeGrace_RoundTripAndDeleteOnZero mirrors the merge_grace registry
// contract: a positive value round-trips, zero deletes.
func TestSetFinalizeGrace_RoundTripAndDeleteOnZero(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "fg-group"
	assert.Zero(t, lookupFinalizeGrace(group), "unset group must return 0")

	setFinalizeGraceForGroup(group, int64(7*time.Minute))
	assert.Equal(t, int64(7*time.Minute), lookupFinalizeGrace(group))

	setFinalizeGraceForGroup(group, 0)
	assert.Zero(t, lookupFinalizeGrace(group), "zero must delete the entry")
}

// TestFinalizeConfig_Defaults verifies lookupFinalizeConfig fills unset fields with
// package defaults and that the cooldown falls back to the supplied finalize_grace.
func TestFinalizeConfig_Defaults(t *testing.T) {
	resetRegistries()
	defer resetRegistries()

	const group = "fc-group"
	const graceNs = int64(5 * time.Minute)

	// Unset: all defaults; cooldown == grace.
	got := lookupFinalizeConfig(group, graceNs)
	assert.Equal(t, finalizeFloorBytesDefault, got.floorBytes)
	assert.InDelta(t, finalizeRatioDefault, got.ratio, 1e-9)
	assert.Equal(t, graceNs, got.cooldownNs, "unset cooldown must fall back to finalize_grace")
	assert.Equal(t, finalizeMaxRoundsDefault, got.maxRounds)

	// Explicit overrides are respected.
	setFinalizeConfigForGroup(group, &finalizeConfig{
		floorBytes: 1 << 20,
		ratio:      0.25,
		cooldownNs: int64(90 * time.Second),
		maxRounds:  3,
	})
	got = lookupFinalizeConfig(group, graceNs)
	assert.Equal(t, uint64(1<<20), got.floorBytes)
	assert.InDelta(t, 0.25, got.ratio, 1e-9)
	assert.Equal(t, int64(90*time.Second), got.cooldownNs)
	assert.Equal(t, 3, got.maxRounds)

	// nil removes the override -> back to defaults.
	setFinalizeConfigForGroup(group, nil)
	got = lookupFinalizeConfig(group, graceNs)
	assert.Equal(t, finalizeFloorBytesDefault, got.floorBytes)
}

// TestReconcilePipeline_FinalizeGrace verifies reconcilePipeline stores finalize_grace
// and a finalize config entry, and clears them on a nil config.
func TestReconcilePipeline_FinalizeGrace(t *testing.T) {
	requirePlugin(t)

	resetRegistries()
	defer resetRegistries()

	const group = "finalize-grace-group"

	sp, spErr := makeSamplerPlugin(pluginTestSoName, 100)
	require.NoError(t, spErr)

	const wantGrace = 8 * time.Minute
	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{Name: "lss", Kind: &commonv1.Plugin_Sampler{Sampler: sp}},
		},
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE},
		FinalizeGrace: durationpb.New(wantGrace),
	}

	sr := makeDataSchemaRepo(pluginTestDir)
	sr.reconcilePipeline(group, cfg)

	assert.Equal(t, wantGrace.Nanoseconds(), lookupFinalizeGrace(group),
		"lookupFinalizeGrace must return the configured value in nanoseconds")

	// After nil config, finalize grace must be cleared.
	sr.reconcilePipeline(group, nil)
	assert.Zero(t, lookupFinalizeGrace(group), "finalize grace must be 0 after nil config")
}

// TestReconcilePipeline_FinalizeOnlyRegistersSamplers verifies the DD11 behavior:
// a FINALIZE-only config (no MERGE) with a valid plugin now REGISTERS the group's
// sampler set (finalize reuses the same samplers as merge).
func TestReconcilePipeline_FinalizeOnlyRegistersSamplers(t *testing.T) {
	requirePlugin(t)

	resetRegistries()
	defer resetRegistries()

	const group = "finalize-only-registers"

	sp, spErr := makeSamplerPlugin(pluginTestSoName, 100)
	require.NoError(t, spErr)

	cfg := &commonv1.TracePipelineConfig{
		Enabled: true,
		Plugins: []*commonv1.Plugin{
			{Name: "lss", Kind: &commonv1.Plugin_Sampler{Sampler: sp}},
		},
		EnabledEvents: []commonv1.PipelineEvent{commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE},
	}

	sr := makeDataSchemaRepo(pluginTestDir)
	sr.reconcilePipeline(group, cfg)

	require.NotEmpty(t, lookupSamplers(group),
		"a FINALIZE-only config must register the shared sampler set (DD11)")
}
