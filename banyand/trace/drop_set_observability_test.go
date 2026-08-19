// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// gatherMetric returns the collected families whose name ends in suffix, so a
// test does not have to hard-code the scope prefix the provider prepends.
func gatherMetric(t *testing.T, reg *prometheus.Registry, suffix string) []*dto.Metric {
	t.Helper()
	families, gatherErr := reg.Gather()
	require.NoError(t, gatherErr)
	for _, family := range families {
		if len(family.GetName()) >= len(suffix) && family.GetName()[len(family.GetName())-len(suffix):] == suffix {
			return family.GetMetric()
		}
	}
	return nil
}

// metricLabelValue returns the value of the named label on a collected metric.
func metricLabelValue(m *dto.Metric, name string) string {
	for _, pair := range m.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

// TestFinalizeStateGaugesReportGroupSummary asserts the scan pre-filter rolls
// shard state up to one group-level worst-case summary.
func TestFinalizeStateGaugesReportGroupSummary(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	lfs := fs.NewLocalFileSystem()

	cfg := finalizeConfig{floorBytes: 1, ratio: 0.1, maxRounds: 8}
	// shard-0 is mid-life and still warrants; shard-1 has exhausted its rounds and
	// is therefore terminal even though its Terminal flag was never written.
	shardPaths := []string{filepath.Join(tmpPath, "shard-0"), filepath.Join(tmpPath, "shard-1")}
	lfs.MkdirIfNotExist(shardPaths[0], 0o755)
	lfs.MkdirIfNotExist(shardPaths[1], 0o755)
	require.NoError(t, writeFinalizeState(lfs, shardPaths[0], finalizeState{FinalizeRounds: 2}))
	require.NoError(t, writeFinalizeState(lfs, shardPaths[1], finalizeState{FinalizeRounds: cfg.maxRounds}))

	reg := prometheus.NewRegistry()
	factory := services.NewFactory(prom.NewProvider(pipelineScope, reg), nil, nil)
	sr := &schemaRepo{samplerMeter: newSamplerMetrics(factory)}

	warrants, summary := segmentMayWarrantSummary(lfs, shardPaths, cfg)
	require.True(t, warrants,
		"shard-0 still warrants, so the segment must be reopened")
	sr.samplerMeter.observeFinalizeState("g1", summary.maxRounds, summary.terminal)

	rounds := gatherMetric(t, reg, "finalize_rounds")
	require.Len(t, rounds, 1)
	require.Equal(t, "g1", metricLabelValue(rounds[0], "group"))
	require.Empty(t, metricLabelValue(rounds[0], "seg"))
	require.Empty(t, metricLabelValue(rounds[0], "shard"))
	require.Equal(t, float64(cfg.maxRounds), rounds[0].GetGauge().GetValue())

	terminal := gatherMetric(t, reg, "finalize_terminal")
	require.Len(t, terminal, 1)
	require.Equal(t, "g1", metricLabelValue(terminal[0], "group"))
	require.Empty(t, metricLabelValue(terminal[0], "seg"))
	require.Empty(t, metricLabelValue(terminal[0], "shard"))
	require.Equal(t, float64(1), terminal[0].GetGauge().GetValue(),
		"a terminal shard must make the group-level any-terminal gauge true")
}

// TestSegmentMayWarrantSummaryMatchesDecision asserts summary collection does
// not change the conservative segment pre-filter decision.
func TestSegmentMayWarrantSummaryMatchesDecision(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()
	lfs := fs.NewLocalFileSystem()
	cfg := finalizeConfig{floorBytes: 1, ratio: 0.1, maxRounds: 8}

	for _, tt := range []struct {
		name   string
		states []finalizeState
	}{
		{name: "all-terminal", states: []finalizeState{{Terminal: true}, {FinalizeRounds: cfg.maxRounds}}},
		{name: "one-warrants", states: []finalizeState{{Terminal: true}, {FinalizeRounds: 1}}},
		{name: "never-finalized", states: []finalizeState{{}, {}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := filepath.Join(tmpPath, tt.name)
			shardPaths := make([]string, len(tt.states))
			for idx, st := range tt.states {
				shardPaths[idx] = filepath.Join(base, "shard-"+string(rune('0'+idx)))
				lfs.MkdirIfNotExist(shardPaths[idx], 0o755)
				require.NoError(t, writeFinalizeState(lfs, shardPaths[idx], st))
			}
			warrants, _ := segmentMayWarrantSummary(lfs, shardPaths, cfg)
			require.Equal(t, segmentMayWarrant(lfs, shardPaths, cfg), warrants)
		})
	}
}

// newDropSetMetricsForTest builds a metrics value carrying only the drop-set
// instruments, backed by a real Prometheus provider. The rest of the struct stays
// zero: the helpers under test touch nothing else, and using real instruments is
// the point — the meter panics on a label-count mismatch, which a bypass registry
// would silently accept.
func newDropSetMetricsForTest(reg *prometheus.Registry) *metrics {
	factory := services.NewFactory(prom.NewProvider(tbScope, reg), nil, nil)
	return &metrics{
		pipelineTracesRetainedByCeiling: factory.NewCounter("pipeline_traces_retained_by_ceiling"),
		pipelineMergesCeilingReached:    factory.NewCounter("pipeline_merges_ceiling_reached", "lane"),
		pipelineDropSetBudgetBytes:      factory.NewGauge("pipeline_drop_set_budget_bytes"),
		pipelineDropSetEntries:          factory.NewHistogram("pipeline_drop_set_entries", dropSetEntryBuckets, "lane"),
	}
}

// TestDropSetInstrumentsUseGroupAndLane asserts drop-set instruments do not
// create segment- or shard-level series. The table factory supplies group as a
// constant label, while lane remains dynamic where merge behavior differs.
func TestDropSetInstrumentsUseGroupAndLane(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{
		metrics: newDropSetMetricsForTest(reg),
	}

	tst.observeDropSetUsage(32<<20, 1234, mergeLaneFinalize)
	tst.incPipelineTracesRetainedByCeiling(7)
	tst.incPipelineMergesCeilingReached(mergeLaneFinalize)

	budget := gatherMetric(t, reg, "pipeline_drop_set_budget_bytes")
	require.Len(t, budget, 1)
	require.Equal(t, float64(32<<20), budget[0].GetGauge().GetValue())

	entries := gatherMetric(t, reg, "pipeline_drop_set_entries")
	require.Len(t, entries, 1)
	require.Equal(t, uint64(1), entries[0].GetHistogram().GetSampleCount())
	require.Equal(t, float64(1234), entries[0].GetHistogram().GetSampleSum())
	require.Empty(t, metricLabelValue(entries[0], "seg"))
	require.Empty(t, metricLabelValue(entries[0], "shard"))
	require.Equal(t, mergeLaneFinalize, metricLabelValue(entries[0], "lane"))

	retained := gatherMetric(t, reg, "pipeline_traces_retained_by_ceiling")
	require.Len(t, retained, 1)
	require.Equal(t, float64(7), retained[0].GetCounter().GetValue())
	require.Empty(t, metricLabelValue(retained[0], "seg"))
	require.Empty(t, metricLabelValue(retained[0], "shard"))

	merges := gatherMetric(t, reg, "pipeline_merges_ceiling_reached")
	require.Len(t, merges, 1)
	require.Equal(t, float64(1), merges[0].GetCounter().GetValue())
	require.Empty(t, metricLabelValue(merges[0], "seg"))
	require.Empty(t, metricLabelValue(merges[0], "shard"))
	require.Equal(t, mergeLaneFinalize, metricLabelValue(merges[0], "lane"))
}

// TestDropSetUsageObservedWhenUncapped asserts an uncapped merge still reports its
// drop-set size. This is the whole point of the histogram: the ceiling counters are
// a lagging indicator that only fire after deletion has been lost, so headroom has
// to be visible on merges that did not cap.
func TestDropSetUsageObservedWhenUncapped(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{
		metrics: newDropSetMetricsForTest(reg),
	}

	tst.observeDropSetUsage(32<<20, 10, mergeLaneFast)

	entries := gatherMetric(t, reg, "pipeline_drop_set_entries")
	require.Len(t, entries, 1, "an uncapped merge must still report its drop-set size")
	require.Equal(t, uint64(1), entries[0].GetHistogram().GetSampleCount())
	require.Nil(t, gatherMetric(t, reg, "pipeline_merges_ceiling_reached"),
		"no ceiling was reached, so the lagging counter must stay silent")
}

// TestDropSetUsageSkippedWhenUnlimited asserts a filter with no budget emits
// nothing: a zero gauge would read as "the ceiling is zero bytes" on a dashboard
// rather than "there is no ceiling".
func TestDropSetUsageSkippedWhenUnlimited(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{
		metrics: newDropSetMetricsForTest(reg),
	}

	tst.observeDropSetUsage(0, 500, mergeLaneFast)

	require.Nil(t, gatherMetric(t, reg, "pipeline_drop_set_budget_bytes"))
	require.Nil(t, gatherMetric(t, reg, "pipeline_drop_set_entries"))
}
