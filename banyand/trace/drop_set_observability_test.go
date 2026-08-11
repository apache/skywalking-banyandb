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

	"github.com/apache/skywalking-banyandb/api/common"
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

// TestFinalizeStateGaugesReportTerminalShards asserts the scan pre-filter
// publishes every cooled shard's finalize state, including shards it then skips.
// A terminal shard is precisely the one an operator needs to see — it can never
// delete another trace (spec section 5.2) — and it is also the one no later stage
// of the scan visits, so this pre-filter is the only place it stays visible.
func TestFinalizeStateGaugesReportTerminalShards(t *testing.T) {
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

	require.True(t, sr.segmentMayWarrantObserved("g1", lfs, shardPaths, cfg),
		"shard-0 still warrants, so the segment must be reopened")

	rounds := gatherMetric(t, reg, "finalize_rounds")
	require.Len(t, rounds, 2, "every shard must report, not only the warranting one")
	byShard := map[string]float64{}
	for _, m := range rounds {
		byShard[metricLabelValue(m, "shard")] = m.GetGauge().GetValue()
	}
	require.Equal(t, float64(2), byShard["0"])
	require.Equal(t, float64(cfg.maxRounds), byShard["1"])

	terminal := gatherMetric(t, reg, "finalize_terminal")
	require.Len(t, terminal, 2)
	byShardTerminal := map[string]float64{}
	for _, m := range terminal {
		byShardTerminal[metricLabelValue(m, "shard")] = m.GetGauge().GetValue()
	}
	require.Equal(t, float64(0), byShardTerminal["0"])
	require.Equal(t, float64(1), byShardTerminal["1"],
		"a shard at max_finalize_rounds is terminal and must report as such")
}

// TestSegmentMayWarrantObservedMatchesUnobserved asserts adding telemetry did not
// change the pre-filter's decision, which is the one thing it must not do: it is a
// conservative superset of warrantsFinalize and skipping a warranting segment
// would silently stop finalization.
func TestSegmentMayWarrantObservedMatchesUnobserved(t *testing.T) {
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
			sr := &schemaRepo{samplerMeter: newSamplerMetrics(nil)}
			require.Equal(t, segmentMayWarrant(lfs, shardPaths, cfg),
				sr.segmentMayWarrantObserved("g1", lfs, shardPaths, cfg))
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
		pipelineTracesRetainedByCeiling: factory.NewCounter("pipeline_traces_retained_by_ceiling", common.ShardLabelNames()...),
		pipelineMergesCeilingReached:    factory.NewCounter("pipeline_merges_ceiling_reached", append(common.ShardLabelNames(), "lane")...),
		pipelineDropSetBudgetBytes:      factory.NewGauge("pipeline_drop_set_budget_bytes"),
		pipelineDropSetEntries: factory.NewHistogram("pipeline_drop_set_entries", dropSetEntryBuckets,
			append(common.ShardLabelNames(), "lane")...),
	}
}

// TestDropSetInstrumentsCarryShardAndLane asserts the four drop-set instruments
// emit with the label arity they were declared with, and that the ceiling counters
// carry seg/shard. A wrong label count panics inside the meter, so this test is the
// guard for that; the existing ceiling tests run with nil metrics and cannot catch
// it.
func TestDropSetInstrumentsCarryShardAndLane(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{
		metrics: newDropSetMetricsForTest(reg),
		p:       common.Position{Database: "g1", Segment: "seg-1", Shard: "3"},
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
	require.Equal(t, "3", metricLabelValue(entries[0], "shard"))
	require.Equal(t, mergeLaneFinalize, metricLabelValue(entries[0], "lane"))

	retained := gatherMetric(t, reg, "pipeline_traces_retained_by_ceiling")
	require.Len(t, retained, 1)
	require.Equal(t, float64(7), retained[0].GetCounter().GetValue())
	require.Equal(t, "3", metricLabelValue(retained[0], "shard"),
		"the ceiling counter must localize which shard is under-deleting")
	require.Equal(t, "seg-1", metricLabelValue(retained[0], "seg"))

	merges := gatherMetric(t, reg, "pipeline_merges_ceiling_reached")
	require.Len(t, merges, 1)
	require.Equal(t, float64(1), merges[0].GetCounter().GetValue())
	require.Equal(t, "3", metricLabelValue(merges[0], "shard"))
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
		p:       common.Position{Database: "g1", Segment: "seg-1", Shard: "0"},
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
		p:       common.Position{Database: "g1", Segment: "seg-1", Shard: "0"},
	}

	tst.observeDropSetUsage(0, 500, mergeLaneFast)

	require.Nil(t, gatherMetric(t, reg, "pipeline_drop_set_budget_bytes"))
	require.Nil(t, gatherMetric(t, reg, "pipeline_drop_set_entries"))
}

// TestShardLabelFromPath pins the label derivation, including the fallback for an
// unexpected directory layout: an empty label would silently merge every shard's
// series into one.
func TestShardLabelFromPath(t *testing.T) {
	require.Equal(t, "0", shardLabelFromPath("/data/measure-default/seg-20240101/shard-0"))
	require.Equal(t, "17", shardLabelFromPath("shard-17"))
	require.Equal(t, "unexpected", shardLabelFromPath("/data/unexpected"))
}
