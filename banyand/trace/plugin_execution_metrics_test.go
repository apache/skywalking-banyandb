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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package trace

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

type releaseThenPanicSampler struct {
	release <-chan struct{}
}

func (rtps *releaseThenPanicSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (rtps *releaseThenPanicSampler) Project() sdk.Projection { return sdk.Projection{} }
func (rtps *releaseThenPanicSampler) Close() error            { return nil }
func (rtps *releaseThenPanicSampler) Decide(*sdk.TraceBatch) (sdk.Verdict, error) {
	<-rtps.release
	panic("late panic")
}

func newPluginExecutionMetricsForTest(reg *prometheus.Registry) *metrics {
	factory := services.NewFactory(prom.NewProvider(tbScope.ConstLabels(meter.LabelPairs{"group": "g1"}), reg), nil, nil)
	return &metrics{
		pipelinePluginBatches:         factory.NewCounter("pipeline_plugin_batches", "result"),
		pipelinePluginExecutions:      factory.NewCounter("pipeline_plugin_executions", "plugin_name", "result"),
		pipelinePluginDurationSeconds: factory.NewHistogram("pipeline_plugin_execution_duration_seconds", pipelinePluginDurationBuckets, "plugin_name", "result"),
		pipelinePluginBatchTraces:     factory.NewHistogram("pipeline_plugin_batch_traces", pipelinePluginBatchTraceBuckets, "result"),
		pipelinePluginLinkBypasses:    factory.NewCounter("pipeline_plugin_link_bypasses", "plugin_name", "reason"),
	}
}

func TestPluginExecutionMetricsObserveSuccessfulBatch(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{metrics: newPluginExecutionMetricsForTest(reg)}
	chain := newNamedMergeChain("g1", "", []namedSampler{
		{name: "latency", sampler: &fakeSampler{}},
		{name: "probabilistic", sampler: &fakeSampler{}},
	}, 3)
	chain.observeExecution = tst.observePipelinePluginExecution
	chain.observeLinkExecution = tst.observePipelinePluginLinkExecution
	defer chain.close()

	batch := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 3)}
	verdict, executeErr := chain.Execute(batch, time.Second)
	require.NoError(t, executeErr)
	require.Len(t, verdict.Keep, 3)

	batches := gatherMetric(t, reg, "pipeline_plugin_batches")
	require.Len(t, batches, 1)
	require.Equal(t, pluginExecutionResultSuccess, metricLabelValue(batches[0], "result"))
	require.Equal(t, float64(1), batches[0].GetCounter().GetValue())
	require.Equal(t, "g1", metricLabelValue(batches[0], "group"))
	require.Empty(t, metricLabelValue(batches[0], "seg"))
	require.Empty(t, metricLabelValue(batches[0], "shard"))

	durations := gatherMetric(t, reg, "pipeline_plugin_execution_duration_seconds")
	require.Len(t, durations, 2)
	observedPlugins := make(map[string]struct{}, len(durations))
	for _, duration := range durations {
		observedPlugins[metricLabelValue(duration, "plugin_name")] = struct{}{}
		require.Equal(t, pluginExecutionResultSuccess, metricLabelValue(duration, "result"))
		require.Equal(t, "g1", metricLabelValue(duration, "group"))
		require.Empty(t, metricLabelValue(duration, "seg"))
		require.Empty(t, metricLabelValue(duration, "shard"))
		require.Equal(t, uint64(1), duration.GetHistogram().GetSampleCount())
		require.GreaterOrEqual(t, duration.GetHistogram().GetSampleSum(), float64(0))
	}
	require.Equal(t, map[string]struct{}{"latency": {}, "probabilistic": {}}, observedPlugins)

	executions := gatherMetric(t, reg, "pipeline_plugin_executions")
	require.Len(t, executions, 2)
	for _, execution := range executions {
		require.Contains(t, observedPlugins, metricLabelValue(execution, "plugin_name"))
		require.Equal(t, pluginExecutionResultSuccess, metricLabelValue(execution, "result"))
		require.Equal(t, float64(1), execution.GetCounter().GetValue())
	}

	batchTraces := gatherMetric(t, reg, "pipeline_plugin_batch_traces")
	require.Len(t, batchTraces, 1)
	require.Equal(t, uint64(1), batchTraces[0].GetHistogram().GetSampleCount())
	require.Equal(t, float64(3), batchTraces[0].GetHistogram().GetSampleSum())
}

func TestPluginExecutionMetricsDefaultNamesSkipInactiveSamplers(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{metrics: newPluginExecutionMetricsForTest(reg)}
	chain := newMergeChain("g1", "", []sdk.Sampler{nil, &fakeSampler{}}, 3)
	chain.observeLinkExecution = tst.observePipelinePluginLinkExecution
	defer chain.close()

	_, executeErr := chain.Execute(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{}}}, time.Second)
	require.NoError(t, executeErr)

	executions := gatherMetric(t, reg, "pipeline_plugin_executions")
	require.Len(t, executions, 1)
	require.Equal(t, "plugin_1", metricLabelValue(executions[0], "plugin_name"))
}

func TestPluginExecutionMetricsClassifyTimeoutAndLinkBypass(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{metrics: newPluginExecutionMetricsForTest(reg)}

	bypassChain := newNamedMergeChain("g1", "", []namedSampler{{name: "broken", sampler: &fakeSampler{errNow: true}}}, 3)
	bypassChain.observeExecution = tst.observePipelinePluginExecution
	bypassChain.observeLinkExecution = tst.observePipelinePluginLinkExecution
	_, bypassErr := bypassChain.Execute(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 2)}, time.Second)
	require.NoError(t, bypassErr, "a failed link must fail open without failing the whole chain")
	bypassChain.close()

	timeoutChain := newNamedMergeChain("g1", "", []namedSampler{{name: "slow", sampler: &sleepSampler{d: 10 * time.Millisecond}}}, 1)
	timeoutChain.observeExecution = tst.observePipelinePluginExecution
	timeoutChain.observeLinkExecution = tst.observePipelinePluginLinkExecution
	_, timeoutErr := timeoutChain.Execute(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{}}}, time.Millisecond)
	require.Error(t, timeoutErr)
	_, circuitErr := timeoutChain.Execute(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 4)}, time.Millisecond)
	require.NoError(t, circuitErr, "an open circuit must fail open without invoking the plugin")
	timeoutChain.close()
	time.Sleep(20 * time.Millisecond)

	bypasses := gatherMetric(t, reg, "pipeline_plugin_link_bypasses")
	require.Len(t, bypasses, 1)
	require.Equal(t, "broken", metricLabelValue(bypasses[0], "plugin_name"))
	require.Equal(t, sdk.BypassReasonDecideError, metricLabelValue(bypasses[0], "reason"))
	require.Equal(t, float64(1), bypasses[0].GetCounter().GetValue())

	batches := gatherMetric(t, reg, "pipeline_plugin_batches")
	require.Len(t, batches, 3)
	results := make(map[string]float64, len(batches))
	for _, batchMetric := range batches {
		results[metricLabelValue(batchMetric, "result")] = batchMetric.GetCounter().GetValue()
	}
	require.Equal(t, float64(1), results[pluginExecutionResultSuccess])
	require.Equal(t, float64(1), results[pluginExecutionResultTimeout])
	require.Equal(t, float64(1), results[pluginExecutionResultCircuitOpen])

	executions := gatherMetric(t, reg, "pipeline_plugin_executions")
	require.Len(t, executions, 2)
	linkResults := make(map[string]string, len(executions))
	for _, execution := range executions {
		linkResults[metricLabelValue(execution, "plugin_name")] = metricLabelValue(execution, "result")
	}
	require.Equal(t, pluginExecutionResultDecideError, linkResults["broken"])
	require.Equal(t, pluginExecutionResultLate, linkResults["slow"])
}

func TestPluginExecutionMetricsClassifyLinkFailures(t *testing.T) {
	testCases := []struct {
		sampler sdk.Sampler
		name    string
		result  string
		reason  string
	}{
		{name: "panic", sampler: &fakeSampler{panicNow: true}, result: pluginExecutionResultPanic, reason: sdk.BypassReasonPanic},
		{name: "length mismatch", sampler: &fakeSampler{wrongSize: true}, result: pluginExecutionResultMismatch, reason: sdk.BypassReasonLengthMismatch},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			tst := &tsTable{metrics: newPluginExecutionMetricsForTest(reg)}
			chain := newNamedMergeChain("g1", "", []namedSampler{{name: "broken", sampler: testCase.sampler}}, 3)
			chain.observeLinkExecution = tst.observePipelinePluginLinkExecution
			defer chain.close()

			_, executeErr := chain.Execute(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 2)}, time.Second)
			require.NoError(t, executeErr, "a failed link must fail open without failing the whole chain")

			executions := gatherMetric(t, reg, "pipeline_plugin_executions")
			require.Len(t, executions, 1)
			require.Equal(t, testCase.result, metricLabelValue(executions[0], "result"))
			bypasses := gatherMetric(t, reg, "pipeline_plugin_link_bypasses")
			require.Len(t, bypasses, 1)
			require.Equal(t, testCase.reason, metricLabelValue(bypasses[0], "reason"))
		})
	}
}

func TestPluginExecutionMetricsLatePanicKeepsBothDimensions(t *testing.T) {
	reg := prometheus.NewRegistry()
	tst := &tsTable{metrics: newPluginExecutionMetricsForTest(reg)}
	release := make(chan struct{})
	observed := make(chan pluginLinkExecutionObservation, 1)
	chain := newNamedMergeChain("g1", "", []namedSampler{{name: "slow-panic", sampler: &releaseThenPanicSampler{release: release}}}, 3)
	chain.observeLinkExecution = func(observation pluginLinkExecutionObservation) {
		tst.observePipelinePluginLinkExecution(observation)
		observed <- observation
	}
	defer chain.close()

	_, executeErr := chain.Execute(&sdk.TraceBatch{Traces: []sdk.TraceBlock{{}}}, time.Millisecond)
	require.Error(t, executeErr)
	close(release)

	select {
	case observation := <-observed:
		require.Equal(t, pluginExecutionResultLate, observation.result)
		require.Equal(t, sdk.BypassReasonPanic, observation.bypassReason)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the abandoned plugin call to finish")
	}

	executions := gatherMetric(t, reg, "pipeline_plugin_executions")
	require.Len(t, executions, 1)
	require.Equal(t, pluginExecutionResultLate, metricLabelValue(executions[0], "result"))
	bypasses := gatherMetric(t, reg, "pipeline_plugin_link_bypasses")
	require.Len(t, bypasses, 1)
	require.Equal(t, sdk.BypassReasonPanic, metricLabelValue(bypasses[0], "reason"))
}

func TestPluginExecutionMetricsFeedBenchmarkReport(t *testing.T) {
	chain := newNamedMergeChain("g1", "", []namedSampler{
		{name: "latency", sampler: &fakeSampler{}},
		{name: "probabilistic", sampler: &fakeSampler{}},
	}, 3)
	defer chain.close()
	observation := &mergeEvaluationObservation{}

	_, executeErr := chain.executeObserved(&sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 5)}, time.Second, observation)
	require.NoError(t, executeErr)
	executions := observation.pluginExecutionSnapshot(mergePhaseCooldown)
	require.Len(t, executions, 2)
	require.Equal(t, "latency", executions[0].PluginName)
	require.Equal(t, "probabilistic", executions[1].PluginName)
	for executionIdx := range executions {
		require.Equal(t, mergePhaseCooldown, executions[executionIdx].Phase)
		require.Equal(t, pluginExecutionResultSuccess, executions[executionIdx].Result)
		require.Equal(t, uint64(1), executions[executionIdx].Calls)
		require.GreaterOrEqual(t, executions[executionIdx].ElapsedNanos, int64(0))
	}
	batches := observation.pluginBatchSnapshot(mergePhaseCooldown)
	require.Equal(t, []mergeBenchmarkPluginBatch{{
		Phase: mergePhaseCooldown, Result: pluginExecutionResultSuccess, Batches: 1, Traces: 5,
	}}, batches)
}

func TestBenchmarkPluginExecutionRecordsExactLatencyHistogram(t *testing.T) {
	observation := &mergeEvaluationObservation{}
	for _, elapsed := range []time.Duration{50 * time.Microsecond, 100 * time.Microsecond, 3 * time.Millisecond, 11 * time.Second} {
		observation.recordPluginExecution(pluginLinkExecutionObservation{
			pluginName: "skywalking", result: pluginExecutionResultSuccess, elapsed: elapsed,
		})
	}
	executions := observation.pluginExecutionSnapshot(mergePhaseCooldown)
	require.Len(t, executions, 1)
	execution := executions[0]
	require.Equal(t, uint64(4), execution.Calls)
	require.Equal(t, uint64(1), execution.DurationOverflow)
	require.Equal(t, uint64(2), execution.DurationBuckets[0].Calls)
	require.Equal(t, int64(100*time.Microsecond), execution.DurationBuckets[0].UpperBoundNanos)
	require.Equal(t, uint64(1), execution.DurationBuckets[5].Calls)
	require.Equal(t, int64(5*time.Millisecond), execution.DurationBuckets[5].UpperBoundNanos)
	require.Equal(t, execution.Calls, execution.DurationOverflow+sumPluginDurationBucketCalls(execution.DurationBuckets))
}

func sumPluginDurationBucketCalls(buckets []mergeBenchmarkDurationBucket) uint64 {
	var calls uint64
	for bucketIdx := range buckets {
		calls += buckets[bucketIdx].Calls
	}
	return calls
}
