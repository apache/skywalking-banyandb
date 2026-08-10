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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk/sdktest"
)

const (
	seedSourceEnv   = "TRACE_SAMPLER_BENCH_SOURCE"
	seedCatalogEnv  = "TRACE_SAMPLER_BENCH_CATALOG"
	seedScheduleEnv = "TRACE_SAMPLER_BENCH_SCHEDULE"
	seedExpectedEnv = "TRACE_SAMPLER_BENCH_EXPECTED"
	seedPluginEnv   = "TRACE_SAMPLER_BENCH_PLUGIN"
)

type seedSamplerInputs struct {
	plugin   string
	plan     Plan
	source   Source
	expected SamplerArtifact
}

type seedSamplerCase struct {
	name      string
	config    []byte
	batchSize int
}

type seedTestSampler struct {
	decide     func(*sdk.TraceBatch) (sdk.Verdict, error)
	projection sdk.Projection
}

func (sampler *seedTestSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (sampler *seedTestSampler) Project() sdk.Projection { return sampler.projection }
func (sampler *seedTestSampler) Close() error            { return nil }
func (sampler *seedTestSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	return sampler.decide(batch)
}

func loadSeedSamplerInputs() (seedSamplerInputs, error) {
	sourcePath := os.Getenv(seedSourceEnv)
	catalogPath := os.Getenv(seedCatalogEnv)
	schedulePath := os.Getenv(seedScheduleEnv)
	expectedPath := os.Getenv(seedExpectedEnv)
	pluginPath := os.Getenv(seedPluginEnv)
	if sourcePath == "" || catalogPath == "" || schedulePath == "" || expectedPath == "" || pluginPath == "" {
		return seedSamplerInputs{}, fmt.Errorf("set %s, %s, %s, %s, and %s", seedSourceEnv, seedCatalogEnv, seedScheduleEnv, seedExpectedEnv, seedPluginEnv)
	}
	source, sourceErr := LoadSource(context.Background(), LoadOptions{
		SourcePath: sourcePath, CatalogPath: catalogPath, Format: dumptrace.PartFormatLegacy,
	})
	if sourceErr != nil {
		return seedSamplerInputs{}, sourceErr
	}
	var plan Plan
	if readErr := readJSONFile(schedulePath, &plan); readErr != nil {
		return seedSamplerInputs{}, readErr
	}
	var expected SamplerArtifact
	if readErr := readJSONFile(expectedPath, &expected); readErr != nil {
		return seedSamplerInputs{}, readErr
	}
	return seedSamplerInputs{source: source, plan: plan, expected: expected, plugin: pluginPath}, nil
}

func readJSONFile(path string, target any) error {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return fmt.Errorf("cannot read %q: %w", path, readErr)
	}
	if decodeErr := json.Unmarshal(data, target); decodeErr != nil {
		return fmt.Errorf("cannot decode %q: %w", path, decodeErr)
	}
	return nil
}

func buildSeedBatches(ctx context.Context, builder *SamplerBatchBuilder, instances []Instance, projection sdk.Projection,
	batchSize int,
) ([]sdk.TraceBatch, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}
	batches := make([]sdk.TraceBatch, 0, (len(instances)+batchSize-1)/batchSize)
	for batchStart := 0; batchStart < len(instances); batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, len(instances))
		batch, buildErr := builder.Build(ctx, instances[batchStart:batchEnd], projection)
		if buildErr != nil {
			return nil, buildErr
		}
		batches = append(batches, batch)
	}
	return batches, nil
}

func decideSeedBatches(sampler sdk.Sampler, batches []sdk.TraceBatch) ([]samplerVerdict, error) {
	verdicts := make([]samplerVerdict, 0)
	for batchIdx := range batches {
		verdict, decideErr := decideSeedBatch(sampler, &batches[batchIdx], batchIdx)
		if decideErr != nil {
			return nil, decideErr
		}
		for traceIdx := range verdict.Keep {
			verdicts = append(verdicts, samplerVerdict{traceID: batches[batchIdx].Traces[traceIdx].TraceID, keep: verdict.Keep[traceIdx]})
		}
	}
	return verdicts, nil
}

func decideSeedBatch(sampler sdk.Sampler, batch *sdk.TraceBatch, batchIdx int) (sdk.Verdict, error) {
	verdict, decideErr := sampler.Decide(batch)
	if decideErr != nil {
		return sdk.Verdict{}, decideErr
	}
	if len(verdict.Keep) != len(batch.Traces) {
		return sdk.Verdict{}, fmt.Errorf("batch %d returned %d verdicts for %d traces", batchIdx, len(verdict.Keep), len(batch.Traces))
	}
	return verdict, nil
}

func seedVerdictDigest(verdicts []samplerVerdict) string {
	ordered := append([]samplerVerdict(nil), verdicts...)
	sort.Slice(ordered, func(leftIdx, rightIdx int) bool { return ordered[leftIdx].traceID < ordered[rightIdx].traceID })
	digest := sha256.New()
	for verdictIdx := range ordered {
		mustWriteHashString(digest, fmt.Sprintf("%s\t%t\n", ordered[verdictIdx].traceID, ordered[verdictIdx].keep))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func seedBatchDigest(batches []sdk.TraceBatch) string {
	digest := sha256.New()
	for batchIdx := range batches {
		for traceIdx := range batches[batchIdx].Traces {
			traceBlock := &batches[batchIdx].Traces[traceIdx]
			writeSeedDigestField(digest, []byte(traceBlock.TraceID))
			for columnIdx := range traceBlock.Tags {
				column := &traceBlock.Tags[columnIdx]
				writeSeedDigestField(digest, []byte(column.Name))
				for _, value := range column.Values {
					writeSeedDigestField(digest, value)
				}
			}
			for _, spanID := range traceBlock.SpanIDs {
				writeSeedDigestField(digest, []byte(spanID))
			}
			for _, span := range traceBlock.Spans {
				writeSeedDigestField(digest, span)
			}
		}
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func writeSeedDigestField(digest hash.Hash, value []byte) {
	mustWriteHashString(digest, fmt.Sprintf("%d:", len(value)))
	_, _ = digest.Write(value)
}

func preflightSeedSampler(sampler sdk.Sampler, batches []sdk.TraceBatch) ([]samplerVerdict, error) {
	beforeDigest := seedBatchDigest(batches)
	first, firstErr := decideSeedBatches(sampler, batches)
	if firstErr != nil {
		return nil, firstErr
	}
	second, secondErr := decideSeedBatches(sampler, batches)
	if secondErr != nil {
		return nil, secondErr
	}
	if seedVerdictDigest(first) != seedVerdictDigest(second) {
		return nil, fmt.Errorf("sampler verdict changed across identical decisions")
	}
	if afterDigest := seedBatchDigest(batches); afterDigest != beforeDigest {
		return nil, fmt.Errorf("sampler mutated its read-only input: got %s, want %s", afterDigest, beforeDigest)
	}
	return first, nil
}

func benchmarkSeedDecisions(sampler sdk.Sampler, batches []sdk.TraceBatch) (uint64, error) {
	var kept uint64
	for batchIdx := range batches {
		verdict, decideErr := decideSeedBatch(sampler, &batches[batchIdx], batchIdx)
		if decideErr != nil {
			return 0, decideErr
		}
		for _, keep := range verdict.Keep {
			if keep {
				kept++
			}
		}
	}
	return kept, nil
}

var (
	seedBenchmarkKeepSink    uint64
	seedBenchmarkPayloadSink uint64
)

func benchmarkSeedSamplerCase(b *testing.B, sampler sdk.Sampler, builder *SamplerBatchBuilder, instances []Instance, batchSize int,
	expectedDigest string,
) {
	b.Helper()
	batches, buildErr := buildSeedBatches(context.Background(), builder, instances, sampler.Project(), batchSize)
	if buildErr != nil {
		b.Fatalf("build batches: %v", buildErr)
	}
	preflight, preflightErr := preflightSeedSampler(sampler, batches)
	if preflightErr != nil {
		b.Fatalf("preflight: %v", preflightErr)
	}
	if expectedDigest != "" && seedVerdictDigest(preflight) != expectedDigest {
		b.Fatalf("verdict drift: got %s, want %s", seedVerdictDigest(preflight), expectedDigest)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		kept, decideErr := benchmarkSeedDecisions(sampler, batches)
		if decideErr != nil {
			b.Fatal(decideErr)
		}
		seedBenchmarkKeepSink = kept
	}
	b.StopTimer()
	traceOperations := float64(b.N * len(instances))
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/traceOperations, "ns/trace")
	b.ReportMetric(float64(len(instances)), "traces/op")
}

func TestSeedSamplerArtifact(t *testing.T) {
	inputs, inputErr := loadSeedSamplerInputs()
	if inputErr != nil {
		t.Skipf("real mature seed is opt-in: %v", inputErr)
	}
	sampler, loadErr := sdktest.LoadSO(inputs.plugin, "NewSampler", DefaultSkyWalkingSamplerConfig)
	if loadErr != nil {
		t.Fatalf("load sampler: %v", loadErr)
	}
	defer func() { _ = sampler.Close() }()
	artifact, evaluateErr := EvaluateSampler(context.Background(), inputs.source, inputs.plan, sampler, inputs.plugin,
		DefaultSkyWalkingSamplerConfig)
	if evaluateErr != nil {
		t.Fatalf("evaluate sampler: %v", evaluateErr)
	}
	if artifact.Evaluated != inputs.expected.Evaluated || artifact.Dropped != inputs.expected.Dropped ||
		artifact.VerdictSHA256 != inputs.expected.VerdictSHA256 || artifact.DeletionRatio != inputs.expected.DeletionRatio {
		t.Fatalf("seed verdict drift: got %+v, expected evaluated=%d dropped=%d ratio=%f verdict=%s", artifact,
			inputs.expected.Evaluated, inputs.expected.Dropped, inputs.expected.DeletionRatio, inputs.expected.VerdictSHA256)
	}
}

func TestSeedBatchPartitionAndPreflight(t *testing.T) {
	source := Source{Mature: []LoadedTrace{{
		SourceID: "source", Fragments: []LoadedFragment{{Fragment: Fragment{MinTimestamp: 1, MaxTimestamp: 2}, Rows: []Row{{}}}},
	}}}
	instances := make([]Instance, 5)
	for instanceIdx := range instances {
		instances[instanceIdx] = Instance{SourceID: "source", GeneratedID: fmt.Sprintf("trace-%d", instanceIdx)}
	}
	batches, buildErr := buildSeedBatches(context.Background(), NewSamplerBatchBuilder(source), instances, sdk.Projection{}, 2)
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	if len(batches) != 3 || len(batches[0].Traces) != 2 || len(batches[1].Traces) != 2 || len(batches[2].Traces) != 1 {
		t.Fatalf("unexpected partition: %#v", batches)
	}
	var flattened []string
	for batchIdx := range batches {
		for traceIdx := range batches[batchIdx].Traces {
			flattened = append(flattened, batches[batchIdx].Traces[traceIdx].TraceID)
		}
	}
	require.Equal(t, []string{"trace-0", "trace-1", "trace-2", "trace-3", "trace-4"}, flattened)
	keepsAll := &seedTestSampler{decide: func(batch *sdk.TraceBatch) (sdk.Verdict, error) {
		keep := make([]bool, len(batch.Traces))
		for traceIdx := range keep {
			keep[traceIdx] = true
		}
		return sdk.Verdict{Keep: keep}, nil
	}}
	verdicts, preflightErr := preflightSeedSampler(keepsAll, batches)
	if preflightErr != nil || len(verdicts) != len(instances) {
		t.Fatalf("preflight failed: verdicts=%d err=%v", len(verdicts), preflightErr)
	}

	wrongSize := &seedTestSampler{decide: func(*sdk.TraceBatch) (sdk.Verdict, error) { return sdk.Verdict{}, nil }}
	if _, wrongSizeErr := preflightSeedSampler(wrongSize, batches); wrongSizeErr == nil {
		t.Fatal("preflight accepted a malformed verdict length")
	}
	mutated := false
	mutating := &seedTestSampler{decide: func(batch *sdk.TraceBatch) (sdk.Verdict, error) {
		if !mutated {
			batch.Traces[0].TraceID = "mutated"
			mutated = true
		}
		return sdk.Verdict{Keep: make([]bool, len(batch.Traces))}, nil
	}}
	if _, mutationErr := preflightSeedSampler(mutating, batches); mutationErr == nil {
		t.Fatal("preflight accepted input mutation")
	}
}

func BenchmarkSeedSampler(b *testing.B) {
	inputs, inputErr := loadSeedSamplerInputs()
	if inputErr != nil {
		b.Skipf("real mature seed is opt-in: %v", inputErr)
	}
	cases := []seedSamplerCase{
		{name: "duration", config: []byte(`{"durationThresholdMs":500}`), batchSize: 512},
		{name: "error", config: []byte(`{"keepErrors":true}`), batchSize: 512},
		{name: "hash", config: []byte(`{"healthySampleRate":"0.1"}`), batchSize: 512},
		{name: "tag-equals", config: []byte(`{"keepTagRules":[{"tagKey":"db.type","equals":"PostgreSQL"}]}`), batchSize: 512},
		{name: "tag-regex", config: []byte(`{"keepTagRules":[{"tagKey":"db.type","regex":"Postgre.*"}]}`), batchSize: 512},
		{name: "status-regex", config: []byte(`{"keepTagRules":[{"tagKey":"http.status_code","regex":"5\\d{2}"}]}`), batchSize: 512},
		{name: "default", config: DefaultSkyWalkingSamplerConfig, batchSize: 512},
	}
	for _, batchSize := range []int{1, 16, 64, 256, 512, 4096, 8192, 16384} {
		cases = append(cases,
			seedSamplerCase{name: fmt.Sprintf("default-batch-%d", batchSize), config: DefaultSkyWalkingSamplerConfig, batchSize: batchSize},
			seedSamplerCase{name: fmt.Sprintf("verdict-batch-%d", batchSize), config: []byte(`{"healthySampleRate":0}`), batchSize: batchSize},
		)
	}
	builder := NewSamplerBatchBuilder(inputs.source)
	for caseIdx := range cases {
		benchmarkCase := cases[caseIdx]
		b.Run(benchmarkCase.name, func(b *testing.B) {
			sampler, loadErr := sdktest.LoadSO(inputs.plugin, "NewSampler", benchmarkCase.config)
			if loadErr != nil {
				b.Fatalf("load sampler: %v", loadErr)
			}
			b.Cleanup(func() { _ = sampler.Close() })
			expectedDigest := ""
			if strings.HasPrefix(benchmarkCase.name, "default") {
				expectedDigest = inputs.expected.VerdictSHA256
			}
			benchmarkSeedSamplerCase(b, sampler, builder, inputs.plan.Instances, benchmarkCase.batchSize, expectedDigest)
		})
	}
	projectionCases := []struct {
		consume    func(*sdk.TraceBlock) uint64
		name       string
		projection sdk.Projection
	}{
		{name: "span-bodies", projection: sdk.Projection{Spans: true}, consume: func(trace *sdk.TraceBlock) uint64 {
			var bytesRead uint64
			for _, span := range trace.Spans {
				bytesRead += uint64(len(span))
			}
			return bytesRead
		}},
		{name: "span-ids", projection: sdk.Projection{SpanIDs: true}, consume: func(trace *sdk.TraceBlock) uint64 {
			var bytesRead uint64
			for _, spanID := range trace.SpanIDs {
				bytesRead += uint64(len(spanID))
			}
			return bytesRead
		}},
	}
	for caseIdx := range projectionCases {
		projectionCase := projectionCases[caseIdx]
		b.Run(projectionCase.name, func(b *testing.B) {
			sampler := &seedTestSampler{projection: projectionCase.projection, decide: func(batch *sdk.TraceBatch) (sdk.Verdict, error) {
				keep := make([]bool, len(batch.Traces))
				var payloadBytes uint64
				for traceIdx := range batch.Traces {
					keep[traceIdx] = true
					payloadBytes += projectionCase.consume(&batch.Traces[traceIdx])
				}
				seedBenchmarkPayloadSink = payloadBytes
				return sdk.Verdict{Keep: keep}, nil
			}}
			benchmarkSeedSamplerCase(b, sampler, builder, inputs.plan.Instances, 512, "")
		})
	}
}
