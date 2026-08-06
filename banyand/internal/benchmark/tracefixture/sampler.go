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
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"sort"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

const (
	minimumDeletionRatio = 0.345
	maximumDeletionRatio = 0.355
	samplerBatchSize     = 512
	healthySampleRate    = 0.1
)

// DefaultSkyWalkingSamplerConfig is the production-default benchmark sampler configuration.
var DefaultSkyWalkingSamplerConfig = []byte(`{"durationThresholdMs":500,"keepErrors":true,"healthySampleRate":"0.1"}`)

// SamplerArtifact records the authoritative real-plugin verdict population.
type SamplerArtifact struct {
	PluginSHA256      string  `json:"pluginSHA256"`
	ConfigSHA256      string  `json:"configSHA256"`
	VerdictSHA256     string  `json:"verdictSHA256"`
	GeneratedIDSHA256 string  `json:"generatedIDSHA256"`
	DurationRetained  uint64  `json:"durationRetained"`
	ErrorRetained     uint64  `json:"errorRetained"`
	HealthyRetained   uint64  `json:"healthyRetained"`
	HealthyDropped    uint64  `json:"healthyDropped"`
	Evaluated         uint64  `json:"evaluated"`
	Dropped           uint64  `json:"dropped"`
	Retained          uint64  `json:"retained"`
	DeletionRatio     float64 `json:"deletionRatio"`
}

type samplerVerdict struct {
	traceID string
	reason  string
	keep    bool
}

// EvaluateSampler runs a sampler over complete generated logical traces and enforces the production ratio gate.
func EvaluateSampler(ctx context.Context, source Source, plan Plan, sampler sdk.Sampler, pluginPath string,
	config []byte,
) (SamplerArtifact, error) {
	if sampler == nil {
		return SamplerArtifact{}, fmt.Errorf("sampler is required")
	}
	lookup := buildSourceLookup(source)
	projection := sampler.Project()
	verdicts := make([]samplerVerdict, 0, len(plan.Instances))
	for batchStart := 0; batchStart < len(plan.Instances); batchStart += samplerBatchSize {
		if contextErr := ctx.Err(); contextErr != nil {
			return SamplerArtifact{}, fmt.Errorf("sampler evaluation canceled: %w", contextErr)
		}
		batchEnd := min(batchStart+samplerBatchSize, len(plan.Instances))
		batch := sdk.TraceBatch{Traces: make([]sdk.TraceBlock, 0, batchEnd-batchStart)}
		for instanceIdx := batchStart; instanceIdx < batchEnd; instanceIdx++ {
			block, blockErr := buildSamplerBlock(plan.Instances[instanceIdx], lookup, projection)
			if blockErr != nil {
				return SamplerArtifact{}, blockErr
			}
			batch.Traces = append(batch.Traces, block)
		}
		verdict, decideErr := sampler.Decide(&batch)
		if decideErr != nil {
			return SamplerArtifact{}, fmt.Errorf("default SkyWalking sampler failed: %w", decideErr)
		}
		if len(verdict.Keep) != len(batch.Traces) {
			return SamplerArtifact{}, fmt.Errorf("default SkyWalking sampler returned %d verdicts for %d traces", len(verdict.Keep), len(batch.Traces))
		}
		for batchIdx := range batch.Traces {
			instance := &plan.Instances[batchStart+batchIdx]
			verdicts = append(verdicts, samplerVerdict{
				traceID: batch.Traces[batchIdx].TraceID, keep: verdict.Keep[batchIdx], reason: classifyDefaultSamplerRule(lookup[instance.SourceID]),
			})
		}
	}
	artifact, artifactErr := samplerArtifact(plan, pluginPath, config, verdicts)
	if artifactErr != nil {
		return SamplerArtifact{}, artifactErr
	}
	if artifact.DeletionRatio < minimumDeletionRatio || artifact.DeletionRatio > maximumDeletionRatio {
		return SamplerArtifact{}, fmt.Errorf("default SkyWalking deletion ratio %.6f is outside [%.3f, %.3f]", artifact.DeletionRatio,
			minimumDeletionRatio, maximumDeletionRatio)
	}
	return artifact, nil
}

func classifyDefaultSamplerRule(trace LoadedTrace) string {
	var minStart, maxEnd int64
	hasDuration := false
	hasError := false
	for fragmentIdx := range trace.Fragments {
		for rowIdx := range trace.Fragments[fragmentIdx].Rows {
			row := &trace.Fragments[fragmentIdx].Rows[rowIdx]
			startValue, startOK := row.Tags["start_time"]
			latencyValue, latencyOK := row.Tags["latency"]
			if startOK && latencyOK && len(startValue) >= 8 && len(latencyValue) >= 8 {
				start := convert.BytesToInt64(startValue)
				end := saturatingAddInt64(start, saturatingMultiplyInt64(convert.BytesToInt64(latencyValue), int64(1_000_000)))
				if !hasDuration || start < minStart {
					minStart = start
				}
				if !hasDuration || end > maxEnd {
					maxEnd = end
				}
				hasDuration = true
			}
			if errorValue, ok := row.Tags["is_error"]; ok && len(errorValue) >= 8 && convert.BytesToInt64(errorValue) != 0 {
				hasError = true
			}
		}
	}
	if hasDuration && saturatingSubInt64(maxEnd, minStart) >= int64(500*time.Millisecond) {
		return "duration"
	}
	if hasError {
		return "error"
	}
	return "healthy"
}

func saturatingMultiplyInt64(left, right int64) int64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > 0 && right > 0 && left > math.MaxInt64/right {
		return math.MaxInt64
	}
	if left < 0 && right > 0 && left < math.MinInt64/right {
		return math.MinInt64
	}
	return left * right
}

func buildSamplerBlock(instance Instance, lookup sourceLookup, projection sdk.Projection) (sdk.TraceBlock, error) {
	sourceTrace, ok := lookup[instance.SourceID]
	if !ok {
		return sdk.TraceBlock{}, fmt.Errorf("sampler source trace %q is missing", instance.SourceID)
	}
	rowCount := 0
	minTimestamp, maxTimestamp := loadedTraceBounds(sourceTrace)
	for fragmentIdx := range sourceTrace.Fragments {
		rowCount += len(sourceTrace.Fragments[fragmentIdx].Rows)
	}
	block := sdk.TraceBlock{TraceID: instance.GeneratedID, MinTS: minTimestamp, MaxTS: maxTimestamp}
	if projection.SpanIDs {
		block.SpanIDs = make([]string, 0, rowCount)
	}
	if projection.Spans {
		block.Spans = make([][]byte, 0, rowCount)
	}
	block.Tags = make([]sdk.TagColumn, len(projection.Tags))
	for tagIdx, tagName := range projection.Tags {
		block.Tags[tagIdx] = sdk.TagColumn{Name: tagName, Values: make([][]byte, 0, rowCount)}
	}
	for fragmentIdx := range sourceTrace.Fragments {
		fragment := &sourceTrace.Fragments[fragmentIdx]
		for rowIdx := range fragment.Rows {
			row := &fragment.Rows[rowIdx]
			if projection.SpanIDs {
				block.SpanIDs = append(block.SpanIDs, row.SpanID)
			}
			if projection.Spans {
				block.Spans = append(block.Spans, row.Span)
			}
			for tagIdx, tagName := range projection.Tags {
				value, exists := row.Tags[tagName]
				if exists {
					block.Tags[tagIdx].Values = append(block.Tags[tagIdx].Values, value)
					block.Tags[tagIdx].ValueType = row.TagTypes[tagName]
				} else {
					block.Tags[tagIdx].Values = append(block.Tags[tagIdx].Values, nil)
				}
			}
		}
	}
	return block, nil
}

func samplerArtifact(plan Plan, pluginPath string, config []byte, verdicts []samplerVerdict) (SamplerArtifact, error) {
	pluginData, readErr := os.ReadFile(pluginPath)
	if readErr != nil {
		return SamplerArtifact{}, fmt.Errorf("cannot read sampler plugin %q: %w", pluginPath, readErr)
	}
	pluginDigest := sha256.Sum256(pluginData)
	configDigest := sha256.Sum256(config)
	ordered := append([]samplerVerdict(nil), verdicts...)
	sort.Slice(ordered, func(leftIdx, rightIdx int) bool { return ordered[leftIdx].traceID < ordered[rightIdx].traceID })
	verdictDigest := sha256.New()
	var retained uint64
	var durationRetained, errorRetained, healthyRetained, healthyDropped uint64
	for verdictIdx := range ordered {
		verdict := &ordered[verdictIdx]
		if verdictErr := validateDefaultSamplerVerdict(*verdict); verdictErr != nil {
			return SamplerArtifact{}, verdictErr
		}
		if verdict.keep {
			retained++
		}
		switch verdict.reason {
		case "duration":
			durationRetained++
		case "error":
			errorRetained++
		case "healthy":
			if verdict.keep {
				healthyRetained++
			} else {
				healthyDropped++
			}
		default:
			return SamplerArtifact{}, fmt.Errorf("trace %q has unknown sampler rule %q", verdict.traceID, verdict.reason)
		}
		mustWriteHashString(verdictDigest, fmt.Sprintf("%s\t%t\n", verdict.traceID, verdict.keep))
	}
	evaluated := uint64(len(ordered))
	dropped := evaluated - retained
	if durationRetained+errorRetained+healthyRetained != retained || healthyDropped != dropped {
		return SamplerArtifact{}, fmt.Errorf("sampler rule totals do not reconcile with plugin verdicts")
	}
	deletionRatio := float64(0)
	if evaluated > 0 {
		deletionRatio = float64(dropped) / float64(evaluated)
	}
	return SamplerArtifact{
		PluginSHA256: hex.EncodeToString(pluginDigest[:]), ConfigSHA256: hex.EncodeToString(configDigest[:]),
		VerdictSHA256: hex.EncodeToString(verdictDigest.Sum(nil)), GeneratedIDSHA256: generatedIDManifest(plan.Instances),
		DurationRetained: durationRetained, ErrorRetained: errorRetained, HealthyRetained: healthyRetained, HealthyDropped: healthyDropped,
		Evaluated: evaluated, Dropped: dropped, Retained: retained, DeletionRatio: deletionRatio,
	}, nil
}

func validateDefaultSamplerVerdict(verdict samplerVerdict) error {
	switch verdict.reason {
	case "duration", "error":
		if !verdict.keep {
			return fmt.Errorf("default SkyWalking sampler dropped sure-keep trace %q classified as %s", verdict.traceID, verdict.reason)
		}
	case "healthy":
		expectedKeep := defaultHealthySamplerKeep(verdict.traceID)
		if verdict.keep != expectedKeep {
			return fmt.Errorf("default SkyWalking sampler healthy verdict for trace %q is %t, want %t", verdict.traceID, verdict.keep, expectedKeep)
		}
	default:
		return fmt.Errorf("trace %q has unknown sampler rule %q", verdict.traceID, verdict.reason)
	}
	return nil
}

func defaultHealthySamplerKeep(traceID string) bool {
	digest := fnv.New64a()
	_, _ = digest.Write([]byte(traceID))
	fraction := float64(digest.Sum64()>>11) / (1 << 53)
	return fraction < healthySampleRate
}
