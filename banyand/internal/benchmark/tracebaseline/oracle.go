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

package tracebaseline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"sort"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracefixture"
	dumptrace "github.com/apache/skywalking-banyandb/banyand/internal/dump/trace"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

const samplingOracleBatchSize = 512

// BuildSamplingOracleOptions configures an independent sampler evaluation over an immutable shard copy.
type BuildSamplingOracleOptions struct {
	SourceRoot          string
	PluginPath          string
	ExpectedSamplerPath string
	PluginConfig        []byte
	EvaluationPartIDs   []uint64
}

type oracleTraceBuilder struct {
	block       sdk.TraceBlock
	initialized bool
}

// BuildSamplingOracle evaluates complete source traces outside the measured process and calculates expected output ledgers.
func BuildSamplingOracle(ctx context.Context, options BuildSamplingOracleOptions) (artifact SamplingOracleArtifact, buildErr error) {
	if options.SourceRoot == "" || options.PluginPath == "" {
		return SamplingOracleArtifact{}, fmt.Errorf("sampling oracle source root and plugin are required")
	}
	sampler, samplerErr := sdk.OpenSampler(options.PluginPath, "NewSampler", options.PluginConfig)
	if samplerErr != nil {
		return SamplingOracleArtifact{}, fmt.Errorf("cannot load sampling oracle plugin: %w", samplerErr)
	}
	defer func() { buildErr = errors.Join(buildErr, sampler.Close()) }()
	receiver, receiverErr := storagetrace.NewBenchmarkMergeReceiver( //nolint:contextcheck // The storage constructor has no context parameter.
		options.SourceRoot, storagetrace.BenchmarkMergeReceiverOptions{BlockMerges: true, MergeGrace: 2 * time.Hour},
	)
	if receiverErr != nil {
		return SamplingOracleArtifact{}, fmt.Errorf("cannot open sampling oracle source: %w", receiverErr)
	}
	defer func() { buildErr = errors.Join(buildErr, receiver.Close()) }()
	ledgerSnapshot, ledgerErr := tracefixture.CaptureLogicalLedgerSnapshot(ctx, receiver)
	if ledgerErr != nil {
		return SamplingOracleArtifact{}, fmt.Errorf("cannot capture sampling oracle input ledger: %w", ledgerErr)
	}
	blocks, blocksErr := scanSamplingOracleBlocks(ctx, receiver, sampler.Project(), options.EvaluationPartIDs)
	if blocksErr != nil {
		return SamplingOracleArtifact{}, blocksErr
	}
	artifact, droppedIDs, evaluateErr := evaluateSamplingOracle(ctx, blocks, sampler)
	if evaluateErr != nil {
		return SamplingOracleArtifact{}, evaluateErr
	}
	effectiveDroppedIDs := droppedIDs
	if len(options.EvaluationPartIDs) > 0 {
		var outsideErr error
		effectiveDroppedIDs, outsideErr = effectiveSamplingDrops(ctx, receiver, blocks, droppedIDs, options.EvaluationPartIDs)
		if outsideErr != nil {
			return SamplingOracleArtifact{}, outsideErr
		}
		artifact.Dropped = uint64(len(effectiveDroppedIDs))
		artifact.Retained = artifact.Evaluated - artifact.Dropped
		artifact.DeletionRatio = samplingDeletionRatio(artifact.Dropped, artifact.Evaluated)
	}
	selection := ledgerSnapshot.Excluding(effectiveDroppedIDs)
	artifact.ExpectedLedger = selection.Checksums
	artifact.ExpectedRows = selection.Rows
	pluginSHA256, pluginErr := fileSHA256(options.PluginPath)
	if pluginErr != nil {
		return SamplingOracleArtifact{}, pluginErr
	}
	artifact.PluginSHA256 = pluginSHA256
	configDigest := sha256.Sum256(options.PluginConfig)
	artifact.ConfigSHA256 = hex.EncodeToString(configDigest[:])
	if options.ExpectedSamplerPath != "" {
		expectedSHA256, expectedErr := validateExpectedSamplerArtifact(options.ExpectedSamplerPath, artifact)
		if expectedErr != nil {
			return SamplingOracleArtifact{}, expectedErr
		}
		artifact.ExpectedSamplerSHA = expectedSHA256
	}
	return artifact, nil
}

func effectiveSamplingDrops(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, blocks []sdk.TraceBlock,
	pluginDropped map[string]struct{}, selectedPartIDs []uint64,
) (map[string]struct{}, error) {
	effective := maps.Clone(pluginDropped)
	for blockIdx := range blocks {
		block := &blocks[blockIdx]
		if _, isDropped := effective[block.TraceID]; !isDropped {
			continue
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("sampling oracle fragment check canceled: %w", contextErr)
		}
		maybeOutside, outsideErr := receiver.TraceFragmentMaybeOutsideSelection(
			selectedPartIDs, block.TraceID, block.MinTS, block.MaxTS, time.Minute,
		)
		if outsideErr != nil {
			return nil, fmt.Errorf("cannot evaluate sampling oracle fragment boundary for %q: %w", block.TraceID, outsideErr)
		}
		if maybeOutside {
			delete(effective, block.TraceID)
		}
	}
	return effective, nil
}

func scanSamplingOracleBlocks(ctx context.Context, receiver *storagetrace.BenchmarkPartReceiver, projection sdk.Projection,
	evaluationPartIDs []uint64,
) ([]sdk.TraceBlock, error) {
	partIDs := evaluationPartIDs
	if len(partIDs) == 0 {
		var partIDsErr error
		partIDs, partIDsErr = receiver.ActivePartIDs()
		if partIDsErr != nil {
			return nil, fmt.Errorf("cannot list sampling oracle source parts: %w", partIDsErr)
		}
	}
	builders := make(map[string]*oracleTraceBuilder)
	fileSystem := fs.NewLocalFileSystem()
	for _, partID := range partIDs {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, fmt.Errorf("sampling oracle source scan canceled: %w", contextErr)
		}
		reader, openErr := dumptrace.OpenPart(partID, receiver.Root(), fileSystem)
		if openErr != nil {
			return nil, fmt.Errorf("cannot open sampling oracle source part %016x: %w", partID, openErr)
		}
		iterator := reader.Iterator()
		for iterator.Next() {
			row := iterator.Row()
			builder := builders[row.TraceID]
			if builder == nil {
				builder = newOracleTraceBuilder(row.TraceID, projection)
				builders[row.TraceID] = builder
			}
			appendOracleRow(builder, row, projection)
		}
		partErr := errors.Join(iterator.Err(), iterator.Close(), reader.Close())
		if partErr != nil {
			return nil, fmt.Errorf("cannot scan sampling oracle source part %016x: %w", partID, partErr)
		}
	}
	blocks := make([]sdk.TraceBlock, 0, len(builders))
	for _, builder := range builders {
		blocks = append(blocks, builder.block)
	}
	return blocks, nil
}

func newOracleTraceBuilder(traceID string, projection sdk.Projection) *oracleTraceBuilder {
	builder := &oracleTraceBuilder{block: sdk.TraceBlock{TraceID: traceID}}
	if projection.SpanIDs {
		builder.block.SpanIDs = make([]string, 0, 4)
	}
	if projection.Spans {
		builder.block.Spans = make([][]byte, 0, 4)
	}
	builder.block.Tags = make([]sdk.TagColumn, len(projection.Tags))
	for tagIdx, tagName := range projection.Tags {
		builder.block.Tags[tagIdx].Name = tagName
		builder.block.Tags[tagIdx].Values = make([][]byte, 0, 4)
	}
	return builder
}

func appendOracleRow(builder *oracleTraceBuilder, row dumptrace.Row, projection sdk.Projection) {
	if !builder.initialized || row.Timestamp < builder.block.MinTS {
		builder.block.MinTS = row.Timestamp
	}
	if !builder.initialized || row.Timestamp > builder.block.MaxTS {
		builder.block.MaxTS = row.Timestamp
	}
	builder.initialized = true
	if projection.SpanIDs {
		builder.block.SpanIDs = append(builder.block.SpanIDs, row.SpanID)
	}
	if projection.Spans {
		builder.block.Spans = append(builder.block.Spans, bytes.Clone(row.Span))
	}
	for tagIdx, tagName := range projection.Tags {
		value, found := row.Tags[tagName]
		if found {
			builder.block.Tags[tagIdx].Values = append(builder.block.Tags[tagIdx].Values, bytes.Clone(value))
			builder.block.Tags[tagIdx].ValueType = row.TagTypes[tagName]
		} else {
			builder.block.Tags[tagIdx].Values = append(builder.block.Tags[tagIdx].Values, nil)
		}
	}
}

func validateExpectedSamplerArtifact(path string, actual SamplingOracleArtifact) (string, error) {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return "", fmt.Errorf("cannot read expected sampler artifact: %w", readErr)
	}
	var expected tracefixture.SamplerArtifact
	if decodeErr := json.Unmarshal(data, &expected); decodeErr != nil {
		return "", fmt.Errorf("cannot decode expected sampler artifact: %w", decodeErr)
	}
	if expected.ConfigSHA256 != actual.ConfigSHA256 || expected.VerdictSHA256 != actual.VerdictSHA256 ||
		expected.Evaluated != actual.Evaluated || expected.Retained != actual.PluginRetained || expected.Dropped != actual.PluginDropped {
		return "", fmt.Errorf("sampling oracle verdicts do not match the expected sampler artifact")
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func evaluateSamplingOracle(ctx context.Context, blocks []sdk.TraceBlock, sampler sdk.Sampler) (SamplingOracleArtifact, map[string]struct{}, error) {
	if sampler == nil {
		return SamplingOracleArtifact{}, nil, fmt.Errorf("sampling oracle requires a sampler")
	}
	ordered := append([]sdk.TraceBlock(nil), blocks...)
	sort.Slice(ordered, func(leftIdx, rightIdx int) bool { return ordered[leftIdx].TraceID < ordered[rightIdx].TraceID })
	droppedIDs := make(map[string]struct{})
	verdictDigest := sha256.New()
	var retained uint64
	for batchStart := 0; batchStart < len(ordered); batchStart += samplingOracleBatchSize {
		if contextErr := ctx.Err(); contextErr != nil {
			return SamplingOracleArtifact{}, nil, fmt.Errorf("sampling oracle canceled: %w", contextErr)
		}
		batchEnd := min(batchStart+samplingOracleBatchSize, len(ordered))
		batch := sdk.TraceBatch{Traces: ordered[batchStart:batchEnd]}
		verdict, decideErr := sampler.Decide(&batch)
		if decideErr != nil {
			return SamplingOracleArtifact{}, nil, fmt.Errorf("sampling oracle decide failed: %w", decideErr)
		}
		if len(verdict.Keep) != len(batch.Traces) {
			return SamplingOracleArtifact{}, nil, fmt.Errorf("sampling oracle returned %d verdicts for %d traces", len(verdict.Keep), len(batch.Traces))
		}
		for batchIdx := range batch.Traces {
			traceID := batch.Traces[batchIdx].TraceID
			keep := verdict.Keep[batchIdx]
			if keep {
				retained++
			} else {
				droppedIDs[traceID] = struct{}{}
			}
			_, _ = fmt.Fprintf(verdictDigest, "%s\t%t\n", traceID, keep)
		}
	}
	evaluated := uint64(len(ordered))
	dropped := evaluated - retained
	deletionRatio := samplingDeletionRatio(dropped, evaluated)
	return SamplingOracleArtifact{
		VerdictSHA256: hex.EncodeToString(verdictDigest.Sum(nil)), Evaluated: evaluated, Retained: retained,
		Dropped: dropped, PluginRetained: retained, PluginDropped: dropped, DeletionRatio: deletionRatio, PluginDeletionRatio: deletionRatio, Version: 1,
	}, droppedIDs, nil
}

func samplingDeletionRatio(dropped, evaluated uint64) float64 {
	if evaluated == 0 {
		return 0
	}
	return float64(dropped) / float64(evaluated)
}
