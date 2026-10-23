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

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type preparedTraceMigration struct {
	table         *samplingDecisionTable
	directory     string
	checkpoint    samplingCheckpoint
	manifest      samplingManifest
	replayedBytes uint64
}

func samplingArtifactPath(root, group string, segment *timestamp.TimeRange, shard uint32) string {
	return filepath.Join(root, group, strconv.FormatInt(segment.Start.UnixNano(), 10), strconv.FormatUint(uint64(shard), 10))
}

func samplingCoreParts(shardPath string) ([]string, error) {
	entries, err := os.ReadDir(shardPath)
	if err != nil {
		return nil, err
	}
	var parts []string
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != 16 {
			continue
		}
		if _, err = strconv.ParseUint(entry.Name(), 16, 64); err != nil {
			continue
		}
		parts = append(parts, filepath.Join(shardPath, entry.Name()))
	}
	sort.Strings(parts)
	return parts, nil
}

func samplingInventory(parts []string) ([]samplingInventoryFile, error) {
	var result []samplingInventoryFile
	for _, part := range parts {
		err := filepath.WalkDir(part, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("snapshot inventory contains symlink %s", path)
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			digest, err := samplingFileDigest(path)
			if err != nil {
				return err
			}
			result = append(result, samplingInventoryFile{Path: path, Size: info.Size(), Digest: digest})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func openPreparedTraceMigration(directory string) (*preparedTraceMigration, error) {
	payload, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return nil, err
	}
	prepared := &preparedTraceMigration{directory: directory}
	if err = json.Unmarshal(payload, &prepared.manifest); err != nil {
		return nil, fmt.Errorf("decode sampling manifest: %w", err)
	}
	if err = verifySamplingManifest(&prepared.manifest); err != nil {
		return nil, err
	}
	if prepared.manifest.Version != samplingArtifactVersion {
		return nil, fmt.Errorf("unsupported sampling manifest version %d", prepared.manifest.Version)
	}
	for _, file := range prepared.manifest.Inventory {
		info, statErr := os.Stat(file.Path)
		if statErr != nil {
			return nil, fmt.Errorf("required sampling snapshot: %w", statErr)
		}
		digest, digestErr := samplingFileDigest(file.Path)
		if digestErr != nil {
			return nil, digestErr
		}
		if info.Size() != file.Size || digest != file.Digest {
			return nil, fmt.Errorf("sampling snapshot inventory mismatch: %s", file.Path)
		}
	}
	prepared.table, err = openSamplingDecisionTable(directory, &prepared.manifest)
	if err != nil {
		return nil, err
	}
	payload, err = os.ReadFile(filepath.Join(directory, "checkpoint.json"))
	if err != nil {
		prepared.Close()
		return nil, err
	}
	if prepared.checkpoint, err = readSamplingCheckpoint(payload); err != nil {
		prepared.Close()
		return nil, err
	}
	if prepared.checkpoint.ManifestDigest != prepared.manifest.ManifestDigest || prepared.checkpoint.Sequence > prepared.manifest.KeptSpans {
		prepared.Close()
		return nil, fmt.Errorf("sampling checkpoint does not belong to manifest")
	}
	if err = syncSamplingDirectory(directory); err != nil {
		prepared.Close()
		return nil, err
	}
	if err = syncSamplingDirectory(filepath.Dir(directory)); err != nil {
		prepared.Close()
		return nil, err
	}
	return prepared, nil
}

func prepareTraceMigration(ctx context.Context, directory, shardPath, snapshot string, segment *timestamp.TimeRange,
	shard uint32, group *GroupConfig, policy *pinnedSamplingPolicy,
) (*preparedTraceMigration, error) {
	// A published manifest is authoritative even when policy or schema changed.
	if _, err := os.Stat(directory); err == nil {
		return openPreparedTraceMigration(directory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	start := time.Now()
	parent := filepath.Dir(directory)
	if err := makeSamplingDirectory(parent); err != nil {
		return nil, err
	}
	temporary, err := os.MkdirTemp(parent, ".preparing-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(temporary)
	parts, err := samplingCoreParts(shardPath)
	if err != nil {
		return nil, err
	}
	inventory, err := samplingInventory(parts)
	if err != nil {
		return nil, err
	}
	manifest := samplingManifest{
		Version: samplingArtifactVersion, Snapshot: snapshot, Group: group.Metadata.Name,
		SourceStage: group.SourceStage, TargetStage: group.TargetStage, Shard: shard, SegmentStart: segment.Start.UnixNano(), SegmentEnd: segment.End.UnixNano(),
		TargetShardNum: group.TargetShardNum, Replicas: group.TargetReplicas, Inventory: inventory, Parts: parts,
		PolicyDigest: policy.digest, PluginDigests: policy.binaryDigests, FailOpen: make(map[string]uint64),
	}
	manifest.SourceInterval, err = proto.MarshalOptions{Deterministic: true}.Marshal(group.SegmentInterval)
	if err != nil {
		return nil, err
	}
	manifest.TargetInterval, err = proto.MarshalOptions{Deterministic: true}.Marshal(group.TargetSegmentInterval)
	if err != nil {
		return nil, err
	}
	manifest.Schema, err = proto.MarshalOptions{Deterministic: true}.Marshal(policy.schema)
	if err != nil {
		return nil, err
	}
	manifest.GroupConfig, err = proto.MarshalOptions{Deterministic: true}.Marshal(group.Group)
	if err != nil {
		return nil, err
	}
	manifest.Policy, err = proto.MarshalOptions{Deterministic: true}.Marshal(policy.config)
	if err != nil {
		return nil, err
	}
	writer, err := newSamplingDecisionWriter(temporary)
	if err != nil {
		return nil, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.close()
		}
	}()
	scanner, err := trace.NewProjectedSnapshotScanner(ctx, parts, policy.session.Projection(), trace.SnapshotScanOptions{
		MaxTraceBytes: 32 << 20, MaxMetadataBytes: 8 << 20, MaxOpenReaders: 16, TempDir: temporary,
	})
	if err != nil {
		return nil, err
	}
	defer scanner.Close()
	if err = prepareSamplingDecisions(ctx, scanner, writer, &manifest, policy, segment); err != nil {
		return nil, err
	}
	if err = scanner.Close(); err != nil {
		return nil, err
	}
	if err = writer.close(); err != nil {
		return nil, err
	}
	closed = true
	manifest.TableDigest, err = samplingFileDigest(filepath.Join(temporary, "decisions"))
	if err != nil {
		return nil, err
	}
	manifest.IndexDigest, err = samplingFileDigest(filepath.Join(temporary, "offsets"))
	if err != nil {
		return nil, err
	}
	manifest.PreparationNanos = int64(time.Since(start))
	if err = sealSamplingManifest(&manifest); err != nil {
		return nil, err
	}
	if err = atomicSamplingJSON(filepath.Join(temporary, "manifest.json"), &manifest); err != nil {
		return nil, err
	}
	if err = saveSamplingCheckpoint(filepath.Join(temporary, "checkpoint.json"), samplingCheckpoint{ManifestDigest: manifest.ManifestDigest}); err != nil {
		return nil, err
	}
	if err = syncSamplingDirectory(temporary); err != nil {
		return nil, err
	}
	if err = os.Rename(temporary, directory); err != nil {
		return nil, err
	}
	if err = syncSamplingDirectory(parent); err != nil {
		return nil, err
	}
	return openPreparedTraceMigration(directory)
}

func (p *preparedTraceMigration) Close() error {
	if p.table != nil {
		return p.table.close()
	}
	return nil
}

func prepareSamplingDecisions(ctx context.Context, scanner *trace.ProjectedSnapshotScanner, writer *samplingDecisionWriter,
	manifest *samplingManifest, policy *pinnedSamplingPolicy, segment *timestamp.TimeRange,
) error {
	for {
		assembled, scanErr := scanner.Next(ctx)
		if errors.Is(scanErr, io.EOF) {
			break
		}
		if scanErr != nil {
			return scanErr
		}
		keep := true
		reason := ""
		switch {
		case assembled.InputConflict:
			reason = "input_conflict"
		case assembled.Oversized:
			reason = "oversized_trace"
		default:
			bypasses := make(chan string, max(1, len(policy.samplers)))
			verdict, _, executionErr := policy.session.ExecuteInto(&sdk.TraceBatch{Traces: []sdk.TraceBlock{assembled.Trace}}, policy.host.Timeout, nil,
				func(samplers []sdk.Sampler, batch *sdk.TraceBatch, mask []bool) sdk.Verdict {
					return sdk.EvaluateChainInto(samplers, batch, mask, func(_ int, info sdk.BypassInfo) { bypasses <- info.Reason })
				})
			for len(bypasses) > 0 {
				manifest.FailOpen[<-bypasses]++
			}
			if open, _ := policy.session.State(); open && executionErr == nil {
				reason = "circuit_open"
			}
			switch {
			case executionErr != nil:
				reason = executionErr.Error()
			case len(verdict.Keep) != 1:
				reason = "invalid_verdict"
			default:
				keep = verdict.Keep[0]
			}
		}
		// Apply the same temporal assumptions and edge protection as data-node sampling.
		grace := 2 * time.Hour
		if policy.config != nil && policy.config.MergeGrace != nil {
			grace = policy.config.MergeGrace.AsDuration()
		}
		guardKeep, guardReason := trace.GuardSnapshotTrace(ctx, *assembled, segment.Start.UnixNano(), segment.End.UnixNano(), grace, keep)
		if !keep && guardKeep {
			reason = guardReason
		}
		keep = guardKeep
		if reason != "" {
			manifest.FailOpen[reason]++
		}
		manifest.Traces++
		manifest.Spans += assembled.SpanCount
		if keep {
			manifest.KeptTraces++
			manifest.KeptSpans += assembled.SpanCount
		}
		if err := writer.append(assembled.Trace.TraceID, keep); err != nil {
			return err
		}
	}

	return nil
}
