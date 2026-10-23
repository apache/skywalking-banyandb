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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type sampledSeriesWork struct {
	path   string
	shards []common.ShardID
}

func (mv *traceMigrationVisitor) visitSampledShard(segment *timestamp.TimeRange, shard common.ShardID, shardPath, directory string) error {
	var prepared *preparedTraceMigration
	var err error
	if mv.lfs.IsExist(directory) {
		prepared, err = openPreparedTraceMigration(directory)
	} else {
		if mv.samplingPolicy == nil {
			mv.samplingPolicy, err = resolveSamplingPolicy(mv.ctx, mv.samplingGroup, mv.metadata)
			if err != nil {
				return err
			}
			if mv.samplingPolicy == nil {
				return fmt.Errorf("pending sampled migration has no pinned source policy")
			}
		}
		shardPolicy := mv.samplingPolicy
		if len(shardPolicy.plugins) > 0 {
			shardPolicy, err = mv.samplingPolicy.newShardSession(mv.group)
			if err != nil {
				return err
			}
			defer shardPolicy.close()
		}
		prepared, err = prepareTraceMigration(mv.ctx, directory, shardPath, mv.snapshot, segment, uint32(shard), mv.samplingGroup, shardPolicy)
	}
	if err != nil {
		mv.recordError(scopeShard, segment, shard, err.Error())
		return err
	}
	defer prepared.Close()
	if err = registerPreparedSampling(mv.samplingRoot, prepared); err != nil {
		return err
	}
	sourceInterval, intervalErr := proto.MarshalOptions{Deterministic: true}.Marshal(mv.samplingGroup.SegmentInterval)
	if intervalErr != nil {
		return intervalErr
	}
	targetInterval, intervalErr := proto.MarshalOptions{Deterministic: true}.Marshal(mv.samplingGroup.TargetSegmentInterval)
	if intervalErr != nil {
		return intervalErr
	}
	if prepared.manifest.SourceStage != mv.sourceStage || prepared.manifest.TargetStage != mv.targetStage ||
		prepared.manifest.TargetShardNum != mv.targetShardNum || prepared.manifest.Replicas != mv.replicas ||
		!bytes.Equal(sourceInterval, prepared.manifest.SourceInterval) || !bytes.Equal(targetInterval, prepared.manifest.TargetInterval) {
		return fmt.Errorf("prepared sampling routing differs from resolved stages; retaining source")
	}
	if mv.replayer == nil {
		pinnedSchema := &databasev1.Trace{}
		if err = proto.Unmarshal(prepared.manifest.Schema, pinnedSchema); err != nil {
			return err
		}
		mv.replayer = &traceRowReplayer{
			group: mv.group, targetShardNum: prepared.manifest.TargetShardNum, selector: mv.selector,
			sender: newBatchSender(mv.client, data.TopicTraceWrite, rowReplayMaxBatchRows, int(rowReplayMaxBatchBytes), traceReplayBatchTimeout),
			fs:     mv.lfs, logger: mv.logger, schema: pinnedSchema, traceName: pinnedSchema.Metadata.Name, counter: &mv.partsReplayedRowLevel,
		}
	}
	replayer := mv.replayer
	resumed := prepared.checkpoint.Sequence > 0
	rows, err := prepared.Replay(mv.ctx, replayer, shardPath)
	info, statErr := os.Stat(filepath.Join(directory, "manifest.json"))
	if statErr != nil {
		return statErr
	}
	mv.progress.mu.Lock()
	if mv.progress.TraceSamplingReports == nil {
		mv.progress.TraceSamplingReports = make(map[string]samplingReport)
	}
	report := mv.progress.TraceSamplingReports[directory]
	report.Manifest = prepared.manifest
	report.Artifact = directory
	report.ManifestBytes = info.Size()
	report.ReplayedBytes += prepared.replayedBytes
	if resumed {
		report.ResumeCount++
	}
	mv.progress.TraceSamplingReports[directory] = report
	mv.progress.mu.Unlock()
	if err != nil {
		mv.recordError(scopeShard, segment, shard, err.Error())
		return err
	}
	mv.progress.MarkTraceShardCompleted(mv.group, segment.String(), shard)
	mv.progress.MarkSourceTraceShardCompleted(mv.group, segment.String(), shard)
	mv.progress.AddTraceRowReplay(mv.group, len(prepared.manifest.Parts), rows)
	mv.logger.Info().Str("group", mv.group).Str("source_stage", prepared.manifest.SourceStage).
		Str("target_stage", prepared.manifest.TargetStage).Str("policy_digest", prepared.manifest.PolicyDigest).
		Str("artifact", directory).Uint64("input_traces", prepared.manifest.Traces).Uint64("kept_traces", prepared.manifest.KeptTraces).
		Uint64("dropped_traces", prepared.manifest.Traces-prepared.manifest.KeptTraces).Uint64("input_spans", prepared.manifest.Spans).
		Uint64("kept_spans", prepared.manifest.KeptSpans).Interface("fail_open", prepared.manifest.FailOpen).
		Int64("preparation_ns", prepared.manifest.PreparationNanos).Int("replayed_rows", rows).Msg("sampled trace shard delivered")
	return nil
}

func (mv *traceMigrationVisitor) completeSampledSeries() error {
	for _, work := range mv.sampledSeries {
		for _, entry := range mv.lfs.ReadDir(work.path) {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".seg") {
				continue
			}
			value, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".seg"), 16, 64)
			if err != nil {
				return fmt.Errorf("sampled source series %s: %w", filepath.Join(work.path, entry.Name()), err)
			}
			mv.progress.MarkSourceTraceSeriesCompleted(mv.group, work.path, common.ShardID(value))
		}
	}
	return nil
}

type samplingReport struct {
	Artifact      string           `json:"artifact"`
	Manifest      samplingManifest `json:"manifest"`
	ManifestBytes int64            `json:"manifest_bytes"`
	ReplayedBytes uint64           `json:"replayed_bytes"`
	ResumeCount   uint64           `json:"resume_count"`
}
