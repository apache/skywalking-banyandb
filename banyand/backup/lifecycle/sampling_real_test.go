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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// TestSamplingRealSnapshot is opt-in; downloaded input and output stay outside the checkout.
// REAL_TRACE_DIR contains metadata/{group,trace}.json and source/sw_trace/seg-20261012/shard-1.
// REAL_TRACE_SHARD, REAL_TRACE_BEGIN, REAL_TRACE_END override the shard and its exact source bounds.
func TestSamplingRealSnapshot(t *testing.T) {
	root := os.Getenv("REAL_TRACE_DIR")
	if root == "" {
		t.Skip("set REAL_TRACE_DIR to an independently downloaded native trace snapshot")
	}
	root, err := filepath.Abs(root)
	require.NoError(t, err)
	originalGroup := &commonv1.Group{}
	schema := &databasev1.Trace{}
	readRealTraceMetadata(t, filepath.Join(root, "metadata", "group.json"), "group", originalGroup)
	readRealTraceMetadata(t, filepath.Join(root, "metadata", "trace.json"), "trace", schema)
	shard := os.Getenv("REAL_TRACE_SHARD")
	if shard == "" {
		shard = filepath.Join(root, "source", originalGroup.Metadata.Name, "seg-20261012", "shard-1")
	}
	shardName := filepath.Base(shard)
	shardNumber, shardErr := strconv.ParseUint(strings.TrimPrefix(shardName, "shard-"), 10, 32)
	require.NoError(t, shardErr, "source shard name must identify its actual shard ID")
	begin := realTraceTime(t, "REAL_TRACE_BEGIN", "2026-10-12T00:00:00Z")
	end := realTraceTime(t, "REAL_TRACE_END", "2026-10-13T00:00:00Z")
	segment := &timestamp.TimeRange{Start: begin, End: end, IncludeStart: true}
	output, outputErr := os.MkdirTemp(root, "simulation-")
	require.NoError(t, outputErr)
	pluginDirectory := filepath.Join(output, "plugins")
	require.NoError(t, makeSamplingDirectory(pluginDirectory))
	buildRealTraceSampler(t, pluginDirectory)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })
	localQueue := queue.Local()
	metadataService, err := metadataservice.NewService()
	require.NoError(t, err)
	metrics := obsservice.NewMetricService(metadataService, localQueue, "real-trace-simulation", nil)
	destinationService, err := trace.NewService(metadataService, localQueue, metrics, protector.NewMemory(metrics))
	require.NoError(t, err)
	ports, err := test.AllocateFreePorts(1)
	require.NoError(t, err)
	destinationRoot := filepath.Join(output, "destination")
	stop := test.SetupModules([]string{
		"--schema-server-root-path=" + filepath.Join(output, "metadata-store"),
		fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]), "--schema-server-grpc-host=127.0.0.1",
		"--trace-root-path=" + destinationRoot, "--trace-flush-timeout=200ms",
	}, localQueue, metadataService, destinationService)
	defer stop()
	targetGroup := proto.Clone(originalGroup).(*commonv1.Group)
	targetGroup.Pipeline = nil
	targetGroup.ResourceOpts.Stages = nil
	targetGroup.ResourceOpts.DefaultStages = nil
	targetGroup.ResourceOpts.Ttl = &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3650}
	targetGroup.Metadata.ModRevision = 0
	_, err = metadataService.SchemaRegistry().CreateGroup(context.Background(), targetGroup)
	require.NoError(t, err)
	localSchema := proto.Clone(schema).(*databasev1.Trace)
	localSchema.Metadata.ModRevision = 0
	_, err = metadataService.SchemaRegistry().CreateTrace(context.Background(), localSchema)
	require.NoError(t, err)
	timestampIndex := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: "simulation-timestamp", Group: originalGroup.Metadata.Name},
		Tags:     []string{schema.TimestampTagName}, Type: databasev1.IndexRule_TYPE_TREE,
	}
	_, err = metadataService.SchemaRegistry().CreateIndexRule(context.Background(), timestampIndex)
	require.NoError(t, err)
	_, err = metadataService.SchemaRegistry().CreateIndexRuleBinding(context.Background(), &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: "simulation-timestamp-binding", Group: originalGroup.Metadata.Name},
		Subject:  &databasev1.Subject{Name: schema.Metadata.Name, Catalog: commonv1.Catalog_CATALOG_TRACE}, Rules: []string{timestampIndex.Metadata.Name},
		BeginAt: timestamppb.New(time.Unix(0, 0)), ExpireAt: timestamppb.New(time.Now().AddDate(100, 0, 0)),
	})
	require.NoError(t, err)
	var destination trace.Trace
	require.Eventually(t, func() bool {
		destination, err = destinationService.Trace(localSchema.Metadata)
		return err == nil && len(destination.GetIndexRules()) == 1
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { _, loaded := destinationService.LoadGroup(originalGroup.Metadata.Name); return loaded }, 30*time.Second, 100*time.Millisecond)
	config, err := structpb.NewStruct(map[string]any{"durationThresholdMs": 500, "keepErrors": true, "healthySampleRate": 0.01})
	require.NoError(t, err)
	sourceGroup := proto.Clone(originalGroup).(*commonv1.Group)
	sourceGroup.Pipeline = &commonv1.TracePipelineConfig{
		Enabled: true, SchemaNames: []string{schema.Metadata.Name},
		MergeGrace: originalGroup.GetPipeline().GetMergeGrace(), Stages: []*commonv1.StageRule{{Stage: "hot", Plugins: []*commonv1.Plugin{
			{Name: "real-skywalking", Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{Path: "sw-trace-sampler.so", AbiVersion: sdk.ABIVersion, Config: config}}},
		}}},
	}
	group := &GroupConfig{
		Group: sourceGroup, SourceStage: "hot", TargetStage: "warm", TargetShardNum: targetGroup.ResourceOpts.ShardNum,
		SegmentInterval: sourceGroup.ResourceOpts.SegmentInterval, TargetSegmentInterval: targetGroup.ResourceOpts.SegmentInterval,
		SamplingHost: samplingHostConfig{
			Enabled: true, TrustedDirectory: pluginDirectory, Timeout: time.Second, CircuitBreakN: 3, ArtifactRoot: filepath.Join(output, "artifacts"),
		},
	}
	policy, err := resolveSamplingPolicy(context.Background(), group, metadataService)
	require.NoError(t, err)
	defer policy.close()
	// Bind preparation to the downloaded schema revision, rather than the local registry's revision.
	policy.schema = schema
	selector := singleNodeSelector(t)
	client := queue.NewMockClient(gomock.NewController(t))
	client.EXPECT().NewBatchPublisher(gomock.Any()).DoAndReturn(func(timeout time.Duration) queue.BatchPublisher { return localQueue.NewBatchPublisher(timeout) }).AnyTimes()
	progress := NewProgress(filepath.Join(output, "progress.json"), logger.GetLogger("real-trace-simulation"))
	visitor := newTraceMigrationVisitor(context.Background(), sourceGroup, group.TargetShardNum, 0, selector, client,
		logger.GetLogger("real-trace-simulation"), progress, 1<<20, storage.MustToIntervalRule(group.TargetSegmentInterval), metadataService,
		"hot", "warm", storage.MustToIntervalRule(group.SegmentInterval))
	visitor.samplingPolicy = policy
	visitor.samplingGroup = group
	visitor.samplingRoot = group.SamplingHost.ArtifactRoot
	visitor.snapshot = root
	defer visitor.Close()
	selected, err := selectSamplingMode(visitor.samplingRoot, sourceGroup.Metadata.Name, root, false, true)
	require.NoError(t, err)
	require.True(t, selected)
	start := time.Now()
	require.NoError(t, visitor.VisitShard(segment, common.ShardID(shardNumber), shard))
	directory := samplingArtifactPath(visitor.samplingRoot, sourceGroup.Metadata.Name, segment, uint32(shardNumber))
	prepared, err := openPreparedTraceMigration(directory)
	require.NoError(t, err)
	defer prepared.Close()
	verified, signature := verifyRealTraceReplay(t, prepared, destination, segment)
	require.Eventually(t, func() bool {
		indexCount, indexSignature := queryRealTraceIndex(t, destination, schema, timestampIndex, segment)
		return indexCount == verified && signature == indexSignature
	}, 30*time.Second, 100*time.Millisecond, "rebuilt timestamp index must match every retained source body")
	summary := map[string]any{
		"source": shard, "policy": "SkyWalking duration500ms/errors/healthy0.01", "manifest": prepared.manifest,
		"verified_unique_spans": verified, "payload_signature": hex.EncodeToString(signature[:]), "elapsed_seconds": time.Since(start).Seconds(),
	}
	require.NoError(t, atomicSamplingJSON(filepath.Join(output, "result.json"), summary))
	t.Logf("real simulation: traces=%d kept=%d dropped=%d spans=%d kept_spans=%d verified=%d elapsed=%s output=%s",
		prepared.manifest.Traces, prepared.manifest.KeptTraces, prepared.manifest.Traces-prepared.manifest.KeptTraces,
		prepared.manifest.Spans, prepared.manifest.KeptSpans, verified, time.Since(start), output)
}

func readRealTraceMetadata(t *testing.T, path, key string, message proto.Message) {
	t.Helper()
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var wrapper map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wrapper))
	if wrapped := wrapper[key]; wrapped != nil {
		payload = wrapped
	}
	require.NoError(t, protojson.Unmarshal(payload, message))
}

func realTraceTime(t *testing.T, key, fallback string) time.Time {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		value = fallback
	}
	parsed, err := time.Parse(time.RFC3339, value)
	require.NoError(t, err)
	return parsed
}

func buildRealTraceSampler(t *testing.T, directory string) {
	t.Helper()
	root, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err = os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		require.NotEqual(t, root, parent)
		root = parent
	}
	args := []string{"build", "-buildmode=plugin", "-o", filepath.Join(directory, "sw-trace-sampler.so")}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "-race", "-trimpath":
				if setting.Value == "true" {
					args = append(args, setting.Key)
				}
			case "-tags":
				if setting.Value != "" {
					args = append(args, "-tags", setting.Value)
				}
			}
		}
	}
	args = append(args, "./plugins/skywalking/sw-trace-sampler")
	command := exec.Command("go", args...)
	command.Dir = root
	command.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "native sampler build: %s", output)
}

func verifyRealTraceReplay(t *testing.T, prepared *preparedTraceMigration, destination trace.Trace, segment *timestamp.TimeRange) (uint64, [32]byte) {
	t.Helper()
	scanner, err := trace.NewProjectedSnapshotScanner(context.Background(), prepared.manifest.Parts, sdk.Projection{SpanIDs: true, Spans: true},
		trace.SnapshotScanOptions{MaxTraceBytes: 32 << 20, MaxMetadataBytes: 8 << 20, MaxOpenReaders: 16, TempDir: prepared.directory})
	require.NoError(t, err)
	defer scanner.Close()
	var count uint64
	var signature [32]byte
	batch := make(map[string]map[string][32]byte)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ids := make([]string, 0, len(batch))
		for id := range batch {
			ids = append(ids, id)
		}
		got := make(map[string]map[string][32]byte)
		require.Eventually(t, func() bool {
			got = make(map[string]map[string][32]byte)
			results, queryErr := destination.Query(context.Background(), model.TraceQueryOptions{TimeRange: segment, TraceIDs: ids, MaxTraceSize: 100000})
			if queryErr != nil {
				return false
			}
			if results != nil {
				defer results.Release()
				for row := results.Pull(); row != nil; row = results.Pull() {
					if row.Error != nil {
						return false
					}
					if got[row.TID] == nil {
						got[row.TID] = make(map[string][32]byte)
					}
					for index, id := range row.SpanIDs {
						got[row.TID][id] = sha256.Sum256(row.Spans[index])
					}
				}
			}
			for id, wanted := range batch {
				if len(wanted) == 0 {
					if len(got[id]) != 0 {
						return false
					}
					continue
				}
				if len(got[id]) != len(wanted) {
					return false
				}
				for spanID, digest := range wanted {
					if got[id][spanID] != digest {
						return false
					}
				}
			}
			return true
		}, 30*time.Second, 100*time.Millisecond, "bounded destination query must match retained source bodies and exclude drops")
		for id, spans := range got {
			for spanID, digest := range spans {
				count++
				addRealTraceSignature(&signature, id, spanID, digest)
			}
		}
		clear(batch)
	}
	for {
		assembled, scanErr := scanner.Next(context.Background())
		if errors.Is(scanErr, io.EOF) {
			break
		}
		require.NoError(t, scanErr)
		require.False(t, assembled.Oversized, "real-data verification limits each trace to32MiB")
		keep, lookupErr := prepared.table.lookup(assembled.Trace.TraceID)
		require.NoError(t, lookupErr)
		spans := make(map[string][32]byte)
		if keep {
			for index, id := range assembled.Trace.SpanIDs {
				spans[id] = sha256.Sum256(assembled.Trace.Spans[index])
			}
		}
		batch[assembled.Trace.TraceID] = spans
		if len(batch) >= 128 {
			flush()
		}
	}
	flush()
	return count, signature
}

func addRealTraceSignature(signature *[32]byte, id, spanID string, body [32]byte) {
	digest := sha256.New()
	_, _ = io.WriteString(digest, id)
	_, _ = digest.Write([]byte{0})
	_, _ = io.WriteString(digest, spanID)
	_, _ = digest.Write(body[:])
	value := digest.Sum(nil)
	for index := range signature {
		signature[index] ^= value[index]
	}
}

func queryRealTraceIndex(t *testing.T, destination trace.Trace, schema *databasev1.Trace, rule *databasev1.IndexRule,
	segment *timestamp.TimeRange,
) (uint64, [32]byte) {
	t.Helper()
	var count uint64
	var signature [32]byte
	results, err := destination.Query(context.Background(), model.TraceQueryOptions{
		TimeRange: segment, Name: schema.Metadata.Name,
		Entities: [][]*modelv1.TagValue{{}}, Order: &index.OrderBy{Index: rule, Sort: modelv1.Sort_SORT_ASC},
		MinVal: math.MinInt64, MaxVal: math.MaxInt64, MaxTraceSize: 100000,
	})
	require.NoError(t, err)
	if results == nil {
		return 0, signature
	}
	defer results.Release()
	for row := results.Pull(); row != nil; row = results.Pull() {
		require.NoError(t, row.Error)
		for index, spanID := range row.SpanIDs {
			count++
			addRealTraceSignature(&signature, row.TID, spanID, sha256.Sum256(row.Spans[index]))
		}
	}
	return count, signature
}
