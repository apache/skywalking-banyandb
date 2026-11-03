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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	metadataservice "github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestSamplingDestinationRebuildsIndexesAcrossRoutingChanges(t *testing.T) {
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	gomega.RegisterFailHandler(func(message string, _ ...int) { panic(message) })
	cases := []struct {
		name        string
		storageUnit storage.IntervalUnit
		unit        commonv1.IntervalRule_Unit
		shards      uint32
	}{
		{name: "same-interval", unit: commonv1.IntervalRule_UNIT_DAY, storageUnit: storage.DAY, shards: 2},
		{name: "split-interval", unit: commonv1.IntervalRule_UNIT_HOUR, storageUnit: storage.HOUR, shards: 2},
		{name: "changed-shard-count", unit: commonv1.IntervalRule_UNIT_DAY, storageUnit: storage.DAY, shards: 3},
	}
	for _, scenario := range cases {
		t.Run(scenario.name, func(t *testing.T) {
			localQueue := queue.Local()
			metadataService, err := metadataservice.NewService()
			require.NoError(t, err)
			metrics := obsservice.NewMetricService(metadataService, localQueue, "sampling-destination", nil)
			traceService, err := trace.NewService(metadataService, localQueue, metrics, protector.NewMemory(metrics))
			require.NoError(t, err)
			ports, err := test.AllocateFreePorts(1)
			require.NoError(t, err)
			targetRoot := t.TempDir()
			stop := test.SetupModules([]string{
				"--schema-server-root-path=" + t.TempDir(), fmt.Sprintf("--schema-server-grpc-port=%d", ports[0]),
				"--schema-server-grpc-host=127.0.0.1", "--trace-root-path=" + targetRoot, "--trace-flush-timeout=200ms",
			}, localQueue, metadataService, traceService)
			defer stop()
			targetInterval := &commonv1.IntervalRule{Unit: scenario.unit, Num: 1}
			sourceInterval := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
			targetGroup := &commonv1.Group{
				Metadata: &commonv1.Metadata{Name: roundtripTraceGroup}, Catalog: commonv1.Catalog_CATALOG_TRACE,
				ResourceOpts: &commonv1.ResourceOpts{
					ShardNum:        scenario.shards,
					SegmentInterval: targetInterval,
					Ttl: &commonv1.IntervalRule{
						Unit: commonv1.IntervalRule_UNIT_DAY,
						Num:  7,
					},
				},
			}
			_, err = metadataService.SchemaRegistry().CreateGroup(context.Background(), targetGroup)
			require.NoError(t, err)
			traceSchema := &databasev1.Trace{Metadata: &commonv1.Metadata{Name: roundtripTraceName, Group: roundtripTraceGroup}, Tags: []*databasev1.TraceTagSpec{
				{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
			}, TraceIdTagName: "trace_id", SpanIdTagName: "span_id", TimestampTagName: "timestamp"}
			_, err = metadataService.SchemaRegistry().CreateTrace(context.Background(), traceSchema)
			require.NoError(t, err)

			timestampIndex := &databasev1.IndexRule{
				Metadata: &commonv1.Metadata{Name: "migration-timestamp", Group: roundtripTraceGroup},
				Tags:     []string{"timestamp"}, Type: databasev1.IndexRule_TYPE_TREE,
			}
			_, err = metadataService.SchemaRegistry().CreateIndexRule(context.Background(), timestampIndex)
			require.NoError(t, err)
			_, err = metadataService.SchemaRegistry().CreateIndexRuleBinding(context.Background(), &databasev1.IndexRuleBinding{
				Metadata: &commonv1.Metadata{Name: "migration-timestamp-binding", Group: roundtripTraceGroup},
				Subject:  &databasev1.Subject{Name: roundtripTraceName, Catalog: commonv1.Catalog_CATALOG_TRACE},
				Rules:    []string{timestampIndex.Metadata.Name},
				BeginAt:  timestamppb.New(time.Now().Add(-7 * 24 * time.Hour)),
				ExpireAt: timestamppb.New(time.Now().Add(365 * 24 * time.Hour)),
			})
			require.NoError(t, err)
			var destination trace.Trace
			require.Eventually(t, func() bool {
				destination, err = traceService.Trace(traceSchema.Metadata)
				return err == nil && len(destination.GetIndexRules()) == 1
			}, 30*time.Second, 100*time.Millisecond, "destination index binding must be active before replay")
			// The group watcher and write subscriber start independently of schema lookup.
			require.Eventually(t, func() bool { _, ok := traceService.LoadGroup(roundtripTraceGroup); return ok }, 30*time.Second, 100*time.Millisecond)
			sourceRoot := t.TempDir()
			sourceShard := filepath.Join(sourceRoot, "00000000")
			start := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
			segment := &timestamp.TimeRange{Start: start, End: start.Add(24 * time.Hour), IncludeStart: true}
			wanted := []string{"b-keep-1", "b-keep-2", "b-keep-3"}
			for partID := uint64(1); partID <= 3; partID++ {
				ts := start.Add(time.Duration(3+partID*2) * time.Hour)
				var rows []trace.PartEncoderRow
				for _, traceID := range []string{"a-drop", "b-keep"} {
					rows = append(rows,
						trace.PartEncoderRow{
							TraceID: traceID,
							SpanID: fmt.Sprintf("%s-%d",
								traceID,
								partID),
							Span:      []byte("payload"),
							Timestamp: ts.UnixNano(),
							Tags: []trace.PartEncoderTag{{
								Name:  "timestamp",
								Value: trace.EncodedTimestamp(ts.UnixNano()),
							}},
						})
				}
				_, release := trace.EncodePart(sourceShard, fs.NewLocalFileSystem(), partID, rows)
				release()
			}
			// Invalid original index bytes prove sampled visitation never ships or opens them.
			seriesPath := filepath.Join(sourceRoot, "series")
			require.NoError(t, os.MkdirAll(seriesPath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(seriesPath, "0000000000000000.seg"), []byte("unshippable source series"), 0o600))
			require.NoError(t, os.MkdirAll(filepath.Join(sourceShard, "sidx"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(sourceShard, "sidx", "invalid"), []byte("unshippable source sidx"), 0o600))
			sampler := &migrationTestSampler{mode: "mixed"}
			session := pipeline.NewSession([]sdk.Sampler{sampler}, 3)
			policy := &pinnedSamplingPolicy{
				schema:  traceSchema,
				config:  &commonv1.TracePipelineConfig{Enabled: true},
				session: session,
				host:    samplingHostConfig{Timeout: time.Second},
				digest:  "destination-test",
			}
			sourceGroup := &GroupConfig{
				Group: &commonv1.Group{
					Metadata: targetGroup.Metadata,
					Catalog:  commonv1.Catalog_CATALOG_TRACE,
				},
				SourceStage:           "hot",
				TargetStage:           "warm",
				SegmentInterval:       sourceInterval,
				TargetSegmentInterval: targetInterval,
				TargetShardNum:        scenario.shards,
				TargetReplicas:        1,
			}
			// Only batch publication is allowed; chunk/file transfer would fail this mock.
			client := queue.NewMockClient(gomock.NewController(t))
			client.EXPECT().NewBatchPublisher(gomock.Any()).DoAndReturn(func(timeout time.Duration) queue.BatchPublisher {
				return localQueue.NewBatchPublisher(timeout)
			}).AnyTimes()
			log := logger.GetLogger("sampling-destination")
			progress := NewProgress(filepath.Join(t.TempDir(), "progress.json"), log)
			visitor := newTraceMigrationVisitor(context.Background(), sourceGroup.Group, scenario.shards, 1, singleNodeSelector(t), client, log, progress, 1<<20,
				storage.IntervalRule{Unit: scenario.storageUnit, Num: 1}, metadataService, "hot", "warm", storage.IntervalRule{Unit: storage.DAY, Num: 1})
			visitor.samplingPolicy = policy
			visitor.samplingGroup = sourceGroup
			visitor.samplingRoot = t.TempDir()
			visitor.snapshot = sourceRoot
			sampled, modeErr := selectSamplingMode(visitor.samplingRoot, roundtripTraceGroup, sourceRoot, false, true)
			require.NoError(t, modeErr)
			require.True(t, sampled)
			selected, modeErr := selectSamplingMode(visitor.samplingRoot, sourceGroup.Metadata.Name, sourceRoot, false, true)
			require.NoError(t, modeErr)
			require.True(t, selected)
			defer visitor.Close()
			require.NoError(t, visitor.VisitSeries(segment, seriesPath, []common.ShardID{0}))
			require.Empty(t, progress.SourceCompletedTraceSeries)
			require.NoError(t, visitor.VisitShard(segment, 0, sourceShard))
			require.NoError(t, visitor.completeSampledSeries())
			chunked, _ := visitor.CounterSnapshot()
			require.Zero(t, chunked)
			require.Equal(t, []int{3, 3}, sampler.sizes)
			require.True(t, progress.IsTraceShardCompleted(roundtripTraceGroup, segment.String(), 0))
			queryRange := &timestamp.TimeRange{Start: start, End: start.Add(24 * time.Hour), IncludeStart: true, IncludeEnd: true}
			query := func(indexed bool) ([]string, []string, error) {
				options := model.TraceQueryOptions{
					TimeRange:     queryRange,
					MaxTraceSize:  100,
					TagProjection: &model.TagProjection{Names: []string{"timestamp"}},
					MinVal:        math.MinInt64,
					MaxVal:        math.MaxInt64,
				}
				if indexed {
					options.Order = &index.OrderBy{Sort: modelv1.Sort_SORT_ASC, Index: timestampIndex}
					options.Name = roundtripTraceName
					options.Entities = [][]*modelv1.TagValue{{}}
				} else {
					options.TraceIDs = []string{"a-drop", "b-keep"}
				}
				results, queryErr := destination.Query(context.Background(), options)
				if queryErr != nil {
					return nil, nil, queryErr
				}
				if results == nil {
					return nil, nil, nil
				}
				defer results.Release()
				var ids, spanIDs []string
				for result := results.Pull(); result != nil; result = results.Pull() {
					if result.Error != nil {
						return nil, nil, result.Error
					}
					ids = append(ids, result.TID)
					spanIDs = append(spanIDs, result.SpanIDs...)
				}
				sort.Strings(spanIDs)
				return ids, spanIDs, nil
			}
			require.Eventually(t, func() bool {
				ids, spans, queryErr := query(false)
				return queryErr == nil && len(ids) == 1 && ids[0] == "b-keep" && fmt.Sprint(spans) == fmt.Sprint(wanted)
			}, 30*time.Second, 100*time.Millisecond, "retained trace must have every span and dropped trace must be absent")
			require.Eventually(t, func() bool {
				ids, spans, queryErr := query(true)
				return queryErr == nil && len(ids) == 1 && ids[0] == "b-keep" && fmt.Sprint(spans) == fmt.Sprint(wanted)
			}, 30*time.Second, 100*time.Millisecond, "rebuilt destination series and SIDX must find only the whole retained trace")
			require.Eventually(t, func() bool { return len(findRoundtripPartDirs(targetRoot)) > 0 }, 30*time.Second, 100*time.Millisecond)
		})
	}
}
