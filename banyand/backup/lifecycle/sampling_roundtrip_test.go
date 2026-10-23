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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/pipeline"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/trace"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/node"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const samplingPartialMode = "partial"

type migrationTestSampler struct {
	mode  string
	sizes []int
	calls int
}

func (*migrationTestSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (*migrationTestSampler) Project() sdk.Projection { return sdk.Projection{Spans: true} }
func (*migrationTestSampler) Close() error            { return nil }
func (s *migrationTestSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	s.calls++
	keep := make([]bool, len(batch.Traces))
	for index, traceBlock := range batch.Traces {
		s.sizes = append(s.sizes, len(traceBlock.Spans))
		keep[index] = s.mode == "keep-all" || ((s.mode == "mixed" || s.mode == samplingPartialMode) && traceBlock.TraceID == "b-keep")
	}
	return sdk.Verdict{Keep: keep}, nil
}

type samplingCapturePublisher struct {
	writes    []*tracev1.InternalWriteRequest
	mu        sync.Mutex
	batches   int
	failAfter int
}

func (p *samplingCapturePublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches++
	for _, message := range messages {
		switch payload := message.Data().(type) {
		case *tracev1.InternalWriteRequest:
			p.writes = append(p.writes, proto.Clone(payload).(*tracev1.InternalWriteRequest))
		case []byte:
			write := &tracev1.InternalWriteRequest{}
			if err := proto.Unmarshal(payload, write); err != nil {
				return nil, err
			}
			p.writes = append(p.writes, write)
		default:
			return nil, fmt.Errorf("unexpected sampling write %T", payload)
		}
	}
	return nil, nil
}
func (*samplingCapturePublisher) Close() (map[string]*common.Error, error) { return nil, nil }

type samplingCaptureStream struct {
	capture *samplingCapturePublisher
	number  int
}

func (p *samplingCaptureStream) Publish(ctx context.Context, topic bus.Topic, messages ...bus.Message) (bus.Future, error) {
	return p.capture.Publish(ctx, topic, messages...)
}

func (p *samplingCaptureStream) Close() (map[string]*common.Error, error) {
	if p.capture.failAfter > 0 && p.number == 2 {
		return nil, fmt.Errorf("injected transport failure")
	}
	return nil, nil
}

func TestSamplingMultipartPreparationReplayAndResume(t *testing.T) {
	for _, mode := range []string{"mixed", "keep-all", "drop-all", samplingPartialMode} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			shard := filepath.Join(t.TempDir(), "00000000")
			for partID := uint64(1); partID <= 3; partID++ {
				rows := make([]trace.PartEncoderRow, 0, 2)
				for _, id := range []string{"a-drop", "b-keep"} {
					rows = append(rows,
						trace.PartEncoderRow{
							TraceID: id,
							SpanID: fmt.Sprintf("%s-%d",
								id,
								partID),
							Span:      []byte("body"),
							Timestamp: now.UnixNano(),
							Tags: []trace.PartEncoderTag{{
								Name:  "timestamp",
								Value: trace.EncodedTimestamp(now.UnixNano()),
							}},
						})
				}
				_, release := trace.EncodePart(shard, fs.NewLocalFileSystem(), partID, rows)
				release()
			}
			schema := &databasev1.Trace{
				Metadata: &commonv1.Metadata{
					Name:  "trace",
					Group: "group",
				},
				Tags: []*databasev1.TraceTagSpec{
					{
						Name: "trace_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "span_id",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "timestamp",
						Type: databasev1.TagType_TAG_TYPE_TIMESTAMP,
					},
				},
				TraceIdTagName:   "trace_id",
				SpanIdTagName:    "span_id",
				TimestampTagName: "timestamp",
			}
			group := &GroupConfig{
				Group: &commonv1.Group{
					Metadata: &commonv1.Metadata{Name: "group"},
					Catalog:  commonv1.Catalog_CATALOG_TRACE,
				},
				SourceStage:    "hot",
				TargetStage:    "warm",
				TargetShardNum: 2,
				TargetReplicas: 1,
			}
			sampler := &migrationTestSampler{mode: mode}
			session := pipeline.NewSession([]sdk.Sampler{sampler}, 3)
			defer session.Close()
			policy := &pinnedSamplingPolicy{
				schema:  schema,
				config:  &commonv1.TracePipelineConfig{Enabled: true},
				session: session,
				host:    samplingHostConfig{Timeout: time.Second},
				digest:  "original-policy",
			}
			segment := &timestamp.TimeRange{Start: now.Add(-24 * time.Hour), End: now.Add(24 * time.Hour)}
			artifactRoot := t.TempDir()
			directory := samplingArtifactPath(artifactRoot, "group", segment, 0)
			selectedMode, modeErr := selectSamplingMode(artifactRoot, "group", filepath.Dir(shard), false, true)
			require.NoError(t, modeErr)
			require.True(t, selectedMode)
			prepared, err := prepareTraceMigration(context.Background(), directory, shard, filepath.Dir(shard), segment, 0, group, policy)
			require.NoError(t, err)
			require.Equal(t, []int{3, 3}, sampler.sizes, "all source parts must be assembled before Decide")
			require.Equal(t, uint64(2), prepared.manifest.Traces)
			require.Equal(t, uint64(6), prepared.manifest.Spans)
			expectedRows := map[string]int{"mixed": 3, "keep-all": 6, "drop-all": 0, samplingPartialMode: 3}[mode]
			require.Equal(t, uint64(expectedRows), prepared.manifest.KeptSpans)
			selector, err := node.NewPickFirstSelector()
			require.NoError(t, err)
			selector.AddNode(&databasev1.Node{Metadata: &commonv1.Metadata{Name: "target"}})
			client := queue.NewMockClient(gomock.NewController(t))
			capture := &samplingCapturePublisher{}
			streamNumber := 0
			client.EXPECT().NewBatchPublisher(gomock.Any()).DoAndReturn(func(time.Duration) queue.BatchPublisher {
				streamNumber++
				return &samplingCaptureStream{capture: capture, number: streamNumber}
			}).AnyTimes()
			replayer := &traceRowReplayer{
				group:    "group",
				selector: selector,
				fs:       fs.NewLocalFileSystem(),
				logger:   logger.GetLogger("sampling-test"),
				sender: newBatchSender(client,
					data.TopicTraceWrite,
					100,
					1<<20,
					time.Second),
			}

			if mode == samplingPartialMode {
				replayer.sender.maxRows = 1
				capture.failAfter = 1
				_, partialErr := prepared.Replay(context.Background(), replayer, shard)
				require.ErrorContains(t, partialErr, "injected transport failure")
				require.False(t, prepared.checkpoint.Delivered)
				require.Equal(t, uint64(1), prepared.checkpoint.Sequence)
				require.GreaterOrEqual(t, len(capture.writes), 2, "the failed acknowledgement may have reached the receiver")
				require.LessOrEqual(t, len(capture.writes), 3, "in-flight delivery remains bounded")
				require.Equal(t, "b-keep-1", capture.writes[0].Request.Tags[1].GetStr().Value)
				capture.writes = nil
				require.NoError(t, prepared.Close())
				priorCalls := sampler.calls
				prepared, err = openPreparedTraceMigration(directory)
				require.NoError(t, err)
				require.Equal(t, priorCalls, sampler.calls)
				_, closeErr := replayer.Close()
				require.NoError(t, closeErr)
				capture.failAfter = 0
				capture.batches = 0
				replayer.sender = newBatchSender(client, data.TopicTraceWrite, 100, 1<<20, time.Second)
			}
			replayed, err := prepared.Replay(context.Background(), replayer, shard)
			require.NoError(t, err)
			if mode == samplingPartialMode {
				require.Equal(t, expectedRows-1, replayed)
			} else {
				require.Equal(t, expectedRows, replayed)
			}
			if mode == samplingPartialMode {
				require.Len(t, capture.writes, expectedRows-1)
				require.Equal(t, "b-keep-2", capture.writes[0].Request.Tags[1].GetStr().Value)
			} else {
				require.Len(t, capture.writes, expectedRows)
			}
			expectedBatches := 0
			if expectedRows > 0 {
				expectedBatches = 1
			}
			require.Equal(t, expectedBatches, capture.batches, "part boundaries must not flush replay batches")
			for _, write := range capture.writes {
				if mode == "mixed" || mode == samplingPartialMode {
					require.Equal(t, "b-keep", write.Request.Tags[0].GetStr().Value)
				}
			}
			require.True(t, prepared.checkpoint.Delivered)
			require.NoError(t, prepared.Close())
			progress := NewProgress("", logger.GetLogger("sampling-test"))
			visitor := &traceMigrationVisitor{
				ctx:            context.Background(),
				group:          "group",
				samplingRoot:   artifactRoot,
				samplingGroup:  group,
				sourceStage:    "hot",
				targetStage:    "warm",
				targetShardNum: 2,
				replicas:       1,
				lfs:            fs.NewLocalFileSystem(),
				replayer:       replayer,
				progress:       progress,
				logger:         logger.GetLogger("sampling-test"),
			}
			require.NoError(t, visitor.VisitSeries(segment, "missing-source-index", []common.ShardID{0}))
			require.Len(t, visitor.sampledSeries, 1, "source index transfer is deferred in sampled mode")
			require.Empty(t, progress.SourceCompletedTraceSeries, "series cannot complete before retained-row delivery")
			require.NoError(t, visitor.VisitShard(segment, 0, shard), "sampled path must precede ordinary chunk transfer even when no live policy remains")
			require.True(t, progress.IsTraceShardCompleted("group", segment.String(), 0))

			calls := sampler.calls
			restored, err := prepareTraceMigration(context.Background(), directory, "missing-new-snapshot", "changed", segment, 0, group, nil)
			require.NoError(t, err)
			defer restored.Close()
			require.Equal(t, "original-policy", restored.manifest.PolicyDigest)
			replayed, err = restored.Replay(context.Background(), replayer, shard)
			require.NoError(t, err)
			require.Zero(t, replayed)
			require.Equal(t, calls, sampler.calls, "delivery resume must not invoke sampler")
			_, err = replayer.Close()
			require.NoError(t, err)
		})
	}
}
