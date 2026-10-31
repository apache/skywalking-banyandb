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

package plan

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

// scriptedCluster answers every scan with the node streams it was given.
type scriptedCluster struct {
	streams []*chunkStream
}

func (c *scriptedCluster) Broadcast(time.Duration, bus.Topic, bus.Message) ([]bus.Future, error) {
	return nil, nil
}

func (c *scriptedCluster) Scatter(context.Context, bus.Topic, bus.Message, bus.ScatterOptions) ([]bus.NodeStream, error) {
	out := make([]bus.NodeStream, 0, len(c.streams))
	for _, stream := range c.streams {
		out = append(out, stream)
	}
	return out, nil
}

func (c *scriptedCluster) TimeRange() *modelv1.TimeRange {
	return &modelv1.TimeRange{Begin: timestamppb.New(time.Unix(0, 0)), End: timestamppb.New(time.Unix(100, 0))}
}

func (c *scriptedCluster) NodeSelectors() map[string][]string { return nil }

var _ executor.DistributedExecutionContext = (*scriptedCluster)(nil)

// bucketPartial is the partial a data node emits for a time-bucketed sum grouped by svc.
type bucketPartial struct {
	svc    string
	shard  int64
	bucket int64
	sum    int64
}

func bucketPartialChunk(t *testing.T, rows ...bucketPartial) []byte {
	t.Helper()
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleShardID, Name: "shard_id", Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleTag, TagFamily: defaultName, Name: tagSvc, Type: vectorized.ColumnTypeString},
		{Role: vectorized.RoleField, Name: fieldValue, Type: vectorized.ColumnTypeInt64},
	})
	batch := vectorized.NewRecordBatch(schema, len(rows))
	for _, row := range rows {
		batch.Columns[0].(*vectorized.TypedColumn[int64]).Append(row.shard)
		batch.Columns[1].(*vectorized.TypedColumn[int64]).Append(row.bucket)
		batch.Columns[2].(*vectorized.TypedColumn[string]).Append(row.svc)
		batch.Columns[3].(*vectorized.TypedColumn[int64]).Append(row.sum)
		batch.Len++
	}
	body, err := frame.Encode(batch)
	require.NoError(t, err)
	return body
}

func nodeStreamOf(name string, chunks ...[]byte) *chunkStream {
	stream := &chunkStream{node: name}
	for _, body := range chunks {
		stream.chunks = append(stream.chunks, body)
	}
	return stream
}

func bucketedPlan(t *testing.T, indexMode bool) *DistributedPlan {
	t.Helper()
	req := &measurev1.QueryRequest{
		Name: "demo", Groups: []string{defaultName}, TagProjection: projTagProj(),
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{fieldValue}},
		GroupBy: &measurev1.QueryRequest_GroupBy{
			TagProjection: projTagProj(),
			TimeBucket:    &measurev1.QueryRequest_GroupBy_TimeBucket{Width: "1s"},
		},
		Agg: &measurev1.QueryRequest_Aggregation{Function: modelv1.AggregationFunction_AGGREGATION_FUNCTION_SUM, FieldName: fieldValue},
	}
	schema := testMeasureSchema()
	schema.IndexMode = indexMode
	plan, err := AnalyzeDistributed(req, []*databasev1.Measure{schema}, nil, vmeasure.VectorizedConfig{BatchSize: 16, QueryMemoryMiB: 64})
	require.NoError(t, err)
	return plan
}

func runBucketed(t *testing.T, plan *DistributedPlan, cluster *scriptedCluster) (rows []bucketPartial, firstBatchServed []int) {
	t.Helper()
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	ctx := executor.WithDistributedExecutionContext(t.Context(), cluster)
	result, err := plan.ExecuteBatches(ctx)
	require.NoError(t, err)
	first := true
	for {
		batch, pullErr := result.NextBatch(ctx)
		require.NoError(t, pullErr)
		if batch == nil {
			break
		}
		if first {
			first = false
			for _, stream := range cluster.streams {
				firstBatchServed = append(firstBatchServed, stream.servedChunks())
			}
		}
		schema := batch.Schema
		tsCol := batch.Columns[schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
		svcIdx, ok := schema.TagIndex(defaultName, tagSvc)
		require.True(t, ok)
		valIdx, ok := schema.FieldIndex(fieldValue)
		require.True(t, ok)
		for row := range batch.ActiveLen() {
			rows = append(rows, bucketPartial{
				bucket: tsCol.Data()[row],
				svc:    batch.Columns[svcIdx].(*vectorized.TypedColumn[string]).Data()[row],
				sum:    batch.Columns[valIdx].(*vectorized.TypedColumn[int64]).Data()[row],
			})
		}
	}
	require.NoError(t, result.Close())
	for _, stream := range cluster.streams {
		require.True(t, stream.closed, "closing the result must close the node stream %s", stream.node)
	}
	return rows, firstBatchServed
}

func TestBucketWatermarkEmitsABucketOnceEveryNodeHasPassedIt(t *testing.T) {
	pins := vectorized.OutstandingOwnerPins()
	cluster := &scriptedCluster{streams: []*chunkStream{
		nodeStreamOf("a",
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 1}, bucketPartial{svc: "y", shard: 0, bucket: 1000, sum: 2}),
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 2000, sum: 3}),
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 3000, sum: 5}),
		),
		nodeStreamOf("b",
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 1, bucket: 1000, sum: 10}),
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 1, bucket: 2000, sum: 20}),
			bucketPartialChunk(t, bucketPartial{svc: "y", shard: 1, bucket: 3000, sum: 7}),
		),
	}}
	rows, served := runBucketed(t, bucketedPlan(t, false), cluster)
	require.Equal(t, []bucketPartial{
		{bucket: 1000, svc: "x", sum: 11}, {bucket: 1000, svc: "y", sum: 2},
		{bucket: 2000, svc: "x", sum: 23},
		{bucket: 3000, svc: "x", sum: 5}, {bucket: 3000, svc: "y", sum: 7},
	}, rows)
	require.Equal(t, []int{2, 2}, served, "the first bucket is out once both nodes have sent a later bucket, before they end")
	require.Equal(t, pins, vectorized.OutstandingOwnerPins())
}

func TestBucketWatermarkCountsAReplicatedShardOnce(t *testing.T) {
	same := func() []byte {
		return bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 4}, bucketPartial{svc: "x", shard: 0, bucket: 2000, sum: 6})
	}
	cluster := &scriptedCluster{streams: []*chunkStream{nodeStreamOf("a", same()), nodeStreamOf("b", same())}}
	rows, _ := runBucketed(t, bucketedPlan(t, false), cluster)
	require.Equal(t, []bucketPartial{{bucket: 1000, svc: "x", sum: 4}, {bucket: 2000, svc: "x", sum: 6}}, rows)
}

func TestBucketWatermarkWaitsForNodeWithNoRowsToEndAndSkipsEmptyBuckets(t *testing.T) {
	cluster := &scriptedCluster{streams: []*chunkStream{
		nodeStreamOf("empty"),
		nodeStreamOf("a",
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 1}),
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 5000, sum: 2}),
		),
	}}
	rows, _ := runBucketed(t, bucketedPlan(t, false), cluster)
	require.Equal(t, []bucketPartial{{bucket: 1000, svc: "x", sum: 1}, {bucket: 5000, svc: "x", sum: 2}}, rows,
		"buckets nobody sent rows for produce nothing")
}

func TestBucketWatermarkRejectsAPartialThatIsNotTimeOrdered(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	cluster := &scriptedCluster{streams: []*chunkStream{nodeStreamOf("a",
		bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 2000, sum: 1}),
		bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 2}),
	)}}
	ctx := executor.WithDistributedExecutionContext(t.Context(), cluster)
	result, err := bucketedPlan(t, false).ExecuteBatches(ctx)
	require.NoError(t, err)
	var pullErr error
	for pullErr == nil {
		var batch *vectorized.RecordBatch
		batch, pullErr = result.NextBatch(ctx)
		require.False(t, batch == nil && pullErr == nil, "an unordered partial must fail the query")
	}
	require.ErrorContains(t, pullErr, "time ordered")
	require.NoError(t, result.Close())
}

func TestBucketedIndexModeReducesAfterTheScanAndStaysTimeOrdered(t *testing.T) {
	cluster := &scriptedCluster{streams: []*chunkStream{
		nodeStreamOf("a",
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 3000, sum: 5}),
			bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 1}),
		),
		nodeStreamOf("b", bucketPartialChunk(t, bucketPartial{svc: "x", shard: 1, bucket: 2000, sum: 20})),
	}}
	rows, served := runBucketed(t, bucketedPlan(t, true), cluster)
	require.Equal(t, []bucketPartial{{bucket: 1000, svc: "x", sum: 1}, {bucket: 2000, svc: "x", sum: 20}, {bucket: 3000, svc: "x", sum: 5}}, rows)
	require.Equal(t, []int{2, 1}, served, "an index-mode measure has no time order, so nothing is emitted before every node has ended")
}

func TestFailedNodeFailsAnAggregationAndARowsQuery(t *testing.T) {
	previous := data.MeasureWireModeRaw()
	data.SetMeasureWireModeRaw(true)
	t.Cleanup(func() { data.SetMeasureWireModeRaw(previous) })
	pins := vectorized.OutstandingOwnerPins()
	broken := errors.New("node went away")
	for name, build := range map[string]func() (*DistributedPlan, *scriptedCluster){
		"bucketed aggregation": func() (*DistributedPlan, *scriptedCluster) {
			failing := nodeStreamOf("b", bucketPartialChunk(t, bucketPartial{svc: "x", shard: 1, bucket: 1000, sum: 1}))
			failing.err = broken
			return bucketedPlan(t, false), &scriptedCluster{streams: []*chunkStream{
				nodeStreamOf("a", bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 1})), failing,
			}}
		},
		"index-mode aggregation": func() (*DistributedPlan, *scriptedCluster) {
			failing := nodeStreamOf("b")
			failing.err = broken
			return bucketedPlan(t, true), &scriptedCluster{streams: []*chunkStream{
				nodeStreamOf("a", bucketPartialChunk(t, bucketPartial{svc: "x", shard: 0, bucket: 1000, sum: 1})), failing,
			}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan, cluster := build()
			ctx := executor.WithDistributedExecutionContext(t.Context(), cluster)
			result, err := plan.ExecuteBatches(ctx)
			if err == nil {
				for err == nil {
					var batch *vectorized.RecordBatch
					batch, err = result.NextBatch(ctx)
					require.False(t, batch == nil && err == nil, "the query ended cleanly although a node failed")
				}
				require.NoError(t, result.Close())
			}
			require.ErrorIs(t, err, broken)
			for _, stream := range cluster.streams {
				require.True(t, stream.closed, "node stream %s must be closed", stream.node)
			}
			require.Equal(t, pins, vectorized.OutstandingOwnerPins())
		})
	}
}
