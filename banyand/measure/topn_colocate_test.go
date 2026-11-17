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

package measure

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/partition"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/schema/topnschema"
)

// capturePublisher captures the rows a processor publishes, also from the processor's own goroutine.
type capturePublisher struct {
	rows []*measurev1.InternalWriteRequest
	mu   sync.Mutex
}

func (c *capturePublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range messages {
		c.rows = append(c.rows, m.Data().(*measurev1.InternalWriteRequest))
	}
	return nil, nil
}

func (c *capturePublisher) Close() (map[string]*common.Error, error) { return nil, nil }

type captureClient struct {
	queue.Client
	publisher queue.BatchPublisher
}

func (c *captureClient) NewBatchPublisher(time.Duration) queue.BatchPublisher { return c.publisher }

func topNTuple(entity string, value int64) *streaming.Tuple2[int64] {
	evs := []*modelv1.TagValue{pbv1.EntityStrValue(entity)}
	data := flow.Data{evs, "g", value, uint64(0)}
	return &streaming.Tuple2[int64]{V1: value, V2: flow.NewStreamRecord(data, 0)}
}

// TestTopNResultRowsAreCoLocatedOnOneShard proves that the windows of one group are published to the shard that
// locates the group's series, whatever source shards their data points came from, and hold every tuple of the window.
func TestTopNResultRowsAreCoLocatedOnOneShard(t *testing.T) {
	const shardNum = 7
	publisher := &capturePublisher{}
	proc := newTopNStreamingProcessor[int64](
		&databasev1.Measure{Entity: &databasev1.Entity{TagNames: []string{"service_id"}}},
		logger.GetLogger("topn-colocate-test"), time.Minute,
		&databasev1.TopNAggregation{
			Metadata:       &commonv1.Metadata{Name: "topn", Group: "g"},
			FieldName:      "value",
			CountersNumber: 10,
		},
		modelv1.Sort_SORT_DESC, nil, nil, &captureClient{publisher: publisher}, "", func(string) uint32 { return shardNum },
	)

	windows := []map[string][]*streaming.Tuple2[int64]{
		{"g": {topNTuple("e0", 10), topNTuple("e1", 60)}},
		{"g": {topNTuple("e0", 90), topNTuple("e1", 60), topNTuple("e2", 5)}},
		{"g": {topNTuple("e2", 15)}},
	}
	for i, window := range windows {
		require.NoError(t, proc.writeStreamRecord(flow.NewStreamRecord(window, time.Unix(int64(i)*60, 0).UnixMilli()), nil))
	}
	require.Len(t, publisher.rows, len(windows), "one row per group and window, rows of one window must not overwrite each other")

	// The shard the measure write path locates for the row's series, from the _top_n_result schema.
	resultMeasure := topnschema.Measure(topnschema.Metadata("g"))
	row := publisher.rows[0].GetRequest()
	_, locatorShard, err := partition.NewEntityLocator(resultMeasure.GetTagFamilies(), resultMeasure.GetEntity(), 0).
		Locate(row.GetMetadata().GetName(), row.GetDataPoint().GetTagFamilies(), shardNum)
	require.NoError(t, err)
	want := uint32(locatorShard)

	decoder := &encoding.BytesBlockDecoder{}
	sums := map[string]int64{}
	for i, row := range publisher.rows {
		assert.Equal(t, want, row.ShardId, "window %d is stored on the shard of the group's series", i)
		value := GenerateTopNValueInt()
		require.NoError(t, value.Unmarshal(row.Request.DataPoint.Fields[0].GetBinaryData(), decoder))
		_, _, values, entities := value.Values()
		for j := range values {
			sums[entities[j][0].GetStr().GetValue()] += values[j]
		}
		ReleaseTopNValue(value)
	}
	assert.Equal(t, map[string]int64{"e0": 100, "e1": 120, "e2": 20}, sums, "all windows of the group are aggregated together")
}
