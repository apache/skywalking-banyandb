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
	"sort"
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
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// syncCapturePublisher captures the rows a running processor publishes from its own goroutine.
type syncCapturePublisher struct {
	rows []*measurev1.InternalWriteRequest
	mu   sync.Mutex
}

func (c *syncCapturePublisher) Publish(_ context.Context, _ bus.Topic, messages ...bus.Message) (bus.Future, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range messages {
		c.rows = append(c.rows, m.Data().(*measurev1.InternalWriteRequest))
	}
	return nil, nil
}

func (c *syncCapturePublisher) Close() (map[string]*common.Error, error) { return nil, nil }

// windowValues returns the values of the first published row of the window starting at windowStart.
func (c *syncCapturePublisher) windowValues(t *testing.T, windowStart time.Time) ([]int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, row := range c.rows {
		if !row.Request.DataPoint.Timestamp.AsTime().Equal(windowStart) {
			continue
		}
		value := GenerateTopNValueInt()
		defer ReleaseTopNValue(value)
		require.NoError(t, value.Unmarshal(row.Request.DataPoint.Fields[0].GetBinaryData(), &encoding.BytesBlockDecoder{}))
		_, _, values, _ := value.Values()
		got := append([]int64(nil), values...)
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		return got, true
	}
	return nil, false
}

// TestTopNProcessorRanksByItsOwnDirection runs the processors that a TopN aggregation without a field value sort
// gets, one per direction, and checks that each keeps the counters of its own direction: the ASC list holds the
// smallest values and the DESC list the largest.
func TestTopNProcessorRanksByItsOwnDirection(t *testing.T) {
	windowStart := time.Unix(1_700_000_040, 0).Truncate(time.Minute)
	tests := []struct {
		want []int64
		sort modelv1.Sort
	}{
		{sort: modelv1.Sort_SORT_ASC, want: []int64{10, 20}},
		{sort: modelv1.Sort_SORT_DESC, want: []int64{30, 40}},
	}
	for _, tt := range tests {
		t.Run(tt.sort.String(), func(t *testing.T) {
			publisher := &syncCapturePublisher{}
			srcCh := make(chan any)
			src, err := sources.NewChannel(srcCh)
			require.NoError(t, err)
			proc := newTopNStreamingProcessor[int64](
				&databasev1.Measure{Entity: &databasev1.Entity{TagNames: []string{"service_id"}}},
				logger.GetLogger("topn-order-test"), time.Minute,
				&databasev1.TopNAggregation{
					Metadata:       &commonv1.Metadata{Name: "topn", Group: "g"},
					FieldName:      "value",
					FieldValueSort: modelv1.Sort_SORT_UNSPECIFIED,
					CountersNumber: 2,
					LruSize:        5,
				},
				tt.sort, srcCh, streaming.New("topn-order-test", src), &captureClient{publisher: publisher}, "",
			).withShardNum(func(string) uint32 { return 2 }).start(context.Background())
			defer func() { require.NoError(t, proc.Close()) }()

			for i, v := range []int64{30, 10, 40, 20} {
				evs := []*modelv1.TagValue{pbv1.EntityStrValue(string(rune('a' + i)))}
				proc.Write(flow.NewStreamRecord(flow.Data{evs, "", v, uint64(i + 1)}, windowStart.UnixMilli()))
			}
			// A point three windows later moves the watermark past the window, which flushes it.
			proc.Write(flow.NewStreamRecord(flow.Data{[]*modelv1.TagValue{pbv1.EntityStrValue("late")}, "", int64(1), uint64(99)},
				windowStart.Add(3*time.Minute).UnixMilli()))

			var got []int64
			require.Eventually(t, func() bool {
				var ok bool
				got, ok = publisher.windowValues(t, windowStart)
				return ok
			}, 10*time.Second, 20*time.Millisecond, "the window is never flushed")
			assert.Equal(t, tt.want, got)
		})
	}
}
