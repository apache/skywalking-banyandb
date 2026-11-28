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

package stream

import (
	"context"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/meter/prom"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

const (
	activeSeriesGroup  = "active-series-group"
	activeSeriesStream = "active-series-stream"
)

var activeSeriesBase = time.Date(2027, 1, 15, 10, 0, 0, 0, time.UTC)

// activeSeriesGaugeFactory exposes the gauge it creates through a private Prometheus registry.
type activeSeriesGaugeFactory struct {
	reg *prometheus.Registry
}

func (f *activeSeriesGaugeFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	return prom.NewProvider(observability.RootScope.SubScope("stream_active_series_test"), f.reg).Gauge(name, labelNames...)
}

func (f *activeSeriesGaugeFactory) NewCounter(string, ...string) meter.Counter { return nil }

func (f *activeSeriesGaugeFactory) NewHistogram(string, meter.Buckets, ...string) meter.Histogram {
	return nil
}

func (f *activeSeriesGaugeFactory) Close() {}

// exported returns the active_series value of every shard label.
func (f *activeSeriesGaugeFactory) exported(tb testing.TB) map[string]float64 {
	tb.Helper()
	families, err := f.reg.Gather()
	require.NoError(tb, err)
	values := make(map[string]float64)
	for _, family := range families {
		if !strings.HasSuffix(family.GetName(), "_active_series") {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard" {
					values[l.GetValue()] = m.GetGauge().GetValue()
				}
			}
		}
	}
	return values
}

type activeSeriesRepository struct {
	resourceSchema.Repository
	group resourceSchema.Group
	res   resourceSchema.Resource
}

func (r activeSeriesRepository) LoadGroup(name string) (resourceSchema.Group, bool) {
	return r.group, name == activeSeriesGroup
}

func (r activeSeriesRepository) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	return r.res, metadata.GetGroup() == activeSeriesGroup && metadata.GetName() == activeSeriesStream
}

type activeSeriesQueueGroup struct {
	resourceSchema.Group
	queue *wqueue.Queue[*tsTable, option]
}

func (g activeSeriesQueueGroup) SupplyTSDB() io.Closer { return g.queue }

type activeSeriesResource struct {
	resourceSchema.Resource
	s *stream
}

func (r activeSeriesResource) Delegated() resourceSchema.IndexListener { return r.s }

// activeSeriesClient accepts every publish and every part sync.
type activeSeriesClient struct {
	queue.Client
}

func (activeSeriesClient) Publish(context.Context, bus.Topic, ...bus.Message) (bus.Future, error) {
	return activeSeriesFuture{}, nil
}

func (activeSeriesClient) NewChunkedSyncClient(string, uint32) (queue.ChunkedSyncClient, error) {
	return &mockChunkedSyncClient{}, nil
}

type activeSeriesFuture struct{}

func (activeSeriesFuture) Get() (bus.Message, error)      { return bus.Message{}, nil }
func (activeSeriesFuture) GetAll() ([]bus.Message, error) { return nil, nil }

type liaisonActiveSeriesHarness struct {
	clock    timestamp.MockClock
	factory  *activeSeriesGaugeFactory
	queue    *wqueue.Queue[*tsTable, option]
	callback *writeQueueCallback
}

// newLiaisonActiveSeriesHarness opens a real write queue. It counts active series when countActive is true.
// getNodes decides the nodes of every shard.
func newLiaisonActiveSeriesHarness(tb testing.TB, countActive bool, shardNum uint32, getNodes []string) *liaisonActiveSeriesHarness {
	tb.Helper()
	clock := timestamp.NewMockClock()
	clock.Set(activeSeriesBase)
	h := &liaisonActiveSeriesHarness{clock: clock, factory: &activeSeriesGaugeFactory{reg: prometheus.NewRegistry()}}
	opts := wqueue.Opts[*tsTable, option]{
		Group:           activeSeriesGroup,
		ShardNum:        shardNum,
		SegmentInterval: storage.IntervalRule{Unit: storage.DAY, Num: 1},
		Option:          option{mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}, tire2Client: activeSeriesClient{}},
		SubQueueCreator: newWriteQueue,
		GetNodes:        func(common.ShardID) []string { return getNodes },
	}
	if countActive {
		opts.ActiveSeriesFactory = h.factory
	}
	path, deferFn := test.Space(require.New(tb))
	tb.Cleanup(deferFn)
	opts.Location = path
	ctx := timestamp.SetClock(common.SetPosition(context.Background(), func(p common.Position) common.Position {
		p.Module = "stream"
		p.Database = activeSeriesGroup
		return p
	}), clock)
	q, err := wqueue.Open(ctx, opts, activeSeriesGroup)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, q.Close()) })
	h.queue = q

	s := &stream{schema: &databasev1.Stream{
		Metadata: &commonv1.Metadata{Group: activeSeriesGroup, Name: activeSeriesStream},
		Entity:   &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "searchable",
			Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
	}}
	s.parseSpec()
	repo := &schemaRepo{Repository: activeSeriesRepository{
		group: activeSeriesQueueGroup{queue: q},
		res:   activeSeriesResource{s: s},
	}}
	h.callback = &writeQueueCallback{
		l:           logger.GetLogger("test-stream-active-series"),
		schemaRepo:  repo,
		tire2Client: activeSeriesClient{},
		pm:          protector.Nop{},
	}
	return h
}

// event writes an element of the series seriesName on shardID.
func activeSeriesEvent(shardID uint32, seriesName string, elementID string) *streamv1.InternalWriteRequest {
	id := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: seriesName}}}
	return &streamv1.InternalWriteRequest{
		ShardId:      shardID,
		EntityValues: []*modelv1.TagValue{id},
		Request: &streamv1.WriteRequest{
			Metadata: &commonv1.Metadata{Group: activeSeriesGroup, Name: activeSeriesStream},
			Element: &streamv1.ElementValue{
				ElementId:   elementID,
				Timestamp:   timestamppb.New(activeSeriesBase.Add(time.Minute)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{id}}},
			},
		},
	}
}

// activeSeriesBatch has seriesPerShard series on each shard and elementsPerSeries elements for each series.
func activeSeriesBatch(shards, seriesPerShard, elementsPerSeries int) []any {
	events := make([]any, 0, shards*seriesPerShard*elementsPerSeries)
	for shard := 0; shard < shards; shard++ {
		for s := 0; s < seriesPerShard; s++ {
			name := "svc-" + strconv.Itoa(shard) + "-" + strconv.Itoa(s)
			for e := 0; e < elementsPerSeries; e++ {
				events = append(events, activeSeriesEvent(uint32(shard), name, name+"-"+strconv.Itoa(e)))
			}
		}
	}
	return events
}

// collect ends the window the batches were written in and returns the exported values.
func (h *liaisonActiveSeriesHarness) collect(tb testing.TB) map[string]float64 {
	tb.Helper()
	h.clock.Set(activeSeriesBase.Add(time.Hour))
	storage.ActiveSeriesOf(h.queue).Collect()
	return h.factory.exported(tb)
}

func TestActiveSeriesLiaisonCountsDistinctSeriesPerShard(t *testing.T) {
	const shards, seriesPerShard = 2, 2000
	h := newLiaisonActiveSeriesHarness(t, true, shards, []string{"data-node"})
	h.callback.Rev(context.Background(), bus.NewMessage(1, activeSeriesBatch(shards, seriesPerShard, 3)))

	got := h.collect(t)
	require.Len(t, got, shards)
	for shard := 0; shard < shards; shard++ {
		assert.InEpsilon(t, float64(seriesPerShard), got[strconv.Itoa(shard)], 0.02, "shard %d", shard)
	}
}

func TestActiveSeriesLiaisonCountsASeriesOnce(t *testing.T) {
	h := newLiaisonActiveSeriesHarness(t, true, 1, []string{"data-node"})
	var first, second []any
	for i := 0; i < 50; i++ {
		first = append(first, activeSeriesEvent(0, "svc", "a"+strconv.Itoa(i)))
		second = append(second, activeSeriesEvent(0, "svc", "b"+strconv.Itoa(i)))
	}
	h.callback.Rev(context.Background(), bus.NewMessage(1, first))
	h.callback.Rev(context.Background(), bus.NewMessage(2, second))

	assert.Equal(t, map[string]float64{"0": 1}, h.collect(t))
}

func TestActiveSeriesLiaisonCountsWhenNoNodeServesTheShard(t *testing.T) {
	const seriesPerShard = 500
	h := newLiaisonActiveSeriesHarness(t, true, 1, nil)
	h.callback.Rev(context.Background(), bus.NewMessage(1, activeSeriesBatch(1, seriesPerShard, 2)))

	got := h.collect(t)
	require.Contains(t, got, "0")
	assert.InEpsilon(t, float64(seriesPerShard), got["0"], 0.02)
}

func TestActiveSeriesLiaisonWithoutFactoryDoesNotCount(t *testing.T) {
	h := newLiaisonActiveSeriesHarness(t, false, 1, []string{"data-node"})
	require.Nil(t, h.queue.ActiveSeries())
	require.NotPanics(t, func() {
		h.callback.Rev(context.Background(), bus.NewMessage(1, activeSeriesBatch(1, 10, 2)))
	})
	assert.Empty(t, h.collect(t))
}

// TestActiveSeriesLiaisonSubQueueSyncsToNodes checks that the harness sub-queues can sync parts to their nodes,
// which their background syncer does after every flush.
func TestActiveSeriesLiaisonSubQueueSyncsToNodes(t *testing.T) {
	h := newLiaisonActiveSeriesHarness(t, true, 1, []string{"data-node"})
	shard, err := h.queue.GetOrCreateShard(0)
	require.NoError(t, err)
	tst := shard.SubQueue()
	require.NotPanics(t, func() {
		_, err = tst.executeSyncWithRetry(nil, tst.getNodes())
	})
	require.NoError(t, err)
}

// BenchmarkWriteQueueCallbackRev measures a batch of 1000 elements of 200 series, without (off) and with (on) active series counting.
func BenchmarkWriteQueueCallbackRev(b *testing.B) {
	for _, mode := range []struct {
		name        string
		countActive bool
	}{{"off", false}, {"on", true}} {
		b.Run(mode.name, func(b *testing.B) {
			h := newLiaisonActiveSeriesHarness(b, mode.countActive, 1, nil)
			events := activeSeriesBatch(1, 200, 5)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.callback.Rev(ctx, bus.NewMessage(bus.MessageID(i), events))
			}
		})
	}
}
