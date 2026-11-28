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
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
	liaisonActiveSeriesGroup  = "active-series-group"
	liaisonActiveSeriesMetric = "banyandb_liaison_active_series_test_active_series"
	liaisonPlainMeasure       = "plain"
	liaisonIndexModeMeasure   = "index_mode"
	liaisonUnsafeMeasure      = "unsafe_sharding"
)

// liaisonClockStart is aligned to the hour, so the first window the tracker sees is complete once the clock advances by one window.
var liaisonClockStart = time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

// liaisonActiveSeriesFactory exports the active_series gauge into its own registry.
type liaisonActiveSeriesFactory struct {
	reg *prometheus.Registry
}

func (f *liaisonActiveSeriesFactory) NewGauge(name string, labelNames ...string) meter.Gauge {
	scope := observability.RootScope.SubScope("liaison_active_series_test")
	return prom.NewProvider(scope, f.reg).Gauge(name, labelNames...)
}

func (*liaisonActiveSeriesFactory) NewCounter(string, ...string) meter.Counter { return nil }

func (*liaisonActiveSeriesFactory) NewHistogram(string, meter.Buckets, ...string) meter.Histogram {
	return nil
}

func (*liaisonActiveSeriesFactory) Close() {}

// liaisonRepository serves one group, backed by the queue, and the measures registered in measures.
type liaisonRepository struct {
	dropRepository
	measures map[string]*measure
}

func (r liaisonRepository) LoadResource(metadata *commonv1.Metadata) (resourceSchema.Resource, bool) {
	m, ok := r.measures[metadata.GetName()]
	if !ok || metadata.GetGroup() != heldGroup {
		return nil, false
	}
	return measureResource{m: m}, true
}

// publishStub acknowledges every publish and counts them. The tables' part sync to the data nodes always fails.
type publishStub struct {
	queue.Client
	published atomic.Int64
}

type ackFuture struct{}

func (ackFuture) Get() (bus.Message, error)      { return bus.Message{}, nil }
func (ackFuture) GetAll() ([]bus.Message, error) { return nil, nil }

func (*publishStub) NewChunkedSyncClient(string, uint32) (queue.ChunkedSyncClient, error) {
	return nil, errors.New("no data node in the test")
}

func (p *publishStub) Publish(context.Context, bus.Topic, ...bus.Message) (bus.Future, error) {
	p.published.Add(1)
	return ackFuture{}, nil
}

// liaisonHarness drives a real writeQueueCallback over a real wqueue.Queue.
type liaisonHarness struct {
	queue    *wqueue.Queue[*tsTable, option]
	callback *writeQueueCallback
	clock    timestamp.MockClock
	factory  *liaisonActiveSeriesFactory
	client   *publishStub
	nodes    atomic.Pointer[[]string]
	tb       testing.TB
	cleanup  func()
	seq      int
}

func newLiaisonMeasure(name string, entity []string, shardingKey []string, indexMode bool) *measure {
	tags := make([]*databasev1.TagSpec, 0, len(entity))
	for _, n := range entity {
		tags = append(tags, &databasev1.TagSpec{Name: n, Type: databasev1.TagType_TAG_TYPE_STRING})
	}
	schema := &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Group: heldGroup, Name: name},
		Entity:      &databasev1.Entity{TagNames: entity},
		TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: tags}},
		Fields:      []*databasev1.FieldSpec{{Name: "v", FieldType: databasev1.FieldType_FIELD_TYPE_INT}},
		IndexMode:   indexMode,
	}
	if len(shardingKey) > 0 {
		schema.ShardingKey = &databasev1.ShardingKey{TagNames: shardingKey}
	}
	m := &measure{schema: schema, countActiveSeries: countsActiveSeries(schema)}
	var is indexSchema
	is.parse(schema)
	m.indexSchema.Store(is)
	return m
}

// newActiveSeriesLiaisonHarness opens a queue that tracks active series when countActive is true and
// tracks nothing otherwise.
func newActiveSeriesLiaisonHarness(tb testing.TB, countActive bool) *liaisonHarness {
	tb.Helper()
	require.NoError(tb, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	path, cleanup := test.Space(require.New(tb))
	h := &liaisonHarness{tb: tb, cleanup: cleanup, clock: timestamp.NewMockClock(), client: &publishStub{}}
	h.clock.Set(liaisonClockStart)
	h.SetNodes("node-1")

	opts := wqueue.Opts[*tsTable, option]{
		Group:           heldGroup,
		ShardNum:        4,
		SegmentInterval: storage.IntervalRule{Unit: storage.DAY, Num: 1},
		Location:        path,
		Option: option{
			protector:    protector.Nop{},
			tire2Client:  h.client,
			flushTimeout: time.Hour,
			syncInterval: time.Hour,
			mergePolicy:  newDefaultMergePolicyForTesting(),
		},
		SubQueueCreator: newWriteQueue,
		GetNodes:        func(common.ShardID) []string { return *h.nodes.Load() },
	}
	if countActive {
		h.factory = &liaisonActiveSeriesFactory{reg: prometheus.NewRegistry()}
		opts.ActiveSeriesFactory = h.factory
	}
	ctx := timestamp.SetClock(common.SetPosition(context.Background(), func(common.Position) common.Position {
		return common.Position{Module: "measure", Database: heldGroup}
	}), h.clock)
	q, err := wqueue.Open(ctx, opts, heldGroup)
	require.NoError(tb, err)
	h.queue = q

	repo := &schemaRepo{Repository: liaisonRepository{
		dropRepository: dropRepository{db: q},
		measures: map[string]*measure{
			liaisonPlainMeasure:     newLiaisonMeasure(liaisonPlainMeasure, []string{"id"}, nil, false),
			TopNSchemaName:          newLiaisonMeasure(TopNSchemaName, []string{"id"}, nil, false),
			liaisonUnsafeMeasure:    newLiaisonMeasure(liaisonUnsafeMeasure, []string{"a", "b"}, []string{"c"}, false),
			liaisonIndexModeMeasure: newLiaisonMeasure(liaisonIndexModeMeasure, []string{"id"}, nil, true),
		},
	}}
	h.callback = &writeQueueCallback{
		l:           logger.GetLogger("test-measure-active-series-liaison"),
		schemaRepo:  repo,
		tire2Client: h.client,
	}
	return h
}

// SetNodes sets the data nodes that GetNodes returns for every shard.
func (h *liaisonHarness) SetNodes(nodes ...string) {
	h.nodes.Store(&nodes)
}

// Rev sends one batch to the callback and requires that no row of it is dropped.
func (h *liaisonHarness) Rev(batch []any) {
	h.seq++
	recorder := common.NewWriteRecorder(time.Time{})
	h.callback.Rev(common.WithWriteRecorder(context.Background(), recorder), bus.NewMessage(bus.MessageID(h.seq), batch))
	require.Empty(h.tb, recorder.RowFailures(), "no row of the batch is dropped")
}

// AdvanceAndCollect moves the clock forward by d and lets the tracker rotate its windows.
func (h *liaisonHarness) AdvanceAndCollect(d time.Duration) {
	h.clock.Add(d)
	h.queue.ActiveSeries().Collect()
}

// Gauge returns the exported active_series value of shard and whether it is exported.
func (h *liaisonHarness) Gauge(shard common.ShardID) (float64, bool) {
	h.tb.Helper()
	require.NotNil(h.tb, h.factory, "the harness tracks no active series")
	families, err := h.factory.reg.Gather()
	require.NoError(h.tb, err)
	label := fmt.Sprint(uint32(shard))
	for _, family := range families {
		if family.GetName() != liaisonActiveSeriesMetric {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "shard" && l.GetValue() == label {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// Close closes the queue and removes its files.
func (h *liaisonHarness) Close() {
	h.tb.Helper()
	var closer io.Closer = h.queue
	require.NoError(h.tb, closer.Close())
	h.cleanup()
}

// liaisonRow writes one row of series number series of measure name to shard.
func liaisonRow(name string, shard common.ShardID, series, row int) *measurev1.InternalWriteRequest {
	entity := []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("series-%d-%d", shard, series)}}}}
	tags := entity
	if name == liaisonUnsafeMeasure {
		other := &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "b"}}}
		entity = append(entity, other)
		tags = entity
	}
	return &measurev1.InternalWriteRequest{
		ShardId:      uint32(shard),
		EntityValues: entity,
		Request: &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Group: heldGroup, Name: name},
			DataPoint: &measurev1.DataPointValue{
				Timestamp:   timestamppb.New(time.Unix(1_800_000_000, int64(row)*int64(time.Millisecond))),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: tags}},
				Fields:      []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(row)}}}},
			},
		},
	}
}

// liaisonBatch has rowsPerSeries rows for each of seriesPerShard series of every shard in shards.
func liaisonBatch(name string, shards []common.ShardID, seriesPerShard, rowsPerSeries int) []any {
	batch := make([]any, 0, len(shards)*seriesPerShard*rowsPerSeries)
	for row := 0; row < rowsPerSeries; row++ {
		for _, shard := range shards {
			for series := 0; series < seriesPerShard; series++ {
				batch = append(batch, liaisonRow(name, shard, series, row))
			}
		}
	}
	return batch
}

func TestActiveSeriesLiaisonCountsDistinctSeriesPerShard(t *testing.T) {
	const seriesPerShard = 1000
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()

	shards := []common.ShardID{0, 1, 2}
	h.Rev(liaisonBatch(liaisonPlainMeasure, shards, seriesPerShard, 3))
	h.Rev(liaisonBatch(liaisonPlainMeasure, shards, seriesPerShard, 3))
	_, exported := h.Gauge(0)
	require.False(t, exported, "the window has not ended")

	h.AdvanceAndCollect(time.Hour)
	for _, shard := range shards {
		got, ok := h.Gauge(shard)
		require.True(t, ok, "shard %d", shard)
		require.InEpsilon(t, seriesPerShard, got, 0.02, "shard %d", shard)
	}
	require.Positive(t, h.client.published.Load(), "the rows reach the data nodes as well")
}

func TestActiveSeriesLiaisonCountsASeriesOncePerBatch(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()

	h.Rev(liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 1, 50))
	h.Rev(liaisonBatch(liaisonPlainMeasure, []common.ShardID{1}, 40, 3))
	h.AdvanceAndCollect(time.Hour)

	single, ok := h.Gauge(0)
	require.True(t, ok)
	require.InDelta(t, 1, single, 0.5, "50 rows of one series are one ID")
	several, ok := h.Gauge(1)
	require.True(t, ok)
	require.InDelta(t, 40, several, 0.5, "3 rows of each of 40 series are 40 IDs")
}

func TestActiveSeriesLiaisonSkipsTopNAndUnsafeShardingKey(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()

	h.Rev(liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 20, 3))
	h.Rev(liaisonBatch(TopNSchemaName, []common.ShardID{1}, 20, 3))
	h.Rev(liaisonBatch(liaisonUnsafeMeasure, []common.ShardID{2}, 20, 3))
	h.AdvanceAndCollect(time.Hour)

	counted, ok := h.Gauge(0)
	require.True(t, ok)
	require.InDelta(t, 20, counted, 0.5)
	_, ok = h.Gauge(1)
	require.False(t, ok, "a Top-N result measure is not counted")
	_, ok = h.Gauge(2)
	require.False(t, ok, "a measure whose sharding key is not a subset of the entity is not counted")
}

func TestActiveSeriesLiaisonCountsIndexMode(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()

	h.Rev(liaisonBatch(liaisonIndexModeMeasure, []common.ShardID{0}, 30, 3))
	h.AdvanceAndCollect(time.Hour)

	got, ok := h.Gauge(0)
	require.True(t, ok)
	require.InDelta(t, 30, got, 0.5)
	require.Positive(t, h.client.published.Load(), "index-mode documents are published to the data nodes")
}

func TestActiveSeriesLiaisonCountsWithoutDataNodes(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()
	h.SetNodes()

	h.Rev(liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 25, 3))
	h.Rev(liaisonBatch(liaisonIndexModeMeasure, []common.ShardID{1}, 25, 3))
	h.AdvanceAndCollect(time.Hour)

	require.Zero(t, h.client.published.Load(), "no data node receives anything")
	for _, shard := range []common.ShardID{0, 1} {
		got, ok := h.Gauge(shard)
		require.True(t, ok, "shard %d", shard)
		require.InDelta(t, 25, got, 0.5, "shard %d", shard)
	}
}

func TestActiveSeriesLiaisonWithoutTrackerWrites(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, false)
	defer h.Close()

	require.Nil(t, h.queue.ActiveSeries())
	h.Rev(liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 20, 3))
	h.Rev(liaisonBatch(liaisonIndexModeMeasure, []common.ShardID{1}, 20, 3))
	require.Positive(t, h.client.published.Load())
}

// TestActiveSeriesIDsAreRecycled checks that counting adds no allocation once the shard window exists,
// and that a batch returns the pooled ID slice it borrowed.
func TestActiveSeriesIDsAreRecycled(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()
	tracker := h.queue.ActiveSeries()
	require.NotNil(t, tracker)

	ids := make([]uint64, 200)
	for i := range ids {
		ids[i] = uint64(i) * 2654435761
	}
	tracker.AddIDs(0, ids)
	require.Zero(t, testing.AllocsPerRun(100, func() { tracker.AddIDs(0, ids) }), "recording IDs in an existing window")

	before := activeSeriesIDsPool.RefsCount()
	batch := liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 20, 3)
	h.Rev(batch)
	require.Equal(t, before, activeSeriesIDsPool.RefsCount(), "the batch gave back every pooled slice it borrowed")
}

// TestReleaseActiveSeriesIDsClearsLength checks that a released ID slice is empty when it is borrowed again.
func TestReleaseActiveSeriesIDsClearsLength(t *testing.T) {
	before := activeSeriesIDsPool.RefsCount()
	borrowed := generateActiveSeriesIDs()
	require.Equal(t, before+1, activeSeriesIDsPool.RefsCount())
	*borrowed = append(*borrowed, 1, 2, 3)
	releaseActiveSeriesIDs(borrowed)
	require.Empty(t, *borrowed, "the released slice is emptied")
	require.Equal(t, before, activeSeriesIDsPool.RefsCount())
}

// TestActiveSeriesLiaisonCollectsOneIDPerSeriesInBatch checks the IDs a batch hands to the tracker.
func TestActiveSeriesLiaisonCollectsOneIDPerSeriesInBatch(t *testing.T) {
	h := newActiveSeriesLiaisonHarness(t, true)
	defer h.Close()

	dst := make(map[string]*dataPointsInQueue)
	for _, row := range liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 3, 5) {
		req := row.(*measurev1.InternalWriteRequest)
		var err error
		dst, err = h.callback.handle(context.Background(), dst, req, req.GetRequest().GetMetadata(), nil)
		require.NoError(t, err)
	}
	tables := dst[heldGroup].tables
	require.Len(t, tables, 1)
	ids := *tables[0].activeSeriesIDs
	require.Len(t, ids, 3, "15 rows of 3 series are 3 IDs")
	require.Len(t, map[uint64]struct{}{ids[0]: {}, ids[1]: {}, ids[2]: {}}, 3, "the IDs are distinct")
	releaseActiveSeriesIDs(tables[0].activeSeriesIDs)
}

// BenchmarkWriteQueueCallbackRev measures one batch of 1000 rows over 200 series, with the tracker off and on.
func BenchmarkWriteQueueCallbackRev(b *testing.B) {
	for _, mode := range []struct {
		name  string
		track bool
	}{{"off", false}, {"on", true}} {
		b.Run(mode.name, func(b *testing.B) {
			h := newActiveSeriesLiaisonHarness(b, mode.track)
			defer h.Close()
			h.SetNodes() // without data nodes the tables do not start part syncs that would add noise
			batch := liaisonBatch(liaisonPlainMeasure, []common.ShardID{0}, 200, 5)
			h.Rev(batch)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h.Rev(batch)
			}
		})
	}
}
