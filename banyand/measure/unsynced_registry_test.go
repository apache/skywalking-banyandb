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
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const unsyncedMetricPrefix = "banyandb_measure_"

var unsyncedMetricNames = []string{
	unsyncedMetricPrefix + "unsynced_parts",
	unsyncedMetricPrefix + "unsynced_part_bytes",
	unsyncedMetricPrefix + "oldest_unsynced_part_age_seconds",
	unsyncedMetricPrefix + "total_sync_failed_parts",
}

func newPromRegistry() *promRegistry {
	return &promRegistry{MetricsRegistry: observability.BypassRegistry, reg: prometheus.NewRegistry()}
}

// series returns the gathered samples of one metric keyed by "seg/shard"; a metric without those labels uses "".
func (r *promRegistry) series(t *testing.T, name string) map[string]float64 {
	t.Helper()
	families, err := r.reg.Gather()
	require.NoError(t, err)
	result := make(map[string]float64)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			var seg, shard string
			for _, label := range metric.GetLabel() {
				switch label.GetName() {
				case "seg":
					seg = label.GetValue()
				case "shard":
					shard = label.GetValue()
				}
			}
			key := ""
			if seg != "" || shard != "" {
				key = seg + "/" + shard
			}
			if metric.GetGauge() != nil {
				result[key] = metric.GetGauge().GetValue()
			} else {
				result[key] = metric.GetCounter().GetValue()
			}
		}
	}
	return result
}

func TestQueueMetricsExposeUnsyncedSeriesPerShard(t *testing.T) {
	r := newPromRegistry()
	qm, _ := (&queueSupplier{omr: r}).newMetrics(common.Position{Database: "g"})
	m := qm.(*metrics)

	require.Equal(t, map[string]float64{"": 0}, r.series(t, unsyncedMetricPrefix+"total_sync_failed_parts"),
		"the failure counter exists at zero before any failure")

	now := time.Now()
	newTable := func(shardID common.ShardID, size uint64, age time.Duration) *tsTable {
		tst := newUnsyncedTestTable(m, &partWrapper{p: &part{partMetadata: partMetadata{
			TotalCount: 1, CompressedSizeBytes: size, QueuedAtUnixNano: now.Add(-age).UnixNano(),
		}}})
		tst.shardID = shardID
		return tst
	}
	shard0 := newTable(0, 10, 10*time.Second)
	shard1 := newTable(1, 100, 100*time.Second)
	shard0.collectUnsynced()
	shard1.collectUnsynced()

	require.Equal(t, map[string]float64{"seg-1/0": 1, "seg-1/1": 1}, r.series(t, unsyncedMetricPrefix+"unsynced_parts"))
	require.Equal(t, map[string]float64{"seg-1/0": 10, "seg-1/1": 100}, r.series(t, unsyncedMetricPrefix+"unsynced_part_bytes"))
	age := r.series(t, unsyncedMetricPrefix+"oldest_unsynced_part_age_seconds")
	require.Len(t, age, 2)
	require.InDelta(t, 10, age["seg-1/0"], 5)
	require.InDelta(t, 100, age["seg-1/1"], 5)

	shard0.deleteMetrics()
	require.Equal(t, map[string]float64{"seg-1/1": 1}, r.series(t, unsyncedMetricPrefix+"unsynced_parts"))
	require.Equal(t, map[string]float64{"seg-1/1": 100}, r.series(t, unsyncedMetricPrefix+"unsynced_part_bytes"))
	require.Len(t, r.series(t, unsyncedMetricPrefix+"oldest_unsynced_part_age_seconds"), 1)

	shard1.deleteMetrics()
	m.DeleteAll()
	for _, name := range unsyncedMetricNames {
		require.Empty(t, r.series(t, name), name)
	}
}

func TestDataSupplierMetricsHaveNoUnsyncedSeries(t *testing.T) {
	r := newPromRegistry()
	dm, _ := (&supplier{omr: r}).newMetrics(common.Position{Database: "g"})
	m := dm.(*metrics)
	require.Nil(t, m.totalSyncFailedParts)
	require.Nil(t, m.unsyncedParts)

	// A non-empty snapshot makes collectUnsynced reach the gauges, which a data node does not have.
	tst := newUnsyncedTestTable(m, &partWrapper{p: &part{partMetadata: partMetadata{
		TotalCount: 1, CompressedSizeBytes: 10, QueuedAtUnixNano: time.Now().UnixNano(),
	}}})
	require.NotPanics(t, func() {
		tst.collectUnsynced()
		tst.deleteMetrics()
		m.DeleteAll()
		tst.incTotalSyncFailedParts(1)
	})
}

func TestCollectUnsyncedNilInputs(t *testing.T) {
	require.NotPanics(t, func() {
		newUnsyncedTestTable(nil).collectUnsynced()
	})

	m := newUnsyncedTestMetrics()
	require.NotPanics(t, func() {
		(&tsTable{metrics: m, snapshot: nil}).collectUnsynced()
	})
	require.Empty(t, m.unsyncedParts.(*recordedGauge).series)
	require.Empty(t, m.unsyncedPartBytes.(*recordedGauge).series)
	require.Empty(t, m.oldestUnsyncedPartAgeSeconds.(*recordedGauge).series)
}

// The Collect hook that OpenDB hands to the write queue is what refreshes the unsynced gauges in
// production, and closing the queue removes them.
func TestQueueSupplierOpenDBCollectsUnsyncedMetrics(t *testing.T) {
	r := newPromRegistry()
	supplier := &queueSupplier{
		omr:                     r,
		l:                       logger.GetLogger("test"),
		measureDataNodeRegistry: grpc.NewLocalNodeRegistry(),
		path:                    t.TempDir(),
		option: option{
			tire2Client:  &fakeTier2Client{syncFn: func(int32) (*queue.SyncResult, error) { return nil, errors.New("unreachable") }},
			protector:    protector.Nop{},
			syncInterval: time.Hour,
			flushTimeout: time.Hour,
		},
	}
	db, err := supplier.OpenDB(&commonv1.Group{
		Metadata: &commonv1.Metadata{Name: "test-group"},
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        4,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	shard, err := db.(*wqueue.Queue[*tsTable, option]).GetOrCreateShard(common.ShardID(2))
	require.NoError(t, err)
	sub := shard.SubQueue()
	sub.mustAddDataPoints(dpsTS1)

	partsMetric := unsyncedMetricPrefix + "unsynced_parts"
	require.Empty(t, r.series(t, partsMetric), "the gauges are written by the collection, not by the write")
	obsservice.MetricsCollector.Collect()
	require.Equal(t, map[string]float64{"/2": 1}, r.series(t, partsMetric))
	require.Greater(t, r.series(t, unsyncedMetricPrefix+"oldest_unsynced_part_age_seconds")["/2"], float64(0), "the part is stamped with its queue time")
	require.Greater(t, r.series(t, unsyncedMetricPrefix+"unsynced_part_bytes")["/2"], float64(0))

	require.NoError(t, db.Close())
	// Closing the queue removes its gauges from the registry.
	for _, name := range unsyncedMetricNames {
		require.Empty(t, r.series(t, name), name)
	}
}
