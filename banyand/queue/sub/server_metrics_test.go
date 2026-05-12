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

package sub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

type noopCounter struct{}

func (*noopCounter) Inc(_ float64, _ ...string) {}
func (*noopCounter) Delete(_ ...string) bool    { return true }

type noopGauge struct{}

func (*noopGauge) Set(_ float64, _ ...string) {}
func (*noopGauge) Add(_ float64, _ ...string) {}
func (*noopGauge) Delete(_ ...string) bool    { return true }

type noopHistogram struct{}

func (*noopHistogram) Observe(_ float64, _ ...string) {}
func (*noopHistogram) Delete(_ ...string) bool        { return true }

func newTestMetrics() *metrics {
	c := &noopCounter{}
	g := &noopGauge{}
	h := &noopHistogram{}
	return &metrics{
		totalStarted:  c,
		totalFinished: c,
		totalErr:      c,
		totalLatency:  c,

		totalMsgReceived:    c,
		totalMsgReceivedErr: c,
		totalMsgSent:        c,
		totalMsgSentErr:     c,

		outOfOrderChunksReceived: c,
		chunksBuffered:           c,
		bufferTimeouts:           c,
		largeGapsRejected:        c,
		bufferCapacityExceeded:   c,
		finishSyncErr:            c,

		activeSyncSessions: g,
		reorderBuffered:    g,

		chunkedSyncAbortedTotal: c,
		chunkedSyncFailedParts:  c,
		chunkedSyncTotalBytes:   c,
		chunkedSyncDurationSecs: h,
	}
}

type fakeCounter struct {
	total float64
}

func (f *fakeCounter) Inc(delta float64, _ ...string) { f.total += delta }
func (*fakeCounter) Delete(_ ...string) bool          { return true }

type fakeGauge struct {
	value float64
}

func (g *fakeGauge) Set(v float64, _ ...string)     { g.value = v }
func (g *fakeGauge) Add(delta float64, _ ...string) { g.value += delta }
func (*fakeGauge) Delete(_ ...string) bool          { return true }
func newFakeGauge() meter.Gauge                     { return &fakeGauge{} }
func getGaugeValue(g meter.Gauge) float64           { return g.(*fakeGauge).value }

type fakeHistogram struct {
	count int
}

func (h *fakeHistogram) Observe(_ float64, _ ...string) { h.count++ }
func (*fakeHistogram) Delete(_ ...string) bool          { return true }

func TestReleaseMetricsReleasesGauges(t *testing.T) {
	m := &metrics{
		activeSyncSessions: newFakeGauge(),
		reorderBuffered:    newFakeGauge(),
	}

	sess := &syncSession{
		sessionID: "s1",
		startTime: time.Now(),
		metadata:  &clusterv1.SyncMetadata{Topic: "v1:stream-part-sync"},
		chunkBuffer: &chunkBuffer{
			chunks: map[uint32]*clusterv1.SyncPartRequest{
				2: {ChunkIndex: 2},
				3: {ChunkIndex: 3},
			},
		},
	}

	// simulate start increments
	m.activeSyncSessions.Add(1, sess.metadata.Topic)
	m.reorderBuffered.Add(2, sess.metadata.Topic)

	sess.releaseMetrics(m)

	require.Equal(t, float64(0), getGaugeValue(m.activeSyncSessions))
	require.Equal(t, float64(0), getGaugeValue(m.reorderBuffered))
	// idempotent
	sess.releaseMetrics(m)
	require.Equal(t, float64(0), getGaugeValue(m.activeSyncSessions))
	require.Equal(t, float64(0), getGaugeValue(m.reorderBuffered))
}

func TestCompletionOutcomeMetricsRecorded(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})
	require.NoError(t, logInitErr)

	topic := "v1:stream-part-sync"
	totalBytes := float64(10)

	s := &server{
		log: logger.GetLogger("test-server-completion-metrics"),
		metrics: &metrics{
			activeSyncSessions:      newFakeGauge(),
			reorderBuffered:         newFakeGauge(),
			chunkedSyncAbortedTotal: &fakeCounter{},
			chunkedSyncFailedParts:  &fakeCounter{},
			chunkedSyncTotalBytes:   &fakeCounter{},
			chunkedSyncDurationSecs: &fakeHistogram{},
		},
	}

	session := &syncSession{
		sessionID: "s1",
		startTime: time.Now().Add(-2 * time.Second),
		metadata:  &clusterv1.SyncMetadata{Topic: topic},
		partCtx:   &queue.ChunkedSyncPartContext{},
		partsProgress: map[int]*partProgress{
			0: {totalBytes: 10, receivedBytes: 10, completed: true},
			1: {totalBytes: 10, receivedBytes: 0, completed: false},
		},
		totalReceived:  10,
		chunksReceived: 2,
	}

	// Simulate start increments that would have happened on session creation.
	s.metrics.activeSyncSessions.Add(1, topic)

	mockStream := &MockSyncPartStream{}
	req := &clusterv1.SyncPartRequest{
		SessionId:  session.sessionID,
		ChunkIndex: 0,
		Content: &clusterv1.SyncPartRequest_Completion{
			Completion: &clusterv1.SyncCompletion{},
		},
	}
	handleErr := s.handleCompletion(mockStream, session, req)
	require.NoError(t, handleErr)

	require.Equal(t, totalBytes, s.metrics.chunkedSyncTotalBytes.(*fakeCounter).total)
	require.Equal(t, 1, s.metrics.chunkedSyncDurationSecs.(*fakeHistogram).count)
	require.Equal(t, float64(1), s.metrics.chunkedSyncFailedParts.(*fakeCounter).total)
	require.Equal(t, float64(0), getGaugeValue(s.metrics.activeSyncSessions))
	require.True(t, session.completed)
}

func TestCleanupPreviousSessionRecordsAborted(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})
	require.NoError(t, logInitErr)

	topic := "v1:stream-part-sync"
	s := &server{
		log: logger.GetLogger("test-server-switch-abort"),
		metrics: &metrics{
			activeSyncSessions:      newFakeGauge(),
			reorderBuffered:         newFakeGauge(),
			chunkedSyncAbortedTotal: &fakeCounter{},
		},
	}

	prev := &syncSession{
		sessionID: "s1",
		startTime: time.Now().Add(-time.Second),
		metadata:  &clusterv1.SyncMetadata{Topic: topic},
	}
	s.metrics.activeSyncSessions.Add(1, topic)

	s.cleanupPreviousSession(prev)

	require.True(t, prev.abortedRecorded)
	require.Equal(t, float64(1), s.metrics.chunkedSyncAbortedTotal.(*fakeCounter).total)
	require.Equal(t, float64(0), getGaugeValue(s.metrics.activeSyncSessions))
}
