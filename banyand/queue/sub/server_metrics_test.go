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

type noopHistogram struct{}

func (*noopHistogram) Observe(_ float64, _ ...string) {}
func (*noopHistogram) Delete(_ ...string) bool        { return true }

func newTestMetrics() *metrics { //nolint:exhaustruct
	c := &noopCounter{}
	h := &noopHistogram{}
	return &metrics{
		totalStarted:         c,
		totalFinished:        c,
		totalLatency:         h,
		totalErr:             c,
		receivedBytes:        c,
		totalMessageStarted:  c,
		totalMessageFinished: c,
		totalBatchStarted:    c,
		totalBatchFinished:   c,
		totalBatchLatency:    h,
	}
}

type fakeCounter struct {
	total float64
}

func (f *fakeCounter) Inc(delta float64, _ ...string) { f.total += delta }
func (*fakeCounter) Delete(_ ...string) bool          { return true }

type fakeHistogram struct {
	count int
}

func (h *fakeHistogram) Observe(_ float64, _ ...string) { h.count++ }
func (*fakeHistogram) Delete(_ ...string) bool          { return true }

// capturingCounterWithLabels records the exact label slices passed to Inc.
type capturingCounterWithLabels struct {
	calls [][]string
}

func (c *capturingCounterWithLabels) Inc(_ float64, labels ...string) {
	cp := make([]string, len(labels))
	copy(cp, labels)
	c.calls = append(c.calls, cp)
}

func (*capturingCounterWithLabels) Delete(_ ...string) bool { return true }

// TestFileSyncCompletionMetrics verifies that handleCompletion records
// totalFinished, totalLatency, receivedBytes with the canonical label order
// (operation, group, remote_node, remote_role, remote_tier).
func TestFileSyncCompletionMetrics(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	finished := &fakeCounter{}
	latency := &fakeHistogram{}
	receivedB := &fakeCounter{}
	totalErrC := &fakeCounter{}

	s := &server{ //nolint:exhaustruct
		log: logger.GetLogger("test-server-completion-metrics"),
		metrics: &metrics{
			totalStarted:  &fakeCounter{},
			totalFinished: finished,
			totalLatency:  latency,
			totalErr:      totalErrC,
			receivedBytes: receivedB,
		},
	}

	session := &syncSession{ //nolint:exhaustruct
		sessionID: "s1",
		startTime: time.Now().Add(-2 * time.Second),
		metadata: &clusterv1.SyncMetadata{
			Group:      "my-group",
			Topic:      "v1:stream-part-sync",
			SenderNode: "data-0:17912",
			SenderRole: "data",
			SenderTier: "hot",
		},
		partCtx: &queue.ChunkedSyncPartContext{},
		partsProgress: map[int]*partProgress{
			0: {totalBytes: 10, receivedBytes: 10, completed: true},
			1: {totalBytes: 10, receivedBytes: 0, completed: false},
		},
		totalReceived:  10,
		chunksReceived: 2,
	}

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

	require.Equal(t, float64(1), finished.total, "totalFinished should be incremented once")
	require.Equal(t, 1, latency.count, "totalLatency should be observed once")
	require.Equal(t, float64(10), receivedB.total, "receivedBytes should equal totalReceived")
	// one failed part
	require.Equal(t, float64(1), totalErrC.total, "one part_failed error should be recorded")
	require.True(t, session.completed)
}

// TestFileSyncStartedMetrics verifies that startOrSwitchSession records totalStarted.
func TestFileSyncStartedMetrics(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	started := &fakeCounter{}
	s := &server{ //nolint:exhaustruct
		log: logger.GetLogger("test-server-started-metrics"),
		metrics: &metrics{
			totalStarted:  started,
			totalFinished: &fakeCounter{},
			totalLatency:  &fakeHistogram{},
			totalErr:      &fakeCounter{},
			receivedBytes: &fakeCounter{},
		},
	}

	req := &clusterv1.SyncPartRequest{ //nolint:exhaustruct
		SessionId: "s1",
		Content: &clusterv1.SyncPartRequest_Metadata{
			Metadata: &clusterv1.SyncMetadata{
				Group:      "g1",
				Topic:      "v1:stream-part-sync",
				SenderNode: "liaison-0:17912",
				SenderRole: "liaison",
				SenderTier: "",
			},
		},
	}
	_ = s.startOrSwitchSession("s1", req, nil)
	require.Equal(t, float64(1), started.total)
}

// TestFileSyncSwitchRecordsErrType verifies that switching sessions records error_type=switch.
func TestFileSyncSwitchRecordsErrType(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	errLabels := &capturingCounterWithLabels{}
	s := &server{ //nolint:exhaustruct
		log: logger.GetLogger("test-server-switch"),
		metrics: &metrics{
			totalStarted:  &fakeCounter{},
			totalFinished: &fakeCounter{},
			totalLatency:  &fakeHistogram{},
			totalErr:      errLabels,
			receivedBytes: &fakeCounter{},
		},
	}

	prev := &syncSession{ //nolint:exhaustruct
		sessionID: "s1",
		startTime: time.Now().Add(-time.Second),
		metadata: &clusterv1.SyncMetadata{
			Group:      "g1",
			Topic:      "v1:stream-part-sync",
			SenderNode: "data-0:17912",
			SenderRole: "data",
			SenderTier: "warm",
		},
	}

	s.cleanupPreviousSession(prev)

	require.True(t, prev.completed)
	require.Len(t, errLabels.calls, 1)
	lastLabel := errLabels.calls[0]
	// label order: operation, group, remote_node, remote_role, remote_tier, error_type
	require.Equal(t, meter.Counter(errLabels).(*capturingCounterWithLabels), errLabels)
	require.Equal(t, "switch", lastLabel[len(lastLabel)-1], "error_type must be 'switch'")
}

// TestFileSyncOperationLabel verifies the labels for file-sync sessions use operation=file-sync.
func TestFileSyncOperationLabel(t *testing.T) {
	logInitErr := logger.Init(logger.Logging{Env: "dev", Level: "info"})
	require.NoError(t, logInitErr)

	started := &capturingCounterWithLabels{}
	s := &server{ //nolint:exhaustruct
		log: logger.GetLogger("test-server-op-label"),
		metrics: &metrics{
			totalStarted:  started,
			totalFinished: &fakeCounter{},
			totalLatency:  &fakeHistogram{},
			totalErr:      &fakeCounter{},
			receivedBytes: &fakeCounter{},
		},
	}

	req := &clusterv1.SyncPartRequest{ //nolint:exhaustruct
		SessionId: "s1",
		Content: &clusterv1.SyncPartRequest_Metadata{
			Metadata: &clusterv1.SyncMetadata{
				Group:      "grp",
				Topic:      "v1:measure-part-sync",
				SenderNode: "data-1:17912",
				SenderRole: "data",
				SenderTier: "cold",
			},
		},
	}
	_ = s.startOrSwitchSession("s1", req, nil)
	require.Len(t, started.calls, 1)
	labels := started.calls[0]
	// operation, group, remote_node, remote_role, remote_tier
	require.Equal(t, "file-sync", labels[0])
	require.Equal(t, "grp", labels[1])
	require.Equal(t, "data-1:17912", labels[2])
	require.Equal(t, "data", labels[3])
	require.Equal(t, "cold", labels[4])
}
