// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package measure

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

type queueLatencyObservation struct {
	labels []string
	value  float64
}

type queueLatencyHistogram struct {
	observations []queueLatencyObservation
}

func (h *queueLatencyHistogram) Observe(value float64, labelValues ...string) {
	h.observations = append(h.observations, queueLatencyObservation{value: value, labels: append([]string(nil), labelValues...)})
}

func (h *queueLatencyHistogram) Delete(...string) bool {
	return true
}

func TestFileSyncQueuedAtUnixNanoPersistsAndMergeKeepsOldest(t *testing.T) {
	fileSystem := fs.NewLocalFileSystem()
	partPath := t.TempDir()
	wantQueuedAtUnixNano := time.Now().Add(-time.Hour).UnixNano()
	metadata := &partMetadata{QueuedAtUnixNano: wantQueuedAtUnixNano}
	metadata.mustWriteMetadata(fileSystem, partPath)

	var restored partMetadata
	restored.mustReadMetadata(fileSystem, partPath)
	require.Equal(t, wantQueuedAtUnixNano, restored.QueuedAtUnixNano)

	parts := []*partWrapper{
		{p: &part{partMetadata: partMetadata{QueuedAtUnixNano: wantQueuedAtUnixNano + int64(time.Minute)}}},
		{p: &part{partMetadata: partMetadata{}}},
		{p: &part{partMetadata: partMetadata{QueuedAtUnixNano: wantQueuedAtUnixNano}}},
	}
	require.Equal(t, wantQueuedAtUnixNano, oldestQueuedAtUnixNano(parts))
}

func TestStampQueuedAtUnixNanoOnlyAtWriteQueueIntroduction(t *testing.T) {
	queuedAtUnixNano := time.Now().UnixNano()
	queuedMemPart := &memPart{}
	wrappedPart := &partWrapper{mp: queuedMemPart, p: &part{}}
	writeQueue := &tsTable{getNodes: func() []string { return nil }}
	writeQueue.stampQueuedAtUnixNano(wrappedPart, queuedAtUnixNano)
	require.Equal(t, queuedAtUnixNano, wrappedPart.p.partMetadata.QueuedAtUnixNano)
	require.Equal(t, queuedAtUnixNano, wrappedPart.mp.partMetadata.QueuedAtUnixNano)

	writeQueue.stampQueuedAtUnixNano(wrappedPart, queuedAtUnixNano+1)
	require.Equal(t, queuedAtUnixNano, wrappedPart.p.partMetadata.QueuedAtUnixNano)

	storagePart := &partWrapper{mp: &memPart{}, p: &part{}}
	(&tsTable{}).stampQueuedAtUnixNano(storagePart, queuedAtUnixNano)
	require.Zero(t, storagePart.p.partMetadata.QueuedAtUnixNano)
}

func TestObserveAcknowledgedFileSyncPartsSelectsSuccessfulParts(t *testing.T) {
	histogram := &queueLatencyHistogram{}
	tst := &tsTable{metrics: &metrics{fileSyncQueueLatency: histogram}}
	acknowledgedAt := time.Unix(1_000, 0)
	succeeded := &part{partMetadata: partMetadata{ID: 1, QueuedAtUnixNano: acknowledgedAt.Add(-10 * time.Second).UnixNano()}}
	failed := &part{partMetadata: partMetadata{ID: 2, QueuedAtUnixNano: acknowledgedAt.Add(-20 * time.Second).UnixNano()}}
	legacy := &part{partMetadata: partMetadata{ID: 3}}
	future := &part{partMetadata: partMetadata{ID: 4, QueuedAtUnixNano: acknowledgedAt.Add(time.Second).UnixNano()}}

	tst.observeAcknowledgedFileSyncParts([]*part{succeeded, failed, legacy, future}, "data-1", &queue.SyncResult{
		Success:     true,
		FailedParts: []queue.FailedPart{{PartID: "2"}},
	}, acknowledgedAt)
	require.Equal(t, []queueLatencyObservation{{value: 10, labels: []string{"data-1"}}}, histogram.observations)

	tst.observeAcknowledgedFileSyncParts([]*part{failed}, "data-1", &queue.SyncResult{Success: true}, acknowledgedAt)
	require.Equal(t, []queueLatencyObservation{
		{value: 10, labels: []string{"data-1"}},
		{value: 20, labels: []string{"data-1"}},
	}, histogram.observations)

	tst.observeAcknowledgedFileSyncParts([]*part{succeeded}, "data-1", &queue.SyncResult{}, acknowledgedAt)
	require.Len(t, histogram.observations, 2)
}
