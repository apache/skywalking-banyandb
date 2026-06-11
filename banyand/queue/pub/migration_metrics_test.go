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

package pub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/apache/skywalking-banyandb/api/data"
	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

func newPubMigrationMetricsWithErrCapture(totalErr *errReasonCapturerImpl) *pubMigrationMetrics { //nolint:exhaustruct
	return &pubMigrationMetrics{
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           totalErr,
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: &countingCounter{},
		totalBatchLatency:  &noopHistogram{},
	}
}

// TestNewWithoutMetadataNilLeavesMigrationMetricsNil verifies that the
// lifecycle/test publishers built with a nil registry emit neither the
// banyandb_queue_pub_* nor the banyandb_lifecycle_migration_* family.
func TestNewWithoutMetadataNilLeavesMigrationMetricsNil(t *testing.T) {
	pp, ok := NewWithoutMetadata(nil).(*pub)
	require.True(t, ok)
	require.Nil(t, pp.migrationMetrics, "migration metrics must be nil without a registry")
	require.Nil(t, pp.metrics, "queue_pub metrics stay nil for the metadata-less publisher")
}

// TestNewWithoutMetadataWithRegistryRegistersMigrationMetrics verifies that
// supplying a registry registers the banyandb_lifecycle_migration_* family while
// the banyandb_queue_pub_* family stays disabled (no metadata).
func TestNewWithoutMetadataWithRegistryRegistersMigrationMetrics(t *testing.T) {
	pp, ok := NewWithoutMetadata(observability.BypassRegistry).(*pub)
	require.True(t, ok)
	require.NotNil(t, pp.migrationMetrics, "migration metrics must be registered when a registry is supplied")
	require.Nil(t, pp.metrics, "queue_pub family must remain disabled (metadata is nil)")
}

// TestRetrySendMirrorsErrorToMigrationMetrics verifies the batch-write path emits
// the error reason to BOTH families in lock-step when both are present.
func TestRetrySendMirrorsErrorToMigrationMetrics(t *testing.T) {
	pubErr := newErrReasonCapturer()
	migErr := newErrReasonCapturer()
	p := &pub{ //nolint:exhaustruct
		metrics:          newPubMetricsWithErrCapture(pubErr),
		migrationMetrics: newPubMigrationMetricsWithErrCapture(migErr),
		nodeCache:        make(map[string]nodeInfo),
	}
	bp := &batchPublisher{pub: p, topic: topicPtr(data.TopicMeasureWrite)}

	mockStream := NewMockSendClient(context.Background())
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error {
		return status.Error(codes.Unavailable, "transient")
	})

	require.Error(t, bp.retrySend(context.Background(), mockStream, &clusterv1.SendRequest{}, "n1"))
	require.Equal(t, float64(1), pubErr.sum(sendErrReasonRetryExhausted))
	require.Equal(t, float64(1), migErr.sum(sendErrReasonRetryExhausted), "migration family must mirror the pub family")
}

// TestPublishMirrorsStartedFinishedToMigrationMetrics verifies the batch-write
// success path increments started+finished on BOTH families.
func TestPublishMirrorsStartedFinishedToMigrationMetrics(t *testing.T) {
	pubStarted, pubFinished := &countingCounter{}, &countingCounter{}
	migStarted, migFinished := &countingCounter{}, &countingCounter{}
	pm := &pubMetrics{ //nolint:exhaustruct
		totalStarted:  pubStarted,
		totalFinished: pubFinished,
		totalLatency:  &noopHistogram{},
		totalErr:      newErrReasonCapturer(),
		sentBytes:     &countingCounter{},
	}
	p := newPubWithConnMgrForMetrics(t, pm)
	p.migrationMetrics = &pubMigrationMetrics{ //nolint:exhaustruct
		totalStarted:  migStarted,
		totalFinished: migFinished,
		totalLatency:  &noopHistogram{},
		totalErr:      newErrReasonCapturer(),
		sentBytes:     &countingCounter{},
	}

	mockStream := NewMockSendClient(context.Background())
	mockStream.SetSendFunc(func(*clusterv1.SendRequest) error { return nil })
	doneCh := make(chan struct{})
	close(doneCh)

	const nodeName = "node-a"
	bp := p.NewBatchPublisher(10 * time.Second).(*batchPublisher)
	bp.streams[nodeName] = writeStream{client: mockStream, ctxDoneCh: doneCh}

	_, publishErr := bp.Publish(context.Background(), data.TopicMeasureWrite, bus.NewMessageWithNode(1, nodeName, []byte("payload")))
	require.NoError(t, publishErr)

	require.Equal(t, float64(1), pubStarted.count)
	require.Equal(t, float64(1), migStarted.count, "migration started must mirror pub started")
	require.Equal(t, float64(1), pubFinished.count)
	require.Equal(t, float64(1), migFinished.count, "migration finished must mirror pub finished")
}

// TestMigrationMirrorBatchFinishedAndLatency verifies that the migration mirror's
// totalBatchFinished and totalBatchLatency are ticked from listenBatchResponse on a
// terminal (success) response path — the first latency observation in that function for the mirror.
func TestMigrationMirrorBatchFinishedAndLatency(t *testing.T) {
	pubBatchFinished := &countingCounter{}
	migBatchFinished := &countingCounter{}
	pubBatchLatency := &countingHistogram{}
	migBatchLatency := &countingHistogram{}

	pm := &pubMetrics{ //nolint:exhaustruct
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           newErrReasonCapturer(),
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: pubBatchFinished,
		totalBatchLatency:  pubBatchLatency,
	}
	p := newPubWithConnMgrForMetrics(t, pm)
	p.migrationMetrics = &pubMigrationMetrics{ //nolint:exhaustruct
		totalStarted:       &countingCounter{},
		totalFinished:      &countingCounter{},
		totalLatency:       &noopHistogram{},
		totalErr:           newErrReasonCapturer(),
		sentBytes:          &countingCounter{},
		totalBatchStarted:  &countingCounter{},
		totalBatchFinished: migBatchFinished,
		totalBatchLatency:  migBatchLatency,
	}

	bp := &batchPublisher{
		pub:   p,
		topic: topicPtr(data.TopicMeasureWrite),
	}

	ctx := context.Background()
	mockStream := NewMockSendClient(ctx)
	mockStream.SetRecvFunc(func() (*clusterv1.SendResponse, error) {
		return &clusterv1.SendResponse{}, nil
	})

	bc := make(chan batchEvent, 1)
	bp.listenBatchResponse(ctx, mockStream, func() {}, bc, "node-a", time.Now(), "test-group")

	require.Equal(t, float64(1), pubBatchFinished.count, "pub total_batch_finished must be 1 on success")
	require.Equal(t, float64(1), migBatchFinished.count, "migration total_batch_finished must mirror pub on success")
	require.Equal(t, 1, pubBatchLatency.count, "pub total_batch_latency must be observed once on success")
	require.Equal(t, 1, migBatchLatency.count, "migration total_batch_latency must mirror pub on success")
}
