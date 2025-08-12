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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

func Test_newWriteQueue(t *testing.T) {
	tmpPath, defFn := test.Space(require.New(t))
	defer defFn()

	// Create mock tire2Client
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockTire2Client := queue.NewMockClient(ctrl)

	// Mock Publish method
	mockTire2Client.EXPECT().
		Publish(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(bus.Future(nil), nil).
		AnyTimes()

	// Mock NewChunkedSyncClient method
	mockChunkedSyncClient := &mockChunkedSyncClient{}
	mockTire2Client.EXPECT().
		NewChunkedSyncClient(gomock.Any(), gomock.Any()).
		Return(mockChunkedSyncClient, nil).
		AnyTimes()

	// Create tsTable using newWriteQueue
	opts := option{
		tire2Client: mockTire2Client,
		protector:   protector.Nop{},
	}

	tst, err := newWriteQueue(
		fs.NewLocalFileSystem(),
		tmpPath,
		common.Position{},
		logger.GetLogger("test"),
		opts,
		nil,
		"test-group",
		common.ShardID(1),
		func() []string { return []string{"node1", "node2"} },
	)
	require.NoError(t, err)
	require.NotNil(t, tst)
	defer tst.Close()

	// Wait for the creator to be set to snapshotCreatorMemPart
	assert.Eventually(t, func() bool {
		tst.mustAddDataPoints(dpsTS1)
		time.Sleep(100 * time.Millisecond)
		s := tst.currentSnapshot()
		if s == nil {
			return false
		}
		defer s.decRef()
		return s.creator == snapshotCreatorSyncer
	}, flags.EventuallyTimeout, 500*time.Millisecond, "wait for snapshot creator to be set to mem part")

	// Verify that the tsTable has the expected group and shardID
	assert.Equal(t, "test-group", tst.group)
	assert.Equal(t, common.ShardID(1), tst.shardID)

	// Verify that the tire2Client was properly set
	assert.Equal(t, mockTire2Client, tst.option.tire2Client)
}

// mockChunkedSyncClient implements queue.ChunkedSyncClient for testing.
type mockChunkedSyncClient struct{}

func (m *mockChunkedSyncClient) SyncStreamingParts(_ context.Context, parts []queue.StreamingPartData) (*queue.SyncResult, error) {
	return &queue.SyncResult{
		Success:     true,
		SessionID:   "mock-session",
		TotalBytes:  0,
		DurationMs:  0,
		ChunksCount: 0,
		PartsCount:  uint32(len(parts)),
	}, nil
}

func (m *mockChunkedSyncClient) Close() error {
	return nil
}
