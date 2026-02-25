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

package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

func TestGroupRepo_AcquireAndRelease(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	require.NoError(t, gr.acquireRequest("test-group"))
	require.NoError(t, gr.acquireRequest("test-group"))
	gr.releaseRequest("test-group")
	gr.releaseRequest("test-group")

	gr.RWMutex.RLock()
	item, ok := gr.inflight["test-group"]
	gr.RWMutex.RUnlock()
	assert.True(t, ok)
	assert.Nil(t, item.done)
}

func TestGroupRepo_AcquireDuringPendingDeletion(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	_ = gr.startPendingDeletion("del-group")
	err := gr.acquireRequest("del-group")
	assert.ErrorIs(t, err, errGroupPendingDeletion)
}

func TestGroupRepo_PendingDeletion(t *testing.T) {
	tests := []struct {
		name       string
		acquireNum int
	}{
		{name: "completes after release", acquireNum: 1},
		{name: "no inflight", acquireNum: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gr := &groupRepo{
				log:          logger.GetLogger("test"),
				resourceOpts: make(map[string]*commonv1.ResourceOpts),
				inflight:     make(map[string]*groupInflight),
			}
			for range tc.acquireNum {
				require.NoError(t, gr.acquireRequest("g"))
			}
			done := gr.startPendingDeletion("g")
			if tc.acquireNum > 0 {
				select {
				case <-done:
					t.Fatal("done channel should not be closed while requests are in flight")
				default:
				}
				for range tc.acquireNum {
					gr.releaseRequest("g")
				}
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("done channel did not close after all requests were released")
			}
		})
	}
}

func TestGroupRepo_ClearPendingDeletion(t *testing.T) {
	gr := &groupRepo{
		log:          logger.GetLogger("test"),
		resourceOpts: make(map[string]*commonv1.ResourceOpts),
		inflight:     make(map[string]*groupInflight),
	}

	gr.startPendingDeletion("g3")
	gr.clearPendingDeletion("g3")

	gr.RWMutex.RLock()
	_, ok := gr.inflight["g3"]
	gr.RWMutex.RUnlock()
	assert.False(t, ok)
}
