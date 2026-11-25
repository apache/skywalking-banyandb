// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for additional
// information regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

package sidx

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// refundLease is a query lease with no limit that tracks what is charged and the highest it gets.
type refundLease struct {
	used atomic.Uint64
	peak atomic.Uint64
}

func (l *refundLease) Charge(bytes uint64) error {
	used := l.used.Add(bytes)
	for {
		peak := l.peak.Load()
		if used <= peak || l.peak.CompareAndSwap(peak, used) {
			return nil
		}
	}
}

func (l *refundLease) Refund(bytes uint64) { l.used.Add(^(bytes - 1)) }
func (*refundLease) Limit() uint64         { return 1 << 40 }
func (l *refundLease) Used() uint64        { return l.used.Load() }
func (l *refundLease) Owner() any          { return l }

// Releasing a block cursor gives back what its block was charged, and a cursor charged to no lease releases without effect.
func TestBlockCursorReleaseRefundsItsCharge(t *testing.T) {
	lease := &refundLease{}
	ctx := query.WithBudgetLease(context.Background(), lease)
	require.NoError(t, query.Charge(ctx, 300))
	bc := generateBlockCursor()
	bc.holdCharge(ctx, 300)
	require.Equal(t, uint64(300), lease.Used())
	releaseBlockCursor(bc)
	require.Zero(t, lease.Used(), "the charge goes back with the cursor")
	require.Nil(t, bc.lease, "a pooled cursor holds no lease")
	require.Zero(t, bc.charged)

	unleased := generateBlockCursor()
	unleased.holdCharge(context.Background(), 300)
	require.Zero(t, unleased.charged)
	releaseBlockCursor(unleased)
}

// A sidx query charges the blocks it reads while it runs and has refunded every one of them when it returns: the blocks are
// decoded into cursors that are gone by then, and what remains is the small result. Keeping the charges for the rest of the
// query made a limited trace query that read several blocks fail with a budget the later phases still needed.
func TestSIDX_QuerySyncRefundsTheBlocksItRead(t *testing.T) {
	s := createTestSIDX(t)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	const seriesCount = 80
	for i := range seriesCount {
		key := int64(i + 1)
		writeTestData(t, s, []WriteRequest{
			createTestWriteRequest(common.SeriesID(i+1), key, fmt.Sprintf("data%03d", key)),
		}, 44, uint64(i+1))
	}
	waitForIntroducerLoop()
	syncQuerier, ok := s.(interface {
		QuerySync(context.Context, QueryRequest) ([]*QueryResponse, error)
	})
	require.True(t, ok)
	seriesIDs := make([]common.SeriesID, 0, seriesCount)
	for i := range seriesCount {
		seriesIDs = append(seriesIDs, common.SeriesID(i+1))
	}
	queryReq := createTestQueryRequest(seriesIDs...)
	queryReq.MaxBatchSize = 10

	lease := &refundLease{}
	ctx := query.WithBudgetLease(context.Background(), lease)
	responses, err := syncQuerier.QuerySync(ctx, queryReq)
	require.NoError(t, err)
	require.NotEmpty(t, responses)
	require.NotZero(t, lease.peak.Load(), "the query charged the blocks it read")
	require.Zero(t, lease.Used(), "and has given every charge back when it returns")
}
