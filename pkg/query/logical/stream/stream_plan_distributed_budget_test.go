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
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/query"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
)

type distributedAccountingLease struct {
	charged  []uint64
	refunded uint64
	failOn   int
}

func (l *distributedAccountingLease) Charge(bytes uint64) error {
	if len(l.charged)+1 == l.failOn {
		return errors.New("alignment allocation denied")
	}
	l.charged = append(l.charged, bytes)
	return nil
}
func (l *distributedAccountingLease) Refund(bytes uint64) { l.refunded += bytes }
func (*distributedAccountingLease) Limit() uint64         { return 1 << 30 }
func (l *distributedAccountingLease) Used() uint64 {
	var total uint64
	for _, bytes := range l.charged {
		total += bytes
	}
	return total - l.refunded
}
func (*distributedAccountingLease) Owner() any { return "distributed-accounting-test" }

func TestDistributedAlignmentChargesRemainOnQueryLease(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	for _, failOn := range []int{0, 1, 2, 3} {
		t.Run(string(rune('0'+failOn)), func(t *testing.T) {
			lease := &distributedAccountingLease{failOn: failOn}
			fake := &fakeDistributedContext{streams: []*fakeNodeStream{
				{name: "one", chunks: []any{projectedFrameChunk(t, []nodeRow{{ts: 1, elementID: 1}}, []string{"svc"})}},
				{name: "two", chunks: []any{projectedFrameChunk(t, []nodeRow{{ts: 2, elementID: 2}}, []string{"svc"})}},
			}}
			ctx := query.WithBudgetLease(context.Background(), lease)
			got, err := newBatchMergePlan(false, 0, 2).Execute(executor.WithDistributedExecutionContext(ctx, fake))
			if failOn == 0 {
				require.NoError(t, err)
				require.Len(t, got, 2)
				// Two alignment copies, then two independent merge retention charges.
				require.Len(t, lease.charged, 4)
				require.Equal(t, lease.charged[2]+lease.charged[3], lease.refunded)
				require.Equal(t, lease.charged[0]+lease.charged[1], lease.Used())
				require.Equal(t, "svc-value", got[0].TagFamilies[0].Tags[0].Value.GetStr().GetValue())
			} else {
				require.ErrorContains(t, err, "alignment allocation denied")
				require.Nil(t, got)
				require.Len(t, lease.charged, failOn-1)
				require.Zero(t, lease.refunded, "no merge charge was accepted before this rejection")
			}
			for _, node := range fake.streams {
				require.True(t, node.closed.Load())
			}
		})
	}
}

func TestDistributedWindowDoesNotHideLateNodeError(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	late := errors.New("late node failure after enough results")
	node := &fakeNodeStream{name: "late", chunks: []any{
		frameChunk(t, []nodeRow{{ts: 1, elementID: 1}}),
		frameChunk(t, []nodeRow{{ts: 2, elementID: 2}}),
	}, err: late}
	fake := &fakeDistributedContext{streams: []*fakeNodeStream{node}}
	got, err := newBatchMergePlan(false, 0, 1).Execute(executor.WithDistributedExecutionContext(context.Background(), fake))
	require.ErrorIs(t, err, late)
	require.Nil(t, got)
	require.Equal(t, 2, node.next)
	require.True(t, node.closed.Load())
}
