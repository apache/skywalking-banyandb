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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/data"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vstream "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream"
	streamframe "github.com/apache/skywalking-banyandb/pkg/query/vectorized/stream/frame"
)

type distributedBatchOwner struct {
	retains  atomic.Int32
	releases atomic.Int32
}

func (o *distributedBatchOwner) Retain()  { o.retains.Add(1) }
func (o *distributedBatchOwner) Release() { o.releases.Add(1) }

func TestDistributedFrameBatchLifetime(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	brokenNode := errors.New("node failed after sending a frame")
	for _, name := range []string{
		"success", "decode error", "cancellation", "node error", "materialization error", "missing order key",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			owner := &distributedBatchOwner{}
			plan := newDistributedWindowPlan(false, 0, 1)
			if name == "missing order key" {
				plan.sortByTime = false
			}
			plan.decodeFrame = func(payload []byte) (*vectorized.RecordBatch, error) {
				batch, err := streamframe.Decode(payload)
				if err == nil {
					// Native decoding currently copies its input. Pin a counting owner
					// to exercise the coordinator's lifetime contract if this changes.
					batch.Owners.Pin(owner)
					if name == "cancellation" {
						cancel()
					}
				}
				return batch, err
			}
			egressErr := errors.New("egress failed")
			plan.buildElement = func(batch *vectorized.RecordBatch, projection []model.TagProjection, row int) (*streamv1.Element, error) {
				require.EqualValues(t, 0, owner.releases.Load(), "source owners must stay live through egress")
				if name == "materialization error" {
					return nil, egressErr
				}
				return vstream.BuildElementFromBatchRow(batch, projection, row)
			}
			node := &fakeNodeStream{name: "node", chunks: []any{frameChunk(t, []nodeRow{{ts: 1, elementID: 1}})}}
			switch name {
			case "decode error":
				node.chunks = append(node.chunks, []byte("invalid frame"))
			case "cancellation":
				node.block = true
			case "node error":
				node.err = brokenNode
			}
			ctx = executor.WithDistributedExecutionContext(ctx, &fakeDistributedContext{streams: []*fakeNodeStream{node}})
			got, err := plan.Execute(ctx)
			switch name {
			case "success":
				require.NoError(t, err)
				require.Equal(t, []string{hexElementID(1)}, elementIDSeq(got))
			case "decode error":
				require.ErrorContains(t, err, "decode stream result frame")
			case "cancellation":
				require.ErrorIs(t, err, context.Canceled)
			case "node error":
				require.ErrorIs(t, err, brokenNode)
			case "materialization error":
				require.ErrorIs(t, err, egressErr)
			case "missing order key":
				require.ErrorContains(t, err, "order key")
			}
			require.EqualValues(t, 1, owner.retains.Load())
			require.Equal(t, owner.retains.Load(), owner.releases.Load(), "every decoded batch must be released exactly once")
			require.True(t, node.closed.Load())
		})
	}
}

func TestDistributedMixedChunksPreserveNodeOrder(t *testing.T) {
	data.SetStreamWireModeRaw(true)
	t.Cleanup(func() { data.SetStreamWireModeRaw(false) })
	for _, desc := range []bool{false, true} {
		first, last := nodeRow{ts: 1, elementID: 1}, nodeRow{ts: 3, elementID: 3}
		want := []string{hexElementID(1), hexElementID(2), hexElementID(3)}
		if desc {
			first, last = last, first
			want = []string{hexElementID(3), hexElementID(2), hexElementID(1)}
		}
		for _, frameFirst := range []bool{false, true} {
			chunks := []any{protoResponseForRows([]nodeRow{first}), frameChunk(t, []nodeRow{last})}
			if frameFirst {
				chunks = []any{frameChunk(t, []nodeRow{first}), protoResponseForRows([]nodeRow{last})}
			}
			got, err := executePlan(t, newDistributedWindowPlan(desc, 0, 3), chunks,
				[]any{protoResponseForRows([]nodeRow{{ts: 2, elementID: 2}})})
			require.NoError(t, err)
			require.Equal(t, want, elementIDSeq(got))
		}
	}
}
