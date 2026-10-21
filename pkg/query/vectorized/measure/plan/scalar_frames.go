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

package plan

import (
	"fmt"
	"math"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	vmeasure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure/frame"
)

func homogeneousScalarFrames(frames [][]byte) (bool, error) {
	for idx, body := range frames {
		layout, err := frame.Inspect(body)
		if err != nil {
			return false, err
		}
		if !layout.Scalar {
			return false, nil
		}
		if idx == 0 {
			continue
		}
		same, err := frame.SameSchema(frames[0], body)
		if err != nil || !same {
			return false, err
		}
	}
	return true, nil
}

// reserveScalarNativeFrames covers the homogeneous time-ordered borrow path.
// Numeric and variable payload buffers borrow the received frame; decoding only
// creates variable cell headers/null bitmaps. Contiguous output shares those
// headers too, while interleaved winners fall back to one bounded batch copy.
// The unchanged128-byte cell allowance covers both header sets, column/schema
// descriptors, row indices, sort keys and worst-case equal-timestamp windows.
func reserveScalarNativeFrames(tracker *vectorized.MemoryTracker, reserved *int64, frames [][]byte, batchSize int, ingressReserved bool) error {
	var workspace int64
	for _, body := range frames {
		layout, err := frame.Inspect(body)
		if err != nil {
			return err
		}
		header := layout.Header
		vmeasure.RecordNativeFrameRows(header.NumRows)
		if !layout.Scalar {
			return fmt.Errorf("native scalar frame contains a protobuf column")
		}
		if header.NumCols > (math.MaxInt64-128)/128 {
			return vectorized.ErrMemoryBudgetExceeded
		}
		rowWidth := header.NumCols*128 + 128
		if header.NumRows > math.MaxInt64/rowWidth || uint64(batchSize) > math.MaxInt64/rowWidth {
			return vectorized.ErrMemoryBudgetExceeded
		}
		if header.NumCols > math.MaxInt64/512 || layout.SchemaBytes > math.MaxInt64/8 {
			return vectorized.ErrMemoryBudgetExceeded
		}
		amount := header.NumRows * rowWidth
		// One complete copy covers the decoder's alignment/poisoning fallback.
		// Unguarded transports additionally charge the original retained body.
		copies := uint64(2)
		if ingressReserved {
			copies--
		}
		extras := []uint64{uint64(len(body)) * copies, layout.ConstantPayloadBytes * 2, layout.SchemaBytes * 8, header.NumCols * 512, 4096}
		for _, extra := range extras {
			if extra > math.MaxInt64-amount {
				return vectorized.ErrMemoryBudgetExceeded
			}
			amount += extra
		}
		if err = tracker.Reserve(int64(amount)); err != nil {
			return err
		}
		*reserved += int64(amount)
		workspace = max(workspace, int64(uint64(batchSize)*rowWidth))
	}
	if err := tracker.Reserve(workspace); err != nil {
		return err
	}
	*reserved += workspace
	return nil
}

func (p *DistributedPlan) reserveSingleGroupFrames(
	tracker *vectorized.MemoryTracker, reserved *int64, frames [][]byte, request *measurev1.QueryRequest, ingressReserved bool,
) (bool, error) {
	borrowRows := request.GetAgg() == nil && request.GetGroupBy() == nil && request.GetTop() == nil && p.orderByTag == nil
	if borrowRows {
		scalar, err := homogeneousScalarFrames(frames)
		if err != nil {
			return false, err
		}
		borrowRows = scalar
	}
	reserveFrames := reserveNativeFrames
	if borrowRows {
		reserveFrames = reserveScalarNativeFrames
	}
	return borrowRows, reserveFrames(tracker, reserved, frames, p.cfg.BatchSize, ingressReserved)
}
