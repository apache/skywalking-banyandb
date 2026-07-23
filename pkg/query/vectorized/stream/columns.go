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

package stream

import "github.com/apache/skywalking-banyandb/pkg/query/vectorized"

// Metadata column names for the stream vectorized batch schema.
const (
	StreamColumnNameTimestamp = "timestamp"
	StreamColumnNameElementID = "elementID"
	StreamColumnNameSeriesID  = "seriesID"
	StreamColumnNameOrderKey  = "orderKey"
)

// activeIndices returns the active row indices of a batch: the selection when
// present, or [0, Len) materialized when the selection is nil.
func activeIndices(batch *vectorized.RecordBatch) []uint16 {
	if batch.Selection != nil {
		return batch.Selection
	}
	out := make([]uint16, batch.Len)
	for rowIdx := range out {
		out[rowIdx] = uint16(rowIdx)
	}
	return out
}

// streamTimestamps returns the timestamp column of a stream batch.
func streamTimestamps(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[batch.Schema.TimestampIndex()].(*vectorized.TypedColumn[int64])
}

// streamElementIDs returns the element-id column of a stream batch.
func streamElementIDs(batch *vectorized.RecordBatch) *vectorized.TypedColumn[int64] {
	return batch.Columns[batch.Schema.ElementIDIndex()].(*vectorized.TypedColumn[int64])
}

// streamOrderKeys returns the order-key column of a stream batch, or nil when
// the batch is not index-ordered (no order-key column present).
func streamOrderKeys(batch *vectorized.RecordBatch) *vectorized.TypedColumn[[]byte] {
	idx := batch.Schema.OrderKeyIndex()
	if idx < 0 {
		return nil
	}
	return batch.Columns[idx].(*vectorized.TypedColumn[[]byte])
}
