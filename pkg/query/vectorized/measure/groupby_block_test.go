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

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// A scan batch may be a whole storage block, many times the pipeline's batchSize.
// The group-by must not reserve for the block as one lump: its transient
// worst-case reservation stays that of a batchSize batch.
func TestBatchGroupByReservesPerBatchSizeChunkOfALargeBatch(t *testing.T) {
	const batchSize, rows, entrySize, rowSize = 16, 16 * 8, 100, 10
	s := groupbyTestSchema()
	b := vectorized.NewRecordBatch(s, rows)
	for i := range rows {
		b.Columns[0].(*vectorized.TypedColumn[string]).Append("same")
		b.Columns[1].(*vectorized.TypedColumn[int64]).Append(int64(i))
	}
	b.Len = rows
	// One chunk's worst case fits; the whole batch's would not.
	tracker := vectorized.NewMemoryTracker(batchSize * (entrySize + rowSize) * 2)
	g := NewBatchGroupBy(s, []int{0}, vectorized.NewBatchPool(s, batchSize), batchSize, tracker, entrySize, rowSize)
	require.NoError(t, g.Init(context.Background()))
	require.NoError(t, g.Consume(context.Background(), b))
	require.NoError(t, g.Close())
	require.EqualValues(t, 0, tracker.Used())
}
