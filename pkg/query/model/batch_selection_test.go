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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

func selectionTestSchema() *vectorized.BatchSchema {
	return vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
}

func TestMeasureBatchActiveRows(t *testing.T) {
	b := AcquireMeasureBatch(selectionTestSchema(), 4)
	defer b.Release()
	b.Timestamps = append(b.Timestamps, 1, 2, 3)
	require.Equal(t, 3, b.RowCount())
	require.Equal(t, 3, b.ActiveRows())
	b.Selection = []uint16{2, 1, 0}
	require.Equal(t, 3, b.ActiveRows())
	b.Selection = []uint16{}
	require.Equal(t, 0, b.ActiveRows())
	require.Equal(t, 0, (*MeasureBatch)(nil).ActiveRows())
}

func TestMeasureBatchBorrowedMetadataIsNeverWrittenAndNotKept(t *testing.T) {
	backing := []int64{10, 20, 30, 0, 0}
	ts := backing[:3]
	versions := []int64{1, 1, 1}
	b := AcquireBorrowingMeasureBatch(selectionTestSchema(), 3)
	b.BorrowMetadata(ts, versions)
	require.True(t, b.MetadataBorrowed())
	require.Equal(t, 3, b.RowCount())
	require.Same(t, &ts[0], &b.Timestamps[0])
	// An append reallocates instead of writing into the owner's spare capacity.
	b.Timestamps = append(b.Timestamps, 40)
	require.Equal(t, []int64{10, 20, 30, 0, 0}, backing)
	b.Selection = []uint16{2, 1, 0}
	b.Release()

	// The pooled batch gives its own buffers back, empty, whatever it borrowed.
	for range 4 {
		next := AcquireMeasureBatch(selectionTestSchema(), 4)
		require.False(t, next.MetadataBorrowed())
		require.Nil(t, next.Selection)
		require.Equal(t, 0, next.RowCount())
		next.Timestamps = append(next.Timestamps, 7, 7, 7, 7)
		next.Versions = append(next.Versions, 7, 7, 7, 7)
		next.Release()
	}
	require.Equal(t, []int64{10, 20, 30, 0, 0}, backing)
	require.Equal(t, []int64{1, 1, 1}, versions)
}
