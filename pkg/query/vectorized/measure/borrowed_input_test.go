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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// The time-bucket operator floors the timestamp column of its input in place. A
// lent batch's timestamps are a view of the storage block, so the write must
// land in a private copy (TypedColumn.SetAt copies on write) and leave the block
// alone.
func TestFloorTimestampColumnDoesNotWriteBorrowedTimestamps(t *testing.T) {
	schema := vectorized.NewBatchSchema([]vectorized.ColumnDef{
		{Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64},
		{Role: vectorized.RoleField, Name: "v", Type: vectorized.ColumnTypeInt64},
	})
	storage := []int64{101, 109, 115, 123}
	ts := vectorized.NewInt64Column(0)
	ts.Borrow(storage)
	b := &vectorized.RecordBatch{Schema: schema, Columns: []vectorized.Column{ts, vectorized.NewInt64Column(0)}, Len: len(storage)}
	floorTimestampColumn(b, 0, 10)
	require.Equal(t, []int64{100, 100, 110, 120}, ts.Data())
	require.Equal(t, []int64{101, 109, 115, 123}, storage)
}
