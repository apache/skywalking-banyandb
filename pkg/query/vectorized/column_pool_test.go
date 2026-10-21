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

package vectorized

import "testing"

func TestAcquireBoundedColumnDropsOversizedPooledBacking(t *testing.T) {
	large := AcquireColumn(ColumnTypeBytes, 16384).(*TypedColumn[[]byte])
	large.Append([]byte("old"))
	large.MarkNullAt(16000)
	ReleaseColumn(large)
	small := AcquireBoundedColumn(ColumnTypeBytes, 16).(*TypedColumn[[]byte])
	defer ReleaseColumn(small)
	if cap(small.Data()) > 2*DefaultBatchSize {
		t.Fatalf("inherited %d rows of cached backing", cap(small.Data()))
	}
	if cap(small.validity.bits) > (2*DefaultBatchSize+63)/64 {
		t.Fatal("oversized validity buffer survived")
	}
	small.Append([]byte("new"))
	if string(small.Data()[0]) != "new" || small.IsNull(0) {
		t.Fatal("pooled column did not reset")
	}
}

func TestColumnReturnsToAcquisitionClassAfterGrowth(t *testing.T) {
	beforeSmall := int64ColumnPool.classes[0].RefsCount()
	beforeLarge := int64ColumnPool.classes[ColumnCapacityClass(4096)].RefsCount()
	column := AcquireBoundedColumn(ColumnTypeInt64, 16).(*TypedColumn[int64])
	column.AppendSlice(make([]int64, 4096))
	ReleaseColumn(column)
	if int64ColumnPool.classes[0].RefsCount() != beforeSmall || int64ColumnPool.classes[ColumnCapacityClass(4096)].RefsCount() != beforeLarge {
		t.Fatal("column growth changed the pool checkout owner")
	}
	bounded := AcquireBoundedColumn(ColumnTypeInt64, 16).(*TypedColumn[int64])
	defer ReleaseColumn(bounded)
	if cap(bounded.Data()) > 2*DefaultBatchSize {
		t.Fatal("grown small-class column escaped the retained capacity bound")
	}
}
