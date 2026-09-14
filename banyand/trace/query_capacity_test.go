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

package trace

import (
	"math"
	"strconv"
	"testing"
)

// Trace queries previously turned limit(+offset) into MaxTraceSize / MaxBatchSize
// and sized the legacy keys map from that value. newTraceBatch must keep the
// initial map hint bounded for oversized requests.
func TestTraceBatchCapacityCapsOversizedLimit(t *testing.T) {
	if got := traceBatchCapacity(math.MaxInt32); got != 1024 {
		t.Fatalf("traceBatchCapacity(MaxInt32)=%d, want 1024", got)
	}
	if got := traceBatchCapacity(16); got != 16 {
		t.Fatalf("traceBatchCapacity(16)=%d, want 16", got)
	}
	// Modest oversized input: a guard regression fails the helper assert above /
	// would only allocate a few KiB here, never MaxInt32 map buckets.
	const oversized = 4096
	if got := traceBatchCapacity(oversized); got != 1024 {
		t.Fatalf("traceBatchCapacity(%d)=%d, want 1024", oversized, got)
	}
	batch := newTraceBatch(0, oversized)
	if batch.keys == nil {
		t.Fatal("expected keys map for positive capped capacity")
	}
	for i := 0; i < 1025; i++ {
		batch.keys[strconv.Itoa(i)] = int64(i)
	}
}
