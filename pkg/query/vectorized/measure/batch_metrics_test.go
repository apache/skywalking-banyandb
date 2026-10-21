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

func TestBatchMetricsBuildMode(t *testing.T) {
	before := BatchExecutionStats()
	RecordBatchExecution(false)
	RecordBatchExecution(true)
	RecordBatchResultOpened()
	RecordNativeLeafCall()
	RecordNativeInputRows(7)
	RecordNativeFrameRows(5)
	recordBatchSerialization(t.Context(), 3)
	recordBatchSerialization(WithFinalBatchSerialization(t.Context()), 2)
	during := BatchExecutionStats()
	RecordBatchResultClosed()
	after := BatchExecutionStats()
	if !vectorized.QueryMetricsEnabled {
		require.Equal(t, BatchExecutionCounters{}, before)
		require.Equal(t, BatchExecutionCounters{}, during)
		require.Equal(t, BatchExecutionCounters{}, after)
		return
	}
	require.Equal(t, BatchExecutionCounters{
		NativeBatchExecutions:  before.NativeBatchExecutions + 1,
		QLBatchExecutions:      before.QLBatchExecutions + 1,
		IntermediateDataPoints: before.IntermediateDataPoints + 3,
		FinalDataPoints:        before.FinalDataPoints + 2,
		LiveResults:            before.LiveResults + 1,
		NativeLeafCalls:        before.NativeLeafCalls + 1,
		NativeInputRows:        before.NativeInputRows + 7,
		NativeFrameRows:        before.NativeFrameRows + 5,
	}, during)
	during.LiveResults--
	require.Equal(t, during, after)
}
