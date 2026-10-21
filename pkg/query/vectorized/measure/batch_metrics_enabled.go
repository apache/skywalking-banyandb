//go:build querymetrics

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
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

var (
	nativeBatchExecutions  atomic.Uint64
	qlBatchExecutions      atomic.Uint64
	intermediateDataPoints atomic.Uint64
	finalDataPoints        atomic.Uint64
	liveBatchResults       atomic.Int64
	nativeLeafCalls        atomic.Uint64
	nativeInputRows        atomic.Uint64
	nativeFrameRows        atomic.Uint64
)

// RecordBatchExecution records a successful native or QL batch activation.
func RecordBatchExecution(ql bool) {
	if ql {
		qlBatchExecutions.Add(1)
	} else {
		nativeBatchExecutions.Add(1)
	}
}

// BatchExecutionStats returns process-wide successful batch activation counts.
func BatchExecutionStats() BatchExecutionCounters {
	return BatchExecutionCounters{
		NativeBatchExecutions:  nativeBatchExecutions.Load(),
		QLBatchExecutions:      qlBatchExecutions.Load(),
		IntermediateDataPoints: intermediateDataPoints.Load(),
		FinalDataPoints:        finalDataPoints.Load(),
		LiveResults:            liveBatchResults.Load(),
		NativeLeafCalls:        nativeLeafCalls.Load(),
		NativeInputRows:        nativeInputRows.Load(),
		NativeFrameRows:        nativeFrameRows.Load(),
	}
}

type finalBatchSerializationKey struct{}

// WithFinalBatchSerialization marks the single final response adapter for accounting.
func WithFinalBatchSerialization(ctx context.Context) context.Context {
	return context.WithValue(vectorized.BeginResponseEncoding(ctx), finalBatchSerializationKey{}, true)
}

func recordBatchSerialization(ctx context.Context, rows int) {
	if final, _ := ctx.Value(finalBatchSerializationKey{}).(bool); final {
		finalDataPoints.Add(uint64(rows))
	} else {
		intermediateDataPoints.Add(uint64(rows))
	}
}

// RecordBatchResultOpened records ownership of one native result handle.
func RecordBatchResultOpened() { liveBatchResults.Add(1) }

// RecordBatchResultClosed records disposal of one native result handle.
func RecordBatchResultClosed() { liveBatchResults.Add(-1) }

// RecordNativeLeafCall records one native storage query invocation.
func RecordNativeLeafCall() { nativeLeafCalls.Add(1) }

// RecordNativeInputRows records storage-source rows before native operators.
func RecordNativeInputRows(rows uint64) { nativeInputRows.Add(rows) }

// RecordNativeFrameRows records rows received in child frames after node operators.
func RecordNativeFrameRows(rows uint64) { nativeFrameRows.Add(rows) }
