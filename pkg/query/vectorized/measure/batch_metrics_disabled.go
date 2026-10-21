//go:build !querymetrics

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

	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// RecordBatchExecution is disabled outside querymetrics builds.
func RecordBatchExecution(bool) {}

// BatchExecutionStats returns zero counters outside querymetrics builds.
func BatchExecutionStats() BatchExecutionCounters { return BatchExecutionCounters{} }

// WithFinalBatchSerialization marks the response encoding boundary.
func WithFinalBatchSerialization(ctx context.Context) context.Context {
	return vectorized.BeginResponseEncoding(ctx)
}

func recordBatchSerialization(context.Context, int) {}

// RecordBatchResultOpened is disabled outside querymetrics builds.
func RecordBatchResultOpened() {}

// RecordBatchResultClosed is disabled outside querymetrics builds.
func RecordBatchResultClosed() {}

// RecordNativeLeafCall is disabled outside querymetrics builds.
func RecordNativeLeafCall() {}

// RecordNativeInputRows is disabled outside querymetrics builds.
func RecordNativeInputRows(uint64) {}

// RecordNativeFrameRows is disabled outside querymetrics builds.
func RecordNativeFrameRows(uint64) {}
