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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

// BatchQueryRequest selects the in-process batch result boundary for a native request.
// It is never serialized or sent to a data node.
type BatchQueryRequest struct {
	Request  *measurev1.QueryRequest
	Patterns []model.IndexedPattern
	// SeriesOrder permits the local source to visit complete series for an order-independent QL aggregate.
	SeriesOrder bool
}

// BatchExecutionCounters records activation of the two real batch callers.
type BatchExecutionCounters struct {
	NativeBatchExecutions  uint64
	QLBatchExecutions      uint64
	IntermediateDataPoints uint64
	FinalDataPoints        uint64
	LiveResults            int64
	NativeLeafCalls        uint64
	NativeInputRows        uint64
	NativeFrameRows        uint64
}

// BatchQueryResult is the transport-free native result contract used by liaison callers.
type BatchQueryResult interface {
	vectorized.PullOperator
	NativeIteratorFrom(context.Context, vectorized.PullOperator) (executor.MIterator, error)
	QueryTrace() *commonv1.Trace
}
