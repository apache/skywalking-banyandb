// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to You under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package measure

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const topNWriteBenchmarkBatchSize = 256

type benchmarkTopNProcessor struct {
	completed *sync.WaitGroup
}

func (b *benchmarkTopNProcessor) In() chan<- flow.StreamRecord            { return nil }
func (b *benchmarkTopNProcessor) Setup(context.Context) error             { return nil }
func (b *benchmarkTopNProcessor) Teardown(context.Context) error          { return nil }
func (b *benchmarkTopNProcessor) Close() error                            { return nil }
func (b *benchmarkTopNProcessor) TopNSchema() *databasev1.TopNAggregation { return nil }

func (b *benchmarkTopNProcessor) Write(flow.StreamRecord) {
	b.completed.Done()
}

// BenchmarkTopNWriteDispatch compares completed write throughput. Each batch waits
// for every record to reach the processor, so the asynchronous baseline cannot
// report only enqueue latency while leaving its goroutines outstanding.
func BenchmarkTopNWriteDispatch(b *testing.B) {
	b.Run("original_async", func(b *testing.B) {
		benchmarkOriginalAsyncTopNWrites(b)
	})
	b.Run("bounded_dispatcher", func(b *testing.B) {
		benchmarkBoundedTopNWrites(b)
	})
}

func benchmarkOriginalAsyncTopNWrites(b *testing.B) {
	completed := &sync.WaitGroup{}
	processor := &benchmarkTopNProcessor{completed: completed}
	measureSchema := benchmarkTopNMeasureSchema()
	manager := &topNProcessorManager{
		m:             measureSchema,
		processorList: []topNProcessor{processor},
	}
	request := benchmarkTopNWriteRequest()
	b.ReportAllocs()
	b.ResetTimer()
	benchmarkTopNWrites(b, completed, func() {
		benchmarkOriginalAsyncTopNWrite(manager, request, measureSchema)
	})
}

func benchmarkBoundedTopNWrites(b *testing.B) {
	completed := &sync.WaitGroup{}
	processor := &benchmarkTopNProcessor{completed: completed}
	measureSchema := benchmarkTopNMeasureSchema()
	manager := &topNProcessorManager{processorList: []topNProcessor{processor}}
	manager.init(context.Background(), measureSchema)
	b.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
	})
	request := benchmarkTopNWriteRequest()
	b.ReportAllocs()
	b.ResetTimer()
	benchmarkTopNWrites(b, completed, func() {
		manager.onMeasureWrite(context.Background(), 1, 1, request, measureSchema)
	})
}

func benchmarkTopNWrites(b *testing.B, completed *sync.WaitGroup, write func()) {
	for writesRemaining := b.N; writesRemaining > 0; {
		batchSize := min(writesRemaining, topNWriteBenchmarkBatchSize)
		completed.Add(batchSize)
		for range batchSize {
			write()
		}
		completed.Wait()
		writesRemaining -= batchSize
	}
}

func benchmarkOriginalAsyncTopNWrite(manager *topNProcessorManager, request *measurev1.InternalWriteRequest, measureSchema *databasev1.Measure) {
	run.Go(context.Background(), "measure.topn.benchmark-original-async", nil, func(_ context.Context) {
		manager.RLock()
		defer manager.RUnlock()
		dp := request.GetRequest().GetDataPoint()
		spec := request.GetRequest().GetDataPointSpec()
		for _, processor := range manager.processorList {
			dpWithEntity := newDataPointWithEntityValues(
				dp,
				request.GetEntityValues(),
				1,
				1,
				spec,
				measureSchema,
			)
			processor.Write(flow.NewStreamRecordWithTimestampPb(dpWithEntity, dp.GetTimestamp()))
		}
	})
}

func benchmarkTopNMeasureSchema() *databasev1.Measure {
	return &databasev1.Measure{Metadata: &commonv1.Metadata{Group: "benchmark", Name: "topn"}}
}

func benchmarkTopNWriteRequest() *measurev1.InternalWriteRequest {
	return &measurev1.InternalWriteRequest{
		Request: &measurev1.WriteRequest{
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(time.UnixMilli(1_000)),
			},
		},
	}
}
