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

package plan

import (
	"context"
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
	measure "github.com/apache/skywalking-banyandb/pkg/query/vectorized/measure"
)

// Execute composes a vec plan tree into a runnable *vectorized.Pipeline and
// returns it as an executor.MIterator. The Scan node(s) in the plan must
// have Source already set — the executor (G8c) does not resolve storage on
// its own; G8d's top-level dispatch is responsible for building the scan
// source from a MeasureExecutionContext and stitching it in.
//
// Lifecycle: a per-pipeline MemoryTracker is constructed from
// cfg.QueryMemoryMiB and threaded through the BuildContext so every
// memory-bookkeeping operator (BatchAggregation, future BatchGroupBy)
// charges against a single budget (G7a). Pipeline.Init is invoked before
// the iterator is returned so breaker stages (which lazily allocate their
// state on Init) are ready for the first Next.
//
// On any error during Build, builder.Build, or Pipeline.Init, the function
// returns the error and closes any partially-constructed pipeline so source
// resources (BatchPool, underlying MeasureBatchResult) are released.
func Execute(ctx context.Context, plan VecPlan, cfg measure.VectorizedConfig) (executor.MIterator, error) {
	if plan == nil {
		return nil, fmt.Errorf("plan.Execute: nil plan")
	}
	if cfgErr := cfg.Validate(); cfgErr != nil {
		return nil, fmt.Errorf("plan.Execute: %w", cfgErr)
	}

	tracker := vectorized.NewMemoryTracker(int64(cfg.QueryMemoryMiB) * 1024 * 1024)
	bc := &BuildContext{
		Builder: vectorized.NewPipelineBuilder().WithMemoryTracker(tracker),
		Tracker: tracker,
		Config:  cfg,
	}

	if buildErr := plan.Build(ctx, bc); buildErr != nil {
		return nil, fmt.Errorf("plan.Execute: %w", buildErr)
	}

	pipeline, pipelineErr := bc.Builder.Build()
	if pipelineErr != nil {
		return nil, fmt.Errorf("plan.Execute: %w", pipelineErr)
	}

	if initErr := pipeline.Init(ctx); initErr != nil {
		_ = pipeline.Close()
		return nil, fmt.Errorf("plan.Execute: %w", initErr)
	}

	// The egress pool matches the terminal operator's output schema. For a
	// schema-rewriting breaker (BatchAggregation), that is the agg output
	// (keys + agg field); for schema-preserving plans, it is the scan
	// schema. plan.Schema() walks to the root node.
	egressPool := vectorized.NewBatchPool(plan.Schema(), cfg.BatchSize)
	return measure.NewIteratorFromPipeline(ctx, pipeline, egressPool), nil
}
