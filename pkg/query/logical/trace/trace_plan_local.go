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

package trace

import (
	"context"
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/iter"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_ logical.Plan             = (*localScan)(nil)
	_ logical.Sorter           = (*localScan)(nil)
	_ logical.VolumeLimiter    = (*localScan)(nil)
	_ executor.TraceExecutable = (*localScan)(nil)
)

type localScan struct {
	schema            logical.Schema
	skippingFilter    index.Filter
	result            model.TraceQueryResult
	ec                executor.TraceExecutionContext
	order             *logical.OrderBy
	metadata          *commonv1.Metadata
	l                 *logger.Logger
	projectionTags    *model.TagProjection
	timeRange         timestamp.TimeRange
	projectionTagRefs [][]*logical.TagRef
	entities          [][]*modelv1.TagValue
	traceIDs          []string
	maxTraceSize      int
	minVal            int64
	maxVal            int64
}

func (i *localScan) Close() {
	if i.result != nil {
		i.result.Release()
	}
}

func (i *localScan) Limit(maxVal int) {
	i.maxTraceSize = maxVal
}

func (i *localScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localScan) Execute(ctx context.Context) (iter.Iterator[model.TraceResult], error) {
	select {
	case <-ctx.Done():
		return iter.Empty[model.TraceResult](), ctx.Err()
	default:
	}

	// If we don't have a result yet, execute the query
	if i.result == nil {
		var orderBy *index.OrderBy
		if i.order != nil {
			orderBy = &index.OrderBy{
				Index: i.order.Index,
				Sort:  i.order.Sort,
			}
		}
		var err error
		if i.result, err = i.ec.Query(ctx, model.TraceQueryOptions{
			Name:           i.metadata.GetName(),
			TimeRange:      &i.timeRange,
			SkippingFilter: i.skippingFilter,
			Order:          orderBy,
			TagProjection:  i.projectionTags,
			Entities:       i.entities,
			MaxTraceSize:   i.maxTraceSize,
			TraceIDs:       i.traceIDs,
		}); err != nil {
			return iter.Empty[model.TraceResult](), err
		}
		if i.result == nil {
			return iter.Empty[model.TraceResult](), nil
		}
	}

	// Return a custom iterator that continuously pulls from i.result
	return &traceResultIterator{result: i.result}, nil
}

// traceResultIterator implements iter.Iterator[model.TraceResult] by continuously
// calling Pull() on the TraceQueryResult until it returns nil or encounters an error.
type traceResultIterator struct {
	result model.TraceQueryResult
	err    error
}

func (tri *traceResultIterator) Next() (model.TraceResult, bool) {
	if tri.err != nil || tri.result == nil {
		return model.TraceResult{}, false
	}

	traceResult := tri.result.Pull()
	if traceResult == nil {
		return model.TraceResult{}, false
	}

	// Check if the result contains an error
	if traceResult.Error != nil {
		tri.err = traceResult.Error
		return *traceResult, false
	}

	return *traceResult, true
}

func (i *localScan) String() string {
	return fmt.Sprintf("TraceScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; orderBy=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.skippingFilter, logical.FormatTagRefs(", ", i.projectionTagRefs...), i.order, i.maxTraceSize)
}

func (i *localScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localScan) Schema() logical.Schema {
	if len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}
