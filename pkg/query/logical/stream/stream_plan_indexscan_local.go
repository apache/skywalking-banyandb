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

package stream

import (
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/query/executor"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	_ logical.Plan          = (*localIndexScan)(nil)
	_ logical.Sorter        = (*localIndexScan)(nil)
	_ logical.VolumeLimiter = (*localIndexScan)(nil)
	_ executor.StreamCloser = (*localIndexScan)(nil)
)

type localIndexScan struct {
	schema            logical.Schema
	invertedFilter    index.Filter
	skippingFilter    index.Filter
	ec                executor.StreamExecutionContext
	order             *logical.OrderBy
	metadata          *commonv1.Metadata
	l                 *logger.Logger
	timeRange         timestamp.TimeRange
	projectionTagRefs [][]*logical.TagRef
	projectionTags    []model.TagProjection
	entities          [][]*modelv1.TagValue
	maxElementSize    int
	// deferLimitToEgress is set when this scan is wrapped by a tagFilterPlan (a
	// criteria query). A non-indexed tag filter is applied AFTER the scan at egress,
	// so a maxElementSize cap inside the vec merge would truncate the ordered set
	// BEFORE the filter runs and starve it (row-path divergence: the row scan streams
	// the whole ordered set in chunks and the filter/limit loops pull as many batches
	// as needed). When set, ExecuteVectorized runs the merge UNCAPPED and the egress
	// applies filter → then the client offset:offset+limit slice.
	deferLimitToEgress bool
}

func (i *localIndexScan) Close() {}

func (i *localIndexScan) Limit(maxVal int) {
	i.maxElementSize = maxVal
}

func (i *localIndexScan) Sort(order *logical.OrderBy) {
	i.order = order
}

func (i *localIndexScan) String() string {
	return fmt.Sprintf("IndexScan: startTime=%d,endTime=%d,Metadata{group=%s,name=%s},conditions=%s; projection=%s; orderBy=%s; limit=%d",
		i.timeRange.Start.Unix(), i.timeRange.End.Unix(), i.metadata.GetGroup(), i.metadata.GetName(),
		i.invertedFilter, logical.FormatTagRefs(", ", i.projectionTagRefs...), i.order, i.maxElementSize)
}

func (i *localIndexScan) Children() []logical.Plan {
	return []logical.Plan{}
}

func (i *localIndexScan) Schema() logical.Schema {
	if len(i.projectionTagRefs) == 0 {
		return i.schema
	}
	return i.schema.ProjTags(i.projectionTagRefs...)
}
