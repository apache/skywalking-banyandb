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

package logical

import (
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ SeriesOp = (*TableScan)(nil)

// TableScan defines parameters for a scan operation
// metadata can be mapped to the underlying storage
type TableScan struct {
	timeRange  *apiv1.RangeQuery
	metadata   *apiv1.Metadata
	projection *apiv1.Projection
}

func (t *TableScan) Projection() []string {
	return parseProjectionFields(t.projection)
}

func (t *TableScan) TimeRange() *apiv1.RangeQuery {
	return t.timeRange
}

func (t *TableScan) Medata() *apiv1.Metadata {
	return t.metadata
}

func NewTableScan(metadata *apiv1.Metadata, timeRange *apiv1.RangeQuery, projection *apiv1.Projection) SeriesOp {
	return &TableScan{
		timeRange:  timeRange,
		metadata:   metadata,
		projection: projection,
	}
}

func (t *TableScan) Name() string {
	return fmt.Sprintf("TableScan{begin=%d,end=%d,metadata={group=%s,name=%s},projection=%v}",
		t.timeRange.Begin(),
		t.timeRange.End(),
		t.metadata.Group(),
		t.metadata.Name(),
		parseProjectionFields(t.projection))
}

func (t *TableScan) OpType() string {
	return OpTableScan
}

var _ SeriesOp = (*ChunkIDsFetch)(nil)

// ChunkIDsFetch defines parameters for retrieving entities from chunkID(s)
// metadata can be mapped to the underlying storage
// since we don't know chunkID(s) in advance, it will be collected in physical operation node
type ChunkIDsFetch struct {
	metadata   *apiv1.Metadata
	projection *apiv1.Projection
}

func (c *ChunkIDsFetch) Projection() []string {
	return parseProjectionFields(c.projection)
}

func (c *ChunkIDsFetch) TimeRange() *apiv1.RangeQuery {
	return nil
}

func (c *ChunkIDsFetch) Medata() *apiv1.Metadata {
	return c.metadata
}

func (c *ChunkIDsFetch) Name() string {
	return fmt.Sprintf("ChunkIDsFetch{metadata={group=%s,name=%s},projection=%v}",
		c.metadata.Group(),
		c.metadata.Name(),
		parseProjectionFields(c.projection),
	)
}

func (c *ChunkIDsFetch) OpType() string {
	return OpTableChunkIDsFetch
}

func NewChunkIDsFetch(metadata *apiv1.Metadata, projection *apiv1.Projection) SeriesOp {
	return &ChunkIDsFetch{
		metadata:   metadata,
		projection: projection,
	}
}

var _ SeriesOp = (*TraceIDFetch)(nil)

// TraceIDFetch defines parameters for fetching TraceID directly
type TraceIDFetch struct {
	metadata   *apiv1.Metadata
	TraceID    string
	projection *apiv1.Projection
}

func (t *TraceIDFetch) Projection() []string {
	return parseProjectionFields(t.projection)
}

func (t *TraceIDFetch) TimeRange() *apiv1.RangeQuery {
	return nil
}

func (t *TraceIDFetch) Medata() *apiv1.Metadata {
	return t.metadata
}

func (t *TraceIDFetch) Name() string {
	return fmt.Sprintf("TraceIDFetch{TraceID=%s,metadata={group=%s,name=%s},projection=%v}",
		t.TraceID, t.metadata.Group(), t.metadata.Name(), parseProjectionFields(t.projection))
}

func (t *TraceIDFetch) OpType() string {
	return OpTableTraceIDFetch
}

func NewTraceIDFetch(metadata *apiv1.Metadata, projection *apiv1.Projection, traceID string) SeriesOp {
	return &TraceIDFetch{
		metadata:   metadata,
		TraceID:    traceID,
		projection: projection,
	}
}

func parseProjectionFields(projection *apiv1.Projection) []string {
	if projection == nil {
		return []string{}
	}
	var projFields []string
	for i := 0; i < projection.KeyNamesLength(); i++ {
		projFields = append(projFields, string(projection.KeyNames(i)))
	}
	return projFields
}
