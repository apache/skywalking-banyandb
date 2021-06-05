package logical

import (
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ SeriesOp = (*tableScan)(nil)

// tableScan defines parameters for a scan operation
// metadata can be mapped to the underlying storage
type tableScan struct {
	timeRange  *apiv1.RangeQuery
	metadata   *apiv1.Metadata
	projection *apiv1.Projection
}

func NewTableScan(metadata *apiv1.Metadata, timeRange *apiv1.RangeQuery, projection *apiv1.Projection) SeriesOp {
	return &tableScan{
		timeRange:  timeRange,
		metadata:   metadata,
		projection: projection,
	}
}

func (t *tableScan) Name() string {
	return fmt.Sprintf("TableScan{begin=%d,end=%d,metadata=%v,projection=%v}",
		t.timeRange.Begin(),
		t.timeRange.End(),
		t.metadata,
		t.projection)
}

func (t *tableScan) OpType() string {
	return TableScan
}

var _ SeriesOp = (*chunkIDsFetch)(nil)

// chunkIDs defines parameters for retrieving entities from chunkID(s)
// metadata can be mapped to the underlying storage
// since we don't know chunkID(s) in advance, it will be collected in physical operation node
type chunkIDsFetch struct {
	metadata   *apiv1.Metadata
	projection *apiv1.Projection
}

func (c *chunkIDsFetch) Name() string {
	return fmt.Sprintf("ChunkIDsFetch{metadata=%v}", c.metadata)
}

func (c *chunkIDsFetch) OpType() string {
	return TableChunkIDsFetch
}

func NewChunkIDsFetch(metadata *apiv1.Metadata, projection *apiv1.Projection) SeriesOp {
	return &chunkIDsFetch{
		metadata:   metadata,
		projection: projection,
	}
}

var _ SeriesOp = (*traceIDFetch)(nil)

// traceIDFetch defines parameters for fetching TraceID directly
type traceIDFetch struct {
	metadata   *apiv1.Metadata
	traceID    string
	projection *apiv1.Projection
}

func (t *traceIDFetch) Name() string {
	return fmt.Sprintf("TraceIDFetch{traceID=%s,metadata+%v,projection=%v}", t.traceID, t.metadata, t.projection)
}

func (t *traceIDFetch) OpType() string {
	return TableTraceIDFetch
}

func NewTraceIDFetch(metadata *apiv1.Metadata, projection *apiv1.Projection, traceID string) SeriesOp {
	return &traceIDFetch{
		metadata:   metadata,
		traceID:    traceID,
		projection: projection,
	}
}
