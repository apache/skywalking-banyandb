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

var _ SeriesOp = (*chunkIDsFetch)(nil)

// chunkIDs defines parameters for retrieving entities from chunkID(s)
// metadata can be mapped to the underlying storage
// since we don't know chunkID(s) in advance, it will be collected in physical operation node
type chunkIDsFetch struct {
	metadata   *apiv1.Metadata
	projection *apiv1.Projection
}

func (c *chunkIDsFetch) Projection() []string {
	return parseProjectionFields(c.projection)
}

func (c *chunkIDsFetch) TimeRange() *apiv1.RangeQuery {
	return nil
}

func (c *chunkIDsFetch) Medata() *apiv1.Metadata {
	return c.metadata
}

func (c *chunkIDsFetch) Name() string {
	return fmt.Sprintf("ChunkIDsFetch{metadata={group=%s,name=%s},projection=%v}",
		c.metadata.Group(),
		c.metadata.Name(),
		parseProjectionFields(c.projection),
	)
}

func (c *chunkIDsFetch) OpType() string {
	return OpTableChunkIDsFetch
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
	TraceID    string
	projection *apiv1.Projection
}

func (t *traceIDFetch) Projection() []string {
	return parseProjectionFields(t.projection)
}

func (t *traceIDFetch) TimeRange() *apiv1.RangeQuery {
	return nil
}

func (t *traceIDFetch) Medata() *apiv1.Metadata {
	return t.metadata
}

func (t *traceIDFetch) Name() string {
	return fmt.Sprintf("TraceIDFetch{TraceID=%s,metadata={group=%s,name=%s},projection=%v}",
		t.TraceID, t.metadata.Group(), t.metadata.Name(), parseProjectionFields(t.projection))
}

func (t *traceIDFetch) OpType() string {
	return OpTableTraceIDFetch
}

func NewTraceIDFetch(metadata *apiv1.Metadata, projection *apiv1.Projection, traceID string) SeriesOp {
	return &traceIDFetch{
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
