package logical

import (
	"github.com/hashicorp/terraform/dag"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

const (
	OpRoot               = "root"
	OpTableScan          = "TableScan"
	OpTableChunkIDsFetch = "tableChunkIDsFetch"
	OpTableTraceIDFetch  = "tableTraceIDFetch"
	OpIndexScan          = "indexScan"
	OpSortedMerge        = "sortedMerge"
	OpPagination         = "pagination"
	OpChunkIDsMerge      = "chunkIDsMerge"
)

type Op interface {
	dag.NamedVertex
	OpType() string
}

type SourceOp interface {
	TimeRange() *apiv1.RangeQuery
	Medata() *apiv1.Metadata
	Op
}

type IndexOp interface {
	SourceOp
}

type SeriesOp interface {
	Projection() []string
	SourceOp
}
