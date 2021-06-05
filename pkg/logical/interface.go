package logical

import (
	"github.com/hashicorp/terraform/dag"
)

const (
	Root               = "root"
	TableScan          = "tableScan"
	TableChunkIDsFetch = "tableChunkIDsFetch"
	TableTraceIDFetch  = "tableTraceIDFetch"
	IndexScan          = "indexScan"
	SortedMerge        = "sortedMerge"
	Pagination         = "pagination"
	ChunkIDsMerge      = "chunkIDsMerge"
)

type Op interface {
	dag.NamedVertex
	OpType() string
}

type SourceOp interface {
	Op
}

type IndexOp interface {
	SourceOp
}

type SeriesOp interface {
	SourceOp
}
