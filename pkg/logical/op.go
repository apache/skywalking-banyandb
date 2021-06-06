package logical

import (
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ Op = (*rootOp)(nil)

type rootOp struct {
}

func (r *rootOp) Name() string {
	return "Root{}"
}

func (r *rootOp) OpType() string {
	return OpRoot
}

func NewRoot() Op {
	return &rootOp{}
}

var _ Op = (*sortedMerge)(nil)

// sortedMerge define parameters for an aggregate operation,
// queryOrder contains sorted field and sort algorithm
type sortedMerge struct {
	queryOrder *apiv1.QueryOrder
}

func (s *sortedMerge) Name() string {
	return fmt.Sprintf("SortedMerge{fieldName=%s,sort=%v}", string(s.queryOrder.KeyName()), s.queryOrder.Sort())
}

func (s *sortedMerge) OpType() string {
	return OpSortedMerge
}

func NewSortedMerge(queryOrder *apiv1.QueryOrder) Op {
	return &sortedMerge{queryOrder: queryOrder}
}

var _ Op = (*pagination)(nil)

// pagination defines parameters for paging
type pagination struct {
	offset uint32
	limit  uint32
}

func (p *pagination) Name() string {
	return fmt.Sprintf("Pagination{offset=%d,limit=%d}", p.offset, p.limit)
}

func (p *pagination) OpType() string {
	return OpPagination
}

func NewPagination(offset, limit uint32) Op {
	return &pagination{
		offset: offset,
		limit:  limit,
	}
}

var _ Op = (*chunkIDsMerge)(nil)

type chunkIDsMerge struct {
}

func (c *chunkIDsMerge) Name() string {
	return "ChunkIDsMerge{}"
}

func (c *chunkIDsMerge) OpType() string {
	return OpChunkIDsMerge
}

func NewChunkIDsMerge() Op {
	return &chunkIDsMerge{}
}
