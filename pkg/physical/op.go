package physical

import (
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

const (
	TableScan = "tableScan"
)

var _ SourceOp = (*tableScan)(nil)

type tableScan struct {
	startTime uint64
	endTime   uint64
}

func (t *tableScan) Name() string {
	return fmt.Sprintf("TableScan{start=%d,end=%d}", t.startTime, t.endTime)
}

func (t *tableScan) OpType() string {
	return TableScan
}

func (t *tableScan) CreateTransform() Transform {
	return &tableScanTransform{
		tableScanOp: t,
	}
}

type chunkIDsFetch struct {
	chunkIDs []uint64
}

type traceIDFetch struct {
	traceID string
}

type indexScan struct {
	indexName string
	startTime uint64
	endTime   uint64
	query     []*apiv1.PairQuery
}

type sortedMerge struct {
	sort      apiv1.Sort
	fieldName string
}

type pagination struct {
	offset uint32
	limit  uint32
}
