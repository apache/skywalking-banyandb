package physical

import apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"

type tableScan struct {
	startTime uint64
	endTime   uint64
}

type chunkIDFetch struct {
	chunkID uint64
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
