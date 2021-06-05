package logical

import (
	"errors"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/hashicorp/terraform/dag"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func Compose(entityCriteria *apiv1.EntityCriteria) (*Plan, error) {
	g := dag.AcyclicGraph{}

	root := NewRoot()
	g.Add(root)

	rangeQuery := entityCriteria.TimestampNanoseconds(nil)
	metadata := entityCriteria.Metadata(nil)
	projection := entityCriteria.Projection(nil)

	var seriesOps []SeriesOp

	if entityCriteria.FieldsLength() == 0 {
		tableScanOp := NewTableScan(metadata, rangeQuery, projection)
		seriesOps = append(seriesOps, tableScanOp)
		g.Add(tableScanOp)
		g.Connect(dag.BasicEdge(root, tableScanOp))
	} else {
		keyQueryMap := make(map[string][]*apiv1.PairQuery)
		for i := 0; i < entityCriteria.FieldsLength(); i++ {
			// group PairQuery by keyName
			var f apiv1.PairQuery
			if ok := entityCriteria.Fields(&f, i); ok {
				condition := f.Condition(nil)
				unionPair := new(flatbuffers.Table)
				if condition.Pair(unionPair) {
					pairType := condition.PairType()
					if pairType == apiv1.TypedPairIntPair {
						unionIntPairQuery := new(apiv1.IntPair)
						unionIntPairQuery.Init(unionPair.Bytes, unionPair.Pos)

						keyName := string(unionIntPairQuery.Key())
						if existingPairQueries, ok := keyQueryMap[keyName]; ok {
							existingPairQueries = append(existingPairQueries, &f)
						} else {
							keyQueryMap[keyName] = []*apiv1.PairQuery{&f}
						}
					} else if pairType == apiv1.TypedPairStrPair {
						unionStrPairQuery := new(apiv1.StrPair)
						unionStrPairQuery.Init(unionPair.Bytes, unionPair.Pos)

						keyName := string(unionStrPairQuery.Key())

						if keyName == "traceID" {
							if f.Op() != apiv1.BinaryOpEQ {
								return nil, errors.New("only `=` operator is supported for traceID")
							}
							traceIDFetchOp := NewTraceIDFetch(metadata, projection, string(unionStrPairQuery.Values(0)))
							seriesOps = append(seriesOps, traceIDFetchOp)
							g.Add(traceIDFetchOp)
							g.Connect(dag.BasicEdge(root, traceIDFetchOp))
							continue
						}
						if existingPairQueries, ok := keyQueryMap[keyName]; ok {
							existingPairQueries = append(existingPairQueries, &f)
						} else {
							keyQueryMap[keyName] = []*apiv1.PairQuery{&f}
						}
					}
				}
			}

			var idxOps []IndexOp

			// Generate IndexScanOp per Entry<string,[]*apiv1.PairQuery> in keyQueryMap
			for k, v := range keyQueryMap {
				idxScanOp := NewIndexScan(metadata, rangeQuery, k, v)
				g.Add(idxScanOp)
				idxOps = append(idxOps, idxScanOp)
				g.Connect(dag.BasicEdge(root, idxScanOp))
			}

			// Merge all ChunkIDs
			chunkIdsMergeOp := NewChunkIDsMerge()
			g.Add(chunkIdsMergeOp)
			for _, idxOp := range idxOps {
				// connect idxOp -> chunkIDsMerge
				g.Connect(dag.BasicEdge(idxOp, chunkIdsMergeOp))
			}

			// Retrieve from Series by chunkID(s)
			chunkIDsFetchOp := NewChunkIDsFetch(metadata, projection)
			seriesOps = append(seriesOps, chunkIDsFetchOp)
			g.Add(chunkIDsFetchOp)
			g.Connect(dag.BasicEdge(chunkIdsMergeOp, chunkIDsFetchOp))
		}
	}

	// Add Sorted-Merge Op
	sortedMergeOp := NewSortedMerge(entityCriteria.OrderBy(nil))
	g.Add(sortedMergeOp)
	for _, sourceOp := range seriesOps {
		e := dag.BasicEdge(sourceOp, sortedMergeOp)
		g.Connect(e)
	}

	// Add Pagination Op
	paginationOp := NewPagination(entityCriteria.Offset(), entityCriteria.Limit())
	g.Add(paginationOp)
	g.Connect(dag.BasicEdge(sortedMergeOp, paginationOp))

	return &Plan{
		AcyclicGraph: g,
	}, nil
}
