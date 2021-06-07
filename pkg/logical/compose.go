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
			// group PairQuery by KeyName
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
							keyQueryMap[keyName] = existingPairQueries
						} else {
							keyQueryMap[keyName] = []*apiv1.PairQuery{&f}
						}
					} else if pairType == apiv1.TypedPairStrPair {
						unionStrPairQuery := new(apiv1.StrPair)
						unionStrPairQuery.Init(unionPair.Bytes, unionPair.Pos)

						keyName := string(unionStrPairQuery.Key())

						if keyName == "TraceID" {
							if f.Op() != apiv1.BinaryOpEQ {
								return nil, errors.New("only `=` operator is supported for TraceID")
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
		}

		var idxOps []IndexOp

		// Generate IndexScanOp per Entry<string,[]*apiv1.PairQuery> in keyQueryMap
		for k, v := range keyQueryMap {
			// TODO(validation): check whether key is indexed?
			idxScanOp := NewIndexScan(metadata, rangeQuery, k, v)
			g.Add(idxScanOp)
			idxOps = append(idxOps, idxScanOp)
			g.Connect(dag.BasicEdge(root, idxScanOp))
		}

		if len(idxOps) > 0 {
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
