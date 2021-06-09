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

package physical

import (
	"errors"

	"github.com/hashicorp/terraform/dag"

	"github.com/apache/skywalking-banyandb/pkg/logical"
)

func ComposePhysicalPlan(logicalPlan *logical.Plan) (*Plan, error) {
	rootVertex, err := logicalPlan.Root()
	if err != nil {
		return nil, err
	}
	var steps = make(map[dag.Vertex]Transform)

	err = logicalPlan.SortedDepthFirstWalk([]dag.Vertex{rootVertex}, func(vertex dag.Vertex, i int) error {
		tf, err := convertToTransform(vertex)
		if err != nil {
			return err
		}
		steps[vertex] = tf
		return nil
	})

	return &Plan{
		logicalPlan,
		steps,
	}, nil
}

func convertToTransform(v dag.Vertex) (Transform, error) {
	if op, ok := v.(logical.Op); ok {
		switch op.OpType() {
		case logical.OpTableScan:
			return NewTableScanTransform(op.(*logical.TableScan)), nil
		case logical.OpTableChunkIDsFetch:
			return NewChunkIDsFetchTransform(op.(*logical.ChunkIDsFetch)), nil
		case logical.OpTableTraceIDFetch:
			return NewTraceIDFetchTransform(op.(*logical.TraceIDFetch)), nil
		case logical.OpIndexScan:
			return NewIndexScanTransform(op.(*logical.IndexScan)), nil
		case logical.OpSortedMerge:
			return NewSortMergeTransform(op.(*logical.SortMerge)), nil
		case logical.OpPagination:
			return NewPaginationTransform(op.(*logical.Pagination)), nil
		case logical.OpChunkIDsMerge:
			return NewChunkIDsMergeTransform(op.(*logical.ChunkIDsMerge)), nil
		case logical.OpRoot:
			return NewRootTransform(op.(*logical.RootOp)), nil
		default:
			return nil, errors.New("unsupported logical op")
		}
	} else {
		return nil, errors.New("unsupported vertex")
	}
}
