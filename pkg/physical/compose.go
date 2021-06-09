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
