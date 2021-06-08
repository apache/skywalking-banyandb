package physical

import (
	"errors"

	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/hashicorp/terraform/dag"
)

func ComposePhysicalPlan(logicalPlan *logical.Plan) (*Plan, error) {
	rootVertex, err := logicalPlan.Root()
	if err != nil {
		return nil, err
	}
	var stack []dag.Vertex
	var visited = make(map[dag.Vertex]struct{})
	dfs(rootVertex, logicalPlan.AcyclicGraph, stack, visited)
	var topologySorted []Transform
	for i := len(stack) - 1; i >= 0; i-- {
		tf, err := convertToTransform(stack[i])
		if err != nil {
			return nil, err
		}
		topologySorted = append(topologySorted, tf)
	}

	return &Plan{
		steps: topologySorted,
	}, nil
}

func dfs(vertex dag.Vertex, graph *dag.AcyclicGraph, stack []dag.Vertex, visited map[dag.Vertex]struct{}) {
	visited[vertex] = struct{}{}
	downEdges := graph.EdgesFrom(vertex)
	for _, downEdge := range downEdges {
		if _, ok := visited[downEdge.Target()]; !ok {
			dfs(downEdge.Target(), graph, stack, visited)
		}
	}

	stack = append(stack, vertex)
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
			panic("")
		case logical.OpPagination:
			return NewPaginationTransform(op.(*logical.Pagination)), nil
		case logical.OpChunkIDsMerge:
			return NewChunkIDsMergeTransform(op.(*logical.ChunkIDsMerge)), nil
		default:
			return nil, errors.New("unsupported logical op")
		}
	} else {
		return nil, errors.New("unsupported vertex")
	}
}
