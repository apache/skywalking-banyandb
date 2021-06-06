package logical

import (
	"github.com/emicklei/dot"
	"github.com/hashicorp/terraform/dag"
)

func (plan *Plan) Plot() string {
	g := dot.NewGraph(dot.Directed)

	var nodes = make(map[dag.Vertex]dot.Node)

	for _, v := range plan.Vertices() {
		nodes[v] = g.Node(dag.VertexName(v))
	}

	for _, e := range plan.Edges() {
		g.Edge(nodes[e.Source()], nodes[e.Target()])
	}

	return g.String()
}
