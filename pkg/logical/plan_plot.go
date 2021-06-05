package logical

import (
	"fmt"

	"github.com/hashicorp/terraform/dag"
)

func (plan *Plan) Plot() string {
	result := "digraph {\n"
	result += "\tcompound = \"true\"\n"
	result += "\tnewrank = \"true\"\n"
	result += "\tsubgraph \"root\" {\n"
	for _, e := range plan.Edges() {
		result += fmt.Sprintf("\t\t\"%s\" -> \"%s\"\n", dag.VertexName(e.Source()), dag.VertexName(e.Target()))
	}
	result += "\t}\n"
	result += "}\n"
	return result
}
