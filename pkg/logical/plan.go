package logical

import "github.com/hashicorp/terraform/dag"

type Plan struct {
	dag.AcyclicGraph
}
