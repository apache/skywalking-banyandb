package physical

import "github.com/hashicorp/terraform/dag"

type plan struct {
	graph dag.AcyclicGraph
}
