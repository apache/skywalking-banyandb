package physical

import (
	"github.com/hashicorp/terraform/dag"

	"github.com/apache/skywalking-banyandb/pkg/types"
)

type Op interface {
	dag.NamedVertex
	OpType() string
	CreateTransform() Transform
}

type SourceOp interface {
	Op
}

type Continuation interface {
	Proceed()
}

// Transform is a vertex for a stateful transformation
// it can be created from Op
type Transform interface {
	Apply(Continuation) (Data, error)
}

type Data interface {
	Schema() types.Schema
}
