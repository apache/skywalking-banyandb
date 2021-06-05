package logical

import (
	"github.com/hashicorp/terraform/dag"
	"github.com/hashicorp/terraform/tfdiags"
)

var validationRegistry = make(map[*vertexMatcher]vertexValidator)

type vertexMatcher func(Op) bool
type vertexValidator func(Op) tfdiags.Diagnostics

func init() {

}

func (plan *Plan) DefaultValidate() (bool, error) {
	diag := plan.Walk(func(vertex dag.Vertex) tfdiags.Diagnostics {
		for matcher, validator := range validationRegistry {
			if op, ok := vertex.(Op); ok {
				if match := (*matcher)(op); match {
					return validator(op)
				}
			}
		}
		return nil
	})

	return diag.HasErrors(), diag.Err()
}

func matchOpType(opType string) vertexMatcher {
	return func(op Op) bool {
		return opType == op.OpType()
	}
}
