package optimizer

import "github.com/apache/skywalking-banyandb/pkg/logical"

type Rule interface {
	Apply(plan logical.Plan) logical.Plan
}
