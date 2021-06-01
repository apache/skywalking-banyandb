package logical

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/types"
)

//go:generate mockery --name Plan --output ../internal/mocks
type Plan interface {
	fmt.Stringer
	Schema() (types.Schema, error)
	Children() []Plan
}

type Expr interface {
	fmt.Stringer
	ToField(plan Plan) (types.Field, error)
}
