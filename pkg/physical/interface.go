package physical

import (
	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

type ExecutionContext interface {
	Series(apiv1.Metadata) interface{}
	Index(apiv1.Metadata) interface{}
	Schema(apiv1.Metadata) (interface{}, error)
}

type Operation func(ExecutionContext, Data) (Data, error)

// Transform is a vertex for a stateful transformation
// it can be created from Op
type Transform interface {
	Run(ctx ExecutionContext, _ Data) (Data, error)
}

type Data interface {
	Schema() types.Schema
}
