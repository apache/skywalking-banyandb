package physical

import (
	"context"

	"github.com/apache/skywalking-banyandb/banyand/series"
)

//go:generate mockgen -destination=./plan_mock.go -package=physical . ExecutionContext
type ExecutionContext interface {
	context.Context
	UniModel() series.UniModel
}

// plan is the physical plan
type plan struct {
	// steps is sorted by topology-sort algorithm
	steps []Transform
}
