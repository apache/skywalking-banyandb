package physical

import (
	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

type Transform interface {
	Run(ExecutionContext) Future
	AppendParent(...Future)
}

var _ Transform = (*tableScanTransform)(nil)

type tableScanTransform struct {
	params  *logical.TableScan
	parents Futures
}

func NewTableScanTransform(params *logical.TableScan) Transform {
	return &tableScanTransform{
		params: params,
	}
}

func (t *tableScanTransform) Run(ec ExecutionContext) Future {
	return NewFuture(func() Result {
		sT, eT := t.params.TimeRange().Begin(), t.params.TimeRange().End()
		entities, err := ec.UniModel().ScanEntity(sT, eT, t.params.Projection())
		if err != nil {
			return Failure(err)
		}
		t := data.NewTraceWithEntities(entities)
		return Success(NewTraceData(t))
	})
}

func (t *tableScanTransform) AppendParent(f ...Future) {
	t.parents = t.parents.Append(f...)
}
