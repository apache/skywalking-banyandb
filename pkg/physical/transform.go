package physical

import (
	"errors"

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
		traceEntities := data.NewTraceWithEntities(entities)
		return Success(NewTraceData(traceEntities))
	})
}

func (t *tableScanTransform) AppendParent(f ...Future) {
	t.parents = t.parents.Append(f...)
}

var _ Transform = (*chunkIDsFetchTransform)(nil)

type chunkIDsFetchTransform struct {
	params  *logical.ChunkIDsFetch
	parents Futures
}

func (c *chunkIDsFetchTransform) Run(ec ExecutionContext) Future {
	return c.parents.Then(func(result Result) (Data, error) {
		if result.Error() != nil {
			return nil, result.Error()
		}
		v := result.Value()
		if v.DataType() == ChunkID {
			entities, err := ec.UniModel().FetchEntity(v.(*chunkIDs).ids, c.params.Projection())
			if err != nil {
				return nil, err
			}
			traceEntities := data.NewTraceWithEntities(entities)
			return NewTraceData(traceEntities), nil
		}
		return nil, errors.New("incompatible upstream data type")
	})
}

func (c *chunkIDsFetchTransform) AppendParent(f ...Future) {
	c.parents = c.parents.Append(f...)
}
