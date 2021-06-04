package physical

var _ Transform = (*tableScanTransform)(nil)

type tableScanTransform struct {
	tableScanOp *tableScan
}

func (t tableScanTransform) Apply(continuation Continuation) (Data, error) {
	// TODO: fetch from storage

	// TODO: given to the downstream transformations
	panic("implement me")
}
