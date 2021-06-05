package logical

import (
	"fmt"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var _ IndexOp = (*indexScan)(nil)

// indexScan defines parameters for index-scan
// metadata together with Catalog can be mapped to the IndexRuleBinding, and thus IndexRule via rule_refs, but how?
type indexScan struct {
	metadata    *apiv1.Metadata
	timeRange   *apiv1.RangeQuery
	keyName     string
	pairQueries []*apiv1.PairQuery
}

func (is *indexScan) Name() string {
	return fmt.Sprintf("IndexScan{begin=%d,end=%d,keyName=%s,conditions=%v,metadata={group=%s,name=%s}}",
		is.timeRange.Begin(), is.timeRange.End(), is.keyName, is.pairQueries, is.metadata.Group(), is.metadata.Name())
}

func (is *indexScan) OpType() string {
	return IndexScan
}

func NewIndexScan(metadata *apiv1.Metadata, timeRange *apiv1.RangeQuery, keyName string, pairQueries []*apiv1.PairQuery) IndexOp {
	return &indexScan{
		metadata:    metadata,
		timeRange:   timeRange,
		keyName:     keyName,
		pairQueries: pairQueries,
	}
}
