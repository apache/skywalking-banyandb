package logical

import (
	"fmt"
	"strconv"
	"strings"

	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var binaryOpStrMap = map[apiv1.BinaryOp]string{
	apiv1.BinaryOpEQ:         "=",
	apiv1.BinaryOpNE:         "!=",
	apiv1.BinaryOpLT:         "<",
	apiv1.BinaryOpGT:         ">",
	apiv1.BinaryOpLE:         "<=",
	apiv1.BinaryOpGE:         ">=",
	apiv1.BinaryOpHAVING:     "having",
	apiv1.BinaryOpNOT_HAVING: "not_having",
}

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
	return fmt.Sprintf("IndexScan{begin=%d,end=%d,keyName=%s,conditions=[%s],metadata={group=%s,name=%s}}",
		is.timeRange.Begin(), is.timeRange.End(), is.keyName, serializePairQueries(is.pairQueries), is.metadata.Group(), is.metadata.Name())
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

func serializePairQueries(queries []*apiv1.PairQuery) string {
	var queriesStr []string
	for _, q := range queries {
		str := binaryOpStrMap[q.Op()]
		p := q.Condition(nil)
		pairTyp := p.PairType()
		t := new(flatbuffers.Table)
		p.Pair(t)
		if pairTyp == apiv1.TypedPairIntPair {
			unionIntPairQuery := new(apiv1.IntPair)
			unionIntPairQuery.Init(t.Bytes, t.Pos)
			l := unionIntPairQuery.ValuesLength()
			if l == 1 {
				str += strconv.FormatInt(unionIntPairQuery.Values(0), 10)
			} else if l > 1 {
				str += "["
				for i := 0; i < l; i++ {
					str += strconv.FormatInt(unionIntPairQuery.Values(i), 10)
				}
				str += "]"
			}
		} else if pairTyp == apiv1.TypedPairStrPair {
			unionStrPairQuery := new(apiv1.StrPair)
			unionStrPairQuery.Init(t.Bytes, t.Pos)
			l := unionStrPairQuery.ValuesLength()
			if l == 1 {
				str += string(unionStrPairQuery.Values(0))
			} else if l > 1 {
				str += "["
				for i := 0; i < l; i++ {
					str += string(unionStrPairQuery.Values(i))
				}
				str += "]"
			}
		}
		queriesStr = append(queriesStr, str)
	}
	return strings.Join(queriesStr, ",")
}
