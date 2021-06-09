// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

var _ IndexOp = (*IndexScan)(nil)

// IndexScan defines parameters for index-scan
// metadata together with Catalog can be mapped to the IndexRuleBinding, and thus IndexRule via rule_refs, but how?
type IndexScan struct {
	metadata    *apiv1.Metadata
	timeRange   *apiv1.RangeQuery
	KeyName     string
	PairQueries []*apiv1.PairQuery
}

func (is *IndexScan) TimeRange() *apiv1.RangeQuery {
	return is.timeRange
}

func (is *IndexScan) Metadata() *apiv1.Metadata {
	return is.metadata
}

func (is *IndexScan) Name() string {
	return fmt.Sprintf("IndexScan{begin=%d,end=%d,KeyName=%s,conditions=[%s],metadata={group=%s,name=%s}}",
		is.timeRange.Begin(), is.timeRange.End(), is.KeyName, serializePairQueries(is.PairQueries), is.metadata.Group(), is.metadata.Name())
}

func (is *IndexScan) OpType() string {
	return OpIndexScan
}

func NewIndexScan(metadata *apiv1.Metadata, timeRange *apiv1.RangeQuery, keyName string, pairQueries []*apiv1.PairQuery) IndexOp {
	return &IndexScan{
		metadata:    metadata,
		timeRange:   timeRange,
		KeyName:     keyName,
		PairQueries: pairQueries,
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
