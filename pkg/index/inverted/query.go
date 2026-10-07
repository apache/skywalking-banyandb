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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"encoding/json"
	"strings"

	"github.com/blugelabs/bluge"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

const (
	termRangeQuery = "termRangeQuery"
	timeRangeQuery = "timeRangeQuery"
)

var _ index.Query = (*queryNode)(nil)

// queryNode is a wrapper for bluge.Query.
type queryNode struct {
	query bluge.Query
	node
}

func (q *queryNode) String() string {
	return q.node.String()
}

// node is the debug/JSON representation a queryNode's tree renders through
// String(). It carries no query-execution semantics of its own; only the node
// kinds the generic Iterator/Range path in inverted.go builds are kept. Every
// other node kind went with the query builders that produced it (NIDX-03 §10,
// §15): the series index, the measure query planner and Property all build
// criteria.Filter calls against pkg/index/native instead, and this store's
// BuildQuery/Search/SeriesSort are retired stubs.
type node interface {
	String() string
}

type mustNode struct {
	subNodes []node
}

func newMustNode() *mustNode {
	return &mustNode{
		subNodes: make([]node, 0),
	}
}

func (m *mustNode) Append(subNode node) {
	m.subNodes = append(m.subNodes, subNode)
}

func (m *mustNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["must"] = m.subNodes
	return json.Marshal(data)
}

func (m *mustNode) String() string {
	return convert.JSONToString(m)
}

type termRangeInclusiveNode struct {
	indexRule        *databasev1.IndexRule
	min              string
	max              string
	minInclusive     bool
	maxInclusive     bool
	isTimeRangeQuery bool
}

func newTermRangeInclusiveNode(minVal, maxVal string, minInclusive, maxInclusive bool, indexRule *databasev1.IndexRule, isTimeRangeQuery bool) *termRangeInclusiveNode {
	return &termRangeInclusiveNode{
		indexRule:        indexRule,
		min:              minVal,
		max:              maxVal,
		minInclusive:     minInclusive,
		maxInclusive:     maxInclusive,
		isTimeRangeQuery: isTimeRangeQuery,
	}
}

func (t *termRangeInclusiveNode) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, 1)
	var builder strings.Builder
	if t.minInclusive {
		builder.WriteString("[")
	} else {
		builder.WriteString("(")
	}
	builder.WriteString(t.min + " ")
	builder.WriteString(t.max)
	if t.maxInclusive {
		builder.WriteString("]")
	} else {
		builder.WriteString(")")
	}
	inner["range"] = builder.String()
	if t.indexRule != nil {
		inner["index"] = t.indexRule.Metadata.Name + ":" + t.indexRule.Metadata.Group
	}
	if t.isTimeRangeQuery {
		inner["queryType"] = timeRangeQuery
	} else {
		inner["queryType"] = termRangeQuery
	}
	data := make(map[string]interface{}, 1)
	data["termRangeInclusive"] = inner
	return json.Marshal(data)
}

func (t *termRangeInclusiveNode) String() string {
	return convert.JSONToString(t)
}

type termNode struct {
	indexRule *databasev1.IndexRule
	term      string
}

func newTermNode(term string, indexRule *databasev1.IndexRule) *termNode {
	return &termNode{
		indexRule: indexRule,
		term:      term,
	}
}

func (t *termNode) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, 1)
	if t.indexRule != nil {
		inner["index"] = t.indexRule.Metadata.Name + ":" + t.indexRule.Metadata.Group
	}
	inner["value"] = t.term
	data := make(map[string]interface{}, 1)
	data["term"] = inner
	return json.Marshal(data)
}

func (t *termNode) String() string {
	return convert.JSONToString(t)
}
