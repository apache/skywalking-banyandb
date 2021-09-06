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

package index

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var ErrNotRangeOperation = errors.New("this is not an range operation")

type Executor interface {
	Execute() (posting.List, error)
}

type Tree interface {
	Executor
}

type Term struct {
	SeriesID  common.SeriesID
	IndexRule string
}

func (t *Term) Marshal() []byte {
	return bytes.Join([][]byte{
		t.SeriesID.Marshal(),
		[]byte(t.IndexRule),
	}, []byte(":"))
}

type Condition map[Term][]ConditionValue

type ConditionValue struct {
	Values [][]byte
	Op     modelv2.Condition_BinaryOp
}

func BuildTree(searcher Searcher, condMap Condition) (Tree, error) {
	root := &andNode{
		node: &node{
			SubNodes: make([]Executor, 0),
			searcher: searcher,
		},
	}
	for term, conds := range condMap {
		key := term.Marshal()
		var rangeLeaf *rangeOp
		for _, cond := range conds {
			if rangeLeaf != nil && !rangeOP(cond.Op) {
				return nil, errors.Wrapf(ErrNotRangeOperation, "op:%s", cond.Op.String())
			}
			if rangeOP(cond.Op) {
				if rangeLeaf == nil {
					rangeLeaf = root.addRangeLeaf(key)
				}
				opts := rangeLeaf.Opts
				switch cond.Op {
				case modelv2.Condition_BINARY_OP_GT:
					opts.Lower = bytes.Join(cond.Values, nil)
				case modelv2.Condition_BINARY_OP_GE:
					opts.Lower = bytes.Join(cond.Values, nil)
					opts.IncludesLower = true
				case modelv2.Condition_BINARY_OP_LT:
					opts.Upper = bytes.Join(cond.Values, nil)
				case modelv2.Condition_BINARY_OP_LE:
					opts.Upper = bytes.Join(cond.Values, nil)
					opts.IncludesUpper = true
				}
				continue
			}
			switch cond.Op {
			case modelv2.Condition_BINARY_OP_EQ:
				root.addEq(key, cond.Values)
			case modelv2.Condition_BINARY_OP_NE:
				root.addNot(key, root.newEq(key, cond.Values))
			case modelv2.Condition_BINARY_OP_HAVING:
				n := root.addOrNode(len(cond.Values))
				for _, v := range cond.Values {
					n.addEq(key, [][]byte{v})
				}
			case modelv2.Condition_BINARY_OP_NOT_HAVING:
				n := root.newOrNode(len(cond.Values))
				for _, v := range cond.Values {
					n.addEq(key, [][]byte{v})
				}
				root.addNot(key, n)
			}
		}
	}
	return root, nil
}

func rangeOP(op modelv2.Condition_BinaryOp) bool {
	switch op {
	case modelv2.Condition_BINARY_OP_GT,
		modelv2.Condition_BINARY_OP_GE,
		modelv2.Condition_BINARY_OP_LT,
		modelv2.Condition_BINARY_OP_LE:
		return true
	}
	return false
}

type logicalOP interface {
	Executor
	merge(posting.List) error
}

type node struct {
	searcher Searcher
	value    posting.List
	SubNodes []Executor `json:"sub_nodes,omitempty"`
}

func (n *node) newEq(key []byte, values [][]byte) *eq {
	return &eq{
		leaf: &leaf{
			Key:      key,
			Values:   values,
			searcher: n.searcher,
		},
	}
}

func (n *node) addEq(key []byte, values [][]byte) {
	n.SubNodes = append(n.SubNodes, n.newEq(key, values))
}

func (n *node) addNot(key []byte, inner Executor) {
	n.SubNodes = append(n.SubNodes, &not{
		Key:      key,
		searcher: n.searcher,
		Inner:    inner,
	})
}

func (n *node) addRangeLeaf(key []byte) *rangeOp {
	r := &rangeOp{
		leaf: &leaf{
			Key:      key,
			searcher: n.searcher,
		},
		Opts: &RangeOpts{},
	}
	n.SubNodes = append(n.SubNodes, r)
	return r
}

func (n *node) newOrNode(size int) *orNode {
	return &orNode{
		node: &node{
			searcher: n.searcher,
			SubNodes: make([]Executor, 0, size),
		},
	}
}

func (n *node) addOrNode(size int) *orNode {
	on := n.newOrNode(size)
	n.SubNodes = append(n.SubNodes, on)
	return on
}

func (n *node) pop() (Executor, bool) {
	if len(n.SubNodes) < 1 {
		return nil, false
	}
	sn := n.SubNodes[0]
	n.SubNodes = n.SubNodes[1:]
	return sn, true
}

func execute(n *node, lp logicalOP) (posting.List, error) {
	ex, hasNext := n.pop()
	if !hasNext {
		return n.value, nil
	}
	r, err := ex.Execute()
	if err != nil {
		return nil, err
	}
	if n.value == nil {
		n.value = r
		return lp.Execute()
	}
	err = lp.merge(r)
	if err != nil {
		return nil, err
	}
	if n.value.IsEmpty() {
		return n.value, nil
	}
	return lp.Execute()
}

type andNode struct {
	*node
}

func (an *andNode) merge(list posting.List) error {
	return an.value.Intersect(list)
}

func (an *andNode) Execute() (posting.List, error) {
	return execute(an.node, an)
}

func (an *andNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["and"] = an.node.SubNodes
	return json.Marshal(data)
}

type orNode struct {
	*node
}

func (on *orNode) merge(list posting.List) error {
	return on.value.Union(list)
}

func (on *orNode) Execute() (posting.List, error) {
	return execute(on.node, on)
}

func (on *orNode) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["or"] = on.node.SubNodes
	return json.Marshal(data)
}

type leaf struct {
	Executor
	Key      []byte
	Values   [][]byte
	searcher Searcher
}

type not struct {
	Executor
	Key      []byte
	searcher Searcher
	Inner    Executor
}

func (n *not) Execute() (posting.List, error) {
	all := n.searcher.MatchField([]byte(n.Key))
	list, err := n.Inner.Execute()
	if err != nil {
		return nil, err
	}
	err = all.Difference(list)
	return all, err
}

func (n *not) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["not"] = n.Inner
	return json.Marshal(data)
}

type eq struct {
	*leaf
}

func (eq *eq) Execute() (posting.List, error) {
	return eq.searcher.MatchTerms(Field{
		Term:  []byte(eq.Key),
		Value: bytes.Join(eq.Values, nil),
	}), nil
}

func (eq *eq) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	data["eq"] = eq.leaf
	return json.Marshal(data)
}

type rangeOp struct {
	*leaf
	Opts *RangeOpts
}

func (r *rangeOp) Execute() (posting.List, error) {
	return r.searcher.Range(r.Key, *r.Opts), nil
}

func (r *rangeOp) MarshalJSON() ([]byte, error) {
	data := make(map[string]interface{}, 1)
	var builder strings.Builder
	if r.Opts.Lower != nil {
		if r.Opts.IncludesLower {
			builder.WriteString("[")
		} else {
			builder.WriteString("(")
		}
	}
	builder.WriteString(base64.StdEncoding.EncodeToString(r.Opts.Lower))
	builder.WriteString(",")
	builder.WriteString(base64.StdEncoding.EncodeToString(r.Opts.Upper))
	if r.Opts.Upper != nil {
		if r.Opts.IncludesUpper {
			builder.WriteString("]")
		} else {
			builder.WriteString(")")
		}
	}
	data["key"] = r.Key
	data["range"] = builder.String()
	return json.Marshal(data)
}
