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
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/banyand/index/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

var ErrNotRangeOperation = errors.New("this is not an range operation")

type executable interface {
	execute() (posting.List, error)
}

type searchTree interface {
	executable
}

func (s *service) Search(series common.Metadata, shardID uint, startTime, endTime uint64, indexObjectName string, conditions []Condition) (posting.List, error) {
	sd, err := s.getShard(series, shardID)
	if err != nil {
		return nil, err
	}
	store := sd.store
	searcher, hasData := store.Window(startTime, endTime)
	if !hasData {
		return roaring.EmptyPostingList, nil
	}
	tree, errBuild := buildSearchTree(searcher, indexObjectName, conditions)
	if errBuild != nil {
		return nil, err
	}
	return tree.execute()
}

func buildSearchTree(searcher tsdb.Searcher, indexObject string, conditions []Condition) (searchTree, error) {
	condMap := toMap(indexObject, conditions)
	root := &andNode{
		node: &node{
			SubNodes: make([]executable, 0),
			searcher: searcher,
		},
	}
	for key, conds := range condMap {
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
				case apiv1.PairQuery_BINARY_OP_GT:
					opts.Lower = bytes.Join(cond.Values...)
				case apiv1.PairQuery_BINARY_OP_GE:
					opts.Lower = bytes.Join(cond.Values...)
					opts.IncludesLower = true
				case apiv1.PairQuery_BINARY_OP_LT:
					opts.Upper = bytes.Join(cond.Values...)
				case apiv1.PairQuery_BINARY_OP_LE:
					opts.Upper = bytes.Join(cond.Values...)
					opts.IncludesUpper = true
				}
				continue
			}
			switch cond.Op {
			case apiv1.PairQuery_BINARY_OP_EQ:
				root.addEq(key, cond.Values)
			case apiv1.PairQuery_BINARY_OP_NE:
				root.addNot(key, root.newEq(key, cond.Values))
			case apiv1.PairQuery_BINARY_OP_HAVING:
				n := root.addOrNode(len(cond.Values))
				for _, v := range cond.Values {
					n.addEq(key, [][]byte{v})
				}
			case apiv1.PairQuery_BINARY_OP_NOT_HAVING:
				n := root.addOrNode(len(cond.Values))
				for _, v := range cond.Values {
					n.addEq(key, [][]byte{v})
				}
				root.addNot(key, n)
			}
		}
	}
	return root, nil
}

func rangeOP(op apiv1.PairQuery_BinaryOp) bool {
	switch op {
	case apiv1.PairQuery_BINARY_OP_GT:
	case apiv1.PairQuery_BINARY_OP_GE:
	case apiv1.PairQuery_BINARY_OP_LT:
	case apiv1.PairQuery_BINARY_OP_LE:
		return true
	}
	return false
}

func toMap(indexObject string, condition []Condition) map[string][]Condition {
	result := make(map[string][]Condition)
	for _, c := range condition {
		key := compositeFieldID(indexObject, c.Key)
		l, ok := result[key]
		if ok {
			l = append(l, c)
			result[key] = l
			continue
		}
		result[key] = []Condition{c}
	}
	return result
}

type logicalOP interface {
	merge(posting.List) error
}

type node struct {
	logicalOP
	executable
	searcher tsdb.Searcher
	value    posting.List
	SubNodes []executable `json:"sub_nodes,omitempty"`
}

func (n *node) newEq(key string, values [][]byte) *eq {
	return &eq{
		leaf: &leaf{
			Key:      []byte(key),
			values:   values,
			searcher: n.searcher,
		},
	}
}

func (n *node) addEq(key string, values [][]byte) {
	n.SubNodes = append(n.SubNodes, n.newEq(key, values))
}

func (n *node) addNot(key string, inner executable) {
	n.SubNodes = append(n.SubNodes, &not{
		Key:      []byte(key),
		searcher: n.searcher,
		Inner:    inner,
	})
}

func (n *node) addRangeLeaf(key string) *rangeOp {
	r := &rangeOp{
		leaf: &leaf{
			Key:      []byte(key),
			searcher: n.searcher,
		},
		Opts: &tsdb.RangeOpts{},
	}
	n.SubNodes = append(n.SubNodes, r)
	return r
}

func (n *node) addOrNode(size int) *orNode {
	on := &orNode{
		node: &node{
			searcher: n.searcher,
			SubNodes: make([]executable, 0, size),
		},
	}
	n.SubNodes = append(n.SubNodes, on)
	return on
}

func (n *node) pop() (executable, bool) {
	if len(n.SubNodes) < 1 {
		return nil, false
	}
	sn := n.SubNodes[0]
	n.SubNodes = n.SubNodes[1:]
	return sn, true
}

func (n *node) execute() (posting.List, error) {
	ex, hasNext := n.pop()
	if !hasNext {
		return n.value, nil
	}
	r, err := ex.execute()
	if err != nil {
		return nil, err
	}
	if n.value == nil {
		n.value = r
		return n.execute()
	}
	err = n.merge(r)
	if err != nil {
		return nil, err
	}
	if n.value.IsEmpty() {
		return n.value, nil
	}
	return n.execute()
}

type andNode struct {
	*node
}

func (an *andNode) merge(list posting.List) error {
	return an.value.Intersect(list)
}

type orNode struct {
	*node
}

func (on *orNode) merge(list posting.List) error {
	return on.value.Union(list)
}

type leaf struct {
	executable
	Key      []byte `json:"Key"`
	values   [][]byte
	searcher tsdb.Searcher
}

type not struct {
	executable
	Key      []byte `json:"key"`
	searcher tsdb.Searcher
	Inner    executable `json:"inner,omitempty"`
}

func (n *not) execute() (posting.List, error) {
	all := n.searcher.MatchField(n.Key)
	list, err := n.Inner.execute()
	if err != nil {
		return nil, err
	}
	err = all.Difference(list)
	return all, err
}

type eq struct {
	*leaf
}

func (eq *eq) execute() (posting.List, error) {
	return eq.searcher.MatchTerms(&tsdb.Field{
		Name:  eq.Key,
		Value: bytes.Join(eq.values...),
	}), nil
}

type rangeOp struct {
	*leaf
	Opts *tsdb.RangeOpts `json:"opts"`
}

func (r *rangeOp) execute() (posting.List, error) {
	return r.searcher.Range(r.Key, r.Opts), nil
}
