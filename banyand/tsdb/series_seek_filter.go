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

package tsdb

import (
	"github.com/pkg/errors"

	databasev2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v2"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

var ErrUnsupportedIndexRule = errors.New("the index rule is not supported")

type Condition map[string][]index.ConditionValue

func (s *seekerBuilder) Filter(indexRule *databasev2.IndexRule, condition Condition) SeekerBuilder {
	s.conditions = append(s.conditions, struct {
		indexRuleType databasev2.IndexRule_Type
		indexRule     string
		condition     Condition
	}{
		indexRuleType: indexRule.GetType(),
		indexRule:     indexRule.GetMetadata().GetName(),
		condition:     condition,
	})
	return s
}

func (s *seekerBuilder) buildIndexFilter() (filterFn, error) {
	var treeIndexCondition, invertedIndexCondition []index.Condition
	for _, condition := range s.conditions {
		if len(condition.condition) > 1 {
			//TODO:// should support composite index rule
			return nil, ErrUnsupportedIndexRule
		}
		var cond index.Condition
		term := index.Term{
			SeriesID:  s.seriesSpan.seriesID,
			IndexRule: condition.indexRule,
		}
		for _, c := range condition.condition {
			cond[term] = c
			break
		}
		switch condition.indexRuleType {
		case databasev2.IndexRule_TYPE_TREE:
			treeIndexCondition = append(treeIndexCondition, cond)
		case databasev2.IndexRule_TYPE_INVERTED:
			invertedIndexCondition = append(invertedIndexCondition, cond)
		}
	}
	allItemIDs := roaring.NewPostingList()
	addIDs := func(allList posting.List, searcher index.Searcher, cond index.Condition) error {
		tree, err := index.BuildTree(searcher, cond)
		if err != nil {
			return err
		}
		list, err := tree.Execute()
		if err != nil {
			return err
		}
		err = allList.Union(list)
		if err != nil {
			return err
		}
		return nil
	}
	for _, b := range s.seriesSpan.blocks {
		for _, tCond := range treeIndexCondition {
			err := addIDs(allItemIDs, b.lsmIndexReader(), tCond)
			if err != nil {
				return nil, err
			}
		}
		for _, iCond := range invertedIndexCondition {
			err := addIDs(allItemIDs, b.invertedIndexReader(), iCond)
			if err != nil {
				return nil, err
			}
		}
	}
	return func(item Item) bool {
		return allItemIDs.Contains(item.ID())
	}, nil
}

type filterFn func(item Item) bool
