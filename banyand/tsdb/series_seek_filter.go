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

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
)

var ErrUnsupportedIndexRule = errors.New("the index rule is not supported")

type Condition map[string][]index.ConditionValue

func (s *seekerBuilder) Filter(indexRule *databasev1.IndexRule, condition Condition) SeekerBuilder {
	s.conditions = append(s.conditions, struct {
		indexRuleType databasev1.IndexRule_Type
		indexRuleID   uint32
		condition     Condition
	}{
		indexRuleType: indexRule.GetType(),
		indexRuleID:   indexRule.GetMetadata().GetId(),
		condition:     condition,
	})
	return s
}

type condWithIRT struct {
	indexRuleType databasev1.IndexRule_Type
	condition     index.Condition
}

func (s *seekerBuilder) buildConditions() ([]condWithIRT, error) {
	if len(s.conditions) < 1 {
		return nil, nil
	}
	conditions := make([]condWithIRT, 0, len(s.conditions))
	for _, condition := range s.conditions {
		if len(condition.condition) > 1 {
			//TODO:// should support composite index rule
			return nil, ErrUnsupportedIndexRule
		}
		cond := make(index.Condition)
		term := index.FieldKey{
			SeriesID:    s.seriesSpan.seriesID,
			IndexRuleID: condition.indexRuleID,
		}
		for _, c := range condition.condition {
			cond[term] = c
			break
		}
		conditions = append(conditions, condWithIRT{indexRuleType: condition.indexRuleType, condition: cond})
	}
	return conditions, nil
}

func (s *seekerBuilder) buildIndexFilter(block blockDelegate, conditions []condWithIRT) (filterFn, error) {
	var allItemIDs posting.List
	addIDs := func(allList posting.List, searcher index.Searcher, cond index.Condition) (posting.List, bool, error) {
		tree, err := index.BuildTree(searcher, cond)
		if err != nil {
			return nil, false, err
		}
		rangeOpts, found := tree.TrimRangeLeaf(index.FieldKey{
			SeriesID:    s.seriesSpan.seriesID,
			IndexRuleID: s.indexRuleForSorting.GetMetadata().GetId(),
		})
		if found {
			s.rangeOptsForSorting = rangeOpts
		}
		list, err := tree.Execute()
		if errors.Is(err, index.ErrEmptyTree) {
			return allList, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if allList == nil {
			allList = list
		} else {
			err = allList.Intersect(list)
			if err != nil {
				return nil, false, err
			}
		}
		return allList, true, nil
	}
	allInvalid := true
	for i, condition := range conditions {
		var valid bool
		var err error
		switch condition.indexRuleType {
		case databasev1.IndexRule_TYPE_INVERTED:
			allItemIDs, valid, err = addIDs(allItemIDs, block.invertedIndexReader(), condition.condition)
		case databasev1.IndexRule_TYPE_TREE:
			allItemIDs, valid, err = addIDs(allItemIDs, block.lsmIndexReader(), condition.condition)
		default:
			return nil, ErrUnsupportedIndexRule
		}
		if err != nil {
			return nil, err
		}
		if i > 0 && allItemIDs.IsEmpty() {
			return func(_ Item) bool {
				return false
			}, nil
		}
		allInvalid = allInvalid && !valid
	}

	if allInvalid {
		return nil, nil
	}
	return func(item Item) bool {
		valid := allItemIDs.Contains(item.ID())
		s.seriesSpan.l.Trace().Int("valid_item_num", allItemIDs.Len()).Bool("valid", valid).Msg("filter item by index")
		return valid
	}, nil
}

type filterFn func(item Item) bool
