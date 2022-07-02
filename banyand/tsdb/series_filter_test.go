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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func newItem(itemID common.ItemID) *item {
	return &item{
		itemID: itemID,
	}
}

func Test_Filter_LogicalOperations(t *testing.T) {
	tests := []struct {
		name          string
		filterFactory func() (ItemFilter, error)
		positiveItems []Item
		negativeItems []Item
	}{
		{
			name: "Empty AND FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().And(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(0)},
			negativeItems: []Item{newItem(1)},
		},
		{
			name: "Empty OR FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().Or(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(0)},
			negativeItems: []Item{newItem(1)},
		},
		{
			name: "(Empty AND FilterFn) OR FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().And(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				filterFunc, err = filterFunc.Or(FilterFn(func(item Item) bool {
					return item.ID()%3 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(0), newItem(2), newItem(3)},
			negativeItems: []Item{newItem(5)},
		},
		{
			name: "(Empty OR FilterFn) AND FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().Or(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				filterFunc, err = filterFunc.And(FilterFn(func(item Item) bool {
					return item.ID()%3 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(6), newItem(12), newItem(18)},
			negativeItems: []Item{newItem(2), newItem(3)},
		},
		{
			name: "(Empty AND FilterFn...) OR FilterFn...",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().And(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}), FilterFn(func(item Item) bool {
					return item.ID()%3 == 0
				}))
				if err != nil {
					return nil, err
				}
				filterFunc, err = filterFunc.Or(FilterFn(func(item Item) bool {
					return item.ID()%5 == 0
				}), FilterFn(func(item Item) bool {
					return item.ID()%7 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(5), newItem(6), newItem(7), newItem(12)},
			negativeItems: []Item{newItem(2), newItem(13)},
		},
		{
			name: "(Empty OR FilterFn...) AND FilterFn...",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := EmptyFilter().Or(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}), FilterFn(func(item Item) bool {
					return item.ID()%3 == 0
				}))
				if err != nil {
					return nil, err
				}
				filterFunc, err = filterFunc.And(FilterFn(func(item Item) bool {
					return item.ID()%5 == 0
				}), FilterFn(func(item Item) bool {
					return item.ID()%7 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(70), newItem(105)},
			negativeItems: []Item{newItem(2), newItem(3), newItem(35)},
		},
		{
			name: "Empty(and{}) AND FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := ItemFilter(and{}).And(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(0)},
			negativeItems: []Item{newItem(1)},
		},
		{
			name: "Empty(or{}) OR FilterFn",
			filterFactory: func() (ItemFilter, error) {
				filterFunc, err := ItemFilter(or{}).Or(FilterFn(func(item Item) bool {
					return item.ID()%2 == 0
				}))
				if err != nil {
					return nil, err
				}
				return filterFunc, nil
			},
			positiveItems: []Item{newItem(0)},
			negativeItems: []Item{newItem(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			filterFunc, err := tt.filterFactory()
			assert.NoError(err)
			assert.NotNil(filterFunc)
			for _, pi := range tt.positiveItems {
				assert.True(filterFunc.Predicate(pi))
			}
			for _, ni := range tt.negativeItems {
				assert.False(filterFunc.Predicate(ni))
			}
		})
	}
}

var (
	_ ItemFilter              = (*mockConditionFactory)(nil)
	_ LogicalIndexedCondition = (*mockConditionFactory)(nil)
)

type mockConditionFactory struct {
	list posting.List
}

func (m *mockConditionFactory) build(seriesSpan SeriesSpan, b BlockDelegate) (ItemFilter, error) {
	return &roaringBitmapFilter{list: m.list}, nil
}

func (m *mockConditionFactory) Predicate(i Item) bool {
	return m.list.Contains(i.ID())
}

func (m *mockConditionFactory) And(filters ...ItemFilter) (ItemFilter, error) {
	allList := m.list
	for _, filter := range filters {
		if rbf, ok := filter.(*roaringBitmapFilter); ok {
			innerErr := allList.Intersect(rbf.list)
			return nil, innerErr
		}
	}
	return &mockConditionFactory{
		list: allList,
	}, nil
}

func (m *mockConditionFactory) Or(filters ...ItemFilter) (ItemFilter, error) {
	allList := m.list
	for _, filter := range filters {
		if rbf, ok := filter.(*roaringBitmapFilter); ok {
			innerErr := allList.Union(rbf.list)
			return nil, innerErr
		}
	}
	return &mockConditionFactory{
		list: allList,
	}, nil
}

func Test_RoaringBitmapFilter(t *testing.T) {
	tests := []struct {
		name             string
		conditionBuilder LogicalIndexedCondition
		positiveItems    []Item
		negativeItems    []Item
	}{
		{
			name: "AND Two",
			conditionBuilder: AndConditions(&mockConditionFactory{
				list: roaring.NewRange(0, 1000),
			}, &mockConditionFactory{
				list: roaring.NewRange(999, 2000),
			}),
			positiveItems: []Item{newItem(999)},
			negativeItems: []Item{newItem(0), newItem(1000)},
		},
		{
			name: "AND Multiple Not Empty Intersection",
			conditionBuilder: AndConditions(&mockConditionFactory{
				list: roaring.NewRange(0, 100),
			}, &mockConditionFactory{
				list: roaring.NewRange(50, 150),
			}, &mockConditionFactory{
				list: roaring.NewRange(75, 200),
			}),
			positiveItems: []Item{newItem(75), newItem(99)},
			negativeItems: []Item{newItem(49), newItem(100)},
		},
		{
			name: "AND Multiple Empty Intersection",
			conditionBuilder: AndConditions(&mockConditionFactory{
				list: roaring.NewRange(0, 100),
			}, &mockConditionFactory{
				list: roaring.NewRange(100, 200),
			}, &mockConditionFactory{
				list: roaring.NewRange(200, 300),
			}),
			positiveItems: []Item{},
			negativeItems: []Item{newItem(0), newItem(50), newItem(100), newItem(150), newItem(200)},
		},
		{
			name: "OR Two",
			conditionBuilder: OrConditions(&mockConditionFactory{
				list: roaring.NewRange(0, 10),
			}, &mockConditionFactory{
				list: roaring.NewRange(90, 100),
			}),
			positiveItems: []Item{newItem(0), newItem(99)},
			negativeItems: []Item{newItem(10), newItem(100)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			filterFunc, err := tt.conditionBuilder.build(nil, nil)
			assert.NoError(err)
			assert.NotNil(filterFunc)
			for _, pi := range tt.positiveItems {
				assert.True(filterFunc.Predicate(pi))
			}
			for _, ni := range tt.negativeItems {
				assert.False(filterFunc.Predicate(ni))
			}
		})
	}
}
