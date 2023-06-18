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

// Package testcases implements common helpers for testing inverted and lsm indices.
package testcases

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

var duration = index.FieldKey{
	// duration
	IndexRuleID: 3,
}

// SimpleStore is subset of index.Store for testing.
type SimpleStore interface {
	index.FieldIterable
	index.Writer
	MatchTerms(field index.Field) (list posting.List, err error)
}

type args struct {
	termRange index.RangeOpts
	fieldKey  index.FieldKey
	orderType modelv1.Sort
}

type result struct {
	items []int
	key   int
}

// RunDuration executes duration related cases.
func RunDuration(t *testing.T, data map[int]posting.List, store SimpleStore) {
	tester := assert.New(t)
	is := require.New(t)
	tests := []struct {
		name string
		want []int
		args args
	}{
		{
			name: "sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
			},
			want: []int{50, 200, 500, 1000, 2000},
		},
		{
			name: "sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
			},
			want: []int{2000, 1000, 500, 200, 50},
		},
		{
			name: "scan in (lower, upper) and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Lower: convert.Int64ToBytes(50),
					Upper: convert.Int64ToBytes(2000),
				},
			},
			want: []int{200, 500, 1000},
		},
		{
			name: "scan in (lower, upper) and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Lower: convert.Int64ToBytes(50),
					Upper: convert.Int64ToBytes(2000),
				},
			},
			want: []int{1000, 500, 200},
		},
		{
			name: "scan in [lower, upper] and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(200),
					IncludesLower: true,
					Upper:         convert.Int64ToBytes(1000),
					IncludesUpper: true,
				},
			},
			want: []int{200, 500, 1000},
		},
		{
			name: "scan in [lower, upper] and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(200),
					IncludesLower: true,
					Upper:         convert.Int64ToBytes(1000),
					IncludesUpper: true,
				},
			},
			want: []int{1000, 500, 200},
		},
		{
			name: "scan in [lower, undefined)  and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(200),
					IncludesLower: true,
				},
			},
			want: []int{200, 500, 1000, 2000},
		},
		{
			name: "scan in [lower, undefined) and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(200),
					IncludesLower: true,
				},
			},
			want: []int{2000, 1000, 500, 200},
		},
		{
			name: "scan in (undefined, upper] and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Upper:         convert.Int64ToBytes(1000),
					IncludesUpper: true,
				},
			},
			want: []int{50, 200, 500, 1000},
		},
		{
			name: "scan in (undefined, upper] and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Upper:         convert.Int64ToBytes(1000),
					IncludesUpper: true,
				},
			},
			want: []int{1000, 500, 200, 50},
		},
		{
			name: "scan splice in (lower, upper) and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Lower: convert.Int64ToBytes(50 + 100),
					Upper: convert.Int64ToBytes(2000 - 100),
				},
			},
			want: []int{200, 500, 1000},
		},
		{
			name: "scan splice in (lower, upper) and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Lower: convert.Int64ToBytes(50 + 100),
					Upper: convert.Int64ToBytes(2000 - 100),
				},
			},
			want: []int{1000, 500, 200},
		},
		{
			name: "scan splice in [lower, upper] and sort in asc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_ASC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(50 + 100),
					IncludesLower: true,
					Upper:         convert.Int64ToBytes(2000 - 100),
					IncludesUpper: true,
				},
			},
			want: []int{200, 500, 1000},
		},
		{
			name: "scan splice in [lower, upper] and sort in desc order",
			args: args{
				fieldKey:  duration,
				orderType: modelv1.Sort_SORT_DESC,
				termRange: index.RangeOpts{
					Lower:         convert.Int64ToBytes(50 + 100),
					IncludesLower: true,
					Upper:         convert.Int64ToBytes(2000 - 100),
					IncludesUpper: true,
				},
			},
			want: []int{1000, 500, 200},
		},
		{
			name: "no field key",
			args: args{},
		},
		{
			name: "unknown field key",
			args: args{
				fieldKey: index.FieldKey{
					IndexRuleID: 0,
				},
			},
		},
		{
			name: "default order",
			args: args{
				fieldKey: duration,
			},
			want: []int{50, 200, 500, 1000, 2000},
		},
		{
			name: "invalid range",
			args: args{
				fieldKey: duration,
				termRange: index.RangeOpts{
					Lower: convert.Int64ToBytes(100),
					Upper: convert.Int64ToBytes(50),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := store.Iterator(tt.args.fieldKey, tt.args.termRange, tt.args.orderType)
			is.NoError(err)
			if iter == nil {
				tester.Empty(tt.want)
				return
			}
			defer func() {
				tester.NoError(iter.Close())
				for i := 0; i < 10; i++ {
					is.False(iter.Next())
				}
			}()
			is.NotNil(iter)
			got := make([]result, 0)
			for iter.Next() {
				got = append(got, result{
					key:   int(convert.BytesToInt64(iter.Val().Term)),
					items: toArray(iter.Val().Value),
				})
			}
			for i := 0; i < 10; i++ {
				is.False(iter.Next())
			}
			wants := make([]result, 0, len(tt.want))
			for _, w := range tt.want {
				wants = append(wants, result{
					key:   w,
					items: toArray(data[w]),
				})
			}
			tester.Equal(wants, got, tt.name)
		})
	}
}

func toArray(list posting.List) []int {
	ints := make([]int, 0, list.Len())
	iter := list.Iterator()
	defer func(iter posting.Iterator) {
		_ = iter.Close()
	}(iter)
	for iter.Next() {
		ints = append(ints, int(iter.Current()))
	}
	return ints
}

// SetUpDuration initializes data for testing duration related cases.
func SetUpDuration(t *assert.Assertions, store index.Writer) map[int]posting.List {
	r := map[int]posting.List{
		50:   roaring.NewPostingList(),
		200:  roaring.NewPostingList(),
		500:  roaring.NewPostingList(),
		1000: roaring.NewPostingList(),
		2000: roaring.NewPostingList(),
	}
	return setUpPartialDuration(t, store, r)
}

func setUpPartialDuration(t *assert.Assertions, store index.Writer, r map[int]posting.List) map[int]posting.List {
	idx := make([]int, 0, len(r))
	for key := range r {
		idx = append(idx, key)
	}
	sort.Ints(idx)
	for i := 100; i < 200; i++ {
		id := uint64(i)
		for i2, term := range idx {
			if i%len(idx) != i2 || r[term] == nil {
				continue
			}
			t.NoError(store.Write([]index.Field{{
				Key:  duration,
				Term: convert.Int64ToBytes(int64(term)),
			}}, id))
			r[term].Insert(id)
		}
	}
	return r
}
