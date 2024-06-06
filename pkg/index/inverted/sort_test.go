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

package inverted

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const indexRuleID = 3

func TestStore_Sort(t *testing.T) {
	tester := assert.New(t)
	path, fn := setUp(require.New(t))
	s, err := NewStore(StoreOpts{
		Path:   path,
		Logger: logger.GetLogger("test"),
	})
	defer func() {
		tester.NoError(s.Close())
		fn()
	}()
	tester.NoError(err)
	data := setUpDuration(require.New(t), s)
	s.(*store).flush()

	tests := []struct {
		name string
		want []int
		args args
	}{
		{
			name: "sort all in asc order",
			args: args{
				orderType: modelv1.Sort_SORT_ASC,
			},
			want: []int{50, 200, 500, 1000, 2000},
		},
		{
			name: "sort all in desc order",
			args: args{
				orderType: modelv1.Sort_SORT_DESC,
			},
			want: []int{2000, 1000, 500, 200, 50},
		},
		{
			name: "sort all in asc order with sids",
			args: args{
				sids:      []common.SeriesID{1, 2},
				orderType: modelv1.Sort_SORT_ASC,
			},
			want: []int{50, 200, 500, 1000, 2000},
		},
		{
			name: "sort all in desc order with sids",
			args: args{
				sids:      []common.SeriesID{1, 2},
				orderType: modelv1.Sort_SORT_DESC,
			},
			want: []int{2000, 1000, 500, 200, 50},
		},
		{
			name: "sort sid 1 in asc order",
			args: args{
				sids:      []common.SeriesID{1},
				orderType: modelv1.Sort_SORT_ASC,
			},
			want: []int{50, 500, 2000},
		},
		{
			name: "sort sid 2 in asc order",
			args: args{
				sids:      []common.SeriesID{2},
				orderType: modelv1.Sort_SORT_ASC,
			},
			want: []int{200, 1000},
		},
		{
			name: "sort sid 1 in desc order",
			args: args{
				sids:      []common.SeriesID{1},
				orderType: modelv1.Sort_SORT_DESC,
			},
			want: []int{2000, 500, 50},
		},
		{
			name: "sort sid 2 in desc order",
			args: args{
				sids:      []common.SeriesID{2},
				orderType: modelv1.Sort_SORT_DESC,
			},
			want: []int{1000, 200},
		},

		{
			name: "default order",
			args: args{},
			want: []int{50, 200, 500, 1000, 2000},
		},
	}
	preLoadSizes := []int{7, 20, 50}
	allTests := make([]struct {
		name        string
		want        []int
		args        args
		preloadSize int
	}, 0, len(tests)*len(preLoadSizes))

	for _, size := range preLoadSizes {
		for _, t := range tests {
			allTests = append(allTests, struct {
				name        string
				want        []int
				args        args
				preloadSize int
			}{
				name:        t.name + " preLoadSize " + fmt.Sprint(size),
				want:        t.want,
				preloadSize: size,
				args:        t.args,
			})
		}
	}
	for _, tt := range allTests {
		t.Run(tt.name, func(t *testing.T) {
			tester := assert.New(t)
			is := require.New(t)
			iter, err := s.Sort(tt.args.sids, index.FieldKey{IndexRuleID: indexRuleID}, tt.args.orderType, tt.preloadSize)
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
			var got result
			for iter.Next() {
				docID, _, _ := iter.Val()
				got.items = append(got.items, docID)
			}
			for i := 0; i < 10; i++ {
				is.False(iter.Next())
			}
			var wants result
			for _, w := range tt.want {
				pl := data[w]
				wants.items = append(wants.items, pl.ToSlice()...)
			}
			tester.Equal(wants, got, tt.name)
		})
	}
}

type args struct {
	sids      []common.SeriesID
	orderType modelv1.Sort
}

type result struct {
	items []uint64
}

func setUpDuration(t *require.Assertions, store index.Writer) map[int]posting.List {
	r := map[int]posting.List{
		50:   roaring.NewPostingList(),
		200:  roaring.NewPostingList(),
		500:  roaring.NewPostingList(),
		1000: roaring.NewPostingList(),
		2000: roaring.NewPostingList(),
	}
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
			sid := i2%2 + 1
			t.NoError(store.Write([]index.Field{{
				Key: index.FieldKey{
					SeriesID:    common.SeriesID(sid),
					IndexRuleID: indexRuleID,
				},
				Term: convert.Int64ToBytes(int64(term)),
			}}, id))
			r[term].Insert(id)
		}
	}
	return r
}
