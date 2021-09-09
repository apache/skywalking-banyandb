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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv2 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v2"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/posting"
	"github.com/apache/skywalking-banyandb/pkg/index/posting/roaring"
)

func TestMemTable_Range(t *testing.T) {
	type args struct {
		fieldName []byte
		opts      index.RangeOpts
	}
	m := NewMemTable("sw")
	setUp(t, m)
	tests := []struct {
		name     string
		args     args
		wantList posting.List
	}{
		{
			name: "in range",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower: convert.Uint16ToBytes(100),
					Upper: convert.Uint16ToBytes(500),
				},
			},
			wantList: m.MatchTerms(index.Field{
				Key:  []byte("duration"),
				Term: convert.Uint16ToBytes(200),
			}),
		},
		{
			name: "excludes edge",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower: convert.Uint16ToBytes(50),
					Upper: convert.Uint16ToBytes(1000),
				},
			},
			wantList: union(m,
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(200),
				},
			),
		},
		{
			name: "includes lower",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesLower: true,
				},
			},
			wantList: union(m,
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(50),
				},
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(200),
				},
			),
		},
		{
			name: "includes upper",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesUpper: true,
				},
			},
			wantList: union(m,
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(200),
				},
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(1000),
				},
			),
		},
		{
			name: "includes edges",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesUpper: true,
					IncludesLower: true,
				},
			},
			wantList: union(m,
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(50),
				},
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(200),
				},
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(1000),
				},
			),
		},
		{
			name: "match one",
			args: args{
				fieldName: []byte("duration"),
				opts: index.RangeOpts{
					Lower:         convert.Uint16ToBytes(200),
					Upper:         convert.Uint16ToBytes(200),
					IncludesUpper: true,
					IncludesLower: true,
				},
			},
			wantList: union(m,
				index.Field{
					Key:  []byte("duration"),
					Term: convert.Uint16ToBytes(200),
				},
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotList := m.Range(tt.args.fieldName, tt.args.opts); !reflect.DeepEqual(gotList, tt.wantList) {
				t.Errorf("Range() = %v, want %v", gotList.Len(), tt.wantList.Len())
			}
		})
	}
}

func TestMemTable_Iterator(t *testing.T) {
	tester := assert.New(t)
	type args struct {
		fieldName []byte
		orderType modelv2.QueryOrder_Sort
	}
	m := NewMemTable("sw")
	setUp(t, m)
	tests := []struct {
		name string
		args args
		want [][]byte
	}{
		{
			name: "sort asc",
			args: args{
				fieldName: []byte("duration"),
				orderType: modelv2.QueryOrder_SORT_ASC,
			},
			want: [][]byte{convert.Uint16ToBytes(50), convert.Uint16ToBytes(200), convert.Uint16ToBytes(1000)},
		},
		{
			name: "sort desc",
			args: args{
				fieldName: []byte("duration"),
				orderType: modelv2.QueryOrder_SORT_DESC,
			},
			want: [][]byte{convert.Uint16ToBytes(1000), convert.Uint16ToBytes(200), convert.Uint16ToBytes(50)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := m.FieldIterator(tt.args.fieldName, tt.args.orderType)
			tester.NotNil(iter)
			var got [][]byte
			defer func() {
				_ = iter.Close()
			}()
			for iter.Next() {
				got = append(got, iter.Val().Key)
			}
			tester.Equal(tt.want, got)
		})
	}
}

func union(memTable *MemTable, fields ...index.Field) posting.List {
	result := roaring.NewPostingList()
	for _, f := range fields {
		_ = result.Union(memTable.MatchTerms(f))
	}
	return result
}

func setUp(t *testing.T, mt *MemTable) {
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			assert.NoError(t, mt.Insert(index.Field{
				Key:  []byte("service_name"),
				Term: []byte("gateway"),
			}, common.ItemID(i)))
		} else {
			assert.NoError(t, mt.Insert(index.Field{
				Key:  []byte("service_name"),
				Term: []byte("webpage"),
			}, common.ItemID(i)))
		}
	}
	for i := 100; i < 200; i++ {
		switch {
		case i%3 == 0:
			assert.NoError(t, mt.Insert(index.Field{
				Key:  []byte("duration"),
				Term: convert.Uint16ToBytes(50),
			}, common.ItemID(i)))
		case i%3 == 1:
			assert.NoError(t, mt.Insert(index.Field{
				Key:  []byte("duration"),
				Term: convert.Uint16ToBytes(200),
			}, common.ItemID(i)))
		case i%3 == 2:
			assert.NoError(t, mt.Insert(index.Field{
				Key:  []byte("duration"),
				Term: convert.Uint16ToBytes(1000),
			}, common.ItemID(i)))
		}
	}
}
