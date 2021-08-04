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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	apiv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

func Test_service_Search(t *testing.T) {
	tester := assert.New(t)
	type args struct {
		indexObjectName string
		conditions      []Condition
	}
	tests := []struct {
		name    string
		args    args
		want    posting.List
		wantErr bool
	}{
		{
			name: "str equal",
			args: args{
				indexObjectName: "endpoint",
				conditions: []Condition{
					{
						Key:    "endpoint",
						Op:     apiv1.PairQuery_BINARY_OP_EQ,
						Values: [][]byte{[]byte("/product")},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(1),
		},
		{
			name: "str not equal",
			args: args{
				indexObjectName: "endpoint",
				conditions: []Condition{
					{
						Key:    "endpoint",
						Op:     apiv1.PairQuery_BINARY_OP_NE,
						Values: [][]byte{[]byte("/product")},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(2, 3),
		},
		{
			name: "str having",
			args: args{
				indexObjectName: "endpoint",
				conditions: []Condition{
					{
						Key:    "endpoint",
						Op:     apiv1.PairQuery_BINARY_OP_HAVING,
						Values: [][]byte{[]byte("/product"), []byte("/sales")},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(1, 3),
		},
		{
			name: "str not having",
			args: args{
				indexObjectName: "endpoint",
				conditions: []Condition{
					{
						Key:    "endpoint",
						Op:     apiv1.PairQuery_BINARY_OP_NOT_HAVING,
						Values: [][]byte{[]byte("/product"), []byte("/sales")},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(2),
		},
		{
			name: "int equal",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_EQ,
						Values: [][]byte{convert.Int64ToBytes(500)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(12),
		},
		{
			name: "int not equal",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_NE,
						Values: [][]byte{convert.Int64ToBytes(500)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(11, 13, 14),
		},
		{
			name: "int having",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_HAVING,
						Values: [][]byte{convert.Int64ToBytes(500), convert.Int64ToBytes(50)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(11, 12),
		},
		{
			name: "int not having",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_NOT_HAVING,
						Values: [][]byte{convert.Int64ToBytes(500), convert.Int64ToBytes(50)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(13, 14),
		},
		{
			name: "int in range",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_GT,
						Values: [][]byte{convert.Int64ToBytes(50)},
					},
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_LT,
						Values: [][]byte{convert.Int64ToBytes(5000)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(13, 12),
		},
		{
			name: "int includes edges",
			args: args{
				indexObjectName: "duration",
				conditions: []Condition{
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_GE,
						Values: [][]byte{convert.Int64ToBytes(50)},
					},
					{
						Key:    "duration",
						Op:     apiv1.PairQuery_BINARY_OP_LE,
						Values: [][]byte{convert.Int64ToBytes(5000)},
					},
				},
			},
			want: roaring.NewPostingListWithInitialData(13, 12, 11, 14),
		},
	}
	s := setUpModules(tester)
	setupData(tester, s)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.Search(*common.NewMetadataByNameAndGroup("sw", "default"), 0, 0, math.MaxInt64, tt.args.indexObjectName, tt.args.conditions)
			if (err != nil) != tt.wantErr {
				t.Errorf("Search() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("Search() got = %v, want %v", got.ToSlice(), tt.want.ToSlice())
			}
		})
	}
}

func setupData(tester *assert.Assertions, s *service) {
	fields := []*Field{
		{
			ChunkID: common.ChunkID(1),
			Name:    "endpoint",
			Value:   []byte("/product"),
		},
		{
			ChunkID: common.ChunkID(2),
			Name:    "endpoint",
			Value:   []byte("/home"),
		},
		{
			ChunkID: common.ChunkID(3),
			Name:    "endpoint",
			Value:   []byte("/sales"),
		},
		{
			ChunkID: common.ChunkID(11),
			Name:    "duration",
			Value:   convert.Int64ToBytes(50),
		},
		{
			ChunkID: common.ChunkID(12),
			Name:    "duration",
			Value:   convert.Int64ToBytes(500),
		},
		{
			ChunkID: common.ChunkID(13),
			Name:    "duration",
			Value:   convert.Int64ToBytes(100),
		},
		{
			ChunkID: common.ChunkID(14),
			Name:    "duration",
			Value:   convert.Int64ToBytes(5000),
		},
	}
	for _, field := range fields {
		if err := s.Insert(*common.NewMetadataByNameAndGroup("sw", "default"), 0, field); err != nil {
			tester.NoError(err)
		}
	}
}
