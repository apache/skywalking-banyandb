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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/posting"
	"github.com/apache/skywalking-banyandb/pkg/posting/roaring"
)

func TestMemTable_Initialize(t *testing.T) {
	type args struct {
		fields []FieldSpec
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "golden path",
			args: args{
				fields: []FieldSpec{
					{
						Name: "service_name",
					},
					{
						Name: "duration",
					},
				},
			},
		},
		{
			name:    "fields absent",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMemTable("sw", "group")
			var err error
			if err = m.Initialize(tt.args.fields); (err != nil) != tt.wantErr {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			assert.Equal(t, len(m.terms.repo), len(tt.args.fields))
		})
	}
}

func TestMemTable_Range(t *testing.T) {
	type args struct {
		fieldName []byte
		opts      *RangeOpts
	}
	m := NewMemTable("sw", "group")
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
				opts: &RangeOpts{
					Lower: convert.Uint16ToBytes(100),
					Upper: convert.Uint16ToBytes(500),
				},
			},
			wantList: m.MatchTerms(&Field{
				name:  []byte("duration"),
				value: convert.Uint16ToBytes(200),
			}),
		},
		{
			name: "excludes edge",
			args: args{
				fieldName: []byte("duration"),
				opts: &RangeOpts{
					Lower: convert.Uint16ToBytes(50),
					Upper: convert.Uint16ToBytes(1000),
				},
			},
			wantList: union(m,
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(200),
				},
			),
		},
		{
			name: "includes lower",
			args: args{
				fieldName: []byte("duration"),
				opts: &RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesLower: true,
				},
			},
			wantList: union(m,
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(50),
				},
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(200),
				},
			),
		},
		{
			name: "includes upper",
			args: args{
				fieldName: []byte("duration"),
				opts: &RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesUpper: true,
				},
			},
			wantList: union(m,
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(200),
				},
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(1000),
				},
			),
		},
		{
			name: "includes edges",
			args: args{
				fieldName: []byte("duration"),
				opts: &RangeOpts{
					Lower:         convert.Uint16ToBytes(50),
					Upper:         convert.Uint16ToBytes(1000),
					IncludesUpper: true,
					IncludesLower: true,
				},
			},
			wantList: union(m,
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(50),
				},
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(200),
				},
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(1000),
				},
			),
		},
		{
			name: "match one",
			args: args{
				fieldName: []byte("duration"),
				opts: &RangeOpts{
					Lower:         convert.Uint16ToBytes(200),
					Upper:         convert.Uint16ToBytes(200),
					IncludesUpper: true,
					IncludesLower: true,
				},
			},
			wantList: union(m,
				&Field{
					name:  []byte("duration"),
					value: convert.Uint16ToBytes(200),
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

func union(memTable *MemTable, fields ...*Field) posting.List {
	result := roaring.NewPostingList()
	for _, f := range fields {
		_ = result.Union(memTable.MatchTerms(f))
	}
	return result
}

func setUp(t *testing.T, mt *MemTable) {
	assert.NoError(t, mt.Initialize([]FieldSpec{
		{
			Name: "service_name",
		},
		{
			Name: "duration",
		},
	}))
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			assert.NoError(t, mt.Insert(&Field{
				name:  []byte("service_name"),
				value: []byte("gateway"),
			}, common.ChunkID(i)))
		} else {
			assert.NoError(t, mt.Insert(&Field{
				name:  []byte("service_name"),
				value: []byte("webpage"),
			}, common.ChunkID(i)))
		}
	}
	for i := 100; i < 200; i++ {
		switch {
		case i%3 == 0:
			assert.NoError(t, mt.Insert(&Field{
				name:  []byte("duration"),
				value: convert.Uint16ToBytes(50),
			}, common.ChunkID(i)))
		case i%3 == 1:
			assert.NoError(t, mt.Insert(&Field{
				name:  []byte("duration"),
				value: convert.Uint16ToBytes(200),
			}, common.ChunkID(i)))
		case i%3 == 2:
			assert.NoError(t, mt.Insert(&Field{
				name:  []byte("duration"),
				value: convert.Uint16ToBytes(1000),
			}, common.ChunkID(i)))
		}
	}
}
