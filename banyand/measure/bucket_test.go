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

package measure

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func createTestBlockCursor() *blockCursor {
	bc := &blockCursor{
		timestamps: []int64{100, 200, 300},
		versions:   []int64{1, 1, 1},
		fields: columnFamily{
			name: "",
			columns: []column{
				{
					name:      "int_field",
					valueType: pbv1.ValueTypeInt64,
					values:    [][]byte{convert.Int64ToBytes(10), convert.Int64ToBytes(20), convert.Int64ToBytes(30)},
				},
				{
					name:      "str_field",
					valueType: pbv1.ValueTypeStr,
					values:    [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")},
				},
			},
		},
	}
	return bc
}

func createAnotherTestBlockCursor() *blockCursor {
	bc := &blockCursor{
		timestamps: []int64{400, 500, 600},
		versions:   []int64{1, 1, 1},
		fields: columnFamily{
			name: "",
			columns: []column{
				{
					name:      "int_field",
					valueType: pbv1.ValueTypeInt64,
					values:    [][]byte{convert.Int64ToBytes(40), convert.Int64ToBytes(50), convert.Int64ToBytes(60)},
				},
				{
					name:      "str_field",
					valueType: pbv1.ValueTypeStr,
					values:    [][]byte{[]byte("value4"), []byte("value5"), []byte("value6")},
				},
			},
		},
	}
	return bc
}

func TestWriteAndReadBucket(t *testing.T) {
	wanted := []*block{
		{
			timestamps: []int64{200, 300},
			versions:   []int64{1, 1},
			field: columnFamily{
				name: "",
				columns: []column{
					{
						name:      "int_field",
						valueType: pbv1.ValueTypeInt64,
						values:    [][]byte{convert.Int64ToBytes(20), convert.Int64ToBytes(30)},
					},
					{
						name:      "str_field",
						valueType: pbv1.ValueTypeStr,
						values:    [][]byte{[]byte("value2"), []byte("value3")},
					},
				},
			},
		},
		{
			timestamps: []int64{200, 300, 400, 500, 600},
			versions:   []int64{1, 1, 1, 1, 1},
			field: columnFamily{
				name: "",
				columns: []column{
					{
						name:      "int_field",
						valueType: pbv1.ValueTypeInt64,
						values:    [][]byte{convert.Int64ToBytes(20), convert.Int64ToBytes(30), convert.Int64ToBytes(40), convert.Int64ToBytes(50), convert.Int64ToBytes(60)},
					},
					{
						name:      "str_field",
						valueType: pbv1.ValueTypeStr,
						values:    [][]byte{[]byte("value2"), []byte("value3"), []byte("value4"), []byte("value5"), []byte("value6")},
					},
				},
			},
		},
	}

	seriesID := common.SeriesID(1)
	b := newBucket(seriesID)

	bc1 := createTestBlockCursor()
	b.addRange(bc1, 0)
	err := b.merge()
	assert.NoError(t, err)
	bl, err := b.findRange(200, 500)
	assert.NoError(t, err)
	if !cmp.Equal(bl.timestamps, wanted[0].timestamps) {
		t.Error("Unexpected timestamps")
	}
	if !cmp.Equal(bl.versions, wanted[0].versions) {
		t.Error("Unexpected versions")
	}
	for j := 0; j < len(wanted[0].field.columns); j++ {
		if bl.field.columns[j].name != wanted[0].field.columns[j].name {
			t.Error("Unexpected field name")
		}
		if !cmp.Equal(bl.field.columns[j].values, wanted[0].field.columns[j].values) {
			t.Error("Unexpected field values")
		}
	}

	bc2 := createAnotherTestBlockCursor()
	b.addRange(bc2, 0)
	err = b.merge()
	assert.NoError(t, err)
	bl, err = b.findRange(200, 600)
	assert.NoError(t, err)
	if !cmp.Equal(bl.timestamps, wanted[1].timestamps) {
		t.Error("Unexpected timestamps")
	}
	if !cmp.Equal(bl.versions, wanted[1].versions) {
		t.Error("Unexpected versions")
	}
	for j := 0; j < len(wanted[1].field.columns); j++ {
		if bl.field.columns[j].name != wanted[1].field.columns[j].name {
			t.Error("Unexpected field name")
		}
		if !cmp.Equal(bl.field.columns[j].values, wanted[1].field.columns[j].values) {
			t.Error("Unexpected field values")
		}
	}
}
