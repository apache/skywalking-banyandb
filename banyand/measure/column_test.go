// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestColumn_reset(t *testing.T) {
	c := &column{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		values:    [][]byte{[]byte("value1"), []byte("value2")},
	}

	c.reset()

	assert.Equal(t, "", c.name)
	assert.Equal(t, 0, len(c.values))
}

func TestColumn_resizeValues(t *testing.T) {
	c := &column{
		values: make([][]byte, 2, 5),
	}

	values := c.resizeValues(3)
	assert.Equal(t, 3, len(values))
	assert.Equal(t, 5, cap(values))

	values = c.resizeValues(6)
	assert.Equal(t, 6, len(values))
	assert.True(t, cap(values) >= 6) // The capacity is at least 6, but could be more
}

func TestColumn_mustWriteTo_mustReadValues(t *testing.T) {
	tests := []struct {
		name      string
		values    [][]byte
		valueType pbv1.ValueType
	}{
		{
			name:      "string values with nils",
			valueType: pbv1.ValueTypeStr,
			values:    [][]byte{[]byte("value1"), nil, []byte("value2"), nil},
		},
		{
			name:      "int64 values as 'null'",
			valueType: pbv1.ValueTypeInt64,
			values:    [][]byte{[]byte("null"), nil, []byte("null"), nil},
		},
		{
			name:      "actual int64 values",
			valueType: pbv1.ValueTypeInt64,
			values: [][]byte{
				convert.Int64ToBytes(1),
				convert.Int64ToBytes(2),
				convert.Int64ToBytes(4),
				convert.Int64ToBytes(5),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &column{
				name:      "test",
				valueType: tt.valueType,
				values:    tt.values,
			}

			cm := &columnMetadata{}
			buf := &bytes.Buffer{}
			w := &writer{}
			w.init(buf)
			original.mustWriteTo(cm, w)

			assert.Equal(t, w.bytesWritten, cm.size)
			assert.Equal(t, uint64(len(buf.Buf)), cm.size)
			assert.Equal(t, uint64(0), cm.offset)
			assert.Equal(t, original.name, cm.name)
			assert.Equal(t, original.valueType, cm.valueType)

			decoder := &encoding.BytesBlockDecoder{}
			unmarshaled := &column{}
			unmarshaled.mustReadValues(decoder, buf, *cm, uint64(len(original.values)))

			assert.Equal(t, original.name, unmarshaled.name)
			assert.Equal(t, original.valueType, unmarshaled.valueType)
			assert.Equal(t, original.values, unmarshaled.values)
		})
	}
}

func TestColumnFamily_reset(t *testing.T) {
	cf := &columnFamily{
		name: "test",
		columns: []column{
			{
				name:      "test1",
				valueType: pbv1.ValueTypeStr,
				values:    [][]byte{[]byte("value1"), []byte("value2")},
			},
			{
				name:      "test2",
				valueType: pbv1.ValueTypeInt64,
				values:    [][]byte{[]byte("value3"), []byte("value4")},
			},
		},
	}

	cf.reset()

	assert.Equal(t, "", cf.name)
	assert.Equal(t, 0, len(cf.columns))
}

func TestColumnFamily_resizeColumns(t *testing.T) {
	cf := &columnFamily{
		columns: make([]column, 2, 5),
	}

	columns := cf.resizeColumns(3)
	assert.Equal(t, 3, len(columns))
	assert.Equal(t, 5, cap(columns))

	columns = cf.resizeColumns(6)
	assert.Equal(t, 6, len(columns))
	assert.True(t, cap(columns) >= 6) // The capacity is at least 6, but could be more
}
