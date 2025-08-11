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
	"fmt"
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

func TestColumn_HighCardinalityStringEncoding(t *testing.T) {
	tests := []struct {
		name            string
		description     string
		expectedEncType encoding.EncodeType
		uniqueCount     int
		totalCount      int
	}{
		{
			name:            "exactly 256 unique values - should use dictionary",
			description:     "Dictionary encoding should be used when exactly at the threshold",
			expectedEncType: encoding.EncodeTypeDictionary,
			uniqueCount:     256,
			totalCount:      256,
		},
		{
			name:            "257 unique values - should use plain encoding",
			description:     "Plain encoding should be used when exceeding dictionary threshold",
			expectedEncType: encoding.EncodeTypePlain,
			uniqueCount:     257,
			totalCount:      257,
		},
		{
			name:            "300 unique values - should use plain encoding",
			description:     "Plain encoding should be used for high cardinality strings",
			expectedEncType: encoding.EncodeTypePlain,
			uniqueCount:     300,
			totalCount:      300,
		},
		{
			name:            "1000 unique values - should use plain encoding",
			description:     "Plain encoding should be used for very high cardinality",
			expectedEncType: encoding.EncodeTypePlain,
			uniqueCount:     1000,
			totalCount:      1000,
		},
		{
			name:            "500 total with 200 unique - should use dictionary",
			description:     "Dictionary should be used when unique count is below threshold despite high total count",
			expectedEncType: encoding.EncodeTypeDictionary,
			uniqueCount:     200,
			totalCount:      500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Generate unique string values
			values := make([][]byte, tt.totalCount)

			// Create unique values up to uniqueCount
			for i := 0; i < tt.uniqueCount; i++ {
				values[i] = []byte(fmt.Sprintf("unique_value_%06d", i))
			}

			// If totalCount > uniqueCount, repeat some values to reach totalCount
			for i := tt.uniqueCount; i < tt.totalCount; i++ {
				// Repeat values cyclically
				repeatIndex := i % tt.uniqueCount
				values[i] = []byte(fmt.Sprintf("unique_value_%06d", repeatIndex))
			}

			testColumn := &column{
				name:      "high_cardinality_column",
				valueType: pbv1.ValueTypeStr,
				values:    values,
			}

			// Encode the column
			cm := &columnMetadata{}
			buf := &bytes.Buffer{}
			w := &writer{}
			w.init(buf)

			testColumn.mustWriteTo(cm, w)

			// Verify basic metadata
			assert.Equal(t, w.bytesWritten, cm.size)
			assert.Equal(t, uint64(len(buf.Buf)), cm.size)
			assert.Equal(t, uint64(0), cm.offset)
			assert.Equal(t, testColumn.name, cm.name)
			assert.Equal(t, testColumn.valueType, cm.valueType)

			// Check encoding type by examining the first byte of the encoded data
			assert.True(t, len(buf.Buf) > 0, "Encoded buffer should not be empty")
			actualEncType := encoding.EncodeType(buf.Buf[0])
			assert.Equal(t, tt.expectedEncType, actualEncType,
				"Expected %s encoding (%d), got %d. %s",
				getColumnEncodeTypeName(tt.expectedEncType), tt.expectedEncType, actualEncType, tt.description)

			// Test roundtrip: decode and verify all values are preserved
			decoder := &encoding.BytesBlockDecoder{}
			unmarshaled := &column{}
			unmarshaled.mustReadValues(decoder, buf, *cm, uint64(len(testColumn.values)))

			assert.Equal(t, testColumn.name, unmarshaled.name)
			assert.Equal(t, testColumn.valueType, unmarshaled.valueType)
			assert.Equal(t, len(testColumn.values), len(unmarshaled.values), "Number of values should match")

			// Verify all values are correctly decoded
			for i, originalValue := range testColumn.values {
				assert.Equal(t, originalValue, unmarshaled.values[i],
					"Value at index %d should match original", i)
			}
		})
	}
}

// Helper function to get encode type name for better test output.
func getColumnEncodeTypeName(encType encoding.EncodeType) string {
	switch encType {
	case encoding.EncodeTypePlain:
		return "Plain"
	case encoding.EncodeTypeDictionary:
		return "Dictionary"
	default:
		return fmt.Sprintf("Unknown(%d)", encType)
	}
}
