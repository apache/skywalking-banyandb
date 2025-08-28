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

package trace

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/banyand/internal/encoding"
	pkgbytes "github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pkgencoding "github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func timeToBytes(t time.Time) []byte {
	return pkgencoding.Int64ToBytes(nil, t.UnixNano())
}

func TestTagEncodingDecoding(t *testing.T) {
	t.Run("test int64 tag encoding/decoding", func(t *testing.T) {
		tag := &tag{
			name:      "test_int64",
			valueType: pbv1.ValueTypeInt64,
			values: [][]byte{
				convert.Int64ToBytes(100),
				convert.Int64ToBytes(200),
				convert.Int64ToBytes(300),
			},
		}

		// Test encoding
		bb := &pkgbytes.Buffer{}
		err := encoding.EncodeTagValues(bb, tag.values, tag.valueType)
		assert.NoError(t, err)
		assert.NotNil(t, bb.Buf)
		assert.Greater(t, len(bb.Buf), 0)

		// Test decoding
		decoder := &pkgencoding.BytesBlockDecoder{}
		decodedValues, err := encoding.DecodeTagValues(nil, decoder, bb, tag.valueType, len(tag.values))
		assert.NoError(t, err)
		assert.Equal(t, len(tag.values), len(decodedValues))
		assert.Equal(t, tag.values, decodedValues)
	})

	t.Run("test float64 tag encoding/decoding", func(t *testing.T) {
		tag := &tag{
			name:      "test_float64",
			valueType: pbv1.ValueTypeFloat64,
			values: [][]byte{
				convert.Float64ToBytes(1.5),
				convert.Float64ToBytes(2.5),
				convert.Float64ToBytes(3.5),
			},
		}

		// Test encoding
		bb := &pkgbytes.Buffer{}
		err := encoding.EncodeTagValues(bb, tag.values, tag.valueType)
		assert.NoError(t, err)
		assert.NotNil(t, bb.Buf)
		assert.Greater(t, len(bb.Buf), 0)

		// Test decoding
		decoder := &pkgencoding.BytesBlockDecoder{}
		decodedValues, err := encoding.DecodeTagValues(nil, decoder, bb, tag.valueType, len(tag.values))
		assert.NoError(t, err)
		assert.Equal(t, len(tag.values), len(decodedValues))
		assert.Equal(t, tag.values, decodedValues)
	})

	t.Run("test string tag encoding/decoding", func(t *testing.T) {
		tag := &tag{
			name:      "test_string",
			valueType: pbv1.ValueTypeStr,
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
		}

		// Test encoding
		bb := &pkgbytes.Buffer{}
		err := encoding.EncodeTagValues(bb, tag.values, tag.valueType)
		assert.NoError(t, err)
		assert.NotNil(t, bb.Buf)
		assert.Greater(t, len(bb.Buf), 0)

		// Test decoding
		decoder := &pkgencoding.BytesBlockDecoder{}
		decodedValues, err := encoding.DecodeTagValues(nil, decoder, bb, tag.valueType, len(tag.values))
		assert.NoError(t, err)
		assert.Equal(t, len(tag.values), len(decodedValues))
		assert.Equal(t, tag.values, decodedValues)
	})

	t.Run("test timestamp tag encoding/decoding", func(t *testing.T) {
		// Create test timestamps
		time1 := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
		time2 := time.Date(2023, 1, 2, 12, 0, 0, 0, time.UTC)
		time3 := time.Date(2023, 1, 3, 12, 0, 0, 0, time.UTC)

		tag := &tag{
			name:      "test_timestamp",
			valueType: pbv1.ValueTypeTimestamp,
			values: [][]byte{
				timeToBytes(time1),
				timeToBytes(time2),
				timeToBytes(time3),
			},
		}

		// Test encoding
		bb := &pkgbytes.Buffer{}
		err := encoding.EncodeTagValues(bb, tag.values, tag.valueType)
		assert.NoError(t, err)
		assert.NotNil(t, bb.Buf)
		assert.Greater(t, len(bb.Buf), 0)

		// Test decoding
		decoder := &pkgencoding.BytesBlockDecoder{}
		decodedValues, err := encoding.DecodeTagValues(nil, decoder, bb, tag.valueType, len(tag.values))
		assert.NoError(t, err)
		assert.Equal(t, len(tag.values), len(decodedValues))
		assert.Equal(t, tag.values, decodedValues)

		// Verify the decoded timestamp values can be converted back to time.Time
		for i, decodedValue := range decodedValues {
			expectedTime := []time.Time{time1, time2, time3}[i]
			decodedNanos := pkgencoding.BytesToInt64(decodedValue)
			decodedTime := time.Unix(0, decodedNanos)
			assert.Equal(t, expectedTime.Unix(), decodedTime.Unix())
			// Note: We compare Unix time (seconds) since nanoseconds might have precision differences
		}
	})

	t.Run("test empty values", func(t *testing.T) {
		tag := &tag{
			name:      "test_empty",
			valueType: pbv1.ValueTypeStr,
			values:    [][]byte{},
		}

		// Test encoding
		bb := &pkgbytes.Buffer{}
		err := encoding.EncodeTagValues(bb, tag.values, tag.valueType)
		assert.NoError(t, err)
		assert.Nil(t, bb.Buf)

		// Test decoding
		decoder := &pkgencoding.BytesBlockDecoder{}
		decodedValues, err := encoding.DecodeTagValues(nil, decoder, bb, tag.valueType, 0)
		assert.NoError(t, err)
		assert.Nil(t, decodedValues)
	})
}
