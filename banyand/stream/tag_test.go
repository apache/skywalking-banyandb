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

package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestTag_reset(t *testing.T) {
	tt := &tag{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		values:    [][]byte{[]byte("value1"), []byte("value2")},
	}

	tt.reset()

	assert.Equal(t, "", tt.name)
	assert.Equal(t, 0, len(tt.values))
}

func TestTag_resizeValues(t *testing.T) {
	tt := &tag{
		values: make([][]byte, 2, 5),
	}

	values := tt.resizeValues(3)
	assert.Equal(t, 3, len(values))
	assert.Equal(t, 5, cap(values))

	values = tt.resizeValues(6)
	assert.Equal(t, 6, len(values))
	assert.True(t, cap(values) >= 6) // The capacity is at least 6, but could be more
}

func TestTag_mustWriteTo_mustReadValues(t *testing.T) {
	tests := []struct {
		tag  *tag
		name string
	}{
		{
			name: "string with nils",
			tag: &tag{
				name:      "test",
				valueType: pbv1.ValueTypeStr,
				values:    [][]byte{[]byte("value1"), nil, []byte("value2"), nil},
			},
		},
		{
			name: "int64 with null",
			tag: &tag{
				name:      "test",
				valueType: pbv1.ValueTypeInt64,
				values:    [][]byte{[]byte("null"), nil, []byte("null"), nil},
			},
		},
		{
			name: "valid int64 values",
			tag: &tag{
				name:      "test",
				valueType: pbv1.ValueTypeInt64,
				values: [][]byte{
					convert.Int64ToBytes(1),
					convert.Int64ToBytes(2),
					convert.Int64ToBytes(4),
					convert.Int64ToBytes(5),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := &tagMetadata{}
			buf := &bytes.Buffer{}
			w := &writer{}
			w.init(buf)

			tt.tag.mustWriteTo(tm, w)
			assert.Equal(t, w.bytesWritten, tm.size)
			assert.Equal(t, uint64(len(buf.Buf)), tm.size)
			assert.Equal(t, uint64(0), tm.offset)
			assert.Equal(t, tt.tag.name, tm.name)
			// TODO assert.Equal(t, tt.tag.valueType, tm.valueType)

			decoder := &encoding.BytesBlockDecoder{}
			unmarshaled := &tag{}
			unmarshaled.mustReadValues(decoder, buf, *tm, uint64(len(tt.tag.values)))

			assert.Equal(t, tt.tag.name, unmarshaled.name)
			// TODO assert.Equal(t, tt.tag.valueType, unmarshaled.valueType)
			assert.Equal(t, tt.tag.values, unmarshaled.values)
		})
	}
}

func TestTagFamily_reset(t *testing.T) {
	tf := &tagFamily{
		name: "test",
		tags: []tag{
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

	tf.reset()

	assert.Equal(t, "", tf.name)
	assert.Equal(t, 0, len(tf.tags))
}

func TestTagFamily_resizeTags(t *testing.T) {
	tf := &tagFamily{
		tags: make([]tag, 2, 5),
	}

	tags := tf.resizeTags(3)
	assert.Equal(t, 3, len(tags))
	assert.Equal(t, 5, cap(tags))

	tags = tf.resizeTags(6)
	assert.Equal(t, 6, len(tags))
	assert.True(t, cap(tags) >= 6) // The capacity is at least 6, but could be more
}
