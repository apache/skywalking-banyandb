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

package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_tagMetadata_reset(t *testing.T) {
	tm := &tagMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
	}

	tm.reset()

	assert.Equal(t, "", tm.name)
	assert.Equal(t, pbv1.ValueType(0), tm.valueType)
	assert.Equal(t, dataBlock{}, tm.dataBlock)
}

func Test_tagMetadata_copyFrom(t *testing.T) {
	src := &tagMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
	}

	dest := &tagMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)
	assert.Equal(t, src, dest)
}

func Test_tagMetadata_marshal(t *testing.T) {
	original := &tagMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
	}

	marshaled := original.marshal(nil)
	assert.NotNil(t, marshaled)

	unmarshaled := &tagMetadata{}
	_, err := unmarshaled.unmarshal(marshaled)
	assert.Nil(t, err)

	assert.Equal(t, original, unmarshaled)
}

func Test_tagFamilyMetadata_reset(t *testing.T) {
	tfm := &tagFamilyMetadata{
		tagMetadata: []tagMetadata{
			{
				name:      "test1",
				valueType: pbv1.ValueTypeStr,
				dataBlock: dataBlock{offset: 1, size: 10},
			},
			{
				name:      "test2",
				valueType: pbv1.ValueTypeInt64,
				dataBlock: dataBlock{offset: 2, size: 20},
			},
		},
	}

	tfm.reset()

	assert.Equal(t, 0, len(tfm.tagMetadata))
}

func Test_tagFamilyMetadata_copyFrom(t *testing.T) {
	src := &tagFamilyMetadata{
		tagMetadata: []tagMetadata{
			{
				name:      "test1",
				valueType: pbv1.ValueTypeStr,
				dataBlock: dataBlock{offset: 1, size: 10},
			},
			{
				name:      "test2",
				valueType: pbv1.ValueTypeInt64,
				dataBlock: dataBlock{offset: 2, size: 20},
			},
		},
	}

	dest := &tagFamilyMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)

	assert.Equal(t, src, dest)
}

func Test_tagFamilyMetadata_resizeTagMetadata(t *testing.T) {
	tfm := &tagFamilyMetadata{
		tagMetadata: make([]tagMetadata, 2, 5),
	}

	tms := tfm.resizeTagMetadata(3)
	assert.Equal(t, 3, len(tms))
	assert.Equal(t, 5, cap(tms))

	tms = tfm.resizeTagMetadata(6)
	assert.Equal(t, 6, len(tms))
	assert.True(t, cap(tms) >= 6) // The capacity is at least 6, but could be more
}

func Test_tagFamilyMetadata_marshalUnmarshal(t *testing.T) {
	tests := []struct {
		original *tagFamilyMetadata
		name     string
	}{
		{
			name: "Non-empty tagMetadata",
			original: &tagFamilyMetadata{
				tagMetadata: []tagMetadata{
					{
						name:      "test1",
						valueType: pbv1.ValueTypeStr,
						dataBlock: dataBlock{offset: 1, size: 10},
					},
					{
						name:      "test2",
						valueType: pbv1.ValueTypeInt64,
						dataBlock: dataBlock{offset: 2, size: 20},
					},
				},
			},
		},
		{
			name:     "Empty tagMetadata",
			original: &tagFamilyMetadata{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled := tt.original.marshal(nil)
			assert.NotNil(t, marshaled)

			unmarshaled := &tagFamilyMetadata{}
			_, err := unmarshaled.unmarshal(marshaled)
			assert.Nil(t, err)

			assert.Equal(t, tt.original, unmarshaled)
		})
	}
}
