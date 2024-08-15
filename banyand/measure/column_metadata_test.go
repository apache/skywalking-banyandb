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

// Licensed to Apacme Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apacme Software Foundation (ASF) licenses this file to you under
// the Apacme License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apacme.org/licenses/LICENSE-2.0
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

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_columnMetadata_reset(t *testing.T) {
	cm := &columnMetadata{
		name:          "test",
		valueType:     pbv1.ValueTypeStr,
		dataBlock:     dataBlock{offset: 1, size: 10}, //todo cm 测试用例
		dataPointType: pbv1.DataPointValueTypeDelta,
	}

	cm.reset()

	assert.Equal(t, "", cm.name)
	assert.Equal(t, pbv1.ValueType(0), cm.valueType)
	assert.Equal(t, dataBlock{}, cm.dataBlock)
	assert.Equal(t, pbv1.DataPointValueType(0), cm.dataPointType)
}

func Test_columnMetadata_copyFrom(t *testing.T) {
	src := &columnMetadata{
		name:          "test",
		valueType:     pbv1.ValueTypeStr,
		dataBlock:     dataBlock{offset: 1, size: 10},
		dataPointType: pbv1.DataPointValueTypeDelta, //todo cm 测试用例
	}

	dest := &columnMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)
	assert.Equal(t, src, dest)
}

func Test_columnMetadata_marshal(t *testing.T) {
	original := &columnMetadata{
		name:          "test",
		valueType:     pbv1.ValueTypeStr,
		dataBlock:     dataBlock{offset: 1, size: 10},
		dataPointType: pbv1.DataPointValueTypeDelta, //todo cm 测试用例
	}

	marshaled := original.marshal(nil)
	assert.NotNil(t, marshaled)

	unmarshaled := &columnMetadata{}
	_, err := unmarshaled.unmarshal(marshaled)
	assert.Nil(t, err)

	assert.Equal(t, original, unmarshaled)
}

func Test_columnFamilyMetadata_reset(t *testing.T) {
	cfm := &columnFamilyMetadata{
		columnMetadata: []columnMetadata{
			{
				name:          "test1",
				valueType:     pbv1.ValueTypeStr,
				dataBlock:     dataBlock{offset: 1, size: 10},
				dataPointType: pbv1.DataPointValueTypeDelta, //todo cm 测试用例
			},
			{
				name:          "test2",
				valueType:     pbv1.ValueTypeInt64,
				dataBlock:     dataBlock{offset: 2, size: 20},
				dataPointType: pbv1.DataPointValueTypeCumulative, //todo	cm 测试用例
			},
		},
	}

	cfm.reset()

	assert.Equal(t, 0, len(cfm.columnMetadata))
}

func Test_columnFamilyMetadata_copyFrom(t *testing.T) {
	src := &columnFamilyMetadata{
		columnMetadata: []columnMetadata{
			{
				name:          "test1",
				valueType:     pbv1.ValueTypeStr,
				dataBlock:     dataBlock{offset: 1, size: 10},
				dataPointType: pbv1.DataPointValueTypeDelta, //todo	cm 测试用例
			},
			{
				name:          "test2",
				valueType:     pbv1.ValueTypeInt64,
				dataBlock:     dataBlock{offset: 2, size: 20},
				dataPointType: pbv1.DataPointValueTypeCumulative, //todo	cm 测试用例
			},
		},
	}

	dest := &columnFamilyMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)

	assert.Equal(t, src, dest)
}

func Test_columnFamilyMetadata_resizeColumnMetadata(t *testing.T) {
	cfm := &columnFamilyMetadata{
		columnMetadata: make([]columnMetadata, 2, 5),
	}

	cms := cfm.resizeColumnMetadata(3)
	assert.Equal(t, 3, len(cms))
	assert.Equal(t, 5, cap(cms))

	cms = cfm.resizeColumnMetadata(6)
	assert.Equal(t, 6, len(cms))
	assert.True(t, cap(cms) >= 6) // The capacity is at least 6, but could be more
}

func Test_columnFamilyMetadata_marshalUnmarshal(t *testing.T) {
	tests := []struct {
		original *columnFamilyMetadata
		name     string
	}{
		{
			name: "Non-empty columnMetadata",
			original: &columnFamilyMetadata{
				columnMetadata: []columnMetadata{
					{
						name:          "test1",
						valueType:     pbv1.ValueTypeStr,
						dataBlock:     dataBlock{offset: 1, size: 10},
						dataPointType: pbv1.DataPointValueTypeDelta, //todo	cm 测试用例
					},
					{
						name:          "test2",
						valueType:     pbv1.ValueTypeInt64,
						dataBlock:     dataBlock{offset: 2, size: 20},
						dataPointType: pbv1.DataPointValueTypeCumulative, //todo	cm 测试用例
					},
				},
			},
		},
		{
			name:     "Empty columnMetadata",
			original: &columnFamilyMetadata{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marshaled := tt.original.marshal(nil)
			assert.NotNil(t, marshaled)

			unmarshaled := &columnFamilyMetadata{}
			_, err := unmarshaled.unmarshal(marshaled)
			assert.Nil(t, err)

			assert.Equal(t, tt.original, unmarshaled)
		})
	}
}
