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

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/stretchr/testify/assert"
)

func Test_columnMetadata_reset(t *testing.T) {
	cm := &columnMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
	}

	cm.reset()

	assert.Equal(t, "", cm.name)
	assert.Equal(t, pbv1.ValueType(0), cm.valueType)
	assert.Equal(t, dataBlock{}, cm.dataBlock)
}

func Test_columnMetadata_copyFrom(t *testing.T) {
	src := &columnMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
	}

	dest := &columnMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)
	assert.Equal(t, src, dest)
}

func Test_columnMetadata_marshal(t *testing.T) {
	original := &columnMetadata{
		name:      "test",
		valueType: pbv1.ValueTypeStr,
		dataBlock: dataBlock{offset: 1, size: 10},
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

	cfm.reset()

	assert.Equal(t, 0, len(cfm.columnMetadata))
}

func Test_columnFamilyMetadata_copyFrom(t *testing.T) {
	src := &columnFamilyMetadata{
		columnMetadata: []columnMetadata{
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
	original := &columnFamilyMetadata{
		columnMetadata: []columnMetadata{
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

	marshaled := original.marshal(nil)
	assert.NotNil(t, marshaled)

	unmarshaled := &columnFamilyMetadata{}
	_, err := unmarshaled.unmarshal(marshaled)
	assert.Nil(t, err)

	assert.Equal(t, original, unmarshaled)
}
