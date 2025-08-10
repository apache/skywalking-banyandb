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

package trace

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func Test_tagMetadata_reset(t *testing.T) {
	tm := &tagMetadata{
		name:        "test",
		valueType:   pbv1.ValueTypeStr,
		dataBlock:   dataBlock{offset: 1, size: 10},
		filterBlock: dataBlock{offset: 2, size: 20},
		min:         []byte{1, 2, 3},
		max:         []byte{4, 5, 6},
	}

	tm.reset()

	assert.Equal(t, "", tm.name)
	assert.Equal(t, pbv1.ValueType(0), tm.valueType)
	assert.Equal(t, dataBlock{}, tm.dataBlock)
	assert.Equal(t, dataBlock{}, tm.filterBlock)
	assert.Nil(t, tm.min)
	assert.Nil(t, tm.max)
}

func Test_tagMetadata_copyFrom(t *testing.T) {
	src := &tagMetadata{
		name:        "test",
		valueType:   pbv1.ValueTypeStr,
		dataBlock:   dataBlock{offset: 1, size: 10},
		filterBlock: dataBlock{offset: 2, size: 20},
		min:         []byte{1, 2, 3},
		max:         []byte{4, 5, 6},
	}

	dest := &tagMetadata{}
	assert.NotEqual(t, src, dest)

	dest.copyFrom(src)
	assert.Equal(t, src, dest)
}

func Test_tagMetadata_marshal(t *testing.T) {
	original := &tagMetadata{
		name:        "test",
		valueType:   pbv1.ValueTypeStr,
		dataBlock:   dataBlock{offset: 1, size: 10},
		filterBlock: dataBlock{offset: 2, size: 20},
		min:         []byte{1, 2, 3},
		max:         []byte{4, 5, 6},
	}
	wanted := &tagMetadata{
		dataBlock:   dataBlock{offset: 1, size: 10},
		filterBlock: dataBlock{offset: 2, size: 20},
		min:         []byte{1, 2, 3},
		max:         []byte{4, 5, 6},
	}

	marshaled := original.marshal(nil)
	assert.NotNil(t, marshaled)

	unmarshaled := &tagMetadata{}
	err := unmarshaled.unmarshal(marshaled)
	assert.Nil(t, err)

	assert.Equal(t, wanted, unmarshaled)
}
