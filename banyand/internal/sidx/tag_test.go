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

package sidx

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

func TestTagExportedFields(t *testing.T) {
	// Test that Tag can be created and used outside the package
	tag := Tag{
		Name:      "test-tag",
		Value:     []byte("test-value"),
		ValueType: pbv1.ValueTypeStr,
		Indexed:   true,
	}

	// Test that exported fields are accessible
	assert.Equal(t, "test-tag", tag.Name)
	assert.Equal(t, []byte("test-value"), tag.Value)
	assert.Equal(t, pbv1.ValueTypeStr, tag.ValueType)
	assert.Equal(t, true, tag.Indexed)
}

func TestNewTag(t *testing.T) {
	// Test the NewTag constructor function
	tag := NewTag("service", []byte("order-service"), pbv1.ValueTypeStr, true)

	assert.Equal(t, "service", tag.Name)
	assert.Equal(t, []byte("order-service"), tag.Value)
	assert.Equal(t, pbv1.ValueTypeStr, tag.ValueType)
	assert.Equal(t, true, tag.Indexed)
}

func TestTagReset(t *testing.T) {
	tag := Tag{
		Name:      "test-tag",
		Value:     []byte("test-value"),
		ValueType: pbv1.ValueTypeStr,
		Indexed:   true,
	}

	tag.Reset()

	assert.Equal(t, "", tag.Name)
	assert.Nil(t, tag.Value)
	assert.Equal(t, pbv1.ValueTypeUnknown, tag.ValueType)
	assert.Equal(t, false, tag.Indexed)
}

func TestTagSize(t *testing.T) {
	tag := Tag{
		Name:      "test",
		Value:     []byte("value"),
		ValueType: pbv1.ValueTypeStr,
		Indexed:   true,
	}

	// Size should be len(name) + len(value) + 1 (for valueType)
	expectedSize := len("test") + len("value") + 1
	assert.Equal(t, expectedSize, tag.Size())
}

func TestTagCopy(t *testing.T) {
	original := Tag{
		Name:      "original",
		Value:     []byte("original-value"),
		ValueType: pbv1.ValueTypeStr,
		Indexed:   true,
	}

	copied := original.Copy()

	// Test that copied tag has same values
	assert.Equal(t, original.Name, copied.Name)
	assert.Equal(t, original.Value, copied.Value)
	assert.Equal(t, original.ValueType, copied.ValueType)
	assert.Equal(t, original.Indexed, copied.Indexed)

	// Test that modifying original doesn't affect copy
	original.Name = "modified"
	original.Value = []byte("modified-value")
	assert.Equal(t, "original", copied.Name)
	assert.Equal(t, []byte("original-value"), copied.Value)
}

func TestTagInWriteRequest(t *testing.T) {
	// Test that Tag can be used in WriteRequest
	req := WriteRequest{
		SeriesID: 123,
		Key:      456,
		Data:     []byte("test-data"),
		Tags: []Tag{
			{Name: "service", Value: []byte("order-service"), ValueType: pbv1.ValueTypeStr, Indexed: true},
			{Name: "environment", Value: []byte("prod"), ValueType: pbv1.ValueTypeStr, Indexed: false},
		},
	}

	assert.Equal(t, 2, len(req.Tags))
	assert.Equal(t, "service", req.Tags[0].Name)
	assert.Equal(t, "environment", req.Tags[1].Name)
}
