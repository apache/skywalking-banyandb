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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestMarshalAndUnmarshalEntityValue(t *testing.T) {
	tests := []struct {
		name string
		src  []byte
	}{
		{
			name: "plain text",
			src:  []byte("plainText"),
		},
		{
			name: "text with entityDelimiter",
			src:  []byte("text|with|delimiter"),
		},
		{
			name: "text with escape",
			src:  []byte("text\\with\\escape"),
		},
		{
			name: "text with both special characters",
			src:  []byte("text|with\\both"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := []byte{}

			// Test marshalEntityValue
			marshaled := marshalEntityValue(dest, tt.src)

			// Add assertions
			assert.NotNil(t, marshaled)

			// Test unmarshalEntityValue
			unmarshaledDest, unmarshaledSrc, err := unmarshalEntityValue(dest, marshaled)

			// Add assertions
			assert.NoError(t, err)
			assert.NotNil(t, unmarshaledDest)
			assert.NotNil(t, unmarshaledSrc)

			// Check that unmarshaling the marshaled value gives the original value
			assert.Equal(t, tt.src, unmarshaledDest)
		})
	}
}

func TestMarshalAndUnmarshalSeries(t *testing.T) {
	tests := []struct {
		src  *Series
		name string
	}{
		{
			name: "series with entity values",
			src:  &Series{Subject: "subject", EntityValues: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "stringValue"}}}}},
		},
		{
			name: "series without entity values",
			src:  &Series{Subject: "subject", EntityValues: []*modelv1.TagValue{}},
		},
		{
			name: "series with multiple entity values",
			src: &Series{Subject: "subject", EntityValues: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "stringValue"}}},
				{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 123}}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Series.Marshal
			err := tt.src.marshal()

			// Add assertions
			assert.NoError(t, err)
			assert.True(t, len(tt.src.Buffer) > 0)
			marshaled := make([]byte, len(tt.src.Buffer))
			copy(marshaled, tt.src.Buffer)

			// Test Series.Unmarshal
			tt.src.reset()
			err = tt.src.unmarshal(marshaled)

			// Add assertions
			assert.NoError(t, err)

			// Check that unmarshaling the marshaled value gives the original value
			assert.Equal(t, tt.src.Subject, tt.src.Subject)
			assert.Equal(t, tt.src.EntityValues, tt.src.EntityValues)
		})
	}
}
