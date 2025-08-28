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

package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func TestMarshalAndUnmarshalTagValue(t *testing.T) {
	tests := []struct {
		src  *modelv1.TagValue
		name string
	}{
		{
			name: "string value",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "stringValue"}}},
		},
		{
			name: "int value",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 123}}},
		},
		{
			name: "binary data",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte("binaryData")}},
		},
		{
			name: "timestamp value",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: &timestamppb.Timestamp{Seconds: 1234567890, Nanos: 123456789}}},
		},
		{
			name: "timestamp value with high precision",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: &timestamppb.Timestamp{Seconds: 0, Nanos: 999999999}}},
		},
		{
			name: "unsupported type",
			src:  &modelv1.TagValue{Value: &modelv1.TagValue_Null{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := []byte{}
			// Test marshalTagValue
			dest, err := marshalTagValue(dest, tt.src)
			marshaled := make([]byte, len(dest))
			copy(marshaled, dest)

			// Add assertions
			assert.NoError(t, err)
			assert.True(t, len(marshaled) > 0)

			// Test unmarshalTagValue
			dest = dest[:0]
			_, marshaled, unmarshaled, err := unmarshalTagValue(dest, marshaled)

			// Add assertions
			assert.NoError(t, err)
			assert.True(t, len(marshaled) == 0)
			assert.NotNil(t, unmarshaled)

			// Check that unmarshaling the marshaled value gives the original value
			assert.Equal(t, tt.src, unmarshaled)
		})
	}
}
