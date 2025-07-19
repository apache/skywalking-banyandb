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

package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRangeOpts_Marshal_Unmarshal(t *testing.T) {
	tests := []struct {
		expectedUpper IsTermValue
		expectedLower IsTermValue
		original      RangeOpts
		name          string
		includesUpper bool
		includesLower bool
	}{
		{
			name: "string range with both bounds",
			original: RangeOpts{
				Upper:         &BytesTermValue{Value: []byte("zebra")},
				Lower:         &BytesTermValue{Value: []byte("apple")},
				IncludesUpper: true,
				IncludesLower: false,
			},
			expectedUpper: &BytesTermValue{Value: []byte("zebra")},
			expectedLower: &BytesTermValue{Value: []byte("apple")},
			includesUpper: true,
			includesLower: false,
		},
		{
			name: "int range with both bounds",
			original: RangeOpts{
				Upper:         &FloatTermValue{Value: 100.0},
				Lower:         &FloatTermValue{Value: 1.0},
				IncludesUpper: false,
				IncludesLower: true,
			},
			expectedUpper: &FloatTermValue{Value: 100.0},
			expectedLower: &FloatTermValue{Value: 1.0},
			includesUpper: false,
			includesLower: true,
		},
		{
			name: "string range with both bounds inclusive",
			original: RangeOpts{
				Upper:         &BytesTermValue{Value: []byte("zebra")},
				Lower:         &BytesTermValue{Value: []byte("apple")},
				IncludesUpper: true,
				IncludesLower: true,
			},
			expectedUpper: &BytesTermValue{Value: []byte("zebra")},
			expectedLower: &BytesTermValue{Value: []byte("apple")},
			includesUpper: true,
			includesLower: true,
		},
		{
			name: "int range with both bounds exclusive",
			original: RangeOpts{
				Upper:         &FloatTermValue{Value: 100.0},
				Lower:         &FloatTermValue{Value: 1.0},
				IncludesUpper: false,
				IncludesLower: false,
			},
			expectedUpper: &FloatTermValue{Value: 100.0},
			expectedLower: &FloatTermValue{Value: 1.0},
			includesUpper: false,
			includesLower: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			marshaled, err := tt.original.Marshal()
			require.NoError(t, err)
			assert.NotNil(t, marshaled)
			assert.Greater(t, len(marshaled), 0)

			// Unmarshal
			unmarshaled := &RangeOpts{}
			err = unmarshaled.Unmarshal(marshaled)
			require.NoError(t, err)

			// Verify upper bound
			if tt.expectedUpper != nil {
				assert.NotNil(t, unmarshaled.Upper)
				switch expected := tt.expectedUpper.(type) {
				case *BytesTermValue:
					actual, ok := unmarshaled.Upper.(*BytesTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				case *FloatTermValue:
					actual, ok := unmarshaled.Upper.(*FloatTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				}
			} else {
				assert.Nil(t, unmarshaled.Upper)
			}

			// Verify lower bound
			if tt.expectedLower != nil {
				assert.NotNil(t, unmarshaled.Lower)
				switch expected := tt.expectedLower.(type) {
				case *BytesTermValue:
					actual, ok := unmarshaled.Lower.(*BytesTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				case *FloatTermValue:
					actual, ok := unmarshaled.Lower.(*FloatTermValue)
					require.True(t, ok)
					assert.Equal(t, expected.Value, actual.Value)
				}
			} else {
				assert.Nil(t, unmarshaled.Lower)
			}

			// Verify inclusion flags
			assert.Equal(t, tt.includesUpper, unmarshaled.IncludesUpper)
			assert.Equal(t, tt.includesLower, unmarshaled.IncludesLower)
		})
	}
}
