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

package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalVarArray(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{
			name:     "empty",
			input:    []byte{},
			expected: []byte{'|'},
		},
		{
			name:     "no special chars",
			input:    []byte("abc"),
			expected: []byte("abc|"),
		},
		{
			name:     "with delimiter",
			input:    []byte("a|b"),
			expected: []byte("a\\|b|"),
		},
		{
			name:     "with escape",
			input:    []byte("a\\b"),
			expected: []byte("a\\\\b|"),
		},
		{
			name:     "with delimiter and escape",
			input:    []byte("a|\\b"),
			expected: []byte("a\\|\\\\b|"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, MarshalVarArray(nil, tt.input))
		})
	}

	t.Run("multiple values", func(t *testing.T) {
		var result []byte
		result = MarshalVarArray(result, []byte("a|b"))
		result = MarshalVarArray(result, []byte("c\\d"))
		assert.Equal(t, []byte("a\\|b|c\\\\d|"), result)
	})
}

func TestUnmarshalVarArray_ErrorAndEmpty(t *testing.T) {
	t.Run("idx beyond length", func(t *testing.T) {
		end, next, err := UnmarshalVarArray([]byte{}, 0)
		assert.Error(t, err)
		assert.Equal(t, 0, end)
		assert.Equal(t, 0, next)

		end, next, err = UnmarshalVarArray([]byte("abc"), 3)
		assert.Error(t, err)
		assert.Equal(t, 0, end)
		assert.Equal(t, 0, next)
	})

	t.Run("empty value", func(t *testing.T) {
		src := []byte("|")
		end, next, err := UnmarshalVarArray(src, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0, end)
		assert.Equal(t, 1, next)
		// decoded value is src[0:0] == empty
	})

	t.Run("no delimiter", func(t *testing.T) {
		src := []byte("abc")
		end, next, err := UnmarshalVarArray(src, 0)
		assert.Error(t, err)
		assert.Equal(t, 0, end)
		assert.Equal(t, 0, next)
	})

	t.Run("invalid escape at end", func(t *testing.T) {
		src := []byte("abc\\")
		end, next, err := UnmarshalVarArray(src, 0)
		assert.Error(t, err)
		assert.Equal(t, 0, end)
		assert.Equal(t, 0, next)
	})
}

func TestUnmarshalVarArray_NormalAndEscaped(t *testing.T) {
	t.Run("single value without escapes", func(t *testing.T) {
		src := []byte("abc|")
		end, next, err := UnmarshalVarArray(src, 0)
		assert.NoError(t, err)
		assert.Equal(t, 3, end)
		assert.Equal(t, 4, next)
		assert.Equal(t, "abc", string(src[0:end]))
	})

	t.Run("round-trip multiple values with escapes", func(t *testing.T) {
		var encoded []byte
		encoded = MarshalVarArray(encoded, []byte("a|b"))
		encoded = MarshalVarArray(encoded, []byte("c\\d"))

		// encoded should match MarshalVarArray multiple values test
		assert.Equal(t, []byte("a\\|b|c\\\\d|"), encoded)

		// Iterate as documented in UnmarshalVarArray
		var decoded [][]byte
		for idx := 0; idx < len(encoded); {
			end, next, err := UnmarshalVarArray(encoded, idx)
			assert.NoError(t, err)
			decoded = append(decoded, append([]byte(nil), encoded[idx:end]...))
			idx = next
		}

		assert.Len(t, decoded, 2)
		assert.Equal(t, []byte("a|b"), decoded[0])
		assert.Equal(t, []byte("c\\d"), decoded[1])
	})
}
