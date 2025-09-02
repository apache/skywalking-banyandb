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
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
)

func TestNewOptions(t *testing.T) {
	mockMemory := protector.NewMemory(observability.NewBypassRegistry())

	t.Run("valid parameters", func(t *testing.T) {
		opts, err := NewOptions("/tmp/sidx", mockMemory)
		require.NoError(t, err)
		require.NotNil(t, opts)

		assert.Equal(t, "/tmp/sidx", opts.Path)
		assert.Equal(t, mockMemory, opts.Memory)

		// Should validate successfully
		assert.NoError(t, opts.Validate())
	})

	t.Run("empty path", func(t *testing.T) {
		opts, err := NewOptions("", mockMemory)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "path must not be empty")
	})

	t.Run("relative path", func(t *testing.T) {
		opts, err := NewOptions("relative/path", mockMemory)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "path must be absolute")
	})

	t.Run("nil memory", func(t *testing.T) {
		opts, err := NewOptions("/tmp/sidx", nil)
		assert.Error(t, err)
		assert.Nil(t, opts)
		assert.Contains(t, err.Error(), "memory protector must not be nil")
	})
}

func TestNewDefaultOptions(t *testing.T) {
	opts := NewDefaultOptions()

	assert.Equal(t, "/tmp/sidx", opts.Path)
	assert.Nil(t, opts.Memory)

	// Should fail validation due to nil memory
	assert.Error(t, opts.Validate())
}

func TestOptionsValidation(t *testing.T) {
	mockMemory := protector.NewMemory(observability.NewBypassRegistry())

	tests := []struct {
		opts        *Options
		name        string
		errorMsg    string
		expectError bool
	}{
		{
			name: "valid options",
			opts: &Options{
				Path:   "/tmp/sidx",
				Memory: mockMemory,
			},
			expectError: false,
		},
		{
			name: "empty path",
			opts: &Options{
				Path:   "",
				Memory: mockMemory,
			},
			expectError: true,
			errorMsg:    "path must not be empty",
		},
		{
			name: "relative path",
			opts: &Options{
				Path:   "relative/path",
				Memory: mockMemory,
			},
			expectError: true,
			errorMsg:    "path must be absolute",
		},
		{
			name: "nil memory protector",
			opts: &Options{
				Path:   "/tmp/sidx",
				Memory: nil,
			},
			expectError: true,
			errorMsg:    "memory protector must not be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestOptionsWithMethods(t *testing.T) {
	mockMemory := protector.NewMemory(observability.NewBypassRegistry())
	originalOpts := NewDefaultOptions()

	t.Run("WithPath", func(t *testing.T) {
		newPath := "/custom/path"
		newOpts := originalOpts.WithPath(newPath)

		assert.Equal(t, newPath, newOpts.Path)
		assert.Equal(t, originalOpts.Path, "/tmp/sidx") // Original unchanged
	})

	t.Run("WithMemory", func(t *testing.T) {
		newOpts := originalOpts.WithMemory(mockMemory)

		assert.Equal(t, mockMemory, newOpts.Memory)
		assert.Nil(t, originalOpts.Memory)               // Original unchanged
		assert.Equal(t, originalOpts.Path, newOpts.Path) // Other fields copied
	})
}

func TestOptionsConfiguration(t *testing.T) {
	mockMemory := protector.NewMemory(observability.NewBypassRegistry())

	// Test using new mandatory constructor
	baseOpts, err := NewOptions("/base/sidx", mockMemory)
	require.NoError(t, err)

	// Test configuration can be merged and overridden
	customOpts := baseOpts.
		WithPath("/custom/sidx")

	assert.Equal(t, "/custom/sidx", customOpts.Path)
	assert.Equal(t, mockMemory, customOpts.Memory)

	// Ensure validation works with custom configuration
	assert.NoError(t, customOpts.Validate())

	// Test deprecated constructor still works but requires memory to be set
	defaultOpts := NewDefaultOptions()

	// Should fail validation due to nil memory
	assert.Error(t, defaultOpts.Validate())

	// But works after setting memory
	optsWithMemory := defaultOpts.WithMemory(mockMemory)
	assert.NoError(t, optsWithMemory.Validate())
}
