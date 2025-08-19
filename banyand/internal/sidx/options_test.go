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
		assert.NotNil(t, opts.MergePolicy)

		// Verify default merge policy values
		assert.Equal(t, 8, opts.MergePolicy.MaxParts)
		assert.Equal(t, 1.7, opts.MergePolicy.MinMergeMultiplier)
		assert.Equal(t, uint64(1<<30), opts.MergePolicy.MaxFanOutSize)

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
	assert.NotNil(t, opts.MergePolicy)

	// Verify default merge policy values
	assert.Equal(t, 8, opts.MergePolicy.MaxParts)
	assert.Equal(t, 1.7, opts.MergePolicy.MinMergeMultiplier)
	assert.Equal(t, uint64(1<<30), opts.MergePolicy.MaxFanOutSize)

	// Should fail validation due to nil memory
	assert.Error(t, opts.Validate())
}

func TestNewDefaultMergePolicy(t *testing.T) {
	policy := NewDefaultMergePolicy()

	assert.Equal(t, 8, policy.MaxParts)
	assert.Equal(t, 1.7, policy.MinMergeMultiplier)
	assert.Equal(t, uint64(1<<30), policy.MaxFanOutSize)

	// Ensure it validates successfully
	assert.NoError(t, policy.Validate())
}

func TestNewMergePolicy(t *testing.T) {
	policy := NewMergePolicy(10, 2.0, 1<<20)

	assert.Equal(t, 10, policy.MaxParts)
	assert.Equal(t, 2.0, policy.MinMergeMultiplier)
	assert.Equal(t, uint64(1<<20), policy.MaxFanOutSize)

	assert.NoError(t, policy.Validate())
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
				Path:        "/tmp/sidx",
				Memory:      mockMemory,
				MergePolicy: NewDefaultMergePolicy(),
			},
			expectError: false,
		},
		{
			name: "empty path",
			opts: &Options{
				Path:        "",
				Memory:      mockMemory,
				MergePolicy: NewDefaultMergePolicy(),
			},
			expectError: true,
			errorMsg:    "path must not be empty",
		},
		{
			name: "relative path",
			opts: &Options{
				Path:        "relative/path",
				Memory:      mockMemory,
				MergePolicy: NewDefaultMergePolicy(),
			},
			expectError: true,
			errorMsg:    "path must be absolute",
		},
		{
			name: "nil memory protector",
			opts: &Options{
				Path:        "/tmp/sidx",
				Memory:      nil,
				MergePolicy: NewDefaultMergePolicy(),
			},
			expectError: true,
			errorMsg:    "memory protector must not be nil",
		},
		{
			name: "nil merge policy",
			opts: &Options{
				Path:        "/tmp/sidx",
				Memory:      mockMemory,
				MergePolicy: nil,
			},
			expectError: true,
			errorMsg:    "merge policy must not be nil",
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

func TestMergePolicyValidation(t *testing.T) {
	tests := []struct {
		policy      *MergePolicy
		name        string
		errorMsg    string
		expectError bool
	}{
		{
			name: "valid merge policy",
			policy: &MergePolicy{
				MaxParts:           8,
				MinMergeMultiplier: 1.7,
				MaxFanOutSize:      1 << 30,
			},
			expectError: false,
		},
		{
			name: "maxParts too small",
			policy: &MergePolicy{
				MaxParts:           1,
				MinMergeMultiplier: 1.7,
				MaxFanOutSize:      1 << 30,
			},
			expectError: true,
			errorMsg:    "maxParts must be at least 2",
		},
		{
			name: "minMergeMultiplier too small",
			policy: &MergePolicy{
				MaxParts:           8,
				MinMergeMultiplier: 1.0,
				MaxFanOutSize:      1 << 30,
			},
			expectError: true,
			errorMsg:    "minMergeMultiplier must be greater than 1.0",
		},
		{
			name: "zero maxFanOutSize",
			policy: &MergePolicy{
				MaxParts:           8,
				MinMergeMultiplier: 1.7,
				MaxFanOutSize:      0,
			},
			expectError: true,
			errorMsg:    "maxFanOutSize must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
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
		assert.Equal(t, originalOpts.Path, "/tmp/sidx")                // Original unchanged
		assert.Equal(t, originalOpts.MergePolicy, newOpts.MergePolicy) // Other fields copied
	})

	t.Run("WithMemory", func(t *testing.T) {
		newOpts := originalOpts.WithMemory(mockMemory)

		assert.Equal(t, mockMemory, newOpts.Memory)
		assert.Nil(t, originalOpts.Memory)               // Original unchanged
		assert.Equal(t, originalOpts.Path, newOpts.Path) // Other fields copied
	})

	t.Run("WithMergePolicy", func(t *testing.T) {
		customPolicy := NewMergePolicy(10, 2.0, 1<<20)
		newOpts := originalOpts.WithMergePolicy(customPolicy)

		assert.Equal(t, customPolicy, newOpts.MergePolicy)
		assert.NotEqual(t, customPolicy, originalOpts.MergePolicy) // Original unchanged
		assert.Equal(t, originalOpts.Path, newOpts.Path)           // Other fields copied
	})
}

func TestOptionsConfiguration(t *testing.T) {
	mockMemory := protector.NewMemory(observability.NewBypassRegistry())

	// Test using new mandatory constructor
	baseOpts, err := NewOptions("/base/sidx", mockMemory)
	require.NoError(t, err)
	require.NotNil(t, baseOpts.MergePolicy)

	// Test configuration can be merged and overridden
	customOpts := baseOpts.
		WithPath("/custom/sidx").
		WithMergePolicy(NewMergePolicy(5, 1.5, 512<<20))

	assert.Equal(t, "/custom/sidx", customOpts.Path)
	assert.Equal(t, mockMemory, customOpts.Memory)
	assert.Equal(t, 5, customOpts.MergePolicy.MaxParts)
	assert.Equal(t, 1.5, customOpts.MergePolicy.MinMergeMultiplier)
	assert.Equal(t, uint64(512<<20), customOpts.MergePolicy.MaxFanOutSize)

	// Ensure validation works with custom configuration
	assert.NoError(t, customOpts.Validate())

	// Test deprecated constructor still works but requires memory to be set
	defaultOpts := NewDefaultOptions()
	require.NotNil(t, defaultOpts.MergePolicy)

	// Should fail validation due to nil memory
	assert.Error(t, defaultOpts.Validate())

	// But works after setting memory
	optsWithMemory := defaultOpts.WithMemory(mockMemory)
	assert.NoError(t, optsWithMemory.Validate())
}

func TestOptionsEdgeCases(t *testing.T) {
	t.Run("minimum valid merge policy", func(t *testing.T) {
		policy := &MergePolicy{
			MaxParts:           2,
			MinMergeMultiplier: 1.001,
			MaxFanOutSize:      1,
		}
		assert.NoError(t, policy.Validate())
	})

	t.Run("large values", func(t *testing.T) {
		policy := &MergePolicy{
			MaxParts:           1000,
			MinMergeMultiplier: 100.0,
			MaxFanOutSize:      1 << 60,
		}
		assert.NoError(t, policy.Validate())
	})
}
