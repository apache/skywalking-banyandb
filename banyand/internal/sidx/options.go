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
	"fmt"
	"path/filepath"

	"github.com/apache/skywalking-banyandb/banyand/protector"
)

// Options contains configuration options for the Secondary Index File System (SIDX).
type Options struct {
	// MergePolicy controls the merge behavior for parts.
	// It determines which parts should be merged together to optimize
	// storage efficiency and query performance.
	MergePolicy *MergePolicy

	// Memory is the memory protector that controls resource limits.
	// It monitors memory usage and prevents OOM by applying backpressure
	// when memory consumption exceeds configured thresholds.
	Memory protector.Memory

	// Path is the directory where SIDX files are stored.
	// This includes part directories, metadata files, and temporary files.
	Path string
}

// MergePolicy defines the strategy for merging parts in SIDX.
// It aims to choose an optimal combination that has the lowest write amplification.
type MergePolicy struct {
	// MaxParts is the maximum number of parts that can be merged in a single operation.
	// Higher values allow more aggressive merging but consume more resources.
	MaxParts int

	// MinMergeMultiplier is the minimum ratio between output size and largest input part size
	// required to proceed with a merge. This prevents merging parts with too small benefit.
	MinMergeMultiplier float64

	// MaxFanOutSize is the maximum total size of parts that can be merged together.
	// This limits the memory and disk I/O requirements of merge operations.
	MaxFanOutSize uint64
}

// NewDefaultOptions creates Options with sensible default values.
func NewDefaultOptions() *Options {
	return &Options{
		Path:        "/tmp/sidx",
		Memory:      nil, // Must be provided by caller
		MergePolicy: NewDefaultMergePolicy(),
	}
}

// NewDefaultMergePolicy creates a MergePolicy with default parameters optimized
// for typical SIDX workloads.
func NewDefaultMergePolicy() *MergePolicy {
	return &MergePolicy{
		MaxParts:           8,
		MinMergeMultiplier: 1.7,
		MaxFanOutSize:      1 << 30, // 1GB
	}
}

// NewMergePolicy creates a MergePolicy with custom parameters.
func NewMergePolicy(maxParts int, minMergeMultiplier float64, maxFanOutSize uint64) *MergePolicy {
	return &MergePolicy{
		MaxParts:           maxParts,
		MinMergeMultiplier: minMergeMultiplier,
		MaxFanOutSize:      maxFanOutSize,
	}
}

// Validate validates the options and returns an error if any configuration is invalid.
func (o *Options) Validate() error {
	if o.Path == "" {
		return fmt.Errorf("path must not be empty")
	}

	if !filepath.IsAbs(o.Path) {
		return fmt.Errorf("path must be absolute, got: %s", o.Path)
	}

	if o.Memory == nil {
		return fmt.Errorf("memory protector must not be nil")
	}

	if o.MergePolicy == nil {
		return fmt.Errorf("merge policy must not be nil")
	}

	return o.MergePolicy.Validate()
}

// Validate validates the merge policy configuration.
func (mp *MergePolicy) Validate() error {
	if mp.MaxParts < 2 {
		return fmt.Errorf("maxParts must be at least 2, got: %d", mp.MaxParts)
	}

	if mp.MinMergeMultiplier <= 1.0 {
		return fmt.Errorf("minMergeMultiplier must be greater than 1.0, got: %f", mp.MinMergeMultiplier)
	}

	if mp.MaxFanOutSize == 0 {
		return fmt.Errorf("maxFanOutSize must be greater than 0")
	}

	return nil
}

// WithPath returns a copy of the options with the specified path.
func (o *Options) WithPath(path string) *Options {
	opts := *o
	opts.Path = path
	return &opts
}

// WithMemory returns a copy of the options with the specified memory protector.
func (o *Options) WithMemory(memory protector.Memory) *Options {
	opts := *o
	opts.Memory = memory
	return &opts
}

// WithMergePolicy returns a copy of the options with the specified merge policy.
func (o *Options) WithMergePolicy(policy *MergePolicy) *Options {
	opts := *o
	opts.MergePolicy = policy
	return &opts
}
