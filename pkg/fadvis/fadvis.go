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
//go:build linux
// +build linux

// Package fadvis provides utilities for file access advice optimization.
package fadvis

import (
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fs"
)

// memoryProtector stores the global instance of the memory protector
var memoryProtector *protector.Memory

// SetMemoryProtector sets the global memory protector instance for fadvis threshold management.
func SetMemoryProtector(mp *protector.Memory) {
	memoryProtector = mp

	// Register the memory protector with the filesystem
	fs.SetThresholdProvider(thresholdAdapter{mp})
}

// ShouldCache returns whether a file at the given path should be cached.
// This is a simplified implementation that always returns true.
func ShouldCache(_ string) bool {
	return true
}

// CleanupForTesting is maintained for API compatibility with tests.
func CleanupForTesting() {
	// No-op since we don't have background goroutines anymore
}

// thresholdAdapter adapts the Memory protector to the fs.ThresholdProvider interface
type thresholdAdapter struct {
	mp *protector.Memory
}

// ShouldApplyFadvis implements the fs.ThresholdProvider interface
func (t thresholdAdapter) ShouldApplyFadvis(fileSize int64) bool {
	if t.mp == nil {
		return fileSize >= 64*1024*1024 // Default 64MB
	}
	// Pass the fileSize as maxSize parameter to GetThreshold
	return fileSize >= t.mp.GetThreshold(fileSize)
}
