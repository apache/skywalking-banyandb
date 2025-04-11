// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package fadvis provides utilities for applying posix_fadvise
// to optimize file system performance for large files.
package fadvis

import (
	"os"
	"sync"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/fadvis"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Default threshold for file size, files larger than this will use fadvis.
var (
	// LargeFileThreshold is the threshold in bytes for considering a file as large.
	LargeFileThreshold atomic.Int64
	// Log is the logger for fadvis related operations.
	Log = logger.GetLogger("fadvis-helper")
	// Only use once to ensure SetMemoryProtector is called only once.
	memoryProtectorOnce sync.Once
)

func init() {
	// Default 64MB.
	LargeFileThreshold.Store(64 * 1024 * 1024)
}

// SetMemoryProtector sets the threshold from the Memory protector instance.
func SetMemoryProtector(mp *protector.Memory) {
	memoryProtectorOnce.Do(func() {
		if mp != nil {
			// Get threshold from Memory protector
			if threshold := mp.GetThreshold(); threshold > 0 {
				SetThreshold(threshold)
			}
		}
	})
}

// SetThreshold sets the large file threshold.
func SetThreshold(threshold int64) {
	if threshold > 0 {
		LargeFileThreshold.Store(threshold)
		Log.Info().Int64("threshold", threshold).Msg("set large file threshold for fadvis")
	}
}

// GetThreshold returns the current large file threshold.
func GetThreshold() int64 {
	return LargeFileThreshold.Load()
}

// ApplyIfLarge applies fadvis to the file if it's larger than the threshold.
func ApplyIfLarge(filePath string) {
	if fileInfo, err := os.Stat(filePath); err == nil {
		threshold := LargeFileThreshold.Load()
		if fileInfo.Size() > threshold {
			Log.Info().Str("path", filePath).Msg("applying fadvis for large file")
			if err := fadvis.Apply(filePath); err != nil {
				Log.Warn().Err(err).Str("path", filePath).Msg("failed to apply fadvis to file")
			}
		}
	}
}

// MustApplyIfLarge applies fadvis to the file if it's larger than the threshold, panics on error.
func MustApplyIfLarge(filePath string) {
	if fileInfo, err := os.Stat(filePath); err == nil {
		threshold := LargeFileThreshold.Load()
		if fileInfo.Size() > threshold {
			Log.Info().Str("path", filePath).Msg("applying fadvis for large file")
			fadvis.MustApply(filePath)
		}
	}
}
