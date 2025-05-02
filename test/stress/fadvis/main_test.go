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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fadvis provides stress testing functionality for the file advice (fadvis) system
// which optimizes memory page cache usage for file operations.
package fadvis

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/fadvis"
	"github.com/apache/skywalking-banyandb/test/stress/fadvis/utils"
	"os"
	"regexp"
	"runtime"
	"testing"
)

// TestMain is the entry point for the test package, used to initialize the test environment.
// It runs before all tests and benchmarks are executed.
func TestMain(m *testing.M) {
	// Perform initialization before tests start
	fmt.Println("Initializing test environment...")

	// Precompile all regular expressions
	_ = regexp.MustCompile(`Rss:\s+(\d+)\s+kB`)
	_ = regexp.MustCompile(`Pss:\s+(\d+)\s+kB`)
	_ = regexp.MustCompile(`Shared_Clean:\s+(\d+)\s+kB`)

	// Warm up the memory manager
	utils.SetRealisticThreshold()

	// Force a garbage collection
	runtime.GC()

	// Ensure fadvis Manager is initialized
	if fadvis.GetManager() == nil {
		fadvis.SetManager(fadvis.NewManager(nil))
	}

	// Wait for a while to ensure system stability
	//time.Sleep(100 * time.Millisecond)

	// Run all tests and benchmarks
	code := m.Run()

	// Clean up fadvis Manager
	fadvis.CleanupForTesting()

	// Return test result
	os.Exit(code)
}
