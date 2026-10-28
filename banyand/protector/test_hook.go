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

package protector

import "sync"

// The functions in this file are a test-only hook. In-process integration tests run several nodes in one process and
// cannot reach each node's protector, so every protector created by NewMemory is recorded in creation order and a test
// can force one of them into the high-pressure state.

var (
	memoryRosterMu sync.Mutex
	memoryRoster   []*memory
)

func registerMemory(m *memory) {
	memoryRosterMu.Lock()
	memoryRoster = append(memoryRoster, m)
	memoryRosterMu.Unlock()
}

// CountMemories returns the number of protectors created by NewMemory in this process.
// It is intended for tests.
func CountMemories() int {
	memoryRosterMu.Lock()
	defer memoryRosterMu.Unlock()
	return len(memoryRoster)
}

// ForceHighByIndex makes the i-th protector created by NewMemory report StateHigh while high is true, regardless of
// its real usage. It reports whether such a protector exists. It is intended for tests.
func ForceHighByIndex(i int, high bool) bool {
	memoryRosterMu.Lock()
	defer memoryRosterMu.Unlock()
	if i < 0 || i >= len(memoryRoster) {
		return false
	}
	memoryRoster[i].forcedHigh.Store(high)
	return true
}
