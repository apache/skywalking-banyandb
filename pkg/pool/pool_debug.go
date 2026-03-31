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

//go:build !slim

package pool

// EnableStackTracking enables or disables stack tracking for all pools.
func EnableStackTracking(enabled bool) {
	stackTrackingEnabled.Store(enabled)
}

// AllRefsCount returns the reference count of all pools.
func AllRefsCount() map[string]int {
	result := make(map[string]int)
	poolMap.Range(func(key, value any) bool {
		result[key.(string)] = value.(Trackable).RefsCount()
		return true
	})
	return result
}

// AllStacks returns all recorded stack traces for leaked objects from all pools.
func AllStacks() map[string][]string {
	result := make(map[string][]string)
	poolMap.Range(func(key, value any) bool {
		if st, ok := value.(StackTracker); ok {
			stacks := st.Stacks()
			if len(stacks) > 0 {
				result[key.(string)] = stacks
			}
		}
		return true
	})
	return result
}
