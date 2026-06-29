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

package trace

import (
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// localSamplerRegistry maps group → ordered samplers for in-process, flag-gated
// activation of the in-merge filter hook. It is consulted ONLY when
// option.nativePipelineEnabled is true. The config-driven startup loader
// (svc_standalone PreRun, via --trace-pipeline-config) also populates this
// registry at boot; unit tests may register samplers directly for fast
// in-process coverage.
var localSamplerRegistry = struct {
	m  map[string][]sdk.Sampler
	mu sync.RWMutex
}{m: make(map[string][]sdk.Sampler)}

// registerSampler registers s under group and returns a deregister closure.
// Callers must invoke the returned function (e.g., via t.Cleanup) to remove
// the sampler when the test ends.
func registerSampler(group string, s sdk.Sampler) func() {
	localSamplerRegistry.mu.Lock()
	localSamplerRegistry.m[group] = append(localSamplerRegistry.m[group], s)
	localSamplerRegistry.mu.Unlock()
	return func() {
		localSamplerRegistry.mu.Lock()
		defer localSamplerRegistry.mu.Unlock()
		samplers := localSamplerRegistry.m[group]
		for i, existing := range samplers {
			if existing == s {
				localSamplerRegistry.m[group] = append(samplers[:i], samplers[i+1:]...)
				break
			}
		}
		if len(localSamplerRegistry.m[group]) == 0 {
			delete(localSamplerRegistry.m, group)
		}
	}
}

// lookupSamplers returns a snapshot of the registered samplers for group, or nil if none.
func lookupSamplers(group string) []sdk.Sampler {
	localSamplerRegistry.mu.RLock()
	defer localSamplerRegistry.mu.RUnlock()
	s := localSamplerRegistry.m[group]
	if len(s) == 0 {
		return nil
	}
	out := make([]sdk.Sampler, len(s))
	copy(out, s)
	return out
}
