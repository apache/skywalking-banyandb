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

package querybench

import "testing"

func TestQLBT03Config(t *testing.T) {
	t.Setenv(envRunBench, "true")
	t.Setenv(envInContainer, "1")
	t.Setenv(envMode, "vec")
	t.Setenv(envScenario, "qlb_t03")
	t.Setenv(envCardinality, "1024")
	t.Setenv(envQueryWorkers, "1")
	t.Setenv(envQueryIters, "1000")
	t.Setenv(envWarmupIters, "20")
	cfg := LoadConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.QueryIterations = 10 },
		func(c *Config) { c.QueryWorkers = 2 },
		func(c *Config) { c.WarmupIterations = 0 },
	} {
		invalid := cfg
		change(&invalid)
		if err := invalid.Validate(); err == nil {
			t.Fatal("T03 cannot silently run a partial benchmark matrix")
		}
	}
}
