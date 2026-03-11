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

package observability

import (
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = g.Describe("Native self-observability metrics in _monitoring group", func() {
	g.It("collects up_time metric", func() {
		gm.Eventually(func() (float64, error) {
			points, err := QueryObservabilityMeasure("up_time")
			if err != nil {
				return 0, err
			}
			if len(points) == 0 {
				return 0, nil
			}
			var maxValue float64
			for _, p := range points {
				if p.Value > maxValue {
					maxValue = p.Value
				}
			}
			return maxValue, nil
		}, 90*time.Second, 5*time.Second).Should(gm.BeNumerically(">", 0.0))
	})

	g.It("collects cpu_state metric with reasonable fractions", func() {
		gm.Eventually(func() (bool, error) {
			points, err := QueryObservabilityMeasure("cpu_state", "kind")
			if err != nil {
				return false, err
			}
			if len(points) == 0 {
				return false, nil
			}
			var hasUserOrSystem bool
			for _, p := range points {
				kind, ok := p.Tags["kind"]
				if !ok {
					continue
				}
				if kind == "user" || kind == "system" {
					if p.Value < 0.0 || p.Value > 1.0 {
						return false, nil
					}
					hasUserOrSystem = true
				}
			}
			return hasUserOrSystem, nil
		}, 90*time.Second, 5*time.Second).Should(gm.BeTrue())
	})

	g.It("collects memory_state metric with basic sanity", func() {
		// Note: memory_state may be created by liaison without "kind" tag, so we query with base tags only.
		// Validate that we get at least one datapoint with a positive value (total/used bytes or used_percent).
		gm.Eventually(func() (bool, error) {
			points, err := QueryObservabilityMeasure("memory_state")
			if err != nil {
				return false, err
			}
			if len(points) == 0 {
				return false, nil
			}
			for _, p := range points {
				if p.Value > 0 {
					return true, nil
				}
			}
			return false, nil
		}, 90*time.Second, 5*time.Second).Should(gm.BeTrue())
	})
})
