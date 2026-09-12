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
	"fmt"
	"slices"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// System host metrics that native self-observability must publish into _monitoring.
// Extra tags beyond the shared entity tags must match the dashboard query shape.
var systemNativeMetrics = []struct {
	name      string
	extraTags []string
}{
	{name: "up_time"},
	{name: "cpu_num"},
	{name: "cpu_state", extraTags: []string{"kind"}},
	{name: "memory_state", extraTags: []string{"kind"}},
	{name: "disk", extraTags: []string{"path", "kind"}},
	{name: "net_state", extraTags: []string{"kind", "name"}},
}

var (
	cpuStateKinds    = []string{"user", "system", "idle", "nice", "iowait", "irq", "softirq", "steal"}
	memoryStateKinds = []string{"used", "total", "used_percent"}
	diskStateKinds   = []string{"used", "total", "used_percent"}
)

var _ = g.Describe("Native self-observability metrics in _monitoring group", func() {
	g.It("declares the expected system measure schemas and label tags", func() {
		for _, metric := range systemNativeMetrics {
			expected := append(append([]string{}, baseEntityTags...), metric.extraTags...)
			gm.Eventually(func() error {
				tags, err := GetObservabilityMeasureTags(metric.name)
				if err != nil {
					return err
				}
				for _, want := range expected {
					if !slices.Contains(tags, want) {
						return fmt.Errorf("measure %s missing tag %q; have %v", metric.name, want, tags)
					}
				}
				return nil
			}, 90*time.Second, 2*time.Second).Should(gm.Succeed(),
				"measure %s must keep entity tags plus %v", metric.name, metric.extraTags)
		}
	})

	g.It("keeps liaison load-shedding memory pressure off the system memory_state measure", func() {
		// The liaison gauge used to register as "memory_state" and stole the
		// system schema (no kind tag), breaking dashboard queries that project kind.
		gm.Eventually(func() error {
			tags, err := GetObservabilityMeasureTags("memory_load_shedding_state")
			if err != nil {
				return err
			}
			for _, want := range baseEntityTags {
				if !slices.Contains(tags, want) {
					return fmt.Errorf("memory_load_shedding_state missing tag %q; have %v", want, tags)
				}
			}
			return nil
		}, 90*time.Second, 2*time.Second).Should(gm.Succeed(),
			"liaison load-shedding must use a distinct measure name")

		tags, err := GetObservabilityMeasureTags("memory_state")
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(tags).To(gm.ContainElement("kind"),
			"system memory_state must retain the kind tag after liaison metrics register")
	})

	g.It("serves dashboard-shaped queries with well-formed label values", func() {
		gm.Eventually(func() error {
			if _, err := QueryObservabilityMeasure("up_time"); err != nil {
				return err
			}
			cpuPoints, err := QueryObservabilityMeasure("cpu_state", "kind")
			if err != nil {
				return err
			}
			if err := requireLabeledSeries(cpuPoints, "kind", cpuStateKinds, true); err != nil {
				return err
			}
			memoryPoints, err := QueryObservabilityMeasure("memory_state", "kind")
			if err != nil {
				return err
			}
			if err := requireLabeledSeries(memoryPoints, "kind", memoryStateKinds, true); err != nil {
				return err
			}
			diskPoints, err := QueryObservabilityMeasure("disk", "path", "kind")
			if err != nil {
				return err
			}
			if err := requireLabeledSeries(diskPoints, "kind", diskStateKinds, false); err != nil {
				return err
			}
			if _, err := QueryObservabilityMeasure("cpu_num"); err != nil {
				return err
			}
			// net_state may be empty when the host has no eth*/en* interfaces; schema+query must still succeed.
			if _, err := QueryObservabilityMeasure("net_state", "kind", "name"); err != nil {
				return err
			}
			return nil
		}, 90*time.Second, 2*time.Second).Should(gm.Succeed())
	})

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

	g.It("collects memory_state metric with kind labels", func() {
		gm.Eventually(func() (bool, error) {
			points, err := QueryObservabilityMeasure("memory_state", "kind")
			if err != nil {
				return false, err
			}
			if len(points) == 0 {
				return false, nil
			}
			seen := map[string]bool{}
			for _, p := range points {
				kind, ok := p.Tags["kind"]
				if !ok || !slices.Contains(memoryStateKinds, kind) {
					return false, nil
				}
				if kind == "used" || kind == "total" {
					if p.Value <= 0 {
						return false, nil
					}
				}
				if kind == "used_percent" {
					if p.Value < 0.0 || p.Value > 1.0 {
						return false, nil
					}
				}
				seen[kind] = true
			}
			return seen["used"] && seen["total"] && seen["used_percent"], nil
		}, 90*time.Second, 5*time.Second).Should(gm.BeTrue())
	})
})

func requireLabeledSeries(points []Point, label string, allowed []string, requireData bool) error {
	if requireData && len(points) == 0 {
		return fmt.Errorf("expected datapoints with label %q", label)
	}
	for _, p := range points {
		value, ok := p.Tags[label]
		if !ok || value == "" {
			return fmt.Errorf("datapoint missing non-empty %q label: tags=%v", label, p.Tags)
		}
		if !slices.Contains(allowed, value) {
			return fmt.Errorf("unexpected %q=%q; allowed=%v tags=%v", label, value, allowed, p.Tags)
		}
		for _, entityTag := range baseEntityTags {
			if p.Tags[entityTag] == "" {
				return fmt.Errorf("datapoint missing entity tag %q: tags=%v", entityTag, p.Tags)
			}
		}
	}
	return nil
}
