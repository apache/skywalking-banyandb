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
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// samplerMetrics holds the US-011 observability counters/gauges for the
// dynamic sampler registry. All fields are safe to call on a nil receiver or
// when backed by a bypassFactory — Inc/Set on a bypass counter/gauge is a
// no-op, so callers need no nil checks.
type samplerMetrics struct {
	// samplerActiveCount{group} tracks the live sampler count per group.
	samplerActiveCount meter.Gauge
	// samplerRegisterTotal{group,result} counts apply-config calls (result = success|rejected).
	samplerRegisterTotal meter.Counter
	// samplerUpdateTotal{group,result} counts re-apply calls on an existing group
	// (result = success|rejected). Emitted on the same reconcile path as register;
	// callers distinguish them by whether a previous set existed.
	samplerUpdateTotal meter.Counter
	// samplerRemoveTotal{group} counts explicit removal calls.
	samplerRemoveTotal meter.Counter
	// samplerLoadFailed{group,name,reason} counts individual plugin load failures
	// on the fail-open path.
	samplerLoadFailed meter.Counter
}

// newSamplerMetrics creates a samplerMetrics wired to factory. If factory is
// nil, observability.BypassRegistry is used so all operations are no-ops.
func newSamplerMetrics(factory observability.Factory) *samplerMetrics {
	if factory == nil {
		factory = observability.BypassRegistry.With(pipelineScope)
	}
	return &samplerMetrics{
		samplerActiveCount:   factory.NewGauge("sampler_active_count", "group"),
		samplerRegisterTotal: factory.NewCounter("sampler_register_total", "group", "result"),
		samplerUpdateTotal:   factory.NewCounter("sampler_update_total", "group", "result"),
		samplerRemoveTotal:   factory.NewCounter("sampler_remove_total", "group"),
		samplerLoadFailed:    factory.NewCounter("sampler_load_failed", "group", "name", "reason"),
	}
}

func (m *samplerMetrics) setActiveCount(group string, n int) {
	if m == nil {
		return
	}
	m.samplerActiveCount.Set(float64(n), group)
}

func (m *samplerMetrics) incRegisterTotal(group, result string) {
	if m == nil {
		return
	}
	m.samplerRegisterTotal.Inc(1, group, result)
}

func (m *samplerMetrics) incUpdateTotal(group, result string) {
	if m == nil {
		return
	}
	m.samplerUpdateTotal.Inc(1, group, result)
}

func (m *samplerMetrics) incRemoveTotal(group string) {
	if m == nil {
		return
	}
	m.samplerRemoveTotal.Inc(1, group)
}

func (m *samplerMetrics) incLoadFailed(group, name, reason string) {
	if m == nil {
		return
	}
	m.samplerLoadFailed.Inc(1, group, name, reason)
}
