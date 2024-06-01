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
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/meter"
)

// MetricsCollector is a global metrics collector.
var MetricsCollector = Collector{
	getters:   make(map[string]MetricsGetter),
	providers: []meter.Provider{},
}

// MetricsGetter is a function that collects metrics.
type MetricsGetter func(meter.Provider)

// Collector is a metrics collector.
type Collector struct {
	getters   map[string]MetricsGetter
	providers []meter.Provider
	gMux      sync.RWMutex
}

// RegisterProvider register a metrics provider.
func (c *Collector) RegisterProvider(provider meter.Provider) {
	c.gMux.Lock()
	defer c.gMux.Unlock()
	registerMetrics(provider)
	c.providers = append(c.providers, provider)
}

// Register registers a metrics getter.
func (c *Collector) Register(name string, getter MetricsGetter) {
	c.gMux.Lock()
	defer c.gMux.Unlock()
	c.getters[name] = getter
}

// Unregister unregisters a metrics getter.
func (c *Collector) Unregister(name string) {
	c.gMux.Lock()
	defer c.gMux.Unlock()
	delete(c.getters, name)
}

func (c *Collector) collect() {
	c.gMux.RLock()
	defer c.gMux.RUnlock()
	for _, provider := range c.providers {
		for _, getter := range c.getters {
			getter(provider)
		}
	}
}
