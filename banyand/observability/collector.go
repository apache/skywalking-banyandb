package observability

import (
	"sync"
)

// MetricsCollector is a global metrics collector.
var MetricsCollector = Collector{
	getters: make(map[string]MetricsGetter),
}

// MetricsGetter is a function that collects metrics.
type MetricsGetter func()

// Collector is a metrics collector.
type Collector struct {
	getters map[string]MetricsGetter
	gMux    sync.RWMutex
}

// Register registers a metrics getter.
func (c *Collector) Register(name string, getter MetricsGetter) {
	c.gMux.Lock()
	defer c.gMux.Unlock()
	c.getters[name] = getter
}

func (c *Collector) collect() {
	c.gMux.RLock()
	defer c.gMux.RUnlock()
	for _, getter := range c.getters {
		getter()
	}
}
