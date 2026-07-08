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
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/rs/zerolog"

	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

const (
	// maxSeriesPerInstrument is the cardinality cap for custom label-value tuples
	// per instrument instance.
	maxSeriesPerInstrument = 100
	// maxMetricNameLen is the maximum sanitized metric name length.
	maxMetricNameLen = 64
	// logRatePerSec is the sustained token-bucket refill rate (tokens/second).
	logRatePerSec = 50.0
	// logBurstSize is the maximum burst capacity of the log token bucket.
	logBurstSize = 100.0
	// maxMsgLen is the maximum log message length in bytes.
	maxMsgLen = 1024
	// maxLogFields is the maximum number of structured key/value pairs emitted.
	maxLogFields = 16
	// suppressWarnInterval is the minimum interval between rate-limiter summary WARNs.
	suppressWarnInterval = 5 * time.Second
	// overflowSentinel is the fixed label value substituted when cardinality is exceeded.
	overflowSentinel = "__overflow__"
)

// reservedLabelSet contains label names plugins may not declare; the host
// always prepends group and plugin_name itself.
var reservedLabelSet = map[string]struct{}{
	"group":       {},
	"plugin_name": {},
}

// noopCounter is a zero-allocation sdk.Counter for the bypass path.
type noopCounter struct{}

func (noopCounter) Inc(_ float64, _ ...string) {}

// noopGauge is a zero-allocation sdk.Gauge for the bypass path.
type noopGauge struct{}

func (noopGauge) Set(_ float64, _ ...string) {}
func (noopGauge) Add(_ float64, _ ...string) {}

// noopHistogram is a zero-allocation sdk.Histogram for the bypass path.
type noopHistogram struct{}

func (noopHistogram) Observe(_ float64, _ ...string) {}

// noopMeter is a zero-allocation sdk.Meter for the bypass path.
type noopMeter struct{}

func (noopMeter) Counter(_ string, _ ...string) sdk.Counter                  { return noopCounter{} }
func (noopMeter) Gauge(_ string, _ ...string) sdk.Gauge                      { return noopGauge{} }
func (noopMeter) Histogram(_ string, _ []float64, _ ...string) sdk.Histogram { return noopHistogram{} }

// instrumentKey identifies a registered instrument in the cache by its full
// metric name and joined label names.
type instrumentKey struct {
	fullName   string
	labelNames string // "\x00"-joined
}

// boundInstrumentKind distinguishes the three instrument kinds.
type boundInstrumentKind uint8

const (
	kindCounter   boundInstrumentKind = iota
	kindGauge     boundInstrumentKind = iota
	kindHistogram boundInstrumentKind = iota
)

// cachedInstrument holds one registered instrument together with its kind tag.
type cachedInstrument struct {
	counter   *boundCounter
	gauge     *boundGauge
	histogram *boundHistogram
	kind      boundInstrumentKind
}

// boundCounter wraps a meter.Counter as sdk.Counter, enforcing cardinality.
type boundCounter struct {
	handle     meter.Counter
	sm         *samplerMetrics
	seen       map[string]struct{}
	group      string
	pluginName string
	emitted    [][]string
	arity      int
	mu         sync.Mutex
}

// Inc adds delta for the given custom label values, enforcing the cardinality cap.
func (c *boundCounter) Inc(delta float64, customValues ...string) {
	full := c.resolve(customValues)
	c.handle.Inc(delta, full...)
}

func (c *boundCounter) resolve(customValues []string) []string {
	custom := padOrTrim(customValues, c.arity)
	key := strings.Join(custom, "\x00")
	c.mu.Lock()
	_, known := c.seen[key]
	if !known {
		if len(c.seen) >= maxSeriesPerInstrument {
			c.mu.Unlock()
			c.sm.incSeriesRejected(c.group, c.pluginName)
			return makeOverflowValues(c.group, c.pluginName, c.arity)
		}
		c.seen[key] = struct{}{}
		full := prependHostLabels(c.group, c.pluginName, custom)
		c.emitted = append(c.emitted, full)
		c.mu.Unlock()
		return full
	}
	c.mu.Unlock()
	return prependHostLabels(c.group, c.pluginName, custom)
}

// boundGauge wraps a meter.Gauge as sdk.Gauge, enforcing cardinality.
type boundGauge struct {
	handle     meter.Gauge
	sm         *samplerMetrics
	seen       map[string]struct{}
	group      string
	pluginName string
	emitted    [][]string
	arity      int
	mu         sync.Mutex
}

// Set replaces the gauge value for the given custom label values.
func (g *boundGauge) Set(value float64, customValues ...string) {
	full := g.resolve(customValues)
	g.handle.Set(value, full...)
}

// Add adds delta to the gauge for the given custom label values.
func (g *boundGauge) Add(delta float64, customValues ...string) {
	full := g.resolve(customValues)
	g.handle.Add(delta, full...)
}

func (g *boundGauge) resolve(customValues []string) []string {
	custom := padOrTrim(customValues, g.arity)
	key := strings.Join(custom, "\x00")
	g.mu.Lock()
	_, known := g.seen[key]
	if !known {
		if len(g.seen) >= maxSeriesPerInstrument {
			g.mu.Unlock()
			g.sm.incSeriesRejected(g.group, g.pluginName)
			return makeOverflowValues(g.group, g.pluginName, g.arity)
		}
		g.seen[key] = struct{}{}
		full := prependHostLabels(g.group, g.pluginName, custom)
		g.emitted = append(g.emitted, full)
		g.mu.Unlock()
		return full
	}
	g.mu.Unlock()
	return prependHostLabels(g.group, g.pluginName, custom)
}

// boundHistogram wraps a meter.Histogram as sdk.Histogram, enforcing cardinality.
type boundHistogram struct {
	handle     meter.Histogram
	group      string
	pluginName string
	sm         *samplerMetrics
	seen       map[string]struct{}
	emitted    [][]string
	mu         sync.Mutex
	arity      int
}

// Observe records a single observation for the given custom label values.
func (h *boundHistogram) Observe(value float64, customValues ...string) {
	full := h.resolve(customValues)
	h.handle.Observe(value, full...)
}

func (h *boundHistogram) resolve(customValues []string) []string {
	custom := padOrTrim(customValues, h.arity)
	key := strings.Join(custom, "\x00")
	h.mu.Lock()
	_, known := h.seen[key]
	if !known {
		if len(h.seen) >= maxSeriesPerInstrument {
			h.mu.Unlock()
			h.sm.incSeriesRejected(h.group, h.pluginName)
			return makeOverflowValues(h.group, h.pluginName, h.arity)
		}
		h.seen[key] = struct{}{}
		full := prependHostLabels(h.group, h.pluginName, custom)
		h.emitted = append(h.emitted, full)
		h.mu.Unlock()
		return full
	}
	h.mu.Unlock()
	return prependHostLabels(h.group, h.pluginName, custom)
}

// pluginMeter is the sdk.Meter adapter for one (group, pluginName) pair.
type pluginMeter struct {
	factory    observability.Factory
	sm         *samplerMetrics
	cache      map[instrumentKey]*cachedInstrument
	group      string
	pluginName string
	mu         sync.Mutex
	bypass     bool
}

// Counter returns or creates a bounded sdk.Counter for the plugin.
func (m *pluginMeter) Counter(name string, labelNames ...string) sdk.Counter {
	if m.bypass {
		return noopCounter{}
	}
	clean, filtered, ok := m.prepare(name, labelNames)
	if !ok {
		return noopCounter{}
	}
	key := instrumentKey{fullName: clean, labelNames: strings.Join(filtered, "\x00")}
	m.mu.Lock()
	if ci, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return ci.counter
	}
	m.mu.Unlock()

	allLabels := prependHostLabels("group", "plugin_name", filtered)
	var handle meter.Counter
	func() {
		defer func() {
			if r := recover(); r != nil {
				m.sm.incSeriesRejected(m.group, m.pluginName)
				m.sm.incTelemetryPanic(m.group, m.pluginName)
			}
		}()
		handle = m.factory.NewCounter(clean, allLabels...)
	}()
	if handle == nil {
		return noopCounter{}
	}
	bc := &boundCounter{
		handle:     handle,
		group:      m.group,
		pluginName: m.pluginName,
		sm:         m.sm,
		seen:       make(map[string]struct{}),
		arity:      len(filtered),
	}
	m.mu.Lock()
	if existing, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return existing.counter
	}
	m.cache[key] = &cachedInstrument{kind: kindCounter, counter: bc}
	m.mu.Unlock()
	return bc
}

// Gauge returns or creates a bounded sdk.Gauge for the plugin.
func (m *pluginMeter) Gauge(name string, labelNames ...string) sdk.Gauge {
	if m.bypass {
		return noopGauge{}
	}
	clean, filtered, ok := m.prepare(name, labelNames)
	if !ok {
		return noopGauge{}
	}
	key := instrumentKey{fullName: clean, labelNames: strings.Join(filtered, "\x00")}
	m.mu.Lock()
	if ci, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return ci.gauge
	}
	m.mu.Unlock()

	allLabels := prependHostLabels("group", "plugin_name", filtered)
	var handle meter.Gauge
	func() {
		defer func() {
			if r := recover(); r != nil {
				m.sm.incSeriesRejected(m.group, m.pluginName)
				m.sm.incTelemetryPanic(m.group, m.pluginName)
			}
		}()
		handle = m.factory.NewGauge(clean, allLabels...)
	}()
	if handle == nil {
		return noopGauge{}
	}
	bg := &boundGauge{
		handle:     handle,
		group:      m.group,
		pluginName: m.pluginName,
		sm:         m.sm,
		seen:       make(map[string]struct{}),
		arity:      len(filtered),
	}
	m.mu.Lock()
	if existing, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return existing.gauge
	}
	m.cache[key] = &cachedInstrument{kind: kindGauge, gauge: bg}
	m.mu.Unlock()
	return bg
}

// Histogram returns or creates a bounded sdk.Histogram for the plugin.
func (m *pluginMeter) Histogram(name string, buckets []float64, labelNames ...string) sdk.Histogram {
	if m.bypass {
		return noopHistogram{}
	}
	clean, filtered, ok := m.prepare(name, labelNames)
	if !ok {
		return noopHistogram{}
	}
	key := instrumentKey{fullName: clean, labelNames: strings.Join(filtered, "\x00")}
	m.mu.Lock()
	if ci, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return ci.histogram
	}
	m.mu.Unlock()

	allLabels := prependHostLabels("group", "plugin_name", filtered)
	var handle meter.Histogram
	func() {
		defer func() {
			if r := recover(); r != nil {
				m.sm.incSeriesRejected(m.group, m.pluginName)
				m.sm.incTelemetryPanic(m.group, m.pluginName)
			}
		}()
		handle = m.factory.NewHistogram(clean, meter.Buckets(buckets), allLabels...)
	}()
	if handle == nil {
		return noopHistogram{}
	}
	bh := &boundHistogram{
		handle:     handle,
		group:      m.group,
		pluginName: m.pluginName,
		sm:         m.sm,
		seen:       make(map[string]struct{}),
		arity:      len(filtered),
	}
	m.mu.Lock()
	if existing, ok2 := m.cache[key]; ok2 {
		m.mu.Unlock()
		return existing.histogram
	}
	m.cache[key] = &cachedInstrument{kind: kindHistogram, histogram: bh}
	m.mu.Unlock()
	return bh
}

// prepare sanitizes the name and filters reserved label names. Returns the full
// metric name, filtered label names, and true on success; false means the caller
// must return a no-op instrument.
func (m *pluginMeter) prepare(name string, labelNames []string) (string, []string, bool) {
	sanitized := sanitizeMetricName(name)
	if sanitized == "" {
		m.sm.incSeriesRejected(m.group, m.pluginName)
		return "", nil, false
	}
	full := "plugin_" + sanitized
	filtered := make([]string, 0, len(labelNames))
	for _, ln := range labelNames {
		if _, reserved := reservedLabelSet[ln]; reserved {
			m.sm.incSeriesRejected(m.group, m.pluginName)
			continue
		}
		filtered = append(filtered, ln)
	}
	return full, filtered, true
}

// sanitizeMetricName lowercases the name, replaces characters outside
// [a-z0-9_] with '_', and truncates to maxMetricNameLen.
func sanitizeMetricName(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case unicode.IsUpper(r):
			b.WriteRune(unicode.ToLower(r))
		default:
			b.WriteByte('_')
		}
		if b.Len() >= maxMetricNameLen {
			break
		}
	}
	return b.String()
}

// makeOverflowValues builds the full label-values slice for the overflow sentinel.
func makeOverflowValues(group, pluginName string, customArity int) []string {
	vals := make([]string, 2+customArity)
	vals[0] = group
	vals[1] = pluginName
	for i := range customArity {
		vals[2+i] = overflowSentinel
	}
	return vals
}

// prependHostLabels returns a new slice with the two extra elements prepended to src.
func prependHostLabels(a, b string, src []string) []string {
	out := make([]string, 2+len(src))
	out[0] = a
	out[1] = b
	copy(out[2:], src)
	return out
}

// padOrTrim ensures customValues has exactly arity elements, padding with ""
// or trimming as needed.
func padOrTrim(customValues []string, arity int) []string {
	if len(customValues) == arity {
		return customValues
	}
	out := make([]string, arity)
	copy(out, customValues)
	return out
}

// pluginLogger is the sdk.Logger adapter for one (group, pluginName) pair.
// It enforces a token-bucket rate limit and caps message length and field count.
type pluginLogger struct {
	lastRefill   time.Time
	lastWarnEmit time.Time
	inner        *logger.Logger
	sm           *samplerMetrics
	group        string
	pluginName   string
	tokens       float64
	suppressed   int64
	mu           sync.Mutex
}

func newPluginLogger(baseLogger *logger.Logger, sm *samplerMetrics, group, pluginName string) *pluginLogger {
	inner := baseLogger.Named("plugin", strings.ToUpper(group), strings.ToUpper(pluginName))
	return &pluginLogger{
		inner:      inner,
		group:      group,
		pluginName: pluginName,
		sm:         sm,
		tokens:     logBurstSize,
		lastRefill: time.Now(),
	}
}

// acquire attempts to consume one token. Returns true if the caller may log.
// If a suppression summary is due it is emitted before returning true.
func (l *pluginLogger) acquire() bool {
	now := time.Now()
	l.mu.Lock()
	elapsed := now.Sub(l.lastRefill).Seconds()
	l.lastRefill = now
	l.tokens += elapsed * logRatePerSec
	if l.tokens > logBurstSize {
		l.tokens = logBurstSize
	}
	if l.tokens < 1 {
		l.suppressed++
		l.mu.Unlock()
		l.sm.incLogDropped(l.group, l.pluginName)
		return false
	}
	l.tokens--
	n := l.suppressed
	emitSummary := n > 0 && now.Sub(l.lastWarnEmit) >= suppressWarnInterval
	if emitSummary {
		l.suppressed = 0
		l.lastWarnEmit = now
	}
	l.mu.Unlock()
	if emitSummary {
		l.inner.Warn().Int64("suppressed", n).Msg("plugin log lines suppressed by rate limiter")
	}
	return true
}

// emitEvent attaches keysAndValues to ev and sends it.
func emitEvent(ev *zerolog.Event, msg string, keysAndValues []any) {
	// Truncate message.
	if len(msg) > maxMsgLen {
		msg = msg[:maxMsgLen]
	}
	// Cap fields to maxLogFields pairs.
	if len(keysAndValues) > maxLogFields*2 {
		keysAndValues = keysAndValues[:maxLogFields*2]
	}
	// Ensure even length (trailing odd key gets value "(MISSING)").
	if len(keysAndValues)%2 != 0 {
		keysAndValues = append(keysAndValues, "(MISSING)")
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		k := anyToString(keysAndValues[i])
		v := keysAndValues[i+1]
		if sv, ok := v.(string); ok {
			ev = ev.Str(k, sv)
		} else {
			ev = ev.Interface(k, v)
		}
	}
	ev.Msg(msg)
}

// anyToString converts an any key to string, falling back to fmt.Sprintf.
func anyToString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// Debug is a no-op — the adapter caps at INFO level.
func (l *pluginLogger) Debug(_ string, _ ...any) {}

// Info logs at INFO level with rate-limiting and field/message caps.
func (l *pluginLogger) Info(msg string, keysAndValues ...any) {
	if !l.acquire() {
		return
	}
	emitEvent(l.inner.Info(), msg, keysAndValues)
}

// Warn logs at WARN level with rate-limiting and field/message caps.
func (l *pluginLogger) Warn(msg string, keysAndValues ...any) {
	if !l.acquire() {
		return
	}
	emitEvent(l.inner.Warn(), msg, keysAndValues)
}

// Error logs at ERROR level with rate-limiting and field/message caps.
func (l *pluginLogger) Error(msg string, keysAndValues ...any) {
	if !l.acquire() {
		return
	}
	emitEvent(l.inner.Error(), msg, keysAndValues)
}

// pluginTelemetryRegistry tracks the live pluginTelemetry adapters per group so a
// group's plugin metric series can be Deleted when the group's samplers are
// removed or replaced. It is a leaf lock: it never calls back into pluginCache.
var pluginTelemetryRegistry = struct {
	m  map[string][]*pluginTelemetry
	mu sync.Mutex
}{m: map[string][]*pluginTelemetry{}}

// registerGroupTelemetry appends t to group's adapter slice, deduping by pointer
// so a re-registration of an already-tracked adapter cannot accumulate
// duplicates. In normal flow bindHost only fires on a loader cache-miss (a fresh
// instance ⇒ a fresh adapter), so each distinct adapter is registered once.
func registerGroupTelemetry(group string, t *pluginTelemetry) {
	pluginTelemetryRegistry.mu.Lock()
	defer pluginTelemetryRegistry.mu.Unlock()
	if slices.Contains(pluginTelemetryRegistry.m[group], t) {
		return
	}
	pluginTelemetryRegistry.m[group] = append(pluginTelemetryRegistry.m[group], t)
}

// teardownGroupTelemetry Deletes the metric series of every adapter tracked for
// group, but RETAINS the registry entries. An adapter's lifetime equals its
// immortal, never-Closed sampler instance's (both keyed by
// (path,symbol,configHash,group) in the loader cache): a config revert re-serves
// the cached instance, which re-emits through the SAME retained adapter, so the
// adapter must stay findable for a subsequent teardown — otherwise those
// re-created series would leak. Deleting an already-Deleted tuple is a harmless
// no-op, so re-tearing-down a retained adapter is safe. Retained adapters are
// bounded by the distinct (configHash,group) pairs seen over the process
// lifetime — the same bound as the immortal instances themselves.
func teardownGroupTelemetry(group string) {
	pluginTelemetryRegistry.mu.Lock()
	adapters := slices.Clone(pluginTelemetryRegistry.m[group])
	pluginTelemetryRegistry.mu.Unlock()
	for _, t := range adapters {
		t.teardown()
	}
}

// pluginTelemetry is the sdk.Host implementation for one (group, pluginName) pair.
type pluginTelemetry struct {
	meterAdapter  *pluginMeter
	loggerAdapter *pluginLogger
	sm            *samplerMetrics
}

// newPluginTelemetry constructs a pluginTelemetry adapter. If factory is nil
// the bypass path is used (no-op metrics, zero allocations on Inc/Set/Observe).
func newPluginTelemetry(factory observability.Factory, baseLogger *logger.Logger, sm *samplerMetrics, group, pluginName string) *pluginTelemetry {
	bypass := factory == nil
	if bypass {
		factory = observability.BypassRegistry.With(pipelineScope)
	}
	if baseLogger == nil {
		baseLogger = logger.GetLogger("trace")
	}
	return &pluginTelemetry{
		meterAdapter: &pluginMeter{
			factory:    factory,
			group:      group,
			pluginName: pluginName,
			sm:         sm,
			bypass:     bypass,
			cache:      make(map[instrumentKey]*cachedInstrument),
		},
		loggerAdapter: newPluginLogger(baseLogger, sm, group, pluginName),
		sm:            sm,
	}
}

// Meter returns the metric surface scoped to this plugin.
func (t *pluginTelemetry) Meter() sdk.Meter {
	if t.meterAdapter.bypass {
		return noopMeter{}
	}
	return t.meterAdapter
}

// Logger returns the logging surface scoped to this plugin.
func (t *pluginTelemetry) Logger() sdk.Logger {
	return t.loggerAdapter
}

// teardown deletes all metric series registered by this plugin and clears the
// cache. It is idempotent and panic-safe.
func (t *pluginTelemetry) teardown() {
	defer func() {
		if r := recover(); r != nil {
			t.sm.incTelemetryPanic(t.meterAdapter.group, t.meterAdapter.pluginName)
		}
	}()

	t.meterAdapter.mu.Lock()
	cache := t.meterAdapter.cache
	t.meterAdapter.cache = make(map[instrumentKey]*cachedInstrument)
	t.meterAdapter.mu.Unlock()

	for _, ci := range cache {
		switch ci.kind {
		case kindCounter:
			ci.counter.mu.Lock()
			emitted := ci.counter.emitted
			ci.counter.mu.Unlock()
			for _, labelValues := range emitted {
				safeDelete(func() { ci.counter.handle.Delete(labelValues...) })
			}
		case kindGauge:
			ci.gauge.mu.Lock()
			emitted := ci.gauge.emitted
			ci.gauge.mu.Unlock()
			for _, labelValues := range emitted {
				safeDelete(func() { ci.gauge.handle.Delete(labelValues...) })
			}
		case kindHistogram:
			ci.histogram.mu.Lock()
			emitted := ci.histogram.emitted
			ci.histogram.mu.Unlock()
			for _, labelValues := range emitted {
				safeDelete(func() { ci.histogram.handle.Delete(labelValues...) })
			}
		}
	}
}

// safeDelete calls fn recovering any panic so a single bad Delete cannot abort
// teardown of other series.
func safeDelete(fn func()) {
	defer func() { recover() }() //nolint:errcheck
	fn()
}
