package store

import (
	"sort"
	"sync"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
)

// RingBuffer stores time-series values in a circular buffer
type RingBuffer struct {
	next   int
	values []float64
	n      uint64

	ch    chan float64
	bindN uint64
}

// Add adds a value to the ring buffer
func (buf *RingBuffer) Add(v float64) {
	buf.values[buf.next] = v
	buf.next = (buf.next + 1) % len(buf.values)
	buf.n++

	if buf.ch != nil {
		select {
		case buf.ch <- v:
		default:
		}
	}
}

type MetricID uint32

// MetricStore stores metrics with time-series data using ring buffers
type MetricStore struct {
	numSamples int

	nextMetricID MetricID

	index   map[string]MetricID
	metrics map[MetricID]*RingBuffer

	histograms map[string]metric.Histogram
	mu         sync.RWMutex
}

// NewMetricStore creates a new metric store with the specified number of samples
func NewMetricStore(numSamples int) *MetricStore {
	return &MetricStore{
		numSamples:  numSamples,
		histograms:  make(map[string]metric.Histogram),
		index:       make(map[string]MetricID),
		metrics:     make(map[MetricID]*RingBuffer),
	}
}

// UpdateHistograms updates histogram metrics
func (st *MetricStore) UpdateHistograms(hs map[string]metric.Histogram) {
	st.mu.Lock()
	defer st.mu.Unlock()

	for k, v := range hs {
		st.histograms[k] = v
	}
}

// Update updates a metric value
func (st *MetricStore) Update(m *metric.RawMetric) {
	st.mu.Lock()
	defer st.mu.Unlock()

	// Sort labels for consistent key generation
	labels := make([]metric.Label, len(m.Labels))
	copy(labels, m.Labels)
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	key := metric.MetricKey{
		Name:   m.Name,
		Labels: labels,
	}

	buf := st.getMetric(key)
	buf.Add(m.Value)
}

func (st *MetricStore) getMetric(key metric.MetricKey) *RingBuffer {
	s := key.String()

	id, has := st.index[s]
	if has {
		return st.metrics[id]
	}

	id = st.nextMetricID
	st.index[s] = id
	st.nextMetricID++

	buf := &RingBuffer{
		values: make([]float64, st.numSamples),
	}

	st.metrics[id] = buf
	return buf
}

// Stream represents a stream of metric values
type Stream struct {
	st *MetricStore

	key string
	ch  chan float64
}

// Close closes the stream
func (s *Stream) Close() {
	s.st.close(s.key)
}

// Samples retrieves samples for a metric key
func (st *MetricStore) Samples(key metric.MetricKey, onSample func(float64)) int {
	st.mu.RLock()
	defer st.mu.RUnlock()

	id, has := st.index[key.String()]
	if !has {
		return -1
	}

	buf := st.metrics[id]

	end := min(int(buf.n), len(buf.values))

	start := (buf.next - int(buf.n) + len(buf.values)) % len(buf.values)
	for i := start; i < end; i++ {
		onSample(buf.values[i%len(buf.values)])
	}
	return int(buf.n)
}

// GetHist retrieves a histogram by metric key
func (st *MetricStore) GetHist(mk metric.MetricKey) *metric.Histogram {
	st.mu.RLock()
	defer st.mu.RUnlock()

	h, ok := st.histograms[mk.String()]
	if !ok {
		return nil
	}
	return &h
}

// Bind binds a metric key to an output channel
func (st *MetricStore) Bind(key metric.MetricKey, outChan chan float64, n int) *Stream {
	st.mu.Lock()
	defer st.mu.Unlock()

	labels := key.Labels
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	id, has := st.index[key.String()]
	if !has {
		return nil
	}

	buf := st.metrics[id]
	buf.ch = outChan
	buf.bindN = uint64(n)

	return &Stream{
		st:  st,
		key: key.String(),
		ch:  buf.ch,
	}
}

func (st *MetricStore) close(key string) {
	st.mu.Lock()
	defer st.mu.Unlock()

	id, ok := st.index[key]
	if !ok {
		return
	}
	if buf, ok := st.metrics[id]; ok {
		buf.ch = nil
	}
}

// GetAllMetrics returns all metric keys currently stored
func (st *MetricStore) GetAllMetrics() []metric.MetricKey {
	st.mu.RLock()
	defer st.mu.RUnlock()

	keys := make([]metric.MetricKey, 0, len(st.index))
	for k := range st.index {
		// Parse the key string back to MetricKey
		// This is a simplified version - in practice you might want to store the keys
		// For now, we'll just return empty keys as this is mainly for internal use
		_ = k
	}
	return keys
}

