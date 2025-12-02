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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package poller

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/metric"
)

type MetricsSnapshot struct {
	Timestamp   time.Time
	RawMetrics  []metric.RawMetric
	Histograms  map[string]metric.Histogram
	Errors      []string
}

type MetricsPoller struct {
	url          string
	interval     time.Duration
	client       *http.Client
	mu           sync.RWMutex
	lastSnapshot *MetricsSnapshot
}

func NewMetricsPoller(url string, interval time.Duration) *MetricsPoller {
	return &MetricsPoller{
		url:      url,
		interval: interval,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (p *MetricsPoller) Start(ctx context.Context, outChan chan<- MetricsSnapshot) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	// Initial poll
	p.poll(ctx, outChan)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.poll(ctx, outChan)
		}
	}
}

func (p *MetricsPoller) poll(ctx context.Context, outChan chan<- MetricsSnapshot) {
	snapshot := MetricsSnapshot{
		Timestamp:  time.Now(),
		RawMetrics: []metric.RawMetric{},
		Histograms: make(map[string]metric.Histogram),
		Errors:     []string{},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", p.url, nil)
	if err != nil {
		snapshot.Errors = append(snapshot.Errors, fmt.Sprintf("Failed to create request: %v", err))
		outChan <- snapshot
		return
	}

	resp, err := p.client.Do(req)
	if err != nil {
		snapshot.Errors = append(snapshot.Errors, fmt.Sprintf("Failed to fetch metrics: %v", err))
		outChan <- snapshot
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snapshot.Errors = append(snapshot.Errors, fmt.Sprintf("Non-200 status: %d", resp.StatusCode))
		outChan <- snapshot
		return
	}

	if err := p.parseMetrics(resp.Body, &snapshot); err != nil {
		snapshot.Errors = append(snapshot.Errors, fmt.Sprintf("Failed to parse metrics: %v", err))
	}

	p.mu.Lock()
	p.lastSnapshot = &snapshot
	p.mu.Unlock()

	outChan <- snapshot
}

func (p *MetricsPoller) parseMetrics(reader io.Reader, snapshot *MetricsSnapshot) error {
	scanner := bufio.NewScanner(reader)
	rawMetrics := make([]metric.RawMetric, 0, 100)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse Prometheus metric format: metric_name{labels} value
		m, err := metric.ParseMetricLine(line)
		if err != nil {
			// Skip invalid lines but continue parsing
			continue
		}

		rawMetrics = append(rawMetrics, m)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	// Parse histograms from raw metrics
	histograms, filteredMetrics := metric.ParseHistogram(rawMetrics)
	snapshot.Histograms = histograms
	snapshot.RawMetrics = filteredMetrics

	return nil
}

func (p *MetricsPoller) GetLastSnapshot() *MetricsSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastSnapshot
}

