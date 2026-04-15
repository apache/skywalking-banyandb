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

package benchmark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// PromMetrics contains raw counters and gauges scraped from /metrics endpoints.
type PromMetrics struct {
	CPUTotalSeconds float64
	RSSBytes        float64
	CPUNum          float64
	TotalMemBytes   float64
}

// AggregatedSample represents one aggregated metrics scrape point.
type AggregatedSample struct {
	Timestamp   time.Time
	CPUSeconds  float64
	RSSBytes    float64
	CPUNum      float64
	TotalMemory float64
}

// AggregatedSeries stores samples collected over a benchmark phase.
type AggregatedSeries struct {
	Samples []AggregatedSample
}

// ResourceStatsSeries summarizes CPU and memory usage over a phase.
type ResourceStatsSeries struct {
	MeanCPUPercent float64
	PeakCPUPercent float64
	PeakRSSBytes   int64
	PeakRSSPercent float64
}

func parsePromMetrics(r io.Reader) (PromMetrics, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return PromMetrics{}, err
	}
	metrics := PromMetrics{}
	if mf, ok := families["process_cpu_seconds_total"]; ok {
		for _, m := range mf.Metric {
			if m.Counter != nil {
				metrics.CPUTotalSeconds += m.Counter.GetValue()
			} else if m.Untyped != nil {
				metrics.CPUTotalSeconds += m.Untyped.GetValue()
			}
		}
	}
	if mf, ok := families["process_resident_memory_bytes"]; ok {
		for _, m := range mf.Metric {
			if m.Gauge != nil {
				metrics.RSSBytes += m.Gauge.GetValue()
			} else if m.Untyped != nil {
				metrics.RSSBytes += m.Untyped.GetValue()
			}
		}
	}
	if mf, ok := families["banyandb_system_cpu_num"]; ok {
		for _, m := range mf.Metric {
			if m.Gauge != nil {
				metrics.CPUNum = m.Gauge.GetValue()
				break
			}
			if m.Untyped != nil {
				metrics.CPUNum = m.Untyped.GetValue()
				break
			}
		}
	}
	if mf, ok := families["banyandb_system_memory_state"]; ok {
		for _, m := range mf.Metric {
			val := 0.0
			if m.Gauge != nil {
				val = m.Gauge.GetValue()
			} else if m.Untyped != nil {
				val = m.Untyped.GetValue()
			}
			if val == 0 {
				continue
			}
			for _, label := range m.Label {
				if label.GetName() == "kind" && label.GetValue() == "total" {
					metrics.TotalMemBytes += val
				}
			}
		}
	}
	return metrics, nil
}

func scrapeMetrics(ctx context.Context, url string) (PromMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return PromMetrics{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return PromMetrics{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return PromMetrics{}, fmt.Errorf("metrics endpoint %s returned %s", url, resp.Status)
	}
	return parsePromMetrics(resp.Body)
}

func collectSeries(ctx context.Context, interval time.Duration, endpoints []string) (AggregatedSeries, error) {
	series := AggregatedSeries{}
	if interval <= 0 {
		return series, fmt.Errorf("metrics collection interval must be > 0")
	}
	if len(endpoints) == 0 {
		return series, fmt.Errorf("metrics endpoints must not be empty")
	}

	scrapeAll := func(scrapeCtx context.Context, t time.Time) error {
		var sum PromMetrics
		for _, ep := range endpoints {
			metrics, err := scrapeMetrics(scrapeCtx, ep)
			if err != nil {
				return err
			}
			sum.CPUTotalSeconds += metrics.CPUTotalSeconds
			sum.RSSBytes += metrics.RSSBytes
			sum.CPUNum += metrics.CPUNum
			sum.TotalMemBytes += metrics.TotalMemBytes
		}
		series.Samples = append(series.Samples, AggregatedSample{
			Timestamp:   t,
			CPUSeconds:  sum.CPUTotalSeconds,
			RSSBytes:    sum.RSSBytes,
			CPUNum:      sum.CPUNum,
			TotalMemory: sum.TotalMemBytes,
		})
		return nil
	}

	if err := scrapeAll(ctx, time.Now()); err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return series, nil
		}
		return series, err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_ = scrapeAll(finalCtx, time.Now())
			cancel()
			return series, nil
		case t := <-ticker.C:
			if ctx.Err() != nil {
				return series, nil
			}
			if err := scrapeAll(ctx, t); err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return series, nil
				}
				return series, err
			}
		}
	}
}

func summarizeSeries(series AggregatedSeries) ResourceStatsSeries {
	stats := ResourceStatsSeries{}
	if len(series.Samples) < 2 {
		return stats
	}
	var cpuPercents []float64
	var peakRSS float64
	var peakRSSPercent float64
	for i := 1; i < len(series.Samples); i++ {
		prev := series.Samples[i-1]
		curr := series.Samples[i]
		interval := curr.Timestamp.Sub(prev.Timestamp).Seconds()
		if interval <= 0 {
			continue
		}
		cpuDelta := curr.CPUSeconds - prev.CPUSeconds
		cpuNum := curr.CPUNum
		if cpuNum > 0 {
			cpuPercent := (cpuDelta / interval) / cpuNum * 100
			cpuPercents = append(cpuPercents, cpuPercent)
			if cpuPercent > stats.PeakCPUPercent {
				stats.PeakCPUPercent = cpuPercent
			}
		}
		if curr.RSSBytes > peakRSS {
			peakRSS = curr.RSSBytes
			if curr.TotalMemory > 0 {
				peakRSSPercent = (curr.RSSBytes / curr.TotalMemory) * 100
			}
		}
	}
	stats.PeakRSSBytes = int64(peakRSS)
	stats.PeakRSSPercent = peakRSSPercent
	if len(cpuPercents) > 0 {
		sum := 0.0
		for _, v := range cpuPercents {
			sum += v
		}
		stats.MeanCPUPercent = sum / float64(len(cpuPercents))
	}
	return stats
}

func summarizeLatencies(durations []time.Duration) ReadResult {
	if len(durations) == 0 {
		return ReadResult{}
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	toMs := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	median := percentile(sorted, 0.50)
	p95 := percentile(sorted, 0.95)
	p99 := percentile(sorted, 0.99)
	return ReadResult{
		Samples:  len(sorted),
		MinMs:    toMs(sorted[0]),
		MedianMs: toMs(median),
		P95Ms:    toMs(p95),
		P99Ms:    toMs(p99),
		MaxMs:    toMs(sorted[len(sorted)-1]),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	pos := p * float64(len(sorted)-1)
	idx := int(pos)
	if idx >= len(sorted)-1 {
		return sorted[len(sorted)-1]
	}
	frac := pos - float64(idx)
	lower := sorted[idx]
	upper := sorted[idx+1]
	return lower + time.Duration(frac*float64(upper-lower))
}
