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

// Package pressureprofiler captures heap and goroutine pprof profiles from the
// monitored BanyanDB container when its RSS approaches the cgroup memory limit,
// stores them on a shared volume, and exposes them to the FODC proxy.
package pressureprofiler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/fodc/internal/pprofcapture"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// rssMetricName is the BanyanDB-exposed resident set size used as the usage signal.
	rssMetricName = "process_resident_memory_bytes"
	// limitMetricName is the BanyanDB-exposed raw cgroup memory.max gauge.
	limitMetricName = "banyandb_memory_protector_cgroup_limit_bytes"
	// captureHTTPTimeout bounds each pprof fetch.
	captureHTTPTimeout = 30 * time.Second
	// profileIDLayout names an event directory by its UTC nanosecond capture time.
	profileIDLayout = "20060102T150405.000000000Z"
)

// Config holds the pressure profiler settings derived from the agent flags.
type Config struct {
	RoleProvider   func() string
	Dir            string
	PprofPort      string
	PodName        string
	Cooldown       time.Duration
	MaxDiskBytes   int64
	TriggerPercent int
	MaxArtifacts   int
}

// Profiler watches the flight recorder for memory pressure and captures pprof profiles.
type Profiler struct {
	fr                   metricSource
	log                  *logger.Logger
	store                *store
	httpClient           *http.Client
	captureTotal         meter.Counter
	skippedCooldownTotal meter.Counter
	failuresTotal        meter.Counter
	cfg                  Config
	lastCapture          atomic.Int64
	capturing            atomic.Bool
}

// metricSource is the slice of the flight recorder the profiler depends on.
type metricSource interface {
	LatestValues(names []string, labelFilter map[string]string) map[string]float64
}

// New creates a Profiler and runs the storage self-check, returning an error if the storage
// directory cannot be created or written so the caller can fail startup rather than discover
// the misconfiguration at OOM time. Counters are created on the provider, whose scope prefixes
// their exposed names with "fodc_agent_". Once created, the profiler is driven by OnPollComplete,
// which the watchdog invokes after each metrics poll.
func New(cfg Config, fr metricSource, provider meter.Provider, log *logger.Logger) (*Profiler, error) {
	p := &Profiler{
		cfg:                  cfg,
		fr:                   fr,
		log:                  log,
		store:                newStore(cfg.Dir, cfg.MaxArtifacts, cfg.MaxDiskBytes, log),
		httpClient:           &http.Client{Timeout: captureHTTPTimeout},
		captureTotal:         provider.Counter("pressure_capture_total"),
		skippedCooldownTotal: provider.Counter("pressure_skipped_cooldown_total"),
		failuresTotal:        provider.Counter("pressure_failures_total", "reason"),
	}
	if err := p.store.selfCheck(); err != nil {
		return nil, fmt.Errorf("pressure profiler self-check failed: %w", err)
	}
	log.Info().Str("dir", cfg.Dir).Int("trigger_percent", cfg.TriggerPercent).Msg("pressure profiler started")
	return p, nil
}

// OnPollComplete evaluates memory pressure using the values the watchdog just scraped into
// the flight recorder. The cheap trigger decision runs inline on the poll goroutine; an actual
// capture (which fetches pprof over HTTP) is launched on its own goroutine so it never stalls
// metrics polling, and the capturing guard prevents overlapping captures.
func (p *Profiler) OnPollComplete(ctx context.Context) {
	values := p.fr.LatestValues([]string{rssMetricName, limitMetricName}, nil)
	rss, okRSS := values[rssMetricName]
	limit, okLimit := values[limitMetricName]
	if !okRSS || !okLimit || limit <= 0 {
		return
	}
	threshold := limit * float64(p.cfg.TriggerPercent) / 100
	if rss < threshold {
		p.log.Debug().
			Float64("rss_bytes", rss).
			Float64("cgroup_limit_bytes", limit).
			Float64("threshold_bytes", threshold).
			Int("trigger_percent", p.cfg.TriggerPercent).
			Msg("memory pressure below trigger threshold, skipping capture")
		return
	}

	now := time.Now()
	last := p.lastCapture.Load()
	if last != 0 && now.Sub(time.Unix(0, last)) < p.cfg.Cooldown {
		p.skippedCooldownTotal.Inc(1)
		p.log.Info().
			Float64("rss_bytes", rss).
			Float64("cgroup_limit_bytes", limit).
			Float64("threshold_bytes", threshold).
			Dur("since_last_capture", now.Sub(time.Unix(0, last))).
			Dur("cooldown", p.cfg.Cooldown).
			Msg("memory pressure threshold reached but within cooldown, skipping capture")
		return
	}

	if !p.capturing.CompareAndSwap(false, true) {
		// A capture launched by an earlier poll is still running.
		p.log.Info().
			Float64("rss_bytes", rss).
			Float64("cgroup_limit_bytes", limit).
			Float64("threshold_bytes", threshold).
			Msg("memory pressure threshold reached but a capture is already in progress, skipping")
		return
	}
	p.log.Info().
		Float64("rss_bytes", rss).
		Float64("cgroup_limit_bytes", limit).
		Float64("threshold_bytes", threshold).
		Int("trigger_percent", p.cfg.TriggerPercent).
		Msg("memory pressure threshold reached, launching pprof capture")
	rssBytes, limitBytes, thresholdBytes := uint64(rss), uint64(limit), uint64(threshold)
	run.Go(ctx, "fodc.agent.pressure-capture", p.log, func(ctx context.Context) {
		defer p.capturing.Store(false)
		p.capture(ctx, now, rssBytes, limitBytes, thresholdBytes)
	})
}

// capture pulls every target profile into a unique event directory and finalizes the
// event with a meta.json. A failed fetch is counted but never aborts the event: even
// with no profiles the record preserves the "RSS X near limit Y" moment.
func (p *Profiler) capture(ctx context.Context, at time.Time, rss, limit, threshold uint64) {
	profileID := at.UTC().Format(profileIDLayout)
	dir := p.store.eventDir(profileID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		p.failuresTotal.Inc(1, "mkdir")
		p.log.Warn().Err(err).Str("dir", dir).Msg("failed to create pressure event directory")
		return
	}

	base := fmt.Sprintf("http://localhost:%s", p.cfg.PprofPort)
	infos := make([]ProfileInfo, 0, len(pprofcapture.Targets))
	for _, target := range pprofcapture.Targets {
		filename := target.Type + ".pprof"
		dest := filepath.Join(dir, filename)
		n, err := fetchProfile(ctx, p.httpClient, base+target.Path, dest)
		if err != nil {
			p.failuresTotal.Inc(1, "fetch")
			p.log.Warn().Err(err).Str("type", target.Type).Msg("failed to fetch pprof profile")
			continue
		}
		infos = append(infos, ProfileInfo{
			Type:      target.Type,
			Filename:  filename,
			Filepath:  dest,
			Format:    "pprof",
			SizeBytes: n,
		})
	}

	m := meta{
		CapturedAt:       at.UTC().Format(time.RFC3339Nano),
		PodName:          p.cfg.PodName,
		Role:             p.role(),
		SourceEndpoint:   base,
		RSSBytes:         rss,
		CgroupLimitBytes: limit,
		TriggerPercent:   uint32(p.cfg.TriggerPercent),
		ThresholdBytes:   threshold,
		Profiles:         infos,
	}
	if err := p.store.finalize(profileID, m); err != nil {
		p.failuresTotal.Inc(1, "finalize")
		p.log.Error().Err(err).Str("profile_id", profileID).Msg("failed to finalize pressure event")
		return
	}
	// Start the cooldown only from a successful capture; a failed one leaves lastCapture
	// untouched so the next poll retries immediately. The capturing CAS already prevents
	// overlapping captures, so nothing needs to reserve the window before this point.
	p.lastCapture.Store(at.UnixNano())
	p.captureTotal.Inc(1)
	p.log.Info().
		Str("profile_id", profileID).
		Uint64("rss_bytes", rss).
		Uint64("cgroup_limit_bytes", limit).
		Int("profiles", len(infos)).
		Msg("captured memory-pressure pprof profiles")
}

func (p *Profiler) role() string {
	if p.cfg.RoleProvider == nil {
		return ""
	}
	return p.cfg.RoleProvider()
}

// ListProfileRecords returns the metadata of every complete capture event on disk.
func (p *Profiler) ListProfileRecords() []ProfileRecord {
	return p.store.list()
}

// OpenProfile validates the path is within the storage directory and returns an open
// handle for streaming; the caller closes it.
func (p *Profiler) OpenProfile(path string) (io.ReadCloser, error) {
	return p.store.open(path)
}
