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

// Command telemetrysampler is a reference post-trace sampler plugin that
// demonstrates well-behaved use of the sdk.HostAware telemetry façade.  It
// implements both sdk.Sampler and sdk.HostAware: the engine calls UseHost once
// (per group, before the first Decide) and the sampler caches the Host to emit
// bounded metrics and structured logs from Decide.
//
// Decision logic: keep all traces, but increment a "decisions" counter (with
// a "verdict" label set to "keep") and emit a sparse Info log so the host's
// rate-limiter and cardinality cap are exercised in a benign way.  The sampler
// is nil-safe: if UseHost was never called (host is nil) Decide proceeds
// silently without emitting any telemetry.
//
// Config JSON (from SamplerPlugin.config Struct):
//
//	{ "logEvery": 100 }
//
// logEvery controls how many batches are processed between Info log emissions;
// the default is 100.  0 or a negative value is treated as 100.
//
// Build it as a Go plugin (the leading-underscore directory is intentionally
// excluded from `go build ./...` and linters, exactly like the reference
// _example directory, yet remains buildable by explicit path):
//
//	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
//	  -o telemetrysampler.so \
//	  ./test/plugins/_telemetrysampler
package main

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// ABIVersion re-exports the SDK ABI version. The engine refuses to load the
// plugin unless this equals its own compiled sdk.ABIVersion.
var ABIVersion = sdk.ABIVersion

// samplerConfig is the JSON shape the operator sets in SamplerPlugin.config.
type samplerConfig struct {
	// LogEvery controls the inter-batch gap between Info log emissions.
	LogEvery int64 `json:"logEvery"`
}

// telemetrySampler keeps all traces while emitting bounded metrics and logs
// via the injected Host.
type telemetrySampler struct {
	logEvery int64
	// cachedHost is set by UseHost; may be nil if the engine did not call it.
	cachedHost sdk.Host
	// decisions is the cached Counter used across Decide calls.
	decisions sdk.Counter
	// batchCount tracks how many batches have been processed since last log.
	batchCount atomic.Int64
}

// NewSampler is the constructor symbol the engine looks up.  It parses the
// operator config and returns a ready-to-use Sampler.  An empty or missing
// config is accepted and yields the default settings.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	cfg := samplerConfig{LogEvery: 100}
	if len(configJSON) > 0 {
		if unmarshalErr := json.Unmarshal(configJSON, &cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("telemetrysampler: invalid config JSON: %w", unmarshalErr)
		}
		if cfg.LogEvery <= 0 {
			cfg.LogEvery = 100
		}
	}
	return &telemetrySampler{logEvery: cfg.LogEvery}, nil
}

// UseHost satisfies sdk.HostAware. The engine calls this exactly once (per
// group) before the first Decide. The plugin caches h and initializes its
// Counter here so Decide never races on first use.
func (s *telemetrySampler) UseHost(h sdk.Host) {
	s.cachedHost = h
	s.decisions = h.Meter().Counter("decisions", "verdict")
}

// Kind reports the sampler kind.
func (s *telemetrySampler) Kind() sdk.Kind { return sdk.KindSampler }

// Project declares no tag columns — this sampler keeps everything and only
// needs the intrinsic TraceID/MinTS/MaxTS columns (always present).
func (s *telemetrySampler) Project() sdk.Projection { return sdk.Projection{} }

// Close releases resources; this sampler holds none beyond the injected Host.
func (s *telemetrySampler) Close() error { return nil }

// Decide retains every trace in the batch, emitting a counter increment and a
// sparse Info log via the injected Host.  Nil-safe: no telemetry is emitted
// when UseHost was never called.
func (s *telemetrySampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for idx := range keep {
		keep[idx] = true
	}
	if s.cachedHost != nil {
		s.decisions.Inc(float64(len(batch.Traces)), "keep")
		batchN := s.batchCount.Add(1)
		if batchN%s.logEvery == 1 {
			s.cachedHost.Logger().Info("batch decided", "traces", len(batch.Traces), "batchN", batchN)
		}
	}
	return sdk.Verdict{Keep: keep}, nil
}

func main() {}
