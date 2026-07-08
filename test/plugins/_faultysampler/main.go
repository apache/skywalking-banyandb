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

// Command faultysampler is an adversarial reference post-trace sampler plugin
// that verifies the engine's host-side bounds hold under hostile plugin
// behaviour.  It implements both sdk.Sampler and sdk.HostAware, but its config
// selects one or more fault modes that deliberately abuse the telemetry façade:
//
//   - panicInUseHost: UseHost panics immediately; the engine must isolate the
//     panic so telemetry is disabled for this plugin but the plugin continues
//     to run (fail-open) and does not crash the data node.
//
//   - floodLogs: Decide emits thousands of Logger.Info calls per batch; the
//     host's rate-limiter must clamp throughput to its declared budget (50
//     lines/s burst 100) so the data node's log stream is not flooded.
//
//   - explodeCardinality: Decide calls Meter.Counter with a unique label value
//     per invocation, producing unbounded distinct series; the host's
//     cardinality cap (100 series per plugin) must clamp the overflow so
//     Prometheus scrape memory does not grow without bound.
//
// Decision logic: always retain all traces (fail-open) regardless of fault
// mode, so liveness is not affected by the adversarial telemetry behaviour.
// Nil-safe: if UseHost was never called (host is nil — e.g. because the engine
// isolated a panic in UseHost) Decide proceeds without emitting any telemetry.
//
// Config JSON (from SamplerPlugin.config Struct):
//
//	{
//	  "panicInUseHost":       false,
//	  "floodLogs":            false,
//	  "explodeCardinality":   false,
//	  "floodCount":           10000
//	}
//
// All fields default to false / 10000.  Multiple fault modes may be combined.
//
// Build it as a Go plugin (the leading-underscore directory is intentionally
// excluded from `go build ./...` and linters, exactly like the reference
// _example directory, yet remains buildable by explicit path):
//
//	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
//	  -o faultysampler.so \
//	  ./test/plugins/_faultysampler
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
	// PanicInUseHost causes UseHost to panic; the engine must recover.
	PanicInUseHost bool `json:"panicInUseHost"`
	// FloodLogs causes Decide to emit FloodCount Logger.Info calls per batch.
	FloodLogs bool `json:"floodLogs"`
	// ExplodeCardinality causes Decide to register a new Counter with a unique
	// label value on every call, exhausting the host cardinality cap.
	ExplodeCardinality bool `json:"explodeCardinality"`
	// FloodCount is the number of log lines emitted per batch when FloodLogs is
	// true; defaults to 10000.
	FloodCount int `json:"floodCount"`
}

// faultySampler is a deliberately adversarial plugin for host-bounds testing.
type faultySampler struct {
	cfg        samplerConfig
	cachedHost sdk.Host
	// callSeq is a monotonically increasing call counter used to generate
	// unique cardinality-explosion label values.
	callSeq atomic.Int64
}

// NewSampler is the constructor symbol the engine looks up.  It parses the
// operator config and returns a ready-to-use Sampler.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	cfg := samplerConfig{FloodCount: 10000}
	if len(configJSON) > 0 {
		if unmarshalErr := json.Unmarshal(configJSON, &cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("faultysampler: invalid config JSON: %w", unmarshalErr)
		}
		if cfg.FloodCount <= 0 {
			cfg.FloodCount = 10000
		}
	}
	return &faultySampler{cfg: cfg}, nil
}

// UseHost satisfies sdk.HostAware.  When panicInUseHost is set it panics
// immediately; the engine must recover from the panic so the plugin continues
// to run and the data node does not crash.  Otherwise the Host is cached for
// use in Decide.
func (s *faultySampler) UseHost(h sdk.Host) {
	if s.cfg.PanicInUseHost {
		panic("faultysampler: intentional panic in UseHost (panicInUseHost=true)")
	}
	s.cachedHost = h
}

// Kind reports the sampler kind.
func (s *faultySampler) Kind() sdk.Kind { return sdk.KindSampler }

// Project declares no tag columns — this sampler keeps everything regardless.
func (s *faultySampler) Project() sdk.Projection { return sdk.Projection{} }

// Close releases resources; this sampler holds none.
func (s *faultySampler) Close() error { return nil }

// Decide always retains every trace (fail-open) while exercising the selected
// host-bounds fault modes.  All telemetry calls are nil-safe: they are skipped
// when cachedHost is nil (e.g. because the engine isolated a UseHost panic).
func (s *faultySampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for idx := range keep {
		keep[idx] = true
	}
	if s.cachedHost == nil {
		return sdk.Verdict{Keep: keep}, nil
	}

	if s.cfg.FloodLogs {
		logger := s.cachedHost.Logger()
		for i := 0; i < s.cfg.FloodCount; i++ {
			logger.Info("flood log line", "i", i, "traces", len(batch.Traces))
		}
	}

	if s.cfg.ExplodeCardinality {
		seq := s.callSeq.Add(1)
		uniqueID := fmt.Sprintf("uid-%d", seq)
		s.cachedHost.Meter().Counter("boom", "id").Inc(1, uniqueID)
	}

	return sdk.Verdict{Keep: keep}, nil
}

func main() {}
