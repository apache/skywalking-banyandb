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

// Command latencystatussampler is a post-trace sampler plugin that drops a
// trace when its duration (in milliseconds, read from the "duration" tag) is
// below a configured threshold AND its "status" tag equals a configured success
// value.  All other traces are kept.  The sampler fails open: if either column
// is absent, nil, empty, or decoding returns an error, the trace is retained.
//
// Build it as a Go plugin (the leading-underscore directory is intentionally
// excluded from `go build ./...` and linters, exactly like the reference
// _example directory, yet remains buildable by explicit path):
//
//	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
//	  -o latencystatussampler.so \
//	  ./test/plugins/_latencystatussampler
//
// Config JSON (from SamplerPlugin.config Struct):
//
//	{ "thresholdMs": 500, "successValue": "success" }
//
// Both fields are optional; defaults are 500 and "success" respectively.
package main

import (
	"encoding/json"
	"fmt"

	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// ABIVersion re-exports the SDK ABI version. The engine refuses to load the
// plugin unless this equals its own compiled sdk.ABIVersion.
var ABIVersion = sdk.ABIVersion

// samplerConfig is the JSON shape the operator sets in SamplerPlugin.config.
type samplerConfig struct {
	ThresholdMs  int64  `json:"thresholdMs"`
	SuccessValue string `json:"successValue"`
	// Panic causes Decide to panic unconditionally. Used by the US-010 soak to
	// verify that the engine's fail-open recover wrapper absorbs the panic and
	// the node continues processing without crashing or stalling merges.
	Panic bool `json:"panic"`
}

// latencyStatusSampler drops a trace when duration < thresholdMs && status ==
// successValue, and keeps everything else (fail-open on missing/null columns).
type latencyStatusSampler struct {
	thresholdMs  int64
	successValue string
	// panicOnDecide causes every Decide call to panic. The engine's recover
	// wrapper catches the panic and retains all traces (fail-open).
	panicOnDecide bool
}

// NewSampler is the constructor symbol the engine looks up. It parses the
// operator config and returns a ready sampler. An empty or missing config is
// accepted and yields the default thresholds.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	cfg := samplerConfig{
		ThresholdMs:  500,
		SuccessValue: "success",
	}
	if len(configJSON) > 0 {
		if unmarshalErr := json.Unmarshal(configJSON, &cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("latencystatussampler: invalid config JSON: %w", unmarshalErr)
		}
		// Re-apply defaults for zero values left by an empty JSON object.
		if cfg.ThresholdMs == 0 {
			cfg.ThresholdMs = 500
		}
		if cfg.SuccessValue == "" {
			cfg.SuccessValue = "success"
		}
	}
	return &latencyStatusSampler{
		thresholdMs:   cfg.ThresholdMs,
		successValue:  cfg.SuccessValue,
		panicOnDecide: cfg.Panic,
	}, nil
}

// Kind reports the sampler kind, satisfying the generic sdk.Plugin interface.
func (s *latencyStatusSampler) Kind() sdk.Kind { return sdk.KindSampler }

// Project declares the two tag columns this sampler reads: "duration" (INT64,
// milliseconds) and "status" (STRING). Span bodies and span IDs are not read.
func (s *latencyStatusSampler) Project() sdk.Projection {
	return sdk.Projection{Tags: []string{"duration", "status"}}
}

// Close releases resources; this sampler holds none.
func (s *latencyStatusSampler) Close() error { return nil }

// Decide returns a keep-mask aligned to batch.Traces. The batch is read-only.
// A trace is dropped only when BOTH of the following hold for row 0:
//   - duration column is present, non-nil, decodable, and < thresholdMs
//   - status column is present, non-nil, decodable, and == successValue
//
// Any failure to satisfy either condition results in the trace being kept
// (fail-open).
//
// When panicOnDecide is true the function panics immediately; the engine's
// recover wrapper catches the panic and retains all traces (fail-open). This
// mode is used exclusively by the US-010 soak to verify engine resilience.
func (s *latencyStatusSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	if s.panicOnDecide {
		panic("latencystatussampler: intentional panic for US-010 soak (panicOnDecide=true)")
	}
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = s.keepTrace(&batch.Traces[i])
	}
	return sdk.Verdict{Keep: keep}, nil
}

// keepTrace returns false (drop) only when both the duration and status
// conditions are conclusively met.  It returns true (keep) on any ambiguity.
func (s *latencyStatusSampler) keepTrace(b *sdk.TraceBlock) bool {
	durCol := b.Tag("duration")
	if durCol == nil || len(durCol.Values) == 0 {
		return true // fail open: duration column absent
	}
	statusCol := b.Tag("status")
	if statusCol == nil || len(statusCol.Values) == 0 {
		return true // fail open: status column absent
	}

	durVal, durErr := durCol.At(0)
	if durErr != nil {
		return true // fail open: decode error
	}
	if durVal.IsNull() {
		return true // fail open: null duration
	}

	statusVal, statusErr := statusCol.At(0)
	if statusErr != nil {
		return true // fail open: decode error
	}
	if statusVal.IsNull() {
		return true // fail open: null status
	}

	// Decode duration as int64 milliseconds.
	var durationMs int64
	switch durVal.ValueType() {
	case pbv1.ValueTypeInt64:
		durationMs = durVal.Int64()
	default:
		return true // fail open: unexpected duration type
	}

	// Decode status as string.
	var statusStr string
	switch statusVal.ValueType() {
	case pbv1.ValueTypeStr:
		statusStr = statusVal.Str()
	default:
		return true // fail open: unexpected status type
	}

	// Drop only when both conditions are conclusively met.
	if durationMs < s.thresholdMs && statusStr == s.successValue {
		return false
	}
	return true
}

func main() {}
