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

// Command latencystatussampler is a first-party post-trace sampler plugin
// that drops a trace when its duration (in milliseconds, read from the
// "duration" tag) is below a configured threshold AND its "status" tag
// equals a configured success value. All other traces are kept. The sampler
// fails open: if either column is absent, nil, or fails to decode, the trace
// is retained.
//
// Build it as a Go plugin (see plugins/README.md for the full contract and
// toolchain-lock requirements):
//
//	make build-plugins
//
// or directly:
//
//	CGO_ENABLED=1 go build -buildmode=plugin -trimpath \
//	  -o latencystatussampler.so \
//	  ./plugins/skywalking/latencystatussampler
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

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// ABIVersion re-exports the SDK ABI version. The engine refuses to load the
// plugin unless this equals its own compiled sdk.ABIVersion.
var ABIVersion = sdk.ABIVersion

// Default sampler thresholds, applied only when the config omits the field.
const (
	defaultThresholdMs  = 500
	defaultSuccessValue = "success"
)

// samplerConfig is the JSON shape the operator sets in SamplerPlugin.config.
// The fields are pointers so a defaulted value is distinguishable from an
// explicitly-configured zero: {"thresholdMs":0} means "the duration test can
// never fire" and must be honored verbatim, whereas an omitted thresholdMs
// takes the default. A nil field gets the default; a present field — including
// 0 or "" — is used as-is.
type samplerConfig struct {
	SuccessValue *string `json:"successValue"`
	ThresholdMs  *int64  `json:"thresholdMs"`
}

// latencyStatusSampler drops a trace when duration < thresholdMs && status ==
// successValue, and keeps everything else (fail-open on missing/null columns).
type latencyStatusSampler struct {
	successValue string
	thresholdMs  int64
}

// NewSampler is the constructor symbol the engine looks up. It parses the
// operator config and returns a ready sampler. An empty or missing config is
// accepted and yields the default thresholds; an explicitly-set zero value is
// honored (not overridden).
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	var cfg samplerConfig
	if len(configJSON) > 0 {
		if unmarshalErr := json.Unmarshal(configJSON, &cfg); unmarshalErr != nil {
			return nil, fmt.Errorf("latencystatussampler: invalid config JSON: %w", unmarshalErr)
		}
	}
	sampler := &latencyStatusSampler{
		thresholdMs:  defaultThresholdMs,
		successValue: defaultSuccessValue,
	}
	if cfg.ThresholdMs != nil {
		sampler.thresholdMs = *cfg.ThresholdMs
	}
	if cfg.SuccessValue != nil {
		sampler.successValue = *cfg.SuccessValue
	}
	return sampler, nil
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
func (s *latencyStatusSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for i := range batch.Traces {
		keep[i] = s.keepTrace(&batch.Traces[i])
	}
	return sdk.Verdict{Keep: keep}, nil
}

// keepTrace returns false (drop) only when both the duration and status
// conditions are conclusively met. It returns true (keep) on any ambiguity.
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
	case valuetype.ValueTypeInt64:
		durationMs = durVal.Int64()
	default:
		return true // fail open: unexpected duration type
	}

	// Decode status as string.
	var statusStr string
	switch statusVal.ValueType() {
	case valuetype.ValueTypeStr:
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
