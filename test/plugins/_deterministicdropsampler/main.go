// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Command deterministicdropsampler is the metadata-only native plugin used to
// measure trace deletion and secondary-index pruning independently of application logic.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// ABIVersion declares the SDK ABI implemented by this plugin.
var ABIVersion = sdk.ABIVersion

type samplerConfig struct {
	DropRatio float64 `json:"dropRatio"`
}

type deterministicDropSampler struct {
	dropRatio float64
}

// NewSampler constructs a metadata-only sampler with deterministic trace-ID selection.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	var config samplerConfig
	if unmarshalErr := json.Unmarshal(configJSON, &config); unmarshalErr != nil {
		return nil, fmt.Errorf("deterministicdropsampler: invalid config: %w", unmarshalErr)
	}
	if math.IsNaN(config.DropRatio) || math.IsInf(config.DropRatio, 0) || config.DropRatio < 0 || config.DropRatio > 1 {
		return nil, fmt.Errorf("deterministicdropsampler: dropRatio must be between 0 and 1")
	}
	return deterministicDropSampler{dropRatio: config.DropRatio}, nil
}

func (deterministicDropSampler) Kind() sdk.Kind          { return sdk.KindSampler }
func (deterministicDropSampler) Project() sdk.Projection { return sdk.Projection{} }
func (deterministicDropSampler) Close() error            { return nil }
func (dds deterministicDropSampler) Decide(batch *sdk.TraceBatch) (sdk.Verdict, error) {
	keep := make([]bool, len(batch.Traces))
	for traceIdx := range batch.Traces {
		keep[traceIdx] = !dds.drop(batch.Traces[traceIdx].TraceID)
	}
	return sdk.Verdict{Keep: keep}, nil
}

func (dds deterministicDropSampler) drop(traceID string) bool {
	if dds.dropRatio <= 0 {
		return false
	}
	if dds.dropRatio >= 1 {
		return true
	}
	digest := sha256.Sum256([]byte(traceID))
	bucket := binary.BigEndian.Uint64(digest[:8])
	return math.Ldexp(float64(bucket), -64) < dds.dropRatio
}

func main() {}
