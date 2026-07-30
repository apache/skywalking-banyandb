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

// Command sw-trace-sampler is the first-party post-trace sampler for the
// SkyWalking-native segment schema (group sw_trace, BanyanDB trace "segment").
// It implements the Scenario 6.1 keep logic from
// docs/design/post-trace-pipeline.md on the real segment columns:
//
//   - trace duration from the envelope of the segment "start_time"/"latency" tags (ms),
//   - keepErrors from the first-class is_error tag,
//   - keepTagRules matched against the flattened searchable-tag array "tags"
//     (entries "key=value", e.g. "db.type=PostgreSQL"),
//   - and a deterministic healthySampleRate hash of the trace id.
//
// Config JSON (from SamplerPlugin.config):
//
//	{
//	  "durationThresholdMs": 500,
//	  "keepErrors": true,
//	  "healthySampleRate": 0.1,
//	  "keepTagRules": [
//	    { "tagKey": "db.type",  "equals": "PostgreSQL" },
//	    { "tagKey": "mq.queue", "equals": "queue-songs-ping" }
//	  ]
//	}
//
// Build it as a Go plugin with `make build-plugins`; it must use the same Go
// toolchain and pinned pkg/pipeline/sdk as the data node (see plugins/README.md).
package main

import (
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/plugins/skywalking/internal/tracesampler"
)

// ABIVersion re-exports the SDK ABI version. The engine refuses to load the
// plugin unless this equals its own compiled sdk.ABIVersion.
var ABIVersion = sdk.ABIVersion

// segmentSchema describes how the SkyWalking segment schema stores the columns
// this sampler reads: trace duration is the envelope of the per-segment
// "start_time" (unix-ns timestamp) and "latency" (ms) tags, errors are the
// first-class is_error tag, and searchable tags are flattened into the "tags" array.
var segmentSchema = tracesampler.Schema{
	ArrayTagColumn:          "tags",
	ErrorTag:                "is_error",
	DurationTag:             "latency",
	StartTimeTag:            "start_time",
	DurationTagNanosPerUnit: 1_000_000,
	// Every @Column on OAP's SegmentRecord except "tags", which is the flattened
	// searchable-tag array above. A keepTagRules entry naming one of these could never
	// match, so listing them turns a silently-dropped rule into a startup error. Keep
	// in step with SegmentRecord.java if a column is added there.
	FirstClassColumns: []string{
		"segment_id",
		"trace_id",
		"service_id",
		"service_instance_id",
		"endpoint_id",
		"start_time",
		"latency",
		"is_error",
		"data_binary",
	},
}

// NewSampler is the constructor symbol the engine looks up.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	return tracesampler.New(configJSON, segmentSchema)
}

func main() {}
