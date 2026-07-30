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

// Command zipkin-trace-sampler is the first-party post-trace sampler for the
// Zipkin schema (group sw_zipkinTrace, BanyanDB trace "zipkin_span"). It
// implements the Scenario 6.2 keep logic from
// docs/design/post-trace-pipeline.md on the real Zipkin columns:
//
//   - trace duration from the envelope of the "timestamp_millis"/"duration" tags (duration is µs),
//   - keepTagRules matched against the flattened "query" array, whose entries
//     include both bare keys and "key=value" such as "http.status_code=500",
//   - and a deterministic healthySampleRate hash of the trace id.
//
// The Zipkin schema has no is_error column. keepErrors instead detects Zipkin's
// conventional "error" span tag, which OAP flattens into "query" as both a bare
// key and "error=<message>". That is a tag convention, not an authoritative field:
// instrumentations that only signal failure through http.status_code 5xx or
// otel.status_code need an explicit rule, e.g. 'http.status_code=~5\d\d'.
//
// Config JSON (from SamplerPlugin.config):
//
//	{
//	  "durationThresholdMs": 1000,
//	  "keepErrors": true,
//	  "healthySampleRate": 0.05,
//	  "keepTagRules": [
//	    { "tagKey": "query", "regex": "http\\.status_code=5\\d\\d" }
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

// zipkinSchema describes how the Zipkin schema stores the columns this sampler
// reads: trace duration is the envelope of the per-span "timestamp_millis"
// (unix-ns timestamp) and "duration" (µs) tags, searchable tags are flattened
// into the "query" array, and keepErrors looks for Zipkin's "error" tag inside
// that same array (ErrorTagInArray) since the schema has no error column.
var zipkinSchema = tracesampler.Schema{
	ArrayTagColumn:          "query",
	ErrorTag:                "error",
	ErrorTagInArray:         true,
	DurationTag:             "duration",
	StartTimeTag:            "timestamp_millis",
	DurationTagNanosPerUnit: 1_000,
	// Every @Column on OAP's ZipkinSpanRecord except "query", which is the flattened
	// searchable-tag array above. Note "tags" IS a first-class column here — it holds
	// the span's tags as a JSON blob, and only "query" is searchable — so a rule on it
	// is the same never-match trap. Keep in step with ZipkinSpanRecord.java.
	FirstClassColumns: []string{
		"trace_id",
		"span_id",
		"parent_id",
		"name",
		"duration",
		"kind",
		"timestamp_millis",
		"timestamp",
		"local_endpoint_service_name",
		"local_endpoint_port",
		"remote_endpoint_service_name",
		"remote_endpoint_port",
		"annotations",
		"tags",
		"debug",
		"shared",
	},
}

// NewSampler is the constructor symbol the engine looks up.
func NewSampler(configJSON []byte) (sdk.Sampler, error) {
	return tracesampler.New(configJSON, zipkinSchema)
}

func main() {}
