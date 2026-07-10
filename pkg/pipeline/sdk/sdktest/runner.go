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

package sdktest

import "github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"

// Report is the outcome of a differential Run: the primary Decide error (if
// any) plus, when the sampler's decision depended on a column it never
// projected, the trace IDs where the projected-only and all-columns runs
// disagreed.
type Report struct {
	// Err is the error sampler.Decide(batch) returned on the primary
	// (all-columns) run, if any.
	Err error
	// ProjectionErr is the error sampler.Decide returned on the secondary
	// (projected-only) run, if any. Kept separate from Err so a projection-run
	// failure never masks the primary Verdict/Err.
	ProjectionErr error
	// ProjectionDivergedIDs holds the trace IDs where the projected-only and
	// all-columns verdicts disagreed. Non-empty means the sampler's Decide
	// reads a column it never declared in Project() — the #1 root cause of
	// "unexpected data is being sampled" in production, where the engine
	// decodes ONLY the declared projection.
	ProjectionDivergedIDs []string
}

// Run executes sampler.Decide(batch) and returns its Verdict, plus a Report
// carrying the differential projection guard's result.
//
// Decide is opaque: TraceBlock.Tag returns nil for a column the caller never
// populated, faithfully reproducing what the real engine hands a plugin (the
// engine decodes and materializes only the columns the plugin's Project()
// declared) — so an unprojected-column read cannot be *observed* directly,
// only its effect on the verdict. Run therefore calls Decide a SECOND time on
// a batch built with only the columns sampler.Project() actually requested,
// and flags any trace whose verdict differs between the two runs.
func Run(sampler sdk.Sampler, batch *sdk.TraceBatch) (sdk.Verdict, Report) {
	// Each Decide runs over its OWN deep copy, and neither shares backing
	// arrays with the caller's batch. This is load-bearing, not defensive:
	// DecodeTagValue's str-array path decodes IN PLACE
	// (vararray.UnmarshalVarArray mutates its src slice), so if the two runs
	// shared a TagColumn's Values, run 1 would corrupt run 2's input and the
	// guard would report a spurious "projection divergence". Mirrors the real
	// engine, which hands every Decide a fresh deep copy from its pooled
	// buffers (assembleTraceBlock).
	allColumns := copyProjectedBatch(batch, nil)
	verdict, err := sampler.Decide(allColumns)
	report := Report{Err: err}

	proj := sampler.Project()
	projected := copyProjectedBatch(batch, &proj)
	projVerdict, projErr := sampler.Decide(projected)
	report.ProjectionErr = projErr

	if err == nil && projErr == nil && len(verdict.Keep) == len(projVerdict.Keep) {
		for i := range verdict.Keep {
			if verdict.Keep[i] != projVerdict.Keep[i] {
				report.ProjectionDivergedIDs = append(report.ProjectionDivergedIDs, batch.Traces[i].TraceID)
			}
		}
	}
	return verdict, report
}

// copyProjectedBatch returns a DEEP copy of batch. When proj is nil every
// column is copied (the "all columns" run); when proj is non-nil the copy is
// filtered to that projection, mirroring the real engine's assembleTraceBlock
// name-selection exactly (banyand/trace/pipeline_chain.go): intrinsic columns
// (TraceID, MinTS, MaxTS) are always present; for each name in proj.Tags (in
// that order) the FIRST source tag column with a matching Name is taken and
// the search for that name stops there — so a fixture with more than one
// column sharing a name (the #1126 multi-type-same-tag case) projects
// identically to production, not merely deduplicated. SpanIDs/Spans are
// included only when proj.SpanIDs/proj.Spans is true. Every TagColumn.Values
// element and every Spans element is deep-copied (nil stays nil), so the
// returned batch never aliases the source's backing arrays.
func copyProjectedBatch(batch *sdk.TraceBatch, proj *sdk.Projection) *sdk.TraceBatch {
	out := &sdk.TraceBatch{Traces: make([]sdk.TraceBlock, len(batch.Traces))}
	for i := range batch.Traces {
		src := &batch.Traces[i]
		dst := sdk.TraceBlock{TraceID: src.TraceID, MinTS: src.MinTS, MaxTS: src.MaxTS}
		if proj == nil {
			for _, tag := range src.Tags {
				dst.Tags = append(dst.Tags, copyTagColumn(tag))
			}
			dst.SpanIDs = copyStrings(src.SpanIDs)
			dst.Spans = copyByteSlices(src.Spans)
		} else {
			for _, name := range proj.Tags {
				for _, tag := range src.Tags {
					if tag.Name != name {
						continue
					}
					dst.Tags = append(dst.Tags, copyTagColumn(tag))
					break
				}
			}
			if proj.SpanIDs {
				dst.SpanIDs = copyStrings(src.SpanIDs)
			}
			if proj.Spans {
				dst.Spans = copyByteSlices(src.Spans)
			}
		}
		out.Traces[i] = dst
	}
	return out
}

// copyTagColumn deep-copies a TagColumn, mirroring assembleTraceBlock: each
// row's []byte is copied, and a nil row stays nil.
func copyTagColumn(src sdk.TagColumn) sdk.TagColumn {
	values := make([][]byte, len(src.Values))
	for i, v := range src.Values {
		if v != nil {
			values[i] = append([]byte(nil), v...)
		}
	}
	return sdk.TagColumn{Name: src.Name, ValueType: src.ValueType, Values: values}
}

// copyStrings returns a copy of src (nil stays nil).
func copyStrings(src []string) []string {
	if src == nil {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

// copyByteSlices deep-copies a [][]byte (nil outer stays nil, nil rows stay nil).
func copyByteSlices(src [][]byte) [][]byte {
	if src == nil {
		return nil
	}
	out := make([][]byte, len(src))
	for i, v := range src {
		if v != nil {
			out[i] = append([]byte(nil), v...)
		}
	}
	return out
}
