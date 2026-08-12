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

// Package sdktest is an offline test kit for pkg/pipeline/sdk sampler
// plugins: a fixture builder, a differential projection-guard runner, a
// chain harness, and a real-.so loader — all without a database or a
// cluster. It depends only on pkg/pipeline/sdk and the SDK-blessed leaves
// (pkg/pb/v1/valuetype transitively via sdk); it never imports banyand/trace,
// a logger, or a meter (see importgraph_test.go).
//
// Every marshaling and loader-contract operation here goes through the
// exported pkg/pipeline/sdk helpers (EncodeTagValue, OpenSampler,
// EvaluateChain) — the same functions the real engine uses — so this kit
// cannot silently drift from production behavior; see
// pkg/pipeline/sdk/sdktest's conformance test in banyand/trace for the golden
// check that keeps it that way.
package sdktest

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/pkg/pb/v1/valuetype"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
)

// TraceBuilder builds one sdk.TraceBlock fixture, fluently. Create one with
// NewTrace, add tags/spans, then call Build.
type TraceBuilder struct {
	err      error
	tagVals  map[string][]any
	tagVT    map[string]valuetype.ValueType
	id       string
	tagOrder []string
	spanIDs  []string
	spans    [][]byte
	minTS    int64
	maxTS    int64
}

// NewTrace starts a fixture for the trace identified by id.
func NewTrace(id string) *TraceBuilder {
	return &TraceBuilder{
		id:      id,
		tagVals: make(map[string][]any),
		tagVT:   make(map[string]valuetype.ValueType),
	}
}

// Tag sets a tag value, inferring its ValueType from goVal's Go type (string,
// int64, float64, []byte, []string, or []int64). Because int64 backs both
// ValueTypeInt64 and ValueTypeTimestamp, and []byte could mean
// ValueTypeBinaryData, Tag always infers the more common Int64/none-binary
// reading; call TagAs to disambiguate a Timestamp or to be explicit.
//
// Calling Tag (or TagAs) more than once for the same name appends a
// per-row value to that tag's column (row i gets the i'th call's value); a
// single call broadcasts that one value to every row of the trace. Rows are
// otherwise established by SpanID/Span calls, or default to a single row.
func (b *TraceBuilder) Tag(name string, goVal any) *TraceBuilder {
	vt, inferErr := inferValueType(goVal)
	if inferErr != nil {
		if b.err == nil {
			b.err = fmt.Errorf("tag %q: %w", name, inferErr)
		}
		return b
	}
	return b.TagAs(name, vt, goVal)
}

// TagAs sets a tag value with an explicit ValueType, disambiguating cases
// Tag cannot infer (ValueTypeTimestamp vs plain int64) or is required for a
// consistently-typed column across multiple rows. See Tag for the
// broadcast-vs-per-row append semantics.
func (b *TraceBuilder) TagAs(name string, vt valuetype.ValueType, goVal any) *TraceBuilder {
	if existingVT, exists := b.tagVT[name]; exists && existingVT != vt {
		if b.err == nil {
			b.err = fmt.Errorf("tag %q: value type changed from %d to %d across calls", name, existingVT, vt)
		}
		return b
	}
	if _, exists := b.tagVT[name]; !exists {
		b.tagOrder = append(b.tagOrder, name)
	}
	b.tagVT[name] = vt
	b.tagVals[name] = append(b.tagVals[name], goVal)
	return b
}

// SpanID appends one row to the span-id column.
func (b *TraceBuilder) SpanID(id string) *TraceBuilder {
	b.spanIDs = append(b.spanIDs, id)
	return b
}

// Span appends one row to the span-body column.
func (b *TraceBuilder) Span(body []byte) *TraceBuilder {
	b.spans = append(b.spans, body)
	return b
}

// TS sets the trace's intrinsic MinTS/MaxTS (unix nanoseconds).
func (b *TraceBuilder) TS(minTS, maxTS int64) *TraceBuilder {
	b.minTS = minTS
	b.maxTS = maxTS
	return b
}

// Build assembles the fixture into an sdk.TraceBlock, encoding every tag
// value via sdk.EncodeTagValue (the same encoder the offline tools and, via
// its DecodeTagValue inverse, the real engine's decode path both trust). It
// returns the first error recorded by Tag/TagAs, if any, or an encode error.
func (b *TraceBuilder) Build() (sdk.TraceBlock, error) {
	if b.err != nil {
		return sdk.TraceBlock{}, b.err
	}
	rows := 1
	if len(b.spanIDs) > rows {
		rows = len(b.spanIDs)
	}
	if len(b.spans) > rows {
		rows = len(b.spans)
	}
	for _, name := range b.tagOrder {
		if n := len(b.tagVals[name]); n > rows {
			rows = n
		}
	}

	block := sdk.TraceBlock{TraceID: b.id, MinTS: b.minTS, MaxTS: b.maxTS}
	if len(b.spanIDs) > 0 {
		block.SpanIDs = padStrings(b.spanIDs, rows)
	}
	if len(b.spans) > 0 {
		block.Spans = padBytes(b.spans, rows)
	}
	for _, name := range b.tagOrder {
		vals := b.tagVals[name]
		vt := b.tagVT[name]
		col := sdk.TagColumn{Name: name, ValueType: vt, Values: make([][]byte, rows)}
		for i := 0; i < rows; i++ {
			v := rowValue(vals, i)
			raw, encErr := sdk.EncodeTagValue(vt, v)
			if encErr != nil {
				return sdk.TraceBlock{}, fmt.Errorf("tag %q row %d: %w", name, i, encErr)
			}
			col.Values[i] = raw
		}
		block.Tags = append(block.Tags, col)
	}
	return block, nil
}

// rowValue returns vals[i] if present, the sole broadcast value when only one
// was given, or nil (encodes to a null/absent tag on that row).
func rowValue(vals []any, i int) any {
	switch {
	case len(vals) == 1:
		return vals[0]
	case i < len(vals):
		return vals[i]
	default:
		return nil
	}
}

// inferValueType maps a Go value's concrete type to the corresponding
// valuetype.ValueType. nil and unsupported types return an error; the caller
// (Tag) should fall back to TagAs.
func inferValueType(goVal any) (valuetype.ValueType, error) {
	switch goVal.(type) {
	case string:
		return valuetype.ValueTypeStr, nil
	case int64:
		return valuetype.ValueTypeInt64, nil
	case float64:
		return valuetype.ValueTypeFloat64, nil
	case []byte:
		return valuetype.ValueTypeBinaryData, nil
	case []string:
		return valuetype.ValueTypeStrArr, nil
	case []int64:
		return valuetype.ValueTypeInt64Arr, nil
	case nil:
		return valuetype.ValueTypeUnknown, fmt.Errorf("cannot infer value type from a nil value; use TagAs")
	default:
		return valuetype.ValueTypeUnknown, fmt.Errorf("cannot infer value type from %T; use TagAs", goVal)
	}
}

// padStrings returns vals unchanged if it already has at least n elements,
// otherwise a new right-padded (with "") copy of length n.
func padStrings(vals []string, n int) []string {
	if len(vals) >= n {
		return vals
	}
	out := make([]string, n)
	copy(out, vals)
	return out
}

// padBytes returns vals unchanged if it already has at least n elements,
// otherwise a new right-padded (with nil) copy of length n.
func padBytes(vals [][]byte, n int) [][]byte {
	if len(vals) >= n {
		return vals
	}
	out := make([][]byte, n)
	copy(out, vals)
	return out
}

// Batch assembles a *sdk.TraceBatch from one or more built TraceBlocks — a
// convenience for tests that would otherwise write out the literal struct.
func Batch(blocks ...sdk.TraceBlock) *sdk.TraceBatch {
	return &sdk.TraceBatch{Traces: blocks}
}
