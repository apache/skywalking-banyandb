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

// Package sdk defines the contract between the BanyanDB post-trace pipeline
// engine and user-supplied native Go plugins (each a .so loaded in-process via
// the standard Go plugin package). It is the single pinned surface plugin
// authors build against; see docs/design/post-trace-pipeline.md §2.5.
//
// A plugin is one kind of the generic Plugin interface. Every kind shares Kind,
// Project, and Close; each kind adds its own processing method (the sampler kind
// adds Decide — see Sampler). Today the only kind is the sampler. A plugin is a
// package main built with `-buildmode=plugin` that exports exactly two symbols
// (the constructor name is the kind's convention — NewSampler for the sampler
// kind):
//
//	var  ABIVersion int                          // == sdk.ABIVersion
//	func NewSampler(config []byte) (sdk.Sampler, error)
//
// The engine refuses to load a plugin whose ABIVersion differs from its own
// compiled sdk.ABIVersion, turning a silent miscompile into a fail-fast error.
// The config bytes are the canonical JSON serialization of the
// google.protobuf.Struct set in the plugin's proto payload (e.g.
// SamplerPlugin.config); the plugin unmarshals them into its own typed config.
//
// In a TracePipelineConfig or StageRule, plugins are wired as an ordered chain
// (a sequential pipe): links run in declared order, each processing what the
// previous link kept, and a link that fails is bypassed. For an all-sampler
// chain this is the conjunction of the links' keep/drop verdicts.
//
// The boundary deliberately crosses only stdlib slices plus the engine's
// stable, byte-sized pbv1.ValueType enum, so no banyand/trace-internal struct
// is shared across the .so boundary.
package sdk

import (
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// ABIVersion is compiled into the host and must be re-exported, unchanged, by
// every plugin. The engine refuses to load a plugin whose ABIVersion differs.
const ABIVersion = 1

// Kind identifies a Plugin's role in a chain. It mirrors the set arm of the
// proto Plugin.kind oneof and lets the engine label and cross-check a plugin
// before type-asserting it to the matching kind interface. New kinds are added
// here in lockstep with new oneof arms.
type Kind uint8

const (
	// KindUnspecified is the zero value; a real plugin never reports it.
	KindUnspecified Kind = iota
	// KindSampler is a plugin that owns a keep/drop verdict (see Sampler).
	KindSampler
)

// Plugin is the common interface every plugin kind satisfies — the generic link
// type the engine handles uniformly. The engine constructs a Plugin, checks
// Kind against the proto oneof arm that named it, then type-asserts to that
// kind's interface (e.g. Sampler) for the kind-specific call. Project and Close
// are shared by every kind; the per-kind processing method lives on the kind
// interface. Each kind keeps its own constructor symbol convention (the sampler
// kind defaults to NewSampler; a future kind would use its own, e.g.
// NewTransformer).
type Plugin interface {
	// Kind reports which plugin kind this is, for engine bookkeeping and as a
	// cross-check against the proto oneof arm. It must be constant for the
	// plugin's lifetime.
	Kind() Kind

	// Project is the column-selection handshake, called once at load. The
	// engine honors it for the plugin's lifetime: Tags drives the native tag
	// projection (only those tag columns are decoded); SpanIDs and Spans gate
	// the spans stream. Intrinsic columns (TraceID, MinTS, MaxTS) are always
	// present regardless of the projection.
	Project() Projection

	// Close releases any resources the plugin holds. It is called once when the
	// pipeline config is removed; because Go plugins cannot be unloaded, the .so
	// itself stays mapped until the process restarts.
	Close() error
}

// Sampler is the keep/drop kind of Plugin (Kind reports KindSampler). The engine
// calls Project once at load, then Decide once per batch, then Close at unload.
// In a chain it is a conjunction link: each Sampler narrows the traces the next
// link sees.
type Sampler interface {
	Plugin

	// Decide receives a vectorized batch of assembled per-trace blocks and
	// returns a keep-mask aligned to batch.Traces. The batch is READ-ONLY:
	// Decide must not mutate any slice it receives. The keep-mask is the only
	// output channel; the engine writes retained traces from its own untouched
	// block data, so a returned error or a length-mismatched verdict makes the
	// engine fail open (retain the whole batch).
	Decide(batch *TraceBatch) (Verdict, error)
}

// Projection is the plugin's up-front column request — one handshake covering
// every optional column. Intrinsic columns (TraceID, MinTS, MaxTS) are always
// materialized and are not listed here.
type Projection struct {
	// Tags names the tag columns to decode into TraceBlock.Tags. Empty means no
	// tag columns are decoded.
	Tags []string
	// SpanIDs opts in to the span-id column. Default false. Span ids and span
	// bodies share one encoded data block in the native layout, so requesting
	// span ids forces a read of the spans stream — it is not free metadata.
	SpanIDs bool
	// Spans opts in to the heavy span-body column. Default false: the engine
	// leaves TraceBlock.Spans nil and, on the merge raw fast path, never decodes
	// span bodies. Set true only when the verdict reads them.
	Spans bool
}

// TraceBatch is a vectorized batch of assembled per-trace blocks. It is the
// engine's native columnar trace layout, shared read-only with the plugin.
type TraceBatch struct {
	// Traces holds one block per trace_id. The verdict's keep-mask is aligned
	// to this slice.
	Traces []TraceBlock
}

// TraceBlock mirrors the native trace block: every populated column is indexed
// in lockstep by span row i in [0, Len). Intrinsic columns are always set;
// Tags, SpanIDs, and Spans appear only as requested by Project. Slices are
// shared with the engine and must be treated as read-only.
type TraceBlock struct {
	// TraceID identifies the trace; the keep/drop verdict is per trace_id.
	TraceID string
	// Tags holds the projected tag columns, one per Projection.Tags entry that
	// the trace actually carries.
	Tags []TagColumn
	// SpanIDs is the row-aligned span-id column; nil unless Projection.SpanIDs.
	SpanIDs []string
	// Spans is the row-aligned span-body column (opaque marshaled bytes); nil
	// unless Projection.Spans.
	Spans [][]byte
	// MinTS is the earliest span start in unix nanoseconds.
	MinTS int64
	// MaxTS is the latest span end in unix nanoseconds; trace duration is
	// MaxTS - MinTS.
	MaxTS int64
}

// Len reports the number of span rows in the block. It is available only when a
// row-indexed column (a projected tag, SpanIDs, or Spans) was materialized;
// with a metadata-only projection it returns 0.
func (b *TraceBlock) Len() int {
	if len(b.SpanIDs) > 0 {
		return len(b.SpanIDs)
	}
	if len(b.Spans) > 0 {
		return len(b.Spans)
	}
	for i := range b.Tags {
		if n := len(b.Tags[i].Values); n > 0 {
			return n
		}
	}
	return 0
}

// Tag returns the projected tag column with the given name, or nil if the trace
// did not carry it (or the plugin did not project it).
func (b *TraceBlock) Tag(name string) *TagColumn {
	for i := range b.Tags {
		if b.Tags[i].Name == name {
			return &b.Tags[i]
		}
	}
	return nil
}

// TagColumn mirrors the native tag: a row-aligned column of marshaled values
// plus the value type needed to decode them via At.
type TagColumn struct {
	// Name is the tag key.
	Name string
	// Values holds one marshaled value per span row; a nil element means the
	// tag is absent on that row.
	Values [][]byte
	// ValueType is the engine's stable, byte-sized type tag for every value in
	// the column.
	ValueType pbv1.ValueType
}

// Verdict is the per-trace decision, aligned to TraceBatch.Traces.
type Verdict struct {
	// Keep must have the same length as the batch; Keep[i] true retains
	// Traces[i]. A length mismatch makes the engine fail open.
	Keep []bool
}
