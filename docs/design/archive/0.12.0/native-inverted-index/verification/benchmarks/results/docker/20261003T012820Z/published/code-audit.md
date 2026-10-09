<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain a
copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

# Native performance code audit (separate investigation)

This is a code/metadata investigation, separate from the historical benchmark numbers in [`README.md`](README.md). Source is `nativefix` commit `94160e78199e4f95a71a957663d686b4fe02bf9d`; no production source was changed.

## Exact legacy fixture routing

The committed common fixture used by the probe is not a v1/v2 legacy segment. Read-only decoding of `<external-artifacts>/data-query-legacy/query-legacy-0/sidx/00000000640f.snp` produced:

- snapshot format `3`, 23 segment records;
- every record type `ice`, version `3`;
- physical document counts sum to exactly `1,200,000`;
- the 23 `.seg` files total `105,757,946` bytes; the 995-byte `.snp` makes the reported `105,758,941`-byte manifest total;
- a sampled segment footer ends with ICE version `3` and chunk mode `1025`.

Therefore the native probe's open path does dispatch to the native plugin; it does **not** fall back to the default old ICE implementation. SkyAPM Bluge's default config registers `ice` v3 (`index/config.go:176-180,235-241`), and `WithSegmentPlugin` overwrites the `(type,version)` entry (`index/config.go:126-131`). `NewNativeStore` installs `nativeSegmentPluginNew/Load/Merge` under that same `ice` v3 key (`pkg/index/inverted/inverted.go:403-412`). During `OpenWriter.loadSnapshots`, every snapshot record selects its registered plugin and calls `loadSegment` (`SkyAPM bluge/index/writer.go:625-648`).

## Why open is expensive

The old SkyAPM ICE loader used by the compatibility writer is lazy: it parses the footer, field metadata, stored chunk offsets, and doc-value readers, but does not decode every document or enumerate every term (`SkyAPM ice/load.go:30-72`). The native replacement is eager:

1. `nativeSegmentPluginLoad` reads the segment, decodes every stored document, retains a copied payload, opens/validates the segment, then enumerates fields (`native_plugin.go:238-262`).
2. For every field term it calls both `TermDocuments` and `TermFrequencies` (`native_plugin.go:262-299`). Each reader call constructs a fresh stored reader (`nativeice.go:495-587`), and each term selection reloads the field's dictionary (`nativeice/selection.go:150-196`). This is repeated dictionary/FST and posting work per unique term.
3. It calls `DocValues` for every field (`native_plugin.go:300-315`); `Reader.DocValues` allocates a `[][][]byte` for all physical documents and copies each value (`nativeice.go:589-615`).
4. `rebuild` then creates aggregate stored/terms/frequency/doc-value maps while retaining per-document maps (`native_plugin.go:165-231,318-370`).

The canonical fixture's `_id` values are high-cardinality (one per series), so the 23-segment open performs term work over 1.2M identifiers. This establishes the routing and algorithmic mechanism; the exact wall/RSS share still needs the requested small profile.

## Retained mmap and lock lifetime

`FileSystemDirectory` defaults to `LoadMMapAlways`; `loadSegment` retains the returned closer in `closeOnLastRefCounter` (`SkyAPM bluge/index/directory_fs.go:145-165,180-185`; `writer.go:643-662`). The native loader additionally copies the mmap-backed payload into `nativePluginSegment.payload` and retains the eager object graph. Thus a live root can hold mmap-backed source pages plus the heap payload and reconstructed maps until that segment is released by compaction. This is an established retention path, not a claim that all mapped pages are resident simultaneously.

The harness's owner lifetime is correct: it creates the root `lock`, validates the owner, opens the native store, closes the store, then closes the owner (`harness/main.go:264-282,251-254`). `NativeWriterOwner` only validates the path and keeps the root lock; `nativeDirectory.Lock/Unlock` are no-ops (`inverted.go:183-245`). The open timeout is therefore not a lock/token wait. No file descriptor or mmap ownership leak is proven: Bluge's wrapper owns the closer and releases it on the last segment reference; the concern is intentional retention until release plus duplicate heap representation.

## Merge peak and minimal remedy

`nativeSegmentMerger.WriteTo` copies source documents into a complete in-memory `Generation`; `EncodeSegment` builds the complete output bytes, then Bluge reopens the persisted output through `Load` while source graphs can still be live (`native_plugin.go:386-516`; SkyAPM persister `prepareIntroducePersist`/`loadSegment`). This is the leading merge-peak hypothesis, pending profile attribution.

Minimal remedy: make the native segment payload-backed/lazy for dictionary, postings, stored fields, and doc values; or at minimum parse/cache one reader and dictionary per field and construct each aggregate index once. Bound merge peaks by streaming output and releasing per-document intermediates before reopening the merged segment.

## Focused regression benchmark

Keep the existing 1.2M/8-GiB/4-CPU streaming case as the capacity gate. Add a fast N=10k/100k/300k sweep that records cgroup peak RSS, allocations, `OpenWriter`/first-query wall time, and segment type/version. Include one high-cardinality `_id` fixture and one sparse doc-value fixture. A fix should avoid OOM at 1.2M and remove the per-term dictionary reload slope; startup and dataset construction must be reported separately.
