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

# Native inverted-index performance comparison

Date: 2026-10-03 UTC.

## Query comparison withdrawn

The query-performance comparison previously recorded in this report is
**invalidated and is not design-compliant**. Its Bluge request used
`NewAllMatches` with the default empty `SearcherOptions.Score`; that executes
similarity scoring, while the native series-index design under review is a
membership/no-score workload. The exact-hit, exact-miss, and prefix latency and
peak-memory tables, profiles, and comparative claims have therefore been
removed from this report. They must not be used as a native baseline or as an
adoption claim.

The immutable raw runs remain outside the repository for provenance:
`<external-scratch>/current-20261003T092700Z/`.
Those artifacts are historical evidence only and are not reclassified as
no-score results.

The current read-only harness now uses a benchmark-local request wrapper that
sets `SearcherOptions.Score` explicitly to `"none"` while retaining the
all-matches collector. No production source is changed by that wrapper. No new
performance run was started as part of this cleanup.

## Retained independent evidence

The write-path growth run did not use a scored query during ingestion. Its
2.4M-series result remains descriptive correctness/resource evidence: native
foreground time was 278.877 s versus legacy 251.428 s; native peak was 3.70 GiB
versus legacy 353 MiB; native used about 48.6% fewer final disk bytes; and both
reopen scans returned 2.4M with checksum `450ac1eeb1110cc2`. These values are
not a query-performance comparison.

Build provenance, fixture manifests, and raw profiles remain in the external
scratch archive. No production files were changed by this report cleanup.
