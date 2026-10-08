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

# Prefix-performance benchmark withdrawn

Date: 2026-10-03 UTC.

The prefix query benchmark in this report is **invalidated and is not
design-compliant**. Its read-only Bluge requests used `NewAllMatches` with the
default empty score option, so similarity scoring was implicitly enabled. The
prefix latency, peak-memory, profile, optimization-gate, and native/legacy
comparative tables have been removed; no result here advertises a scored
benchmark or a native baseline.

Historical binaries, profiles, fixtures, and manifests remain untouched in the
external raw archive:
`<external-scratch>/current-20261003T092700Z/`.
They are retained for provenance only and are not reclassified as no-score
measurements.

The current read-only harness now routes all remaining comparable reader cases
through a benchmark-local request wrapper with `SearcherOptions.Score ==
"none"`. This cleanup did not run a replacement performance benchmark and did
not modify production code.
