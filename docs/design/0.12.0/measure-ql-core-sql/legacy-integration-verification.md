# Legacy measure integration regression fixes

Follow-up to T17, requested after committing Catalog B ordering and the deferred
pagination design as `b13cdc6d`. Verification ran on 2026-10-17/18. This work fixes
the nine legacy failures recorded in [T17 verification](t17-verification.md);
it does not implement retained-result sessions or temporary-file spilling.

## Reproduction and causes

The unchanged nine generated cases failed again in standalone Docker before any
production edits: zero passed, nine failed, in 15.172 seconds. Evidence is
`.omx/legacy-nine-standalone-red-v1.{log,json}`. The focused invocation is
`python3 .omx/legacy-nine-e2e.py nine --topology standalone --revision red-v1`.

Eight cases projected indexed arrays, directly or as hidden filter inputs:
`generated_projection_0`, `generated_having_labels`,
`generated_not_having_labels`, and `generated_pair_002/004/008/013/017`.
The writer stores each indexed array element as a separate raw index value.
Storage projection retained only the last value, and the measure reader treated
that scalar as a complete data-block array. String arrays panicked in
`unmarshalVarArray`; integer arrays silently lost earlier elements.

The ninth case, `generated_in_int_list_0`, used an empty entity `IN` list.
Entity extraction correctly produced no matching tuples, but dispatch sent that
set to storage, which requires a series selector. Related Boolean combinations
also treated empty alternatives incorrectly during entity merging.

Once decoding worked, full verification exposed nondeterministic `SELECT *`
column order: the legacy transformer iterated tag and field lookup maps. This
caused one standalone and five distributed array-query result mismatches in the
first repaired run. Wildcard projection now follows schema encounter order while
retaining existing name lookup and deduplication behavior.

That run also exposed a compatibility regression in the new empty-result path.
The native case named `gen: tree depth 5 deep OR` exercises unsupported intersections
of alternative entity selectors, not a query-depth limit. The legacy merger can
return nil for such intersections even when the alternatives overlap. Extraction
now explicitly rejects that unsupported merge rather than claiming no rows match;
proven empty selectors and contradictory single tuples still return empty results.
The first repaired full runs passed 932/934 standalone and 928/934 distributed;
these intermediate results are retained in `.omx/legacy-full-fixed-v1-summary.json`.

## Changes and compatibility

Storage now offers an opt-in multivalue projection alongside the existing scalar
projection. Measure reads request it for indexed string/integer arrays and decode
the raw elements using the schema. Element order and duplicates are retained in
normal and index-mode row/batch consumers. Scalar projections retain their
last-value behavior; scalar-only queries allocate no multivalue projection map.
Additional retained descriptors, values and array decoding are memory-accounted.

Normal measure dispatch returns an empty result before storage when entity
selection is impossible. Entity extraction preserves empty-set identities under
`AND` and `OR`, including remaining indexed predicates. Index-mode queries do not
use these entity selectors and retain their dispatch path. Invalid predicates
still fail validation. The legacy empty bound-array rejection is unchanged.

This is a user-requested native read/dispatch correctness follow-up to the QL
implementation. Protobuf, writer code and persisted formats are unchanged. Empty
indexed arrays and nulls both have no stored elements in the existing format;
this repair does not introduce a distinction the stored data cannot supply.
The separate typed QL membership restriction on indexed arrays remains in place.

## Regression coverage

`TestIndexedArrayProjection` reproduces the panic through the actual writer,
native index and measure projection; it failed in 0.038 seconds before the fix.
The final table covers duplicate/order preservation, escaped and empty strings,
integer extremes, null/empty inputs, sorted/unsorted reads, scalar compatibility,
row/batch consumers and budget rejection followed by cleanup and recovery.

`TestDispatchEmptyEntityINReturnsEmptyWithoutStorage` reproduces the exact
`series is required` error before the fix. Public QL-to-dispatch tests and native
entity-extraction tests cover empty/nonempty/wildcard selectors, contradictions,
Boolean combinations, indexed residuals, validation and resource release.

`TestMeasureWildcardProjectionOrder` checks exact tag and field order across
100 fresh public compilations and direct-transform parity. It failed before the
fix. A minimized native/public QL regression also preserves rejection of
`(service='a' OR service='b') AND (service='b' OR service='c')` when the native
merger cannot represent that intersection.

The independent QL generator adds six cases:
`generated_empty_entity_{and,or}_{left,right,both}`. All 1,695 prior case objects,
including expected results and rejection cases, remain exactly unchanged.
The resulting 1,701-case artifact has SHA256
`a4f19f0995cb5c5e5009cba8d06eaeeceaba3a8a490ca25e5f7204e0e0698d44`.
The preservation audit is `.omx/legacy-empty-in-artifact-audit.json`.

Full storage, measure, bydbql and vectorized measure-plan package tests pass, as do
focused race tests, generator determinism/artifact/parse-bind checks,
`make lint` and whitespace checks. Apache headers were verified for all
12 changed/new Go files: tracked headers are unchanged and new files match the
repository template. `make license-check`
exits successfully but reports zero checked files, so it is not used as coverage
evidence. The explicit header audit is `.omx/legacy-fixes-license-headers-final.log`.
Other verification evidence is
`.omx/legacy-array-*-final-v1.log`, `.omx/legacy-empty-in-*.log`,
`.omx/legacy-wildcard-{green-v2,race-v1,lint-v2}.log`,
`.omx/legacy-empty-in-intersection-{green,race,lint}.log`,
`.omx/legacy-fixes-make-lint-final.log` and `.omx/legacy-fixes-license-check-final.log`.

## End-to-end verification

The focused repaired run passes all nine originals, six generated empty-selector
cases and the native unsupported-intersection regression in each topology.
Standalone selected two additional existing row/on-disk variants of that native
case: all 18 passed. Its driver initially expected 16 and reported a selection-count
assertion after the successful test run; inspection confirmed those two extra cases,
and the distributed selector was narrowed to the vectorized native case (16/16).
Evidence is `.omx/legacy-regression-standalone-fixed-v2.{json,log}` and
`.omx/legacy-regression-distributed-fixed-v2-distributed.{json,log}`.

Verification uses standalone and distributed Docker containers, each limited to
four CPUs, 8 GiB, and no additional swap. The full measure selection includes all
legacy and ticketed QL cases, native measure compatibility and Top-N:

| Topology | Ticketed QL | Legacy QL | Native measure | Top-N | Total passing |
|---|---:|---:|---:|---:|---:|
| Standalone | 637/637 | 157/157 | 120/120 | 20/20 | 934/934 |
| Distributed | 637/637 | 157/157 | 120/120 | 20/20 | 934/934 |

Reports are `.omx/legacy-full-{standalone,distributed}-fixed-v2.{json,log}`
and `.omx/legacy-full-fixed-v2-summary.json`.

Shared storage/transformer compatibility passes on the final source:

| Topology | Stream | Trace | Property | Total passing |
|---|---:|---:|---:|---:|
| Standalone | 97/97 | 68/68 | 24/24 | 189/189 |
| Distributed | 97/97 | 68/68 | 12/12 | 177/177 |

Reports are `.omx/legacy-compat-{standalone,distributed}-fixed-v2.{json,log}`
and `.omx/legacy-compat-fixed-v2-summary.json`.

This correctness follow-up does not rerun performance benchmarks or resolve the
native 32-caller admission limit, large-input RPC evidence gaps, or T02's prior
inconclusive performance acceptance. Historical T17 benchmark results remain
measurements of their recorded source revision.
