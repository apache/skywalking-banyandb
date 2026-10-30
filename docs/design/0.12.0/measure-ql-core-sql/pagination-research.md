# Query browsing with predictable page cost

Status: **design recorded; implementation deferred by user decision on
2026-10-17.** Retained-result sessions and their page index are future work,
outside the current Catalog B implementation. Do not start implementation as
part of Catalog B acceptance. Temporary-file spilling also remains deferred.

Research date: 2026-10-17. This is a design recommendation, not an implemented
pagination contract. DataFusion implementation observations below use the
`55.2.0` source tag; official rolling documentation and dated engineering posts
are identified separately. No production code or protobuf changes accompany
this note.

## Requirement and conclusion

The desired behavior is comparable latency, CPU and memory when fetching the
first, middle or last page, including backward navigation. Two costs must be
distinguished without hiding either:

1. Execute the query and prepare a browsable result.
2. Retrieve a page from that prepared result.

**Recommendation:** for arbitrary measure QL, execute once and retain the final
ordered result in bounded, independently addressable columnar batches. Index
batch row ranges so a page read does not replay earlier rows. Target comparable
page retrieval cost after preparation, and report preparation plus
submit-to-first-page latency separately. Do not claim equal cold latency for
arbitrary queries: global aggregation or arbitrary ordering can require reading
all qualifying inputs before any correct final page is available.

For raw event browsing over a suitable storage order, a complementary keyset
path can avoid preparing the whole result. Its cost depends on actual source
seek/pruning and filter selectivity; a boundary predicate alone is not a
performance guarantee. It cannot jump to an unknown ordinal middle page without
an existing boundary or rank index. These conclusions are engineering inferences
from the execution mechanisms below, not vendor latency guarantees.

## What DataFusion actually provides

### OFFSET still processes preceding rows

DataFusion's logical `PushDownLimit` puts `skip + fetch` into the sort's fetch
bound. Thus `OFFSET 90000 LIMIT 100` requires up to 90,100 candidates, rather
than 100. Its physical `LimitStream::poll_and_skip` polls upstream batches and
discards or slices their preceding rows. This is sequential skipping, not an
ordinal seek. [Logical limit pushdown, 55.2.0](https://github.com/apache/datafusion/blob/55.2.0/datafusion/optimizer/src/push_down_limit.rs),
[physical limit implementation, 55.2.0](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/limit.rs).

Inference: changing only the page offset does not make first, middle and last
pages equally cheap. BanyanDB's current T17 compact heap has the same
`OFFSET + LIMIT` property; its measured large-offset case approaches full-sort
retention and runtime. See [T17 verification](t17-verification.md).

### Ordering knowledge and dynamic pruning reduce source work

DataFusion's July 2026 sort-pushdown description distinguishes proven ordering
from promising but unproven read order. With proven ordering, an unnecessary
sort can be removed. With only partial ordering knowledge, the sort remains,
while promising files and row groups are read first; improving TopK boundaries
can prune remaining input. Without useful ordering knowledge, the engine still
scans input and maintains the best candidates. These optimizations depend on
sort expressions, statistics and physical layout.
[Official sort-pushdown explanation](https://datafusion.apache.org/blog/2026/07/20/sort-pushdown/).

The provider interface accepts pushed filters and a limit, but providers declare
filter support and exactness. Inexact filtering prevents pushing a limit below
the remaining exact filter. This is an important correctness boundary when a
page must contain the requested number of qualifying rows.
[TableProvider contract](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html).

Inference for BanyanDB: borrow ordering properties, exactness proofs, directed
scan scheduling and safe bound pushdown. Do not label a QL keyset predicate a
storage seek until the native scan actually uses it. Aggregate ordering such as
`ORDER BY SUM(value)` generally cannot use a raw timestamp seek to avoid building
the aggregates.

### SessionContext is not a retained page result

DataFusion separates session-level planning resources from per-query and task
state. Queries in one `SessionContext` share configuration and resources; each
query uses its own `SessionState`. A session ID therefore does not itself identify
a frozen ordered result with page positions.
[SessionContext source and state relationships](https://docs.rs/datafusion/latest/src/datafusion/execution/context/mod.rs.html).

`DataFrame::collect` buffers resulting batches; `execute_stream` yields a forward
batch stream and dropping it aborts execution. `DataFrame::cache` provides a
materialization primitive backed by a memory table by default, with an optional
custom cache factory. These APIs supply useful building blocks, not a documented
HTTP/gRPC random-page session service with expiration and row-number indexing.
[DataFrame API](https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html),
[CacheFactory extension](https://docs.rs/datafusion/latest/datafusion/execution/session_state/trait.CacheFactory.html).

Inference: retaining batches can avoid query recomputation, but running SQL
OFFSET repeatedly over a cached table can still traverse earlier batches.
The application must add an addressable page index to get page-depth-independent
retrieval work. Plan reuse and result reuse are separate features; BanyanDB's
[plan reuse contract](plan-reuse.md) explicitly excludes retained result batches,
cursors and live operators.

### Spill solves capacity, not page addressing

DataFusion's external sorter buffers batches, creates sorted runs when memory
is exhausted, and merges runs after consuming input. It accounts for both input
buffering and merge headroom. This supports bounded-memory execution with disk,
but its sorted output remains a stream.
[Sort implementation, 55.2.0](https://github.com/apache/datafusion/blob/55.2.0/datafusion/physical-plan/src/sorts/sort.rs).

Arrow separately defines sequential IPC streams and an IPC file format that
supports random access to record batches. A cumulative row-count index is still
needed to translate an arbitrary result row offset into a batch and in-batch
position. The format distinction is useful for a future disk-backed result
store; it does not turn a live streaming connection into a rewindable session.
[Arrow IPC file format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format),
[Arrow indexed batch reading](https://arrow.apache.org/cookbook/dev/java/io.html).

## Observability systems

The following are documented mechanisms, not evidence that different products
have equal latency or that any vendor guarantees constant page cost.

| System | Documented mechanism | Implication for this requirement |
|---|---|---|
| Splunk | Search submission creates a job SID; result retrieval is separate and supports offset/count. Jobs have lifetime controls. | A useful model for query preparation followed by result browsing; the API alone does not prove constant-cost offsets. |
| Axiom | Recommends timestamp-boundary pagination; cursor pagination is public preview. Cursor boundaries support next/previous requests. | Suitable for sequential event browsing; not a documented ordinal random-access result store. |
| Elasticsearch / OpenSearch | Point-in-time (PIT) plus `search_after` uses the previous page's sort values against a stable index view. | Separates consistent sequential browsing from deep offsets. A PIT is not a retained computed result, and a boundary is not an arbitrary ordinal page address. |
| OpenObserve | Result caching reuses historical query intervals and merges missing data; cache locality influences routing. | Useful reuse across overlapping time windows; no documented guarantee of flat random-page latency. |
| ClickHouse | Query cache retains completed query results; ordered reads can exploit a matching storage sorting-key prefix. | Cache entries do not inherently constitute an addressable full-result page store. The documented ordered-read optimization excludes `GROUP BY`. |

Sources: [Splunk search job tutorial](https://help.splunk.com/en/splunk-enterprise/leverage-rest-apis/rest-api-tutorials/10.4/rest-api-tutorials/creating-searches-using-the-rest-api),
[Splunk search endpoints](https://help.splunk.com/en/splunk-enterprise/rest-api-reference/10.2/search-endpoints/search-endpoint-descriptions),
[Axiom pagination](https://axiom.co/docs/restapi/pagination),
[Elasticsearch pagination](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results),
[OpenSearch pagination](https://docs.opensearch.org/latest/search-plugins/searching-data/paginate/),
[OpenObserve result cache](https://openobserve.ai/docs/user-guide/advanced/query-tuning/result-cache/),
[ClickHouse query cache](https://clickhouse.com/docs/concepts/features/performance/caches/query-cache),
[ClickHouse ordered reads](https://clickhouse.com/docs/reference/statements/select/order-by).

Splunk also documents disk-backed search artifacts and expiration, making its
job model a closer precedent for retained result browsing than a purely
sequential cursor. [Splunk search artifacts](https://help.splunk.com/en/splunk-enterprise/search/search-manual/9.0/manage-jobs/dispatch-directory-and-search-artifacts).
ClickHouse keys query-cache entries using query syntax trees; the conclusion
that changing an OFFSET is not automatically a shared full-result page lookup
is an inference from that contract, not a claim that every cache key retains
every syntactic distinction.

## Suggested BanyanDB design boundary

For an explicitly requested frozen browsing result:

```text
submit QL + bindings + fixed time range
    -> normal cached plan + fresh execution
    -> WHERE / aggregation / HAVING / DISTINCT / ORDER
    -> owned final columnar batches + cumulative row index
    -> publish immutable result handle when complete

fetch(handle, row offset, page size)
    -> validate owner, lifetime and bounds
    -> locate batch by row index
    -> slice only the requested rows and encode response
```

This is a proposed architecture, not existing syntax or an API proposal. The
handle identifies a particular execution's result, not merely query text:
bindings, authorization, time bounds and schema can differ between otherwise
identical QL requests. Freeze the final result after successful completion;
repeated page reads then have stable membership and order. This does not create
a storage-wide point-in-time snapshot that the original execution did not have.

The retained state should contain final visible values, schema and row index,
with its own ownership and budget. Release native scans and blocking operator
state after execution. Avoid retaining borrowed source batches, live operator
trees or mutable plan-cache entries for idle browsing sessions.

An owner directory maps the opaque result handle to its batches and state. In
a distributed server implementation it also routes to the owning node, or to
shared retained storage. Define expiration, explicit close, eviction, node loss,
concurrent reads, cancellation and authorization checks. After expiry, report
that the result is gone; silently rerunning a query can change page membership.

Use a per-result cap, aggregate tenant/process cap and bounded active-build
admission. Charge payloads, column descriptors, row index and serving scratch.
Existing memory/pool tracking should cover result ownership and release. A
bounded final result does not solve memory pressure in earlier native receive,
aggregation, DISTINCT or sorting stages.

Respect the current decision to defer spilling: start, if authorized later,
with bounded memory-only result retention. Reject results exceeding that budget;
do not add an unbounded fallback or silently truncate a promised complete result.
Future final-result spooling is a separate concern from spilling intermediate
aggregate/sort state. Disk-backed page retrieval would additionally need indexed
batch reads, bounded decode buffers, quotas and cleanup.

The current no-protobuf-change constraint leaves a practical application-owned
option: OAP or another client obtains a bounded complete result through the
existing API, retains it, and exposes its own browsing handle. A true BanyanDB
server-side result session needs an explicit handle/control contract in a future
API or deliberately designed metadata extension. Do not conceal this new
contract in ordinary bindings or claim the compiled QL cache already supplies it.

For storage-aligned raw browsing, use a separate keyset path with a total order:
every sort direction and NULL rule must participate in the boundary comparison,
and ties need a stable unique discriminator. Cached page boundaries enable
previously visited-page navigation; reverse scans can support previous-page
fetches where the storage contract permits them. Different query bindings or
sorts start a new browsing state. A fixed event-time range alone does not prevent
late arrivals or updates from changing results between independent executions.

## Verification proposal

Keep evaluation focused on two representative queries: raw ordered rows and a
grouped/HAVING/ordered result. Use identical page sizes and output widths, verify
the same final rows, and measure first, middle and last page positions.

For a retained result, randomize page request order after preparation so page 1
does not always warm the cache for the others. Report p50/p95/p99, CPU, bytes read,
allocations and retained memory for page retrieval. The structural gate is zero
new native scans, aggregate work or sorting per fetch, and work proportional to
the requested page plus batch lookup rather than preceding rows. Variable-width
results must also be compared by encoded bytes, not row count alone.

Report preparation latency/CPU/peak memory, total submit-to-first-page time,
full-session cost and retained idle bytes separately. Include forward/backward
correctness, repeated concurrent reads, expiry/cancel/eviction cleanup and
cross-tenant access rejection. Under memory-only retention, test budget rejection
and successful recovery without changing the existing native memory protector.

For keyset evaluation, record actual scanned/decoded rows and pruning at each
depth. Do not accept equal latency that is achieved by rescanning the entire
input for every page. Do not hide a failed large result behind a small warmed
benchmark or claim strict cold first/middle/last equality from cached reads.
