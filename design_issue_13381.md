# Design: Remove Unnecessary Check Requiring Tags in Criteria to be Present in Projection

## Issue
[Issue #13381](https://github.com/apache/skywalking/issues/13381): In the current implementation of BanyanDB's query engine, a validation check is performed to ensure that tags used in the criteria (e.g., filtering conditions) are also included in the projection. If a tag is present in the criteria but not in the projection, the query fails.

## Problem Analysis
The restriction forces users to include tags in the projection even if they only want to filter by them. This increases network overhead and client-side processing.

Since BanyanDB uses inverted indices for filtering, the data for filtering is accessed via the index. Furthermore, if a tag is in the projection, the underlying block loading will ignore the tag (relying on the index or metadata). Therefore, it is not necessary to retrieve the actual tag value in the result `DataPoint` for the query to be valid, and the block loader is already designed to skip these tags.

## Affected Code
The user identified the following files as relevant:
- `pkg/query/logical/measure/measure_plan_indexscan_local.go`
- `pkg/query/logical/stream/stream_plan_indexscan_local.go` (implied by context)

## Proposed Solution

### 1. Analyze and Remove Validation in Measure Plan
In `pkg/query/logical/measure/measure_plan_indexscan_local.go`, the `Analyze` method of `unresolvedIndexScan` handles the planning.

We need to:
1.  Examine the `Analyze` method and related helper functions to locate the explicit validation that checks if criteria tags are in `projectionTags`.
    -   *Self-Correction*: A preliminary scan of the code did not reveal an obvious loop checking `criteria` against `projectionTags`. It is possible the check is implicitly performed or located in a utility function called during analysis. Alternatively, the issue might be that the `Schema` passed to `BuildQuery` is restricted to projected tags, causing `IndexDefined` to fail.
    -   However, investigation showed `measure_analyzer.go` passes the full schema.
    -   If the explicit check is not found, we must verify if the failure is caused by `CreateTagRef` or `BuildQuery` usage.
2.  If an explicit check exists (e.g. `if !isInProjection(tag) { return error }`), remove it.
3.  Ensure that `inverted.BuildQuery` (or `BuildIndexModeQuery`) continues to receive a Schema that allows resolving the index rules for the criteria tags, even if they are not in the projection.

### 2. Verify Stream Plan
Check `pkg/query/logical/stream/stream_plan_tag_filter.go` and `pkg/query/logical/stream/stream_plan_indexscan_local.go` for similar restrictions. The stream query planning often uses `unresolvedTagFilter` which selects an index scanner. We should ensure no validation restricts criteria tags to the projection set.

### 3. Execution Flow
-   **Planning**: The `IndexScan` node will be configured with:
    -   `criteria`: containing all filtering conditions.
    -   `projectionTags`: containing only the requested tags.
-   **Execution**:
    -   The `localIndexScan` executes the query using `ec.Query`.
    -   `ec.Query` (storage layer) uses the inverted index (derived from `criteria`) to find Series IDs / Global IDs.
    -   The storage engine retrieves data points respecting `TagProjection`:
        -   If a tag is in the projection and is **indexed**, the underlying block loading **ignores** it. The value is expected to be retrieved from the index (if available) or `storedIndexValue`.
        -   If a tag is **not** in the projection, it is not loaded from the block nor the index.
    -   The `IndexScan` iterator constructs `DataPoint`s with only the projected tags.
    -   Since filtering was done via index, and the block loading ignores indexed tags anyway, the missing tags in `DataPoint`s (when not projected) do not affect correctness.

## Test Plan
1.  Create a new test case in `measure/query_test.go` (or similar).
2.  Define a measure with tags `tagA` (indexed) and `tagB`.
3.  Ingest data.
4.  Execute a query: `criteria: tagA == "value"`, `projection: [tagB]`.
5.  **Current Behavior**: Expect failure (based on issue description).
6.  **Expected Behavior**: Query succeeds, returning `DataPoint`s with `tagB` only, filtered by `tagA`.

