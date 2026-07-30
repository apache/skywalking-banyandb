<!--
  Licensed to Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for
  additional information regarding copyright ownership. Apache Software
  Foundation (ASF) licenses this file to you under the Apache License,
  Version 2.0 (the "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations
  under the License.
-->

# Pipelines + Pipelines/TopN — design

Pipelines are continuous processing that maintains derived results over stored
data. v1 has **one** pipeline type — **TopN** — where each "pipeline" is a
`TopNAggregation` schema. Grounded in
`.handoff-import/banyandb/project/screenshots-pipelines/` and the BanyanDB
`database/v1` TopNAggregationRegistryService (`rpc.proto` ~L698).

## 0. Port the handoff source — don't rebuild

The handoff already ships the whole feature. **Port these three files** (JSX +
window-globals → ES-module TSX); the only real change is swapping mock data /
`window` navigation for the live registry API + react-router.

| Handoff file | → Canopy | What it gives us |
|---|---|---|
| `pipelines.jsx` | `web/src/pages/PipelinesPage.tsx` | `PipelinesOverview` (stats tiles + "Pipeline types" cards + Recent), `PIPELINE_TYPES`, `PipelineTypeNotFound`. |
| `topn-page.jsx` | `web/src/pipelines/TopNList.tsx` + `TopNDetail.tsx` | `TopNList` (filter by name/group/source/direction + aggregation cards), `TopNDetail` (source/ranking/group-by/criteria + **Run Top-N query**), `RankBadge`, criteria chips. |
| `topn-form.jsx` | `web/src/pipelines/TopNForms.tsx` | `TopNFormModal` (create/edit), `DeleteTopNModal`, `validateTopN`, `TopNSelect`, `TopNChipPicker` (group-by tags), `CriteriaEditor` (flat ANDed `{tag,op,value}` list). Reuses M3 `Field`/`Modal`. |

**Do NOT port** `topn-results.jsx` — it's the leaderboard result view, already
shipped in M4 as `web/src/query/results/TopNResultView.tsx`. Styling comes from
the existing `canopy.css` (the pipelines/topn classes are in the shared theme).

## 1. The model

A TopN pipeline is a **`TopNAggregation`** (schema.proto `TopNAggregation`):

```
metadata:          { group, name }          # identity (name immutable)
source_measure:    { group, name }          # the measure it ranks
field_name:        string                   # the ranked field
field_value_sort:  model.v1.Sort            # SORT_DESC | SORT_ASC | SORT_UNSPECIFIED
group_by_tag_names:[string]
criteria:          model.v1.Criteria        # optional filter (flat AND list in the UI)
counters_number:   int32                    # e.g. 1000
lru_size:          int32                    # e.g. 10
```

**Direction ⇄ sort** (port `RankBadge` verbatim): `topN` = `SORT_DESC` (largest
first), `bottomN` = `SORT_ASC` (smallest first), `both` = `SORT_UNSPECIFIED`.
The `criteria` uses the same `model.v1.Criteria` as measure/property queries,
but the form edits it as a **flat ANDed condition list** (the handoff's simpler
`CriteriaEditor`), not the recursive WHERE tree.

## 2. API — `database/v1` TopNAggregationRegistryService (`~L698`)

`List` already exists in canopy (`api.ts` `listTopNAggregations`); **add the rest**:

```
List    GET    /api/v1/topn-agg/schema/lists/{group}     (exists)
Get     GET    /api/v1/topn-agg/schema/{group}/{name}
Create  POST   /api/v1/topn-agg/schema                    body: { topNAggregation: {...} }
Update  PUT    /api/v1/topn-agg/schema/{group}/{name}     body: { topNAggregation: {...} }
Delete  DELETE /api/v1/topn-agg/schema/{group}/{name}
```

Add to `DataSource` + `ApiDataSource`: `getTopNAggregation`, `createTopNAggregation`,
`updateTopNAggregation`, `deleteTopNAggregation`. Extend the `TopNAggregationSchema`
DTO (`shared/src/api-dto.ts`) to carry the full shape above (source_measure,
field_value_sort, group_by_tag_names, criteria, counters_number, lru_size).
Note the response key is `topNAggregation` (camelCase, per the existing List).

## 3. Routes & sidebar

- `/pipelines` — `PipelinesOverview` (exists only as a "Coming soon" stub today).
- `/pipelines/topn` — `TopNList` across all measure groups.
- `/pipelines/topn/:group/:name` — `TopNDetail`.
- **Sidebar:** Pipelines is a flat link today. Extend it (minimal, like Property's
  `PropertyNav`) to `Pipelines → TopN (aggregation count)`. The screenshots show
  only `Pipelines → TopN` (one level), so this is simpler than Property's tree.

## 4. Reuse (don't re-port)

- **Forms:** M3 `Field` / `Modal` / dirty-guard / focus-trap / type-to-confirm
  delete (the `TopNFormModal` already targets `Field`/`Modal`).
- **Measure metadata:** the source-group / source-measure / ranked-field pickers
  are populated from the existing `listGroups` + `listResourcesInGroup('measures')`
  + the measure's field names — canopy already fetches these for the query builder.
- **Run Top-N query:** `TopNDetail`'s button deep-links to the M4 TopN query
  console. Reuse the **existing** navigation-state seed already wired from
  `GroupPage` ("Query this resource"): `navigate('/query', { state: seed })` with
  `{ catalog: 'topn', group, resource: aggName }`; `QueryConsole` already consumes
  that seed and dispatches TopN queries to `/v1/measure/topn` (M4, SF5). Do NOT
  build a new query path.
- **Leaderboard view:** `TopNResultView` (M4) renders the run — nothing new.
- **Criteria tag options:** the source measure's tag names (from its schema),
  same source the query builder already uses.

## 5. Adaptations during the port

1. **Mock → live.** `PipelinesOverview`/`TopNList`/`TopNDetail` read a mock
   `groups` prop; replace with `listGroups` (filtered to measure groups) +
   `listTopNAggregations` per group (or aggregate across groups for the "all
   groups" list). `TopNFormModal.onSubmit` → create/update; `DeleteTopNModal` →
   delete. Counts (overview "N aggregations", "groups in use") derive from the
   list calls.
2. **window nav → react-router.** `onNavigate` → `useNavigate`; the detail
   "Run Top-N query" → navigate to `/query` with the seed state.
3. **Read-only role** hides New/Edit/Delete (align with the BFF 403), same as the
   other CRUD pages.

## 6. Validation (advisory; server authoritative)

Port `validateTopN` (name pattern, required source measure + ranked field,
counters/lru numeric bounds, criteria rows have tag+value) as a tested pure
function. Client checks are UX-only — a server rejection must still surface.

## 7. Testing (new framework)

- **Unit:** `validateTopN`; the direction⇄sort mapping; `CriteriaEditor`
  serialization; `TopNList`/`TopNDetail` render from a fixture aggregation.
- **E2E** `e2e/tests/pipelines.spec.ts` `@e2e @pipelines @seed` + a `PipelinesPage`
  POM: overview → TopN list → create aggregation (over a seeded measure) → detail
  renders it → edit → delete. Seed the source measure + aggregation via the
  registry over HTTP (`SeedFactory.createTopNAggregation`, add it). Runs live via
  a temp NOAUTH BFF (as the M4/property e2e did) — verify green before claiming.

## 8. Build order

1. Data layer: `TopNAggregationSchema` DTO (full shape) + Get/Create/Update/Delete
   in DataSource + `api.ts`; `SeedFactory.createTopNAggregation`.
2. Port `topn-form.jsx` → `TopNForms.tsx` (onSubmit → create/update; reuse M3
   Field/Modal; port `CriteriaEditor`, `TopNChipPicker`, `validateTopN`).
3. Port `topn-page.jsx` → `TopNList.tsx` + `TopNDetail.tsx` (live data; Run-query
   deep-link).
4. Port `pipelines.jsx` → `PipelinesPage.tsx`; wire `/pipelines`, `/pipelines/topn`,
   `/pipelines/topn/:group/:name` routes; minimal Pipelines→TopN sidebar nav.
5. Unit tests + e2e `pipelines.spec.ts` + POM; run live and confirm green.

Mirrors the Property MP shape: port the handoff, swap mock→live registry, reuse
M3 forms + M4 TopN query, verify against a live seeded BanyanDB.
