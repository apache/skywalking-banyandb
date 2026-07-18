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

# Property (MP milestone) — design

Property support for Canopy: browse property groups → collections → **documents**,
CRUD documents, and **query documents by reusing the Query console's builder +
code editor**. Grounded in `.handoff-import/banyandb/project/screenshots-property/`
and the BanyanDB property API (`api/proto/banyandb/property/v1/{rpc,property}.proto`).

## 0. Port the handoff source — don't rebuild

The handoff bundle already ships the property UI. **Port these three files
verbatim** (JSX + window-globals → ES-module TSX); the only real change is
swapping the prototype's in-memory mock engine for the live API.

| Handoff file | → Canopy | What it gives us |
|---|---|---|
| `property-query.jsx` | `web/src/query/PropertyQuery.tsx` | the whole builder⇄code console scoped to one property: `PROP_OPS`, `buildPropertyBydbQL`, `pqParseCode` (code-mode parser → **code mode executes**), `PQWhereGroup`/`PQCondition`, accordion + pinned foot + `Run`. Already reuses the shared `QBSection`/`QBChips`/`QB_COMBINATORS`/`qbConn*`. |
| `doc-list.jsx` | `web/src/query/results/DocList.tsx` | `DocList` + `DocCard` + adaptive value cells (JSON pretty-print/highlight, long-text clamp, scalar/int), type pills, role-gated Edit/Delete, pagination. |
| `property-form.jsx` | `web/src/query/PropertyForms.tsx` | `PropertyCollectionModal` (create/edit collection), `PropertyEntryModal` (Apply/Edit doc + tag editor w/ type dropdown + `CodeArea` for JSON/long str), `DeletePropertyEntryModal`, `validateEntry`. Reuses M3 `Field`/`Modal`. |

Styling: reuse `Property Document Styles.html` classes (fold into `canopy.css`).
`prop-doc-patterns.jsx` is a design gallery — styling reference only, do not port.

### Adaptations during the port
1. **Mock → live.** `property-query.jsx` filters an in-memory `entries` prop via
   `pqExecuteState`; replace that with a call to the **property Query RPC** (§5).
   `PropertyEntryModal.onSubmit` → **Apply**; `DeletePropertyEntryModal.onConfirm`
   → **Delete**; `PropertyCollectionModal.onSubmit` → property **registry** create.
2. **Reuse what canopy already has** (don't re-port): `QBSection`, `QBChips`,
   `QB_COMBINATORS`, `qbConnSegments`/`qbConnSummary`, `CodeEditor`, and the M3
   `Field`/`Modal`. `QBSection`/`QBChips` are currently internal to
   `QueryBuilder.tsx` → **export them** (or lift to `query/qb-parts.tsx`) so
   `PropertyQuery` imports them exactly as the prototype does.
3. **Small utils to port** (missing in canopy): `PROP_VALUE_TYPES` /
   `PROP_VALUE_LABEL`, `looksLikeJSON`, `QBSelect` (or use the native
   `<select aria-label>` canopy already uses), and `RoleContext.canWrite` →
   derive from `useAuth().session.role`.

## 1. The model (two layers)

| Layer | What it is | API | UI |
|---|---|---|---|
| **Group** | physical unit, `catalog: CATALOG_PROPERTY` | group registry (`/api/v1/group/schema`) | Properties overview + New group |
| **Property collection** (schema) | a named, schema-free document collection in a group (e.g. `sw/temp_data`) | **`database/v1` PropertyRegistryService** (`/api/v1/property/schema/*`) | group page rows + Create/Delete property |
| **Document** (data) | one entry, keyed `group/name/<id>`, carrying key-value **tags** | **`property/v1` PropertyService** (`/api/v1/property/data/*` — Apply/Delete/Query) | collection detail: DocList + Apply/Edit/Delete |

> **Two protos, do not conflate them.** The *collection* (a "property" in the
> schema sense) is CRUD'd via `database/v1/rpc.proto` PropertyRegistryService
> (Create/Update/Get/List/Delete, ~L807). The *documents under a collection* are
> CRUD'd via `property/v1/rpc.proto` PropertyService (Apply/Delete/Query, ~L100).

A document's identity is the **three-level primary key `group/name/id`** and is
**immutable**; only its tags change. Collections are schema-free — each document
carries its own tags.

## 2. What already exists vs. new

**Already in canopy** (reuse as-is): the Properties **overview** and **group
page** are already served by the Metadata infra — `App.tsx` maps
`properties → CATALOG_PROPERTY`, `Sidebar` has the Properties nav, and
`GroupPage` handles `isProperties` (breadcrumbs, listing, links to
`/properties/{group}/{name}`). The `properties.png` / `properties-group.png`
screens are essentially done.

**New for MP:**
1. **Property data API** (Apply / Delete / Query) — DataSource + `api.ts` + DTOs.
2. **Collection detail page** (`/properties/:group/:name`) — the `property-documents`
   screen: primary-key banner, New document, the **embedded query builder + code
   editor**, and the **DocList** results.
3. **Document CRUD modals** — Apply / Edit / Delete document + the **tag editor**.
4. **Collection schema create/delete** — the New property + Delete property modals
   (property registry create/delete; not yet in `api.ts`).
5. **`bydbql.ts` PROPERTY branch** — so the builder/editor generate property BydbQL.
6. **DocList result view** — render documents (id + tags) with row actions.

## 3. Routes & sidebar

- `/properties` — overview (exists) — group cards, **New group**.
- `/properties/:group` — group page (exists) — collection rows, filter, **New property**, per-row delete.
- `/properties/:group/:name` — **NEW** collection detail (`PropertyDetailRoute`).
- **Sidebar**: reuse the handoff nav. `sidebar.jsx` is a **generic recursive
  `NavRow` tree** (arbitrary depth, `count` badges, collapsed-mode flyout) driven
  by a data model — the Properties menu is just a node whose children are groups,
  each expanding to its collections (see `sidebar-menu.png`). Today canopy renders
  Properties as a **flat link**; the Metadata tree uses a `CatalogNav` component
  (section → groups) with the nav-row / `nav-count` / flyout CSS already in place.

  **Structural nuance:** Properties is one level deeper than a Metadata sub-type —
  `Properties → group (expandable) → collection (doc-count)`, whereas
  `Metadata → Measures → group (leaf, count)`. So `CatalogNav` (section→group-leaf)
  isn't a drop-in.

  **Recommended (minimal):** add a small `PropertyNav` subtree modeled on the
  handoff `NavRow` shape — groups as expandable rows, each listing its collections
  with doc counts — reusing canopy's existing nav CSS. Collection counts come from
  the schema list (collection count per group) + a per-collection document count
  (a `Query` with `limit`, or a count call). **Alternative (fuller port):** replace
  canopy's bespoke `Sidebar` with the handoff generic `NavRow`/`Sidebar` driven by
  one `nav` tree covering Metadata + Properties uniformly — DRYer, but a larger
  refactor that touches the M2 shell tests, so deferred unless we want the cleanup.

## 4. API mapping

### 4a. Collection schema — `database/v1` PropertyRegistryService (`~L807`)

Endpoints (List/Get already used by `api.ts`; **add Create + Delete**):

```
Create  POST   /api/v1/property/schema                      body: { property: { metadata:{group,name} [, tags] } }
Delete  DELETE /api/v1/property/schema/{group}/{name}
List    GET    /api/v1/property/schema/lists/{group}         (exists)
Get     GET    /api/v1/property/schema/{group}/{name}        (exists)
```

Schema-free, so Create's body is minimal (name + group; tag specs optional).

### 4b. Documents — `property/v1` PropertyService (`~L100`)

Add to `DataSource` + `ApiDataSource` (`web/src/data/api.ts`); the BFF already
proxies `/api/v1/*` to the liaison.

```
Apply  (create + update)  PUT    /api/v1/property/data/{group}/{name}/{id}
                          body: { property: { metadata:{group,name}, id, tags:[Tag] }, strategy }
                          → { created: bool, tagsNum: number }
Delete                    DELETE /api/v1/property/data/{group}/{name}/{id}
                          → { deleted: bool }
Query                     POST   /api/v1/property/data/query
                          body: { groups:[group], name, ids?:[], criteria?, tagProjection?:[],
                                  limit?, orderBy?:{tagName,sort}, trace? }
                          → { properties: [Property], trace? }
```

- **Strategy:** `STRATEGY_MERGE` (default — Apply merges/updates tags by key) vs
  `STRATEGY_REPLACE` (replace the whole tag set). Edit uses MERGE; a future
  "replace" affordance can pass REPLACE.
- **Document shape:** `Property { metadata:{group,name}, id, tags:[{key, value:<typed>}], updatedAt }`.
  Reuse the existing `Tag`/`FieldValue` DTOs; tags carry a typed value
  (`str`/`int`/`float`/`binary`/`timestamp`) — the tag editor's type dropdown.

New DTOs in `shared/src/api-dto.ts`: `PropertyDocument`, `PropertyApplyRequest`,
`PropertyQueryRequest`, `PropertyQueryResponse` (replace the `property_result?: unknown`
placeholder).

## 5. Query builder + code editor reuse (the crux)

The detail page embeds a **reduced QueryConsole** scoped to one collection
(`property-documents.png`). Reuse the existing components verbatim, parameterized:

- **FROM** — locked chip `PROPERTY <name> IN <group>` (a `LOCKED` badge; catalog
  picker + resource/group selects hidden). Add `'property'` to `QB_CATALOGS` and a
  `locked`/`fixedFrom` prop to `QueryBuilder`.
- **SELECT** — tag projection chips (`all tags` / `<tag>` …) → `tagProjection`.
- **WHERE** — **reuse the QueryBuilder WHERE tree as-is** (the recursive AND/OR +
  all operators from `where.test.ts`). `id = '…'` maps to the request's `ids`;
  other leaves map to `criteria`. `OPTIONAL`.
- **ORDER BY** — one tag + ASC/DESC → `orderBy`. **LIMIT** → `limit`.
- **No TIME clause** (property has no time dimension) — hidden for `property`.
- **Code tab** — reuse `CodeEditor`; `buildBydbQL` gains a PROPERTY branch
  emitting `SELECT <tags> FROM PROPERTY <name> IN <group> [WHERE …] [ORDER BY <tag>
  ASC|DESC] [LIMIT n]` (matches `test/cases/property/data/input/*.ql`). Builder⇄Code
  eject/resync/dirty-warning reused unchanged.

**Execution — the `property/v1` Query RPC (decided).** Per the API split, document
queries go through the property **Query** RPC, not the generic BydbQL endpoint.
The builder state is translated into a structured `POST /api/v1/property/data/query`:

| Builder | → Query request field |
|---|---|
| SELECT tag chips | `tagProjection` |
| WHERE `id = '…'` / `id IN (…)` leaves | `ids` |
| WHERE other leaves (the recursive tree) | `criteria` (`model.v1.Criteria`) |
| ORDER BY tag + dir | `orderBy: { tagName, sort }` |
| LIMIT | `limit` |
| (FROM lock) | `groups: [group]`, `name` |

The **Code tab** shows the generated BydbQL (`buildBydbQL` PROPERTY branch) for
readability/copy; in **v1 the builder is the source of truth and code-mode is
display-only** (a BydbQL→structured parser so code edits execute is a tracked
follow-up). This keeps full builder/editor component reuse while honoring
"use Query for query." Document **writes** always use Apply/Delete.

## 6. DocList result view

New `web/src/query/results/PropertyDocListView.tsx`, reusing `ResultPanel` chrome
+ the `role="region" aria-label="Query results"` landmark. Each document renders
as a card/row: the **id** (with `CopyableId`), its **tags** (key · type · value),
`updatedAt`, and row actions **Edit** / **Delete**. Honors the N-row cap
("showing first N of M"). Read-only role hides Edit/Delete.

## 7. CRUD modals (reuse M3 primitives)

Reuse the shared `Modal` / dirty-guard / focus-trap / `Field` / type-to-confirm
delete from M3.

| Modal | Screenshot | Fields | Action |
|---|---|---|---|
| Create property | `property-create` | Name * (unique, `[A-Za-z0-9_-]`), Group (read-only) | property **registry** create |
| Delete property | `property-delete` | type-collection-name to confirm | registry delete (removes all docs) |
| Apply document | `property-new-document` | Group/Name read-only, **ID ***, **tags** | **Apply** (MERGE) |
| Edit document | `property-edit-document` | Group/Name/**ID read-only**, tags | **Apply** (MERGE) |
| Delete document | `property-delete-document` | confirm `group/name/id` | **Delete** |

**Tag editor** (shared sub-component): rows of `key` · **type** dropdown
(`string`/`int`/`float`/`binary`/`timestamp`) · `value` · **`<>`** toggle (opens a
small JSON/code value editor for arrays/binary) · **×** remove, plus **Add tag**.
Maps each row to a typed `Tag`. At least one tag is required (proto
`min_items = 1`).

## 8. Validation (advisory; server authoritative)

Port pure `validateProperty()` (collection name) and `validatePropertyDocument()`
(non-empty id, ≥1 tag, unique tag keys, value parses for the chosen type). Client
checks are UX-only — a server rejection must still surface (MF2 negative test).

## 9. Testing (new framework)

- **Unit:** `bydbql` PROPERTY branch cases (extend `where.test.ts`/`bydbql.test.ts`);
  tag-editor typing/serialization; DocList render; `validate*` functions.
- **E2E** `e2e/tests/properties.spec.ts` `@e2e @property @seed`: create collection
  → Apply document → query (builder + code) renders it in DocList → edit → delete →
  delete collection. **Property data seeds over REST** (Apply) via the `SeedFactory`
  — no gRPC seeder needed (per the MP plan), so add `seedPropertyDoc()` to the
  factory. New POM `PropertyPage`.

## 10. Build order

1. **Data layer:** schema Create/Delete + document Apply/Delete/Query in
   DataSource + `api.ts` + DTOs; `SeedFactory.seedPropertyDoc` (REST Apply). Plus
   the builder-state→structured `property/data/query` translation (§5).
2. **Extract shared QB parts:** export `QBSection`/`QBChips` (+ any `QBSelect`)
   from `QueryBuilder.tsx` (or lift to `query/qb-parts.tsx`); port the small utils
   (`PROP_VALUE_TYPES`/`PROP_VALUE_LABEL`, `looksLikeJSON`).
3. **Port `property-query.jsx` → `PropertyQuery.tsx`**, swapping `pqExecuteState`
   for the Query RPC; keep `buildPropertyBydbQL` + `pqParseCode` (code mode runs).
   Unit-test `buildPropertyBydbQL` + `pqParseCode` (round-trip; property `.ql` corpus).
4. **Port `doc-list.jsx` → `DocList.tsx`** (role gate → `useAuth`).
5. **Port `property-form.jsx` → `PropertyForms.tsx`** (onSubmit → Apply/registry;
   reuse M3 `Field`/`Modal`/`CodeEditor`).
6. **`PropertyDetailRoute`** wiring PropertyQuery + DocList + the modals; sidebar
   collection tree + doc counts.
7. **E2E** `properties.spec.ts` + POM; pixel checks vs the handoff screens.

Fits the MP milestone in `.omc/plans/canopy-subproject.md` (Property CRUD + query
built and verified together against a live, seeded BanyanDB).
