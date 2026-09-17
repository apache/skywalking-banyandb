# BanyanDB License-Compliance Design

**Status:** Proposed; not implemented.  
**Baseline inspected:** BanyanDB release-candidate commit `d43de4f5`, using SkyWalking Eyes revision `55373684d1b70e5f8fd9fc8ec114a89ad11a56a3`.

## 1. Objectives

Make licensing compliance reproducible, package-specific, and continuously verified by CI.

The system must:

1. Keep **SkyWalking Eyes responsible for identifying dependency licenses and generating LICENSE content and license texts**.
2. Generate package-specific NOTICE files and prominent third-party disclosures.
3. Detect stale licensing output and dependency legal-document changes.
4. Validate actual release archives before signing.
5. Use the same commands locally, in PR CI, and during release preparation.
6. Reserve human review for legal decisions that automation cannot safely make.

The system must not:

- Introduce a second license detector.
- Require manual edits to generated LICENSE files.
- Treat every declared or production dependency as bundled.
- Copy every upstream NOTICE indiscriminately.
- Add binary dependency notices to source archives that do not bundle those dependencies.
- Automatically commit, publish, or sign changes from ordinary PR CI.

ASF requires LICENSE and NOTICE to represent the contents of the particular distribution. Applicable upstream NOTICE information must be preserved. [ASF licensing guidance](https://infra.apache.org/licensing-howto.html).

## 2. Findings Driving the Design

### 2.1 Release-package findings

The reviewed candidate exposed these issues:

- `banyand` and `fodc-agent` bundle Prometheus code but omit applicable upstream NOTICE attribution.
- `banyand` and `bydbctl` need prominent disclosures for bundled MPL-2.0 components.
- `scripts/release.sh` copies the same `dist/NOTICE` into every binary package.
- `scripts/package-licenses.py` warns, rather than fails, when dependency license text files are missing.
- The existing Go executable inventory retains module paths but discards versions.
- Root header checking excludes all Markdown files.

### 2.2 CI findings

The existing `.github/workflows/ci.yml` runs:

```sh
make license-check
make license-dep
```

However:

- Its earlier consistency check runs before dependency-license generation.
- It does not provide a dedicated, explicit post-generation licensing drift gate.
- PR path exclusions cover UI, documentation, and Markdown changes.
- A normal development build does not establish coverage of every platform shipped in release archives.

### 2.3 SkyWalking Eyes capabilities

The pinned version supports:

```sh
license-eye dep resolve -o <license-directory> -s <summary-template>
```

It also supports an explicit summary output path:

```sh
license-eye dep resolve \
  -o <license-directory> \
  -s <summary-template> \
  -l <summary-output>
```

Its summary template exposes:

- Project license content.
- Dependency name.
- Dependency version.
- Dependency license ID/expression.

It supports Sprig template functions.

The inspected implementation does not expose dependency NOTICE content, homepage, or corresponding-source URLs through the summary context.

Other relevant behaviors:

- Some license-file write failures are logged without failing the command.
- Existing name-derived license filenames are skipped.
- npm resolution installs and prunes dependencies.
- `dep check --weak-compatible` does not establish satisfaction of all legal conditions; its help explicitly requires manual confirmation.

Therefore, Eyes remains the generator, while BanyanDB supplies package-content selection, supplemental obligation handling, and strict output validation.

## 3. Architecture

```text
Exact commit + locked dependencies + pinned SkyWalking Eyes
                         │
                         ▼
            Generate fresh licensing inventory
                         │
                         ▼
           Detect licensing / NOTICE changes
                         │
                         ▼
              Validate reviewed obligations
                         │
                         ▼
           Assemble package-specific legal files
                         │
                         ▼
              Validate actual release archives
                         │
                         ▼
           Upload reports, diffs, unsigned artifacts
```

Separate four responsibilities:

| Responsibility | Owner |
|---|---|
| License identification and generated license text | SkyWalking Eyes |
| Identification of actual bundled components | BanyanDB build/package inventory |
| Review of applicable notices and special disclosures | Reviewed supplemental metadata |
| Assembly and compliance checks | BanyanDB packaging and validation scripts |

## 4. Files and Responsibilities

### Existing files to retain or modify

| Path | Responsibility |
|---|---|
| `.licenserc.yaml` | Root Eyes configuration and reviewed license overrides |
| `ui/.licenserc.yaml` | UI-specific Eyes configuration |
| `mcp/.licenserc.yaml` | MCP-specific Eyes configuration |
| `dist/LICENSE.tpl` | Human-readable Eyes summary template |
| `dist/LICENSE` | Generated dependency license content |
| `dist/licenses/` | Generated dependency license texts |
| `ui/LICENSE.tpl` | UI Eyes summary template |
| `ui/LICENSE` | Generated UI licensing input |
| `mcp/LICENSE.tpl` | MCP Eyes summary template |
| `mcp/LICENSE` | Generated MCP licensing input |
| `dist/NOTICE` | Base ASF notice for binary-package assembly |
| `README.md` | Base README, supplemented during package staging |
| `scripts/package-licenses.py` | Package-specific legal-file assembly |
| `scripts/package-licenses_test.py` | Packaging regression tests |
| `scripts/release.sh` | Archive staging and assembly |
| `scripts/build/license.mk` | Eyes installation and shared licensing configuration |
| `scripts/build/version.mk` | Pinned Eyes revision |
| `Makefile` | Local and CI entry points |
| `.github/workflows/ci.yml` | Existing general CI integration |
| `docs/release.md` | Release procedure and failure-resolution instructions |

### Proposed additions

```text
.github/workflows/license-compliance.yml

dist/legal/
  dependency-catalog.tpl
  catalog/
    go.json
    ui.json
    mcp.json
  obligations.json
  notices/
    prometheus-common.txt

scripts/
  license-compliance.py
  license-compliance_test.py
```

Generated build evidence:

```text
build/license-compliance/
  report.json
  report.md
  generated.patch
  inventories/
  upstream/
  packages/
```

The catalog files are generated outputs, not a manually maintained license database.

## 5. Eyes Generation and Machine-Readable Catalog

### 5.1 Preserve the existing generation workflow

Continue using:

```sh
make license-dep
```

This remains responsible for refreshing Eyes-generated license summaries and texts.

Do not manually edit generated `dist/LICENSE`, `ui/LICENSE`, `mcp/LICENSE`, or dependency license texts to fix detected license classifications. Necessary overrides belong in the relevant `.licenserc.yaml`.

### 5.2 Add a catalog template

Add:

```text
dist/legal/dependency-catalog.tpl
```

Use Eyes’ existing summary-template support to emit records containing:

```json
{
  "schema_version": 1,
  "dependencies": [
    {
      "name": "github.com/prometheus/common",
      "version": "v0.70.1",
      "license_expression": "Apache-2.0"
    }
  ]
}
```

The template must correctly escape strings and produce deterministic ordering.

The catalog-generation command uses the existing Eyes executable:

```sh
bin/license-eye dep resolve \
  -s dist/legal/dependency-catalog.tpl \
  -l build/license-compliance/catalog-go.json
```

UI and MCP catalog generation runs from their respective isolated component workspaces, using their own Eyes configurations.

The implementation must include a contract test against the pinned Eyes version. Do not assume template capabilities remain unchanged after an Eyes upgrade.

### 5.3 Clean, isolated execution

Generation must run in a disposable workspace because npm resolution can install or prune dependencies.

Requirements:

- Use the exact checked-out commit and lockfiles.
- Start with clean generated-output directories.
- Do not reuse stale license output.
- Do not allow generation to silently alter source lockfiles.
- Record the Eyes revision and input hashes in the compliance report.
- Explicitly verify required files after Eyes exits successfully.

## 6. Reviewed Obligation Metadata

Add:

```text
dist/legal/obligations.json
```

This supplements Eyes; it does not duplicate its license classification.

### 6.1 Schema

Each component review is keyed by ecosystem, resolved component identity, and version.

Example structure:

```json
{
  "schema_version": 1,
  "components": [
    {
      "ecosystem": "go",
      "name": "github.com/prometheus/common",
      "version": "v0.70.1",
      "review": {
        "status": "approved",
        "evidence": "review-reference",
        "input_fingerprint": "<sha256-of-reviewed-inputs>"
      },
      "upstream_legal_files": [
        {
          "kind": "NOTICE",
          "path": "NOTICE",
          "url": "https://raw.githubusercontent.com/prometheus/common/v0.70.1/NOTICE",
          "sha256": "<sha256-of-upstream-file>"
        }
      ],
      "notice": {
        "decision": "include",
        "fragment": "notices/prometheus-common.txt",
        "reason": "Applicable upstream attribution."
      },
      "disclosure": null
    },
    {
      "ecosystem": "go",
      "name": "github.com/hashicorp/golang-lru",
      "version": "v1.0.2",
      "review": {
        "status": "approved",
        "evidence": "review-reference",
        "input_fingerprint": "<sha256-of-reviewed-inputs>"
      },
      "upstream_legal_files": [],
      "notice": {
        "decision": "none",
        "fragment": null,
        "reason": "Reviewed; no additional applicable NOTICE text."
      },
      "disclosure": {
        "required": true,
        "homepage": "https://github.com/hashicorp/golang-lru",
        "source_url": "https://github.com/hashicorp/golang-lru/tree/v1.0.2"
      }
    }
  ]
}
```

The example fingerprints and review references are placeholders, not approved review data.

### 6.2 Rules

- License expressions come from Eyes.
- An empty list of upstream legal files is valid only after an explicit review.
- NOTICE decisions are `include` or `none`; absence of a decision is not approval.
- Included fragments must exist.
- Reviewed inputs include the component version and relevant legal-file fingerprints.
- New versions invalidate old reviews.
- Changed legal content invalidates approval even if the nominal version is unchanged.
- Review records may remain for components not shipped in a particular package; those records must not produce package disclosures.

### 6.3 Human-review boundary

Automation discovers upstream legal files and detects changes.

Humans decide:

- Which upstream NOTICE portions apply.
- Whether special disclosure conditions are satisfied.
- How to handle ambiguous or mixed licensing.
- Whether a license exception is appropriate.

Do not automatically copy all upstream NOTICE text into every package.

## 7. Actual Package Inventories

### 7.1 Go executables

Inspect every executable in each package with:

```sh
go version -m <executable>
```

Preserve:

- Module path.
- Module version.
- Replacement path and version.
- Executable association.
- Target platform where available.

Use the union across all shipped binaries and platforms.

A dependency included only in a Darwin executable still belongs to a multi-platform `bydbctl` archive.

Local replacements without reproducible identity require an explicit supported provenance mechanism or must fail release validation.

### 7.2 UI

Use build output metadata and copied-asset inventory to determine which third-party content is embedded or shipped.

Do not equate the full npm production dependency graph with the final bundle.

Include relevant copied fonts, JavaScript, CSS, and other third-party assets in the inventory.

### 7.3 MCP

The inspected package contains transpiled JavaScript with external imports and does not copy `node_modules`.

Therefore:

- Externally installed dependencies are not automatically bundled dependencies.
- Do not append the entire MCP dependency-license fragment merely because `mcp/package.json` lists those dependencies.
- Inspect MCP outputs for genuinely embedded third-party content.
- Keep installation/runtime requirements separate from bundled-license declarations.

### 7.4 Source archive

Validate source contents independently.

Do not propagate notices for separately downloaded Go/npm dependencies into source-root licensing documents as though those dependencies were bundled.

## 8. `scripts/package-licenses.py` Changes

Extend the existing script rather than creating a second packaging implementation.

### 8.1 Inputs

Proposed interface:

```sh
python3 scripts/package-licenses.py \
  --license dist/LICENSE \
  --licenses-dir dist/licenses \
  --catalog dist/legal/catalog/go.json \
  --obligations dist/legal/obligations.json \
  --notice dist/NOTICE \
  --readme README.md \
  --bins build/package/bin \
  --out build/package
```

Additional component inventories supply actual UI/MCP bundled content.

Existing arguments should be retained where practical. New catalog and obligation handling must not bypass existing package filtering.

### 8.2 Processing

1. Inventory all packaged executables and other bundled components.
2. Match exact resolved identities and versions against Eyes output.
3. Select generated license entries and corresponding texts.
4. Validate current obligation reviews.
5. Assemble NOTICE from the ASF base plus applicable reviewed fragments.
6. Generate prominent disclosures near the README License section.
7. Validate staged output.
8. Finalize staging only when every check passes.

### 8.3 Outputs

```text
LICENSE
NOTICE
README.md
licenses/
```

Also produce a machine-readable package inventory in:

```text
build/license-compliance/inventories/
```

### 8.4 Error handling

Change `copy_license_texts()` from warning-only behavior to a hard failure when required content is missing.

Handle Eyes’ name-only license filenames carefully:

- Detect collisions between different versions.
- Do not silently overwrite or reuse an unrelated version’s text.
- Either preserve validated version-specific texts or reject unsupported ambiguity.

Assembly must be deterministic and must not fetch network resources during rendering.

## 9. NOTICE and Prominent Disclosures

Expected initial corrections:

| Package | Correction |
|---|---|
| `banyand` | Applicable Prometheus attribution; MPL disclosure for `hashicorp/golang-lru` |
| `fodc-agent` | Applicable Prometheus attribution |
| `bydbctl` | MPL disclosure for `shoenig/go-m1cpu`, with platform applicability |
| `fodc-proxy` | Only obligations supported by its own inventory |
| Source | Only obligations arising from actual source-archive contents |

A generated README disclosure should identify:

- Component and version.
- License expression from Eyes.
- Homepage.
- Corresponding-source location where relevant.
- Local license-text path.
- Platform applicability when useful.

Example:

```markdown
## Bundled third-party components

This distribution includes HashiCorp golang-lru v1.0.2 under MPL-2.0.

- Homepage: https://github.com/hashicorp/golang-lru
- Corresponding source: https://github.com/hashicorp/golang-lru/tree/v1.0.2
- License: licenses/license-github.com-hashicorp-golang-lru.txt
```

A buried dependency row alone should not substitute for prominent disclosure. [ASF Category B conditions](https://www.apache.org/legal/resolved.html#category-b).

## 10. Make Targets

The following targets define one implementation shared by developers, CI, and release preparation.

| Target | Responsibility |
|---|---|
| `make license-check` | Existing root Eyes header check |
| `make -C ui license-check` | UI header check |
| `make license-dep` | Existing Eyes license generation |
| `make license-catalog` | Generate machine-readable Eyes catalogs |
| `make license-discover` | Discover legal files and calculate review fingerprints |
| `make license-review-check` | Validate reviewed obligations against current inputs |
| `make license-drift-check` | Regenerate in isolation and detect tracked/new/deleted output drift |
| `make license-tests` | Run licensing and packaging tests |
| `make license-compliance` | Aggregate PR-level compliance checks |
| `make release-binary` | Existing unsigned archive assembly |
| `make release-validate` | Validate all final release archives |
| `make release-sign` | Sign only archives that pass validation |
| `make release-assembly` | Build, validate, then sign |
| `make release-push-candidate` | Existing separately authorized publication step |

Local commands:

```sh
# Refresh generated licensing material.
make license-dep
make license-catalog

# Discover changes requiring review.
make license-discover
make license-review-check

# Run the same checks as PR CI.
make license-compliance

# Build and validate unsigned release archives.
make release-binary
make release-validate

# Signing remains a separate trusted operation.
make release-sign
```

Test command:

```sh
python3 -m unittest discover -s scripts -p '*licenses_test.py'
```

The `license-tests` target must also execute:

```sh
python3 -m unittest discover -s scripts -p 'license-compliance_test.py'
```

### 10.1 Release ordering

The required sequence is:

```text
release-binary
      ↓
release-validate
      ↓
release-sign
```

Implement ordering through explicit dependencies or sequential recursive Make invocations. Do not rely on unordered sibling prerequisites, which may execute concurrently under `make -j`.

`make release-sign` must independently validate the archives it is about to sign. A stale success stamp is insufficient.

## 11. Full CI Workflow

### 11.1 Dedicated workflow and triggers

Add:

```text
.github/workflows/license-compliance.yml
```

Support:

- Every `pull_request`, without UI/docs/Markdown exclusions.
- Pushes to `main` and maintained release branches.
- Weekly scheduled checks.
- `workflow_dispatch`.
- `workflow_call` from release-candidate workflows.

The maintained-branch list must be explicit and reviewed as branches enter or leave support. Scheduled execution must explicitly check those branches; it must not imply that GitHub schedules run automatically on every branch.

### 11.2 Step 1 — Checkout and establish identity

Record:

- Exact commit SHA.
- Branch or PR identity.
- Lockfile hashes.
- Eyes revision.
- Relevant build-tool versions.

Use read-only repository permissions.

Do not use `pull_request_target` to execute untrusted PR code.

### 11.3 Step 2 — Prepare isolated generation

Create disposable workspaces for dependency resolution.

Install the pinned Eyes version through the existing tooling in:

```text
scripts/build/license.mk
scripts/build/version.mk
```

Resolve dependencies from the checked-out inputs.

Do not share mutable npm generation state with application build jobs.

### 11.4 Step 3 — Check headers

Run:

```sh
make license-check
make -C ui license-check
```

Replace the blanket Markdown exclusion with narrow, reviewed exceptions.

Project-authored substantive documentation should receive appropriate ASF headers. Third-party content must retain its own licensing notices.

### 11.5 Step 4 — Regenerate Eyes outputs

Run in the isolated workspace:

```sh
make license-dep
make license-catalog
```

Validate output existence and completeness even when Eyes exits successfully.

### 11.6 Step 5 — Check generated drift

Run:

```sh
make license-drift-check
```

Detect:

- Modified tracked files.
- Deleted files.
- Newly generated untracked files.

The drift check runs **after generation**.

On failure, upload:

```text
build/license-compliance/generated.patch
build/license-compliance/report.md
```

Do not silently commit regenerated output.

### 11.7 Step 6 — Discover and validate obligations

Run:

```sh
make license-discover
make license-review-check
```

Behavior:

| Event | Result |
|---|---|
| Known component, unchanged reviewed inputs | Pass |
| New component or version | Fail pending review |
| New/changed upstream NOTICE | Fail with evidence and diff |
| Missing required disclosure | Fail |
| Component removed from a package | Omit its generated package obligation |
| Discovery failure | Report inability to verify; never treat as “no NOTICE” |

Fetch exact-version sources where possible. Branch-head URLs are not acceptable substitutes for versioned provenance.

### 11.8 Step 7 — Run packaging tests

Run:

```sh
make license-tests
```

Tests must include generated-input contracts, inventory parsing, NOTICE selection, disclosure rendering, and negative cases.

### 11.9 Step 8 — PR-level validation

Run:

```sh
make license-compliance
```

This covers:

- Header checks.
- Generated-output drift.
- Review freshness.
- Packaging tests.
- Available built-component inventories.

Report precisely which platforms and components were checked.

**A PR pass does not claim full release-archive validation.**

### 11.10 Step 9 — Full release validation

The release workflow invokes the reusable licensing workflow in full-release mode.

Run:

```sh
make release-binary
make release-validate
```

The job must inspect every platform and variant included in the five archives:

```text
skywalking-banyandb-${VERSION}-src.tgz
skywalking-banyandb-${VERSION}-banyand.tgz
skywalking-banyandb-${VERSION}-bydbctl.tgz
skywalking-banyandb-${VERSION}-fodc-agent.tgz
skywalking-banyandb-${VERSION}-fodc-proxy.tgz
```

Build from the intended source snapshot. Validate actual archives, not only intermediate staging directories.

### 11.11 Step 10 — Upload evidence

Upload compliance evidence even when checks fail:

```text
build/license-compliance/report.json
build/license-compliance/report.md
build/license-compliance/generated.patch
build/license-compliance/inventories/
build/license-compliance/upstream/
```

Full-release runs may also upload unsigned archives for developer review.

Evidence must identify:

- Commit and source identity.
- Tool versions.
- Dependency inventory.
- Package/platform coverage.
- Review freshness.
- Archive hashes.
- Failures and remediation instructions.

### 11.12 Step 11 — Enforce required gates

Configure the licensing workflow’s aggregate status as a required PR check.

If jobs are conditionally skipped, the aggregate job must distinguish intentional non-applicability from failure or cancellation. It must not report success when a required check did not run.

Branch-protection changes are repository administration, not a side effect of editing workflow YAML.

### 11.13 Step 12 — Keep signing and publication trusted

Ordinary PR CI must not receive signing keys or SVN publishing credentials.

Signing occurs only after validation in a trusted release environment:

```sh
make release-sign
```

Publication remains separately authorized:

```sh
make release-push-candidate
```

When signing outside CI, validate the exact downloaded or locally built archives again. Do not assume that another archive with the same filename is the validated artifact.

## 12. Final-Archive Validation Rules

`make release-validate` must reject archives when:

1. LICENSE or NOTICE is absent.
2. A bundled component is missing from the legal inventory.
3. Required license text is absent or empty.
4. Dependency versions disagree with the generated catalog.
5. Required review metadata is stale or absent.
6. Required NOTICE attribution is absent.
7. Required prominent disclosures are absent.
8. Licensing documents describe dependencies not actually bundled.
9. Referenced local legal-document paths do not exist.
10. An expected executable cannot be inspected.
11. License filename collisions are unresolved.
12. Source licensing incorrectly incorporates unbundled binary dependencies.
13. Archive entries are unsafe to extract.

The validator must independently read the tarballs and compare their content with the expected inventory and reviewed obligations.

Compatibility checks are useful inputs, but passing Eyes compatibility checks does not replace these distribution-specific rules.

## 13. Keeping the Procedure Up to Date

1. Keep Eyes pinned for reproducibility.
2. Update Eyes through reviewed dependency-update PRs.
3. Run contract tests before accepting a new Eyes version.
4. Run scheduled freshness checks on supported branches.
5. Invalidate reviews after version or upstream legal-content changes.
6. Generate legal files from current package inventories, not a static repository-wide union.
7. Maintain `docs/release.md` alongside workflow and Make-target changes.
8. Report drift with actionable patches rather than silently updating the repository.
9. Record verification limitations explicitly.
10. Never interpret unavailable upstream evidence as successful verification.

Optional upstream Eyes improvements include propagating license-write failures and exposing NOTICE/provenance metadata. BanyanDB’s corrective work must not depend on those upstream changes.

## 14. Test Coverage

Extend:

```text
scripts/package-licenses_test.py
scripts/license-compliance_test.py
```

Required cases:

| Case | Expected behavior |
|---|---|
| Prometheus bundled | Applicable attribution included |
| Prometheus absent | Attribution excluded |
| MPL component bundled | Required disclosure generated |
| Darwin-only module | Discovered when packaging on Linux |
| Module replacement | Resolved replacement identity used |
| Version mismatch | Failure |
| Missing license file | Failure |
| Missing NOTICE fragment | Failure |
| Changed upstream legal file | Review invalidated |
| New generated file | Drift detected |
| Deleted generated file | Drift detected |
| External MCP dependency | Not declared bundled |
| Embedded UI dependency | Included |
| Multiple versions sharing filename | Explicit handling or failure |
| Repeated generation | Deterministic output |
| Tampered final archive | Validation failure |
| Source-only distribution | No unrelated binary notices |

## 15. Acceptance Criteria

The implementation is complete when all of the following hold:

1. SkyWalking Eyes remains the source of generated license classifications and texts.
2. No manual generated-LICENSE edits are required.
3. All PRs, including UI/docs/Markdown-only PRs, receive licensing checks.
4. Generated drift is checked after generation, including additions and deletions.
5. New or changed bundled dependencies cannot silently inherit stale reviews.
6. Applicable Prometheus attribution appears in affected packages and not unrelated packages.
7. MPL disclosures identify component, version, license, homepage, and corresponding-source location.
8. Inventories cover every executable and platform shipped in release archives.
9. UI/MCP declarations reflect actual bundled content rather than the entire dependency graph.
10. Missing legal files fail packaging.
11. Source and binary licensing remain content-specific.
12. Header-check exclusions are narrow and documented.
13. All five final archives pass independent validation before signing.
14. Local and CI execution use the same Make targets and scripts.
15. Reports identify commit, tool versions, package coverage, and archive hashes.
16. Scheduled runs detect stale legal inputs without silently committing changes.
17. PR jobs have no signing or publishing credentials.
18. Tests prove both successful assembly and required failure behavior.
19. `docs/release.md` documents the complete current procedure.
20. **A bundled dependency cannot reach signing with stale license files, unreviewed NOTICE changes, or missing required disclosures.**
