# Verification report — docker-canonical license generation

Repo: `apache/skywalking-banyandb`, branch `hapi-docker-build`, base commit `8c5542e4`
Base: rebased onto `origin/main` @ `cb15c911`, which already contains `0e3ee0ec` (UI removal).
The work is on `pr/license-determinism`; `hapi-docker-build` and
`backup/hapi-docker-build-pre-rebase` are untouched and hold the 10 unrelated
release/canopy commits this branch carried.
Image: `skywalking-banyandb-build:go1.25.13-node24.6.0-55373684d1b70e5f8fd9fc8ec114a89ad11a56a3`

Every check runs inside a container. The host tree is read-only except where a step explicitly
regenerates artifacts. Raw logs: `/tmp/verify/logs/`; harness: `/tmp/verify/checks/` (both outside the
repository — this report is the retained evidence, not a reproduction script).

## Verdict

The acceptance criterion of [apache/skywalking#13996](https://github.com/apache/skywalking/issues/13996)
holds: the license artifacts are byte-identical across repeated container runs, between the container
and the native path, between two resource envelopes, and against the committed blobs.

**Two rounds of adversarial review were run against this change set, and the second found eight real
defects that the first round's evidence had missed.** All are fixed and each fix has a test or a
reproduction. That is the main thing this report is for: the first version of it claimed a clean
sweep, and it was wrong.

## Artifact counts, and why there are two

**651 artifacts**, on the rebased base. The pre-rebase branch still had `ui/` and produced 742, which
is why earlier revisions of this report said 742; that number is now only historical. The `ui/*`
entries in `LICENSE_PATHS` and `GENERATED_OUTPUTS` were removed with the rebase.

Re-verified on the rebased base:

| Check | Result |
|---|---|
| Cold rebuild from an empty BuildKit cache | `rc=0` |
| Container manifest | 651 artifacts |
| Container vs native `make license-dep` | byte-identical |
| Container vs `manifest-committed` | byte-identical |
| Reinstalls of license-eye after the run starts | 0 (the baked binary is used) |
| `check-license-outputs`, `check-node-version` | clean |
| `go test ./scripts/ci/check/` | 30 pass |
| `license-eye header check` | 4363 files, 0 invalid |
| Container gates | 18/18 |

---

## 1. What was reviewed and fixed

| # | Defect | How it was found | Fix | Regression test |
|---|---|---|---|---|
| 1 | The baked `license-eye` was never used. `scripts/build/license.mk:19` assigns `LICENSE_EYE :=` and a makefile assignment beats the environment, so every container run silently re-ran `go install` | review | passed as a *command-line* make variable, which does win | log evidence below |
| 2 | Copy-back overlaid the generated sets, so a file the container did not produce survived on the host | review + canary | `rm -rf` each generated path, then extract | canary removed |
| 3 | `check-committed` read `:$path` (the **index**), so a staged file counted as committed | review + staged canary | compare against a commit (`HEAD` by default, any ref as a second argument) | 6 new tests |
| 4 | `.node-version` was unnecessary machinery | review, confirmed against `setup-node` source | deleted; workflows use `node-version-file: canopy/package.json` | — |
| 5 | `usage()` was called but never defined in `node_version.sh` | review | defined | bad-argument path |
| 6 | The "exact pin" test was a loose glob: `24.6.0 \|\| 25.0.0` and `24x.6x.0x` both passed | review | anchored numeric match | both inputs rejected |
| 7 | `PLATFORM := linux/amd64` loses to `make PLATFORM=…` on the command line, so "not overridable" was false | review | `override` directive | `make -n PLATFORM=linux/arm64` now emits amd64 |
| 8 | `bump-build-image` could blank `DEBIAN_IMAGE` on a transient `imagetools` failure, and reported success | review, then reproduced | validate a `sha256:<64 hex>` digest and verify the rewrite before replacing the file | reproduced the blanking, then confirmed the file is untouched |
| 9 | `mcp/package-lock.json` still recorded `">=24.6.0"` after `package.json` was pinned | review | `npm install --package-lock-only` | engines consistent across 5 manifests |
| 10 | Dockerfile still held literals: floating `1.7` frontend, an epoch default, a hardcoded `bookworm` | review | frontend pinned by digest, no epoch default, `DEBIAN_DISTRO` passed as a build arg | `grep` gate |
| 11 | `normalize` ran as a sibling prerequisite of `default`, so its position was incidental and racy under `make -j` | review | moved into the root recipe's tail, where order cannot be reordered | `make -n` shows it last |
| 12 | `sed -i` is GNU-only, breaking the promised macOS native path | review | temp file + rename | CRLF file normalized correctly |
| 13 | `docker-run TARGET=check` was advertised but cannot work: `.git` is not streamed | review | contract stated; the example removed | — |
| 14 | A truncated tar stream could reach generation (I had added `\|\| true` earlier) | review | tar status kept, generation refused on a partial tree | — |
| 15 | Entry point cross-checked Node against the deleted `.node-version` | consequence of #4 | reads `mcp` and `canopy` `package.json` directly | — |

Two claims in the earlier report were false and are withdrawn:

- **"`PLATFORM` is not overridable."** It was overridable from the command line; only the Dockerfile's
  architecture assertion stopped the mislabelled image. Now `override` makes the claim true.
- **"18/18 gates" and the image-id equality** were assertions whose harness lives outside the tree.
  The harness and logs are still in `/tmp/verify/`, which is stated above rather than implied.

## 2. Identity results

| Check | Result |
|---|---|
| Container run 1 vs run 2 | byte-identical |
| Container vs native `make license-dep` | byte-identical |
| 2 CPU / 4 GB vs 1 CPU / 1 GB envelope | byte-identical |
| Generated vs `manifest-committed` | byte-identical |
| `check-license-outputs`, `check-node-version` | clean |
| `license-eye header check` | 4498 files, 0 invalid |
| `go test ./scripts/ci/check/` | pass |
| Cold-cache rebuild | `rc=0` |

`manifest-committed` is the strongest form: it is computed from a commit's blobs, not from the index
or the worktree, so it ties the generated bytes to what is actually committed.

## 3. Defect 1 and 2, with evidence

**The baked binary.** After the fix, the image build still shows one `go install` (line 273, the bake
step itself), while the container run invokes the baked path directly:

```
$ grep -n "go install …skywalking-eyes" build.log     →  273:#9 [tools 5/8] …   (the bake)
$ grep -n "entrypoint: running make" build.log        →  331:
$ grep -n "license-eye dep resolve" build.log         →  336:/usr/local/bin/license-eye …
                                                         940:/usr/local/bin/license-eye …
```

**The copy-back.** A canary in `dist/licenses` is not produced by the container:

```
before the fix:  CANARY SURVIVED
after the fix:   REMOVED — the copy-back replaces the generated sets
```

This matters because `license-eye` logs `npm ci` failures instead of returning them, so an incomplete
resolution exits 0. Before the fix, a stale license survived, matched its own committed blob, and
`check-committed` passed. The staged-canary variant of the same hole is now caught too:

```
$ printf 'staged\n' > dist/licenses/license-zzz-staged.txt
$ git add dist/licenses/license-zzz-staged.txt
$ bash scripts/ci/check/license_manifest.sh check-committed .
not present in HEAD: dist/licenses/license-zzz-staged.txt
license artifacts drifted from HEAD; run 'make license-dep' and commit the result   rc=1
```

## 4. What is not verified here, stated plainly

| Item | Why | Where covered |
|---|---|---|
| macOS / Windows generation | no such host in this environment | by decision, not coverage — see design §7.2 |
| `linux/arm64` as a supported target | the image is amd64-only by design; the arch assertion makes a mismatch a build failure | deliberately not supported |
| `make check-format`, `make lint`, `make build` | full-repo operations, slow and unrelated to this change | existing CI |
| A native Windows run of the wrapper | no Windows host. The host-side requirements were reduced (`id` optional, no `sed -i`, no GNU `date`) but not exercised | — |
| `make bump-build-image` happy path | it runs here only when `imagetools` resolves; a transient failure is what exposed defect 8 | `scripts/build/README.md` |
| Release-archive reproducibility | out of scope by design | separate issue |
| A cross-host CI matrix | dropped by decision; `check-license-outputs` fails on any CR byte on whatever host a contributor is on | design §7.2 |

## 5. State of the change

Applied on `pr/license-determinism` over `origin/main` @ `cb15c911`: 13 files modified, 10 added, and
the `ui/*` references dropped with the rebase. Two of the branch's files were conflicts and were
resolved in favour of main where main had moved on: the `license-dep` project list (no `ui`) and the
design index (both entries kept). Version sources, in one place each:

| Value | Declared in |
|---|---|
| Go | `go.mod` — the declaration the Go tooling enforces |
| Node | `mcp/package.json` + `canopy/package.json` `engines.node`, required to be the same exact pin |
| license-eye | `scripts/build/version.mk` |
| Base image digest, apt snapshot, platform, epoch | `scripts/build/version.mk` |
| Go/Node tarball checksums | the vendor's published index, fetched over TLS at build time |

`dockerize.mk` and the Dockerfile contain no version literals. `.node-version`, `images.lock` and the
generated-version-file mechanism are gone.

**Found but not changed — the same class of problem, pre-existing, in the release images:**

```
canopy/Dockerfile:25   FROM --platform=$BUILDPLATFORM node:24.6.0-bookworm AS deps
mcp/Dockerfile:18      FROM node:24.6.0-alpine AS builder
mcp/Dockerfile:36      FROM node:24.6.0-alpine AS final
```

The same fix applies — an `ARG NODE_VERSION` fed from `version.mk` through `DOCKER_BUILD_ARGS` — but
it touches the release-image path for every project and is a separate change.
