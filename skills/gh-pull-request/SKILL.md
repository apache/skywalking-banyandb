---
name: gh-pull-request
description: Create a GitHub pull request for SkyWalking BanyanDB. Use when the user asks to create a PR, submit changes, or open a pull request.
allowed-tools: Bash, Read, Grep, Glob
---

# Creating a Pull Request for SkyWalking BanyanDB

## Branch Rules

**Always create a PR from a new branch.** Never push directly to `main`. This is the same convention as the main Apache SkyWalking repo (https://github.com/apache/skywalking).

```bash
git checkout -b <descriptive-branch-name>
```

## Local Checks Before Creating PR

Run these checks locally before pushing. They mirror the CI `check` job and PR-blocking tests in `.github/workflows/ci.yml`.

### Required: Code Generation and Build

```bash
make generate
make build
```

### Required: Linting and Formatting

```bash
make lint
make check
```

`make check` verifies formatting (gofumpt), go mod tidy, and ensures no uncommitted generated file diffs.

**Why this order:** `build` must succeed before `lint` so generated code exists. `check` validates consistency after linting.

**After `make lint` passes:** If lint introduced any fixes (e.g. auto-formatting, field alignment corrections), commit those changes before running `make check`. Do NOT update CHANGES.md for these fixup commits — just stage all modified tracked files and commit with a message like `chore: fix lint issues`. Then run `make check` on the clean tree.

### Required: License Headers

```bash
make license-check
```

All source files must have Apache 2.0 license headers.

### Required: Update CHANGES.md

Add a one-line entry under the current development version section in `CHANGES.md` (at the repo root). Place it under the appropriate subsection (`### Features`, `### Bug Fixes`, etc.).

### Unit Tests

Run these test packages. Each can run in parallel if the user's machine has enough cores, but it's fine to run them sequentially:

```bash
make test-ci PKG=./banyand/...
make test-ci PKG=./bydbctl/...
make test-ci PKG=./pkg/...
make test-ci PKG=./fodc/...
```

The CI uses these options: `--vv --fail-fast --label-filter \!slow` with coverage flags. For local runs, use a simplified form unless the user asks for full CI parity:

```bash
TEST_CI_OPTS="--vv --fail-fast --label-filter \!slow" make test-ci PKG=./banyand/...
```

### Integration Tests

Run these after unit tests pass:

```bash
make test-ci PKG=./test/integration/standalone/...
make test-ci PKG=./test/integration/distributed/...
```

## Checks NOT Practical Locally

These run in CI only — no need to run locally:
- **e2e tests** — require Docker + OAP stack (90 min timeout)
- **fodc-e2e tests** — require Kind Kubernetes cluster
- **dependency-review** — GitHub-specific action
- **slow/flaky/property-repair tests** — scheduled, not PR-blocking

## Common issues

- **`make lint` fails with field alignment errors**: The linter reports structs with suboptimal field ordering (e.g. `fieldalignment: struct with X pointer bytes could be Y`). Fix them automatically with:
  ```bash
  ~/go/bin/fieldalignment -fix ./path/to/package/...
  ```
  Parse the lint output to find which packages have alignment issues, run `fieldalignment -fix` on those packages, then re-run `make lint` to confirm they're resolved.

- **`make lint` fails with formatting errors**: Run `make format` to auto-fix, then re-run `make lint` to confirm.
- **Test timeout**: Integration tests can be slow; add `TEST_CI_OPTS="--timeout 60m"` if they time out
- **Missing tools**: `make check-req` will tell you what's missing
- **`make lint` or `make check` fails with `buf: not found`**: Install `buf` automatically by running `make -C api generate`, then retry.
- **`make build` fails**: Check if generated files are up to date with `make generate`

## Creating the PR

```bash
git push -u origin <branch-name>
gh pr create --title "<title>" --body "<body>"
```

Follow the standard PR format with a summary and test plan.
