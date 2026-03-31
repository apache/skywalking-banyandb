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

### Required: License Headers

```bash
make license-check
```

All source files must have Apache 2.0 license headers.

### Required: Update CHANGES.md

Add a one-line entry under the current development version section in `CHANGES.md` (at the repo root). Place it under the appropriate subsection (`### Features`, `### Bug Fixes`, etc.).

### Required: Run Unit Tests for Changed Packages

Run tests for the packages you changed:

```bash
make test PKG=./banyand/...
make test PKG=./bydbctl/...
make test PKG=./pkg/...
make test PKG=./fodc/...
```

### Recommended: Integration Tests

```bash
make test PKG=./test/integration/standalone/...
make test PKG=./test/integration/distributed/...
```

### Optional: UI Changes

If UI files were changed:

```bash
cd ui && npm ci && npm run format && cd ..
```

### Optional: Doc Changes

If markdown files were changed, check for dead links:

```bash
make check-req
```

## Checks NOT Practical Locally

These run in CI only — no need to run locally:
- **e2e tests** — require Docker + OAP stack (90 min timeout)
- **fodc-e2e tests** — require Kind Kubernetes cluster
- **dependency-review** — GitHub-specific action
- **slow/flaky/property-repair tests** — scheduled, not PR-blocking

## Creating the PR

```bash
git push -u origin <branch-name>
gh pr create --title "<title>" --body "<body>"
```

Follow the standard PR format with a summary and test plan.
