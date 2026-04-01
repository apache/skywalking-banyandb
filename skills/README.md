# Claude Code Skills

This directory contains [Claude Code](https://github.com/anthropics/claude-code) skills for working with BanyanDB.

## Installing Skills

Skills live in this `skills/` directory and are not auto-discovered by Claude Code.
To use them, create a symlink to your Claude Code skills directory:

```bash
mkdir -p ~/.claude/skills
ln -s "$(pwd)/skills/vendor-update" ~/.claude/skills/vendor-update
```

The skill is auto-triggered when you use the relevant phrases. No manual invocation needed.

## Available Skills

### compiling

Compile and build the SkyWalking BanyanDB project.

**Triggers:** "compile", "build", "generate code"

**What it does:**
1. Runs `make generate` to regenerate protobuf and mock code
2. Runs `make build` to compile all binaries

### gh-pull-request

Create a GitHub pull request following project conventions.

**Triggers:** "create a PR", "submit changes", "open a pull request"

**What it does:**
1. Guides through pre-PR checks (generate, build, lint, check, license, tests)
2. Creates branch and pushes to remote
3. Opens PR with proper format

### vendor-update

Upgrades Go/Node.js vendor dependencies and syncs tool versions.

**Triggers:** "upgrade dependencies", "vendor update", "vendor-upgrade", "bump dependencies", "update packages"

**What it does:**
1. Runs `make vendor-update` across all projects
2. Checks `scripts/build/version.mk` against upgraded `go.mod` to detect tool version drift
3. Updates `version.mk` if a library bump warrants a tool version bump
4. Removes stale binaries from `bin/` when versions change
