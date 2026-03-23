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

### vendor-update

Upgrades Go/Node.js vendor dependencies and syncs tool versions.

**Triggers:** "upgrade dependencies", "vendor update", "vendor-upgrade", "bump dependencies", "update packages"

**What it does:**
1. Runs `make vendor-update` across all projects
2. Checks `scripts/build/version.mk` against upgraded `go.mod` to detect tool version drift
3. Updates `version.mk` if a library bump warrants a tool version bump
4. Removes stale binaries from `bin/` when versions change

**Usage:**
```
$ claude
<user> upgrade dependencies
```

Or trigger explicitly:
```
<user> run vendor-update
```
