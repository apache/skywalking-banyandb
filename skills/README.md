# Claude Code Skills

This directory contains [Claude Code](https://github.com/anthropics/claude-code) skills for working with BanyanDB.

## Installing Skills

Download the `.skill` bundle and install it:

```bash
claude --install-extension vendor-update.skill
```

This installs the skill to `~/.claude/skills/` where Claude Code auto-discovers it.

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
