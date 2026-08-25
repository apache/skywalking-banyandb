---
name: gh-issue
description: Write a BanyanDB issue that someone else can implement from, and break a design-ready issue into tickets. Use when the user asks to file an issue, write up a feature or bug, turn a design or proposal into tickets, or split an issue that is too large.
allowed-tools: Bash, Read, Grep, Glob
---

# Writing a BanyanDB Issue

BanyanDB's issues live in the **apache/skywalking** tracker, not in this
repository — `apache/skywalking-banyandb` has issues disabled. Label them
`database`, which is the component label for BanyanDB.

A good issue is one somebody can implement from **without asking you a
question**. That is a higher bar than "clear", and it is the bar that decides
whether an issue sits untouched for a year.

## The body is the specification

Assume the implementer has your issue and the codebase, and nothing else — no
context from a design meeting, no follow-up in the comments. Comments are where
a specification goes to be missed; if a discussion changed the plan, edit the
body.

Four things have to be derivable from it:

**A boundary.** The seam the change exposes: a Go interface, the exported
functions, the protobuf message, the HTTP surface. *"Add an `Authorizer` that
decides whether a principal may perform an operation on a group"* is a boundary.
*"Improve permissions"* is not.

**Expected values from an independent source.** State the concrete answers, not
just the behaviour. A test whose expected value is derived the way the
implementation derives it passes by construction and can never disagree with the
code — so the issue is where the real answers come from. Write `a reader scoped
to group "ecommerce" sees only that group`, not `scoping should work`.

**Acceptance criteria provable by a command.** If a criterion is "a research
document" or "benchmark results", nobody can tell whether it is met. Those are
worth doing and belong in a proposal, not in an implementable issue.

**Where it lives and how that area is tested.** Name the packages or workspace.
It saves the implementer a discovery pass and it scopes the suite they run:

| area | tests |
|---|---|
| Go — `banyand/`, `pkg/` | `make test-ci PKG=./banyand/trace ./pkg/index` |
| Canopy — `canopy/` | `npm run -w web test`, `npm run -w server test` |
| Canopy end-to-end | `npx playwright test --config=e2e/playwright.config.ts` |
| everything, before pushing | `make generate && make build && make pre-push` |

`make build` before `make pre-push`: `pre-push` runs `lint`, which typechecks
`ui/`, and `ui/` embeds `dist` — which only exists after a build. A fresh clone
fails otherwise, with `pattern dist: no matching files found`.

## What a database issue owes that a service issue does not

Weight the criteria by what fails silently and late. If the change touches any of
these, say so in the issue — the implementer should not have to infer it:

- **Anything persisted or on the wire.** A segment, an index, a protobuf
  message: existing data must still read, and a rolling upgrade must survive a
  mixed-version cluster. State what an old reader does with new bytes and what a
  new reader does with old ones. The project labels this class
  `bydb file compatible change`.
- **Durability and ordering.** For a flush, a WAL append, a compaction or a
  snapshot: what is the state after a crash between any two steps?
- **Concurrency.** Goroutine launches go through the panic-recovery wrappers
  (`run.Go` / `run.GoOrDie` / `run.GoWithSignal`); a raw `go func(` needs an
  explicit `//panicdiag:allow-rawgo` directive and the lint baseline only ever
  shrinks.
- **Resource bounds.** An unbounded buffer, a query that materialises a whole
  series, a retention path that cannot reclaim.

## A shape that works

```markdown
## Summary
One paragraph: what this delivers and why.

## Boundary
The seam this exposes — the interface, exported functions, or protobuf/HTTP
surface. What callers may rely on, and what stays free to change.

## Requirements
R1. <observable behaviour, with its concrete expected value>
R2. ...
one per behaviour, each independently checkable.

## Acceptance criteria
- every requirement has a test that fails today and passes after
- end-to-end: <a real use of the feature, start to finish>
- <compatibility / durability / concurrency criteria, where they apply>

## Scope
Packages: banyand/trace, pkg/index — or canopy/web.
Out of scope: <the neighbouring thing this is NOT>
```

## Breaking a design-ready issue into tickets

When an issue carries a design and a proof of concept, it is usually several
pieces of work. Split it into **vertical slices** — each cutting a complete path
through the layers it touches, each verifiable on its own — and file them as
**sub-issues** of the base.

A slice is the right size when it delivers one behaviour end to end and its
acceptance criteria can be checked by running something. A slice that only makes
sense alongside the next one is not a slice; it is half of one.

**Order them by dependency and say so.** Put `Blocked by: #NNNN` in the body of
anything that needs an earlier ticket, and create them in that order so the
numbers ascend with the graph. **Do not start a blocked ticket before its blocker
has merged** — not merely opened a pull request. Until the merge, `main` does not
contain the seam the next ticket builds on.

**A pure refactor is a poor ticket** if the project's convention is to land
behaviour with a failing test first: a change that alters no behaviour has no
test that fails today. Either land the refactor on its own as an explicit
cleanup, or fold it into the first ticket that adds behaviour.

**Wide, mechanical changes use expand–contract**, one ticket per phase:

1. **Expand** — add the new form alongside the old.
2. **Migrate** — move call sites in batches sized by blast radius.
3. **Contract** — delete the old form once nothing uses it.

### Filing them

Create each child, then attach it to the base by the child's **`id`**, which is
not its number:

```bash
url=$(gh issue create --repo apache/skywalking \
        --title "..." --body-file ./ticket-1.md --label database)
n=${url##*/}
id=$(gh api repos/apache/skywalking/issues/$n -q .id)
gh api --method POST repos/apache/skywalking/issues/<base>/sub_issues -F sub_issue_id=$id
```

`-F`, not `-f`: the endpoint takes an integer, and `-f` sends the string form.
Check the result with:

```bash
gh api repos/apache/skywalking/issues/<base> -q .sub_issues_summary
```

The base issue stays open as the umbrella, and `sub_issues_summary` is the
progress bar. Close it when the last ticket merges.

## Before you file

- Could someone write a failing test from this body alone, without asking you
  anything?
- Does every expected value come from the issue rather than from what the code
  would compute?
- Is every acceptance criterion provable by running something?
- Is it one boundary, or is it a plan with phases?
- If it touches stored or wire data, does it say what happens to existing data
  and to a mixed-version cluster?
