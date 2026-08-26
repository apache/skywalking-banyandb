---
name: gh-issue
description: >-
  Size-audit, write, and split BanyanDB issues that somebody else or an
  automated TDD workflow can implement. Use whenever the user asks to file or
  revise an issue, decide whether an issue is too large, make an issue
  TDD-ready, turn a design into tickets, or split an umbrella into executable
  leaves. Do not draft or file an implementation issue before classifying it as
  a mergeable leaf or a tracking parent.
allowed-tools: Bash, Read, Grep, Glob
---

# Writing a BanyanDB Issue

BanyanDB's issues live in the **apache/skywalking** tracker, not in this
repository — `apache/skywalking-banyandb` has issues disabled. Label them
`database`, which is the component label for BanyanDB.

A good issue is one somebody can implement from **without asking you a
question**. That is a higher bar than "clear", and it is the bar that decides
whether an issue sits untouched for a year.

## Size the work before polishing the issue

Issue length is not issue size. Inspect the design and the relevant production
code before drafting. Count independently testable behaviour, integration seams,
callers, format families, lifecycle domains, fixture work, and test suites. A
short issue that says "replace the index" can be much larger than a long issue
that fixes one query operation.

Always produce a short size audit during review, even if it will not appear in
the filed body:

```markdown
## Size audit
Classification: executable leaf | tracking parent
Boundary: <one seam, or the several seams that make this a parent>
Production activation: <the caller switched by this merge>
RED test: <the command and the observable failure on current main>
End to end: <the real operation proved by the merge>
Format/lifecycle scope: <sections or algorithms; read/write/publish/merge/GC/etc.>
Fixtures and oracle: <who supplies expected values independently of the code under test>
Focused suites: <commands>
Dependencies present on main: yes | no — <evidence>
Decision: <fits one run, or the exact split required>
```

Do not infer the answers only from the proposal. Use repository evidence such as
interfaces, call sites, packages, fixtures, and existing tests. Treat line count
as supporting evidence, never as the sole sizing rule.

### Executable leaf versus tracking parent

An **executable leaf** must satisfy all of these:

- It exposes one agreed boundary and delivers one behaviour end to end.
- The same merge switches at least one named production or CLI caller to it.
- Every requirement has a RED test that fails on current `main` for a stated
  reason and passes after the change.
- One focused end-to-end scenario proves the real caller, not just an isolated
  codec or helper.
- Expected values and persisted bytes come from an independent oracle.
- Its prerequisites and the seam it builds on are already merged to `main`.
- A contract author can create the boundary and RED tests in one workflow turn,
  and an implementer can reasonably complete the production change in the next.

A **tracking parent** describes a milestone, replacement, or multi-PR outcome.
It owns completion criteria, the dependency graph, and links to children, but it
is not itself an implementation contract. Mark it clearly as a tracking parent
and do not put it in an automated implementation queue.

### Mandatory split triggers

Classify the issue as a parent or split it before filing when any of these is
true:

- It mixes lifecycle domains such as read, write, publish/recovery, merge/GC, or
  replication.
- It introduces several independent public seams or several unrelated caller
  cutovers.
- It replaces a whole third-party subsystem rather than one observable operation.
- No test can be named that fails on current `main` without asserting private
  implementation details.
- It needs the production implementation to generate its own expected fixture
  bytes; that is a circular oracle, not a test contract.
- A required boundary exists only in an open or planned PR. Keep the issue as a
  parent and decompose it after the blocker merges.
- It is foundation-only: the merge adds format readers, helpers, or abstractions
  that no production/CLI path uses.

Use this score as a second check when none of the hard triggers is decisive:

| add | signal |
|---:|---|
| +2 | each additional public boundary after the first |
| +2 | each additional lifecycle domain after the first |
| +1 | each additional production caller family |
| +1 | each additional persisted format or algorithm family |
| +1 | each additional independent fixture family |
| +1 | each additional focused package test suite |
| +2 | a prerequisite or seam is not yet on `main` |

Score `0–2`: normally a leaf. Score `3–4`: split unless repository evidence
shows it is still one small behaviour. Score `5+`: a tracking parent. A hard
trigger overrides the score.

For a TDD workflow, also cap a leaf at roughly four observable requirements,
one fixture family, one activation point, and one focused end-to-end scenario.
These are review budgets, not excuses to hide work by combining requirements.

### RED-test feasibility catches false slices

A compatibility-preserving cutover often returns the same result before and
after the change, so a new output assertion may pass against the legacy path and
is not RED. State why the test fails before implementation. Absence of a new
boundary can be a compile-time RED test, but acceptance must also prove that a
real caller uses the boundary; do not use brittle source-text assertions as the
only proof.

If a fixture is required, include its cost in the size audit. Persisted expected
bytes should be generated by an independent legacy/reference writer and checked
in with provenance before the native code consumes them. Do not create a
fixture-only or codec-only ticket when the project requires every merge to
activate production behaviour; put the smallest fixture and decoder beside the
first real caller that uses them.

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

Do not slice a storage replacement horizontally by format section (`container`,
`postings`, `stored fields`) if those tickets would be unused foundations. Slice
by **fixture complexity against one real caller** instead. A first read slice may
activate document count on a deliberately small fixture; a later slice can widen
that same caller to multiple segments and deletion masks; another caller can
then activate stored-field decoding. Each merge stays TDD-sized and live.

Decompose just in time. Keep downstream design workstreams as tracking parents
until their blocker lands. Then inspect the boundary that actually merged and
create their executable leaves. Pre-filing speculative leaves commits to stale
seams and recreates horizontal slicing at the issue layer.

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

If automation selects the oldest issue with a queue label such as `Backlog`,
apply that label only to the oldest unblocked executable leaf. Never queue a
tracking parent or all sibling leaves at once. Create children in dependency
order so issue numbers reinforce rather than fight the execution order.

## Before you file

- Did you show the size audit and explicitly classify the issue as a leaf or a
  tracking parent?
- Could someone write a failing test from this body alone, without asking you
  anything?
- Does the test actually fail on current `main`, or would the legacy path already
  satisfy it?
- Does every expected value come from the issue rather than from what the code
  would compute?
- Is every acceptance criterion provable by running something?
- Is it one boundary and one production activation, or a plan with phases?
- Does this merge activate a real caller rather than land dead foundation code?
- Are all prerequisite seams already merged to `main`?
- If it touches stored or wire data, does it say what happens to existing data
  and to a mixed-version cluster?
