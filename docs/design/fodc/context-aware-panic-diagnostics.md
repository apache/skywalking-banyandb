# Context-Aware Panic Diagnostics Development Design

## Table of Contents
1. [Overview](#overview)
2. [Goals and Non-Goals](#goals-and-non-goals)
3. [Architecture](#architecture)
4. [Component Design](#component-design)
5. [Crash Artifacts](#crash-artifacts)
6. [Deployment and Operations](#deployment-and-operations)
7. [Implementation Plan](#implementation-plan)
8. [Testing Strategy](#testing-strategy)
9. [Appendix](#appendix)

## Overview

Unhandled panics in BanyanDB create blind spots during production incidents. A process can terminate with only a raw runtime stack trace, while the business context that led to the crash is lost. This is especially painful for background goroutines, asynchronous pipelines, and high-throughput query paths where the failure cannot be reproduced cheaply.

Context-Aware Panic Diagnostics is a multi-layered defense-in-depth architecture for BanyanDB and FODC. It ensures every panic is either recovered with rich diagnostics or persisted as a crash artifact that can be collected and analyzed later.

The design has five layers:

1. **Layer 0 - WithRecovery Wrapper**: Recovers panics in managed goroutines, records localized stacks, and updates metrics.
2. **Layer 1 - Global Crash Output**: Uses `debug.SetCrashOutput` during bootstrap so fatal panics and early initialization failures are still persisted.
3. **Layer 2 - Diagnostic Breadcrumbs**: Attaches lightweight execution markers to `context.Context` to reconstruct the causal path to the crash.
4. **Layer 3 - Deep State Serialization**: Serializes selected in-memory state after a panic to capture the data shape that triggered the failure.
5. **Layer 4 - FODC Sidecar Collection**: Watches crash artifacts from a shared volume and forwards them to centralized analysis pipelines.

### Problem Statement

The design addresses the following gaps in current panic handling:

- Standard Go panic output shows where a crash happened, but not the business operation or inputs that caused it.
- Panics in background goroutines can terminate critical loops without enough context for root-cause analysis.
- Initialization panics may happen before logging, metrics, or tracing are ready.
- High-throughput code paths cannot afford expensive diagnostics during normal execution.
- Operators need a uniform artifact format that FODC can collect automatically.

### Design Principles

- **Defense in depth**: One failing layer must not prevent another layer from capturing the crash.
- **Zero overhead on the hot path**: Expensive work happens only after a panic or fatal runtime failure.
- **Explicit scope control**: Only safe, bounded diagnostic state is serialized.
- **Operational consistency**: All artifacts use stable paths and metadata to simplify FODC collection.
- **Incremental adoption**: The wrapper and breadcrumbs can be introduced gradually across subsystems.

## Goals and Non-Goals

### Goals

- Capture every managed goroutine panic with component metadata and stack traces.
- Persist unrecoverable crashes and early startup panics to disk.
- Preserve the execution narrative that led to a panic.
- Dump bounded, structured state for selected high-value code paths.
- Integrate crash artifacts with the FODC sidecar for centralized analysis and notification.
- Provide clear rollout guidance for BanyanDB maintainers.

### Non-Goals

- Prevent all panics from terminating the process.
- Serialize the entire heap or provide a full core dump replacement.
- Add request tracing or distributed tracing semantics to every `context.Context`.
- Introduce noticeable CPU or allocation overhead in steady-state execution.

## Architecture

### Layered Flow

```text
Managed Goroutine / Request Path
        │
        ├── Layer 2: Breadcrumbs attached to context.Context
        │
        └── Layer 0: WithRecovery wrapper
                │
                ├── Recoverable panic
                │      ├── Record panic metadata
                │      ├── Capture stack trace
                │      ├── Serialize bounded state
                │      └── Write crash artifacts
                │
                └── Unrecoverable / escaped panic
                       │
                       └── Layer 1: debug.SetCrashOutput persists runtime crash output

Artifacts written to shared volume (/crash)
        │
        └── Layer 4: FODC sidecar watches files, bundles artifacts, uploads to OAP, and emits alerts
```

### Why the Layers Are Separate

- Layer 0 focuses on panics that occur in explicitly managed goroutines and request handlers.
- Layer 1 covers crashes outside managed recovery boundaries, including bootstrap failures and runtime fatal errors.
- Layer 2 adds low-cost semantic context before anything fails.
- Layer 3 adds deep object state only after a panic is already in progress.
- Layer 4 decouples transport and retention from BanyanDB itself.

## Component Design

### 1. Recovery Runtime

**Purpose**: Provide a uniform wrapper for goroutines that need panic recovery, metrics, and artifact emission.

#### Core Responsibilities

- Wrap background goroutines and request-scoped async execution.
- Recover panic values with `defer` and `recover`.
- Capture localized stack traces for the crashing goroutine.
- Increment panic metrics with component labels.
- Invoke the diagnostic writer with breadcrumb and state data.

#### Core Types

**`RecoveryOptions`**

```go
type RecoveryOptions struct {
	Component    string
	Context      context.Context
	ArtifactRoot string
	StateDumper  StateDumper
	Logger       *logger.Logger
}
```

**`StateDumper`**

```go
type StateDumper interface {
	DumpState(ctx context.Context) (any, error)
}
```

**`PanicRecord`**

```go
type PanicRecord struct {
	OccurredAt      time.Time         `json:"occurredAt"`
	Component       string            `json:"component"`
	PanicValue      string            `json:"panicValue"`
	Recovered       bool              `json:"recovered"`
	GoroutineStack  string            `json:"goroutineStack"`
	Breadcrumbs     []Breadcrumb      `json:"breadcrumbs,omitempty"`
	ProcessMetadata map[string]string `json:"processMetadata,omitempty"`
}
```

#### Key Functions

**`WithRecovery(ctx context.Context, opts RecoveryOptions, fn func(context.Context))`**

- Executes `fn` in a protected goroutine boundary.
- On panic, records the panic value and localized stack.
- Extracts breadcrumbs from `ctx`.
- Invokes optional state serialization.
- Writes a structured panic report and increments `banyandb_panic_total{component=...}`.

**`GoWithRecovery(ctx context.Context, opts RecoveryOptions, fn func(context.Context))`**

- Convenience helper that starts a goroutine and applies `WithRecovery`.
- Used for worker loops, asynchronous maintenance tasks, and proxy-side background jobs.

#### Adoption Guidance

The following categories should be wrapped first:

- Scheduler and compaction goroutines.
- Stream and measure background maintenance loops.
- gRPC handlers that launch child goroutines.
- FODC agent and proxy workers.

### 2. Global Crash Output

**Purpose**: Persist crashes that happen before normal recovery and logging are available.

#### Core Responsibilities

- Register `debug.SetCrashOutput` at the earliest point in `banyand` startup.
- Create a stable crash output file under the configured crash directory.
- Keep the file descriptor valid independently of later logging or runtime corruption.
- Act as the last-resort persistence layer for fatal runtime failures.

#### Initialization Contract

Bootstrap order:

1. Resolve crash artifact directory.
2. Open crash output file.
3. Duplicate or otherwise preserve the file descriptor for runtime crash output.
4. Call `debug.SetCrashOutput`.
5. Continue with logger and subsystem initialization.

#### Failure Modes Covered

- Panics during bootstrap before the logger is initialized.
- Fatal runtime errors such as concurrent map writes.
- Stack overflow and similar runtime-generated crash reports.
- Panics that escape all managed recovery boundaries.

### 3. Diagnostic Breadcrumbs

**Purpose**: Reconstruct the execution narrative that led to a crash.

#### Design

Breadcrumbs are small immutable markers attached to `context.Context`. Each marker describes a meaningful stage transition or resource interaction, for example:

- `opening shard 5`
- `loading measure service_latency`
- `merging memtable into tsdb segment`

Each breadcrumb should be cheap to append and safe to read during panic recovery.

#### Core Types

**`Breadcrumb`**

```go
type Breadcrumb struct {
	Time      time.Time         `json:"time"`
	Stage     string            `json:"stage"`
	Component string            `json:"component,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}
```

#### Key Functions

**`WithBreadcrumb(ctx context.Context, stage string, fields map[string]string) context.Context`**

- Returns a new context carrying the appended breadcrumb chain.
- Must avoid mutating shared state.
- Should use compact storage to limit allocations.

**`BreadcrumbsFromContext(ctx context.Context) []Breadcrumb`**

- Returns the breadcrumb chain in insertion order.
- Used by panic recovery and optional debug endpoints.

#### Authoring Rules

- Add breadcrumbs at semantic boundaries, not every function call.
- Prefer stable identifiers such as shard ID, group, or schema name.
- Never place secrets, payload bodies, or large serialized data in breadcrumb fields.
- Keep stage strings human-readable because operators will read them directly.

### 4. Deep State Serialization

**Purpose**: Capture the in-memory state that explains what the crashing code was processing.

#### Design

After a panic is recovered, the diagnostic path may invoke a targeted dumper that exposes selected named return values, request descriptors, iterators, shard metadata, or aggregation inputs. The result is serialized into a bounded artifact file.

This layer complements breadcrumbs:

- Breadcrumbs explain the execution path.
- State dumps explain the data shape and internal state at the moment of failure.

#### State Selection Rules

- Only serialize high-value diagnostic structures.
- Avoid opaque global state unless it directly explains the failure.
- Redact credentials, tokens, and user-sensitive fields.
- Prefer stable, typed snapshots over raw reflection of arbitrary graphs.

#### Bounded Serialization Contract

- Maximum artifact size: `5 MiB` per dump.
- Serialization must fail closed when the cap is exceeded.
- The panic path must continue even if state dumping fails.
- The dump writer should record truncation or serialization errors in the panic report.

#### Suggested Interface

**`BoundedStateWriter`**

```go
type BoundedStateWriter interface {
	WriteJSON(path string, value any, limitBytes int64) (truncated bool, err error)
}
```

#### Implementation Notes

- Start with JSON output for interoperability.
- Reflection helpers may be used internally, but the external artifact format should stay stable.
- Prefer typed snapshot structs in core database paths instead of dumping arbitrary live objects.

### 5. Artifact Writer

**Purpose**: Materialize crash diagnostics into a stable on-disk layout.

#### Core Responsibilities

- Create a unique crash directory per panic event.
- Write structured metadata, human-readable summaries, and optional deep dumps.
- Guarantee best-effort persistence without turning a panic into a secondary failure.
- Apply rotation or retention hooks compatible with disk quotas.

#### Artifact Layout

```text
/crash/
  <timestamp>-<component>-<pid>/
    panic.json
    crash.txt
    deep-dump.json
    metadata.json
```

#### File Semantics

- `panic.json`: Structured panic record from Layer 0.
- `crash.txt`: Raw runtime output from Layer 1 or a human-readable recovery summary.
- `deep-dump.json`: Optional bounded state snapshot from Layer 3.
- `metadata.json`: Node identity, version, build info, and collection status markers.

### 6. FODC Sidecar Integration

**Purpose**: Detect crash artifacts from BanyanDB and forward them for centralized analysis.

#### Core Responsibilities

- Watch the shared crash volume for newly completed crash directories.
- Validate presence of required files before upload.
- Bundle panic metadata, raw crash output, and deep state dumps into a single incident package.
- Ship artifacts to SkyWalking OAP.
- Emit alerts to configured channels such as Slack.

#### Sidecar Contract

BanyanDB container responsibilities:

- Write crash artifacts into the shared volume.
- Keep file names and directory structure stable.
- Flush writes before process exit when possible.

FODC sidecar responsibilities:

- Detect new artifacts without polling too aggressively.
- Avoid duplicate uploads by maintaining a collection marker.
- Continue collection even if BanyanDB has already exited.

## Crash Artifacts

### Structured Panic Record Example

```json
{
  "occurredAt": "2026-04-01T10:11:12Z",
  "component": "measure-query-worker",
  "panicValue": "runtime error: index out of range [7] with length 4",
  "recovered": true,
  "goroutineStack": "goroutine 9123 [running]:\n...",
  "breadcrumbs": [
    {
      "time": "2026-04-01T10:11:11Z",
      "stage": "opening shard",
      "component": "query",
      "fields": {
        "shardID": "5"
      }
    },
    {
      "time": "2026-04-01T10:11:12Z",
      "stage": "querying measure",
      "component": "query",
      "fields": {
        "group": "sw_metricsMinute",
        "measure": "service_latency"
      }
    }
  ],
  "processMetadata": {
    "node": "banyand-datanode-0",
    "role": "datanode",
    "version": "dev"
  }
}
```

### Artifact Completion Rules

- `panic.json` is the canonical machine-readable entry point.
- `deep-dump.json` is optional and may be omitted on serialization failure or size-limit rejection.
- `metadata.json` should contain version, git revision, pod name, node role, and artifact status flags.
- Sidecar upload should occur only after the artifact directory is marked complete.

## Deployment and Operations

### Shared Volume Model

```text
+---------------------------+        +---------------------------+
| BanyanDB main container   |        | FODC sidecar container    |
|                           |        |                           |
| Layer 0-3 diagnostics     |        | File watcher              |
| write /crash artifacts    | -----> | Bundle and upload         |
|                           | shared | Notify operators          |
+---------------------------+ volume +---------------------------+
```

### Configuration Flags

Suggested flags for BanyanDB:

- `--panic-diagnostics-enabled`
- `--panic-diagnostics-dir=/crash`
- `--panic-diagnostics-state-limit=5MiB`
- `--panic-diagnostics-retention-max-files`

Suggested flags for FODC:

- `--crash-watch-dir=/crash`
- `--crash-upload-enabled`
- `--crash-alert-webhook`

### Performance and Stability

- Breadcrumb insertion must remain allocation-light and constant-time in common paths.
- State serialization is off the hot path and only runs after panic recovery.
- The `5 MiB` state cap prevents dump generation from exhausting memory.
- Operators should reserve headroom with `GOMEMLIMIT` so diagnostic work can complete during memory pressure.
- Crash directory rotation should prevent the shared volume from growing without bounds.

## Implementation Plan

### Phase 1 - Core Runtime Support

- Add the recovery wrapper package and panic counter metric.
- Register global crash output in the `banyand` entry point.
- Introduce artifact writer utilities and stable directory layout.

### Phase 2 - Breadcrumb Foundation

- Add breadcrumb context helpers.
- Instrument high-value query, compaction, and lifecycle paths.
- Standardize stage naming conventions across subsystems.

### Phase 3 - State Dumping

- Add bounded JSON state writer.
- Introduce typed snapshot structures for selected hot diagnostic targets.
- Record truncation and dump failures in `panic.json`.

### Phase 4 - FODC Integration

- Extend the sidecar watcher to collect crash directories.
- Bundle artifacts and upload them to OAP.
- Add notification hooks and collection status markers.

### Phase 5 - Hardening

- Add disk retention policy.
- Add redaction rules and secret scanning for serialized snapshots.
- Validate coverage across all background goroutines.

## Testing Strategy

### Unit Tests

- Recovery wrapper captures panic values, stack traces, and metrics labels.
- Breadcrumb helpers preserve ordering and immutability.
- State writer enforces the configured size limit.
- Artifact writer produces the expected on-disk layout.

### Integration Tests

- Panic inside a wrapped goroutine creates `panic.json` and `deep-dump.json`.
- Startup panic before subsystem initialization is persisted through `debug.SetCrashOutput`.
- Sidecar detects a completed crash directory and marks it as collected.
- Rotation removes old crash directories without affecting the newest artifact.

### Failure Injection Scenarios

- Panic during stream query execution.
- Panic in compaction or shard merge background loops.
- Serialization failure due to unsupported field graph.
- Disk full or permission errors in the crash directory.
- Fatal runtime error that bypasses normal recovery.

### Acceptance Criteria

- Every adopted goroutine entry point uses `WithRecovery` or an equivalent wrapper.
- Every panic artifact contains component name, timestamp, and stack trace.
- Breadcrumbs are present for instrumented query and lifecycle paths.
- State dumping never exceeds the configured memory bound.
- FODC can collect and upload artifacts without BanyanDB participation after the crash.

## Appendix

### Layer Summary

| Layer | Name | Primary Outcome |
|-------|------|-----------------|
| 0 | WithRecovery Wrapper | Recover managed goroutine panics and create structured reports |
| 1 | Global Crash Output | Persist fatal and early crashes that escape recovery |
| 2 | Diagnostic Breadcrumbs | Preserve the execution narrative |
| 3 | Deep State Serialization | Preserve selected in-memory state |
| 4 | FODC Sidecar | Centralize collection, upload, and alerting |

### Expected Operator Value

- Faster MTTR because the crash report includes both the failing stack and the causal path.
- Fewer manual reproductions because state snapshots preserve the most relevant runtime inputs.
- Better fleet-level observability because every panic increments metrics and is exported through FODC.


## Core Components and Workflow
BanyanDB Main Container: This is the primary application container.
BanyanDB Process: The core database service.
debug.SetCrashOutput: A function that directs crash-related data (like stack traces) to a specific output.
Context Breadcrumbs: Metadata or state information collected during execution to provide context for a failure.
Shared Volume (/crash): An ephemeral or persistent storage space accessible by both containers in the pod.
The main container writes two key files here upon failure: crash.txt (likely the stack trace) and deep-dump.json (detailed state/breadcrumbs).
FODC Sidecar Container: A secondary container ("Sidecar") that runs alongside the main application to handle auxiliary tasks.
FS Watcher / inotify: Monitors the /crash directory for filesystem changes.
Crash Analyzer: Once a new file is detected, this component processes the crash data.
Layer 4: FODC Component: Bundles the artifacts and prepares them for external transmission.
External Notification:
Inspection Agent: Receives the uploaded artifacts for further automated or manual review.
SlackChannel: The final step where an Alert / Notify message is sent to a DevOps or SRE team for immediate action.