# Context-Aware Panic Diagnostics Development Design

## Table of Contents
1. [Overview](#overview)
2. [Current Implementation Status](#current-implementation-status)
3. [Architecture](#architecture)
4. [Component Design](#component-design)
5. [Crash Artifacts](#crash-artifacts)
6. [Proxy API and Collection Semantics](#proxy-api-and-collection-semantics)
7. [Deployment and Operations](#deployment-and-operations)
8. [Testing Strategy](#testing-strategy)
9. [Appendix](#appendix)

## Overview

Context-aware panic diagnostics in BanyanDB and FODC provide two complementary capabilities:

1. Recover selected panics with structured diagnostics and persisted artifacts.
2. Surface those artifacts through the FODC agent and proxy for centralized inspection.

The implementation is no longer just a file-watching sidecar concept. The current codebase has an end-to-end flow:

1. `pkg/panicdiag` captures panic context, breadcrumbs, stack traces, and optional state dumps.
2. Recovered panics are written to an artifact directory on disk.
3. The FODC agent collects crash records from both in-process panic reports and an optional watched artifact directory.
4. The FODC proxy requests crash collections from connected agents over the crash diagnostics gRPC stream.
5. The proxy caches deduplicated crash records and exposes them via `GET /diagnostics`.

This document describes the current behavior implemented in code and highlights the parts that remain operational guidance rather than shipped features.

## Current Implementation Status

### Implemented

- `panicdiag.WithRecovery` and `panicdiag.GoWithRecovery` recover panics, capture stack traces, and persist structured artifacts.
- `panicdiag.WithBreadcrumb` and `panicdiag.BreadcrumbsFromContext` preserve semantic execution markers.
- `panicdiag.StateDumper` supports bounded JSON state dumps and a companion spew dump.
- `panicdiag.CrashOutputConfig.InstallGlobalCrashOutput` installs `debug.SetCrashOutput` and configures the default artifact root and retention for processes that enable crash output.
- The FODC agent aggregates crash collections from:
  - An in-process panic reporter.
  - An optional filesystem watcher over the configured crash directory.
- The FODC proxy requests diagnostics from agents over `StreamCrashDiagnostics`.
- The proxy caches crash records keyed by `agentID::artifactDir`.
- `GET /diagnostics` returns the proxy cache and supports filtering by `role` and `pod_name`.

### Not Implemented Here

- Automatic upload of crash artifacts from the proxy or agent to OAP.
- Alert fan-out such as Slack or webhook notifications.
- Proxy-side artifact completeness markers or remote retention coordination.

Those remain future integration possibilities, not current behavior.

## Architecture

### End-to-End Flow

```text
Managed goroutine / instrumented code path
        │
        ├── Breadcrumbs added to context.Context
        │
        └── panicdiag.WithRecovery / GoWithRecovery
                │
                ├── Recover panic value
                ├── Capture goroutine stack
                ├── Read breadcrumbs from context
                ├── Optionally dump bounded state
                └── Write artifact directory on disk
                        │
                        ├── panic.json
                        ├── crash.txt
                        ├── deep-dump.json         (optional)
                        └── deep-dump.spew         (optional)

FODC agent
        │
        ├── In-process panic store
        ├── Optional filesystem watcher over crash directory
        └── MultiCollectionProvider deduplicates by artifactDir
                │
                └── Crash diagnostics gRPC stream to proxy
                        │
                        ├── Proxy sends RequestDiagnostics=true
                        ├── Agent streams one message per collection
                        └── Proxy caches records by agentID::artifactDir
                                │
                                └── HTTP GET /diagnostics
```

### Why the Layers Are Separate

- `panicdiag` is responsible for local panic capture and artifact persistence.
- The FODC agent is responsible for discovering crash collections from local sources.
- The FODC proxy is responsible for requesting, caching, filtering, and serving fleet-wide diagnostics.
- Global crash output remains the last-resort path for fatal runtime crashes that bypass recovery in processes that enable it, such as the FODC agent.

## Component Design

### 1. Recovery Runtime

**Purpose**: Recover managed panics and persist structured diagnostics.

#### Core Types

**`RecoveryOptions`**

```go
type RecoveryOptions struct {
	Counter         meter.Counter
	Logger          *logger.Logger
	StateDumper     StateDumper
	ProcessMetadata map[string]string
	Component       string
	ArtifactRoot    string
	StateLimitBytes int64
}
```

**`StateDumper`**

```go
type StateDumper interface {
	DumpState(context.Context) (any, error)
}
```

**`PanicRecord`**

```go
type PanicRecord struct {
	ProcessMetadata map[string]string `json:"processMetadata,omitempty"`
	StateDump       *StateDumpStatus  `json:"stateDump,omitempty"`
	Component       string            `json:"component"`
	PanicValue      string            `json:"panicValue"`
	GoroutineStack  string            `json:"goroutineStack"`
	OccurredAt      time.Time         `json:"occurredAt"`
	Breadcrumbs     []Breadcrumb      `json:"breadcrumbs,omitempty"`
	Recovered       bool              `json:"recovered"`
}
```

#### Current Behavior

- `WithRecovery` recovers panics with `defer` and `recover`.
- The active `context.Context` is passed by pointer so breadcrumbs appended during execution are visible to recovery.
- If configured, a panic counter is incremented with a `component` label.
- If an artifact root is available, `panic.json` and `crash.txt` are written immediately.
- If a `StateDumper` is configured, the runtime writes:
  - `deep-dump.json`
  - `deep-dump.spew`
- The persisted `panic.json` is then rewritten to include `stateDump` status, including `path`, `spewPath`, `truncated`, or `error`.

### 2. Global Crash Output

**Purpose**: Persist unrecoverable runtime crash output for failures that do not pass through `WithRecovery`.

#### Current Behavior

- `CrashOutputConfig.InstallGlobalCrashOutput` registers `debug.SetCrashOutput`.
- The runtime crash output file is `runtime-crash-<pid>.txt` under `--panic-diagnostics-dir`.
- Installing crash output also sets:
  - the default artifact root used by `panicdiag`
  - the default maximum number of retained artifact directories
- `--max-diagnosis-memory-usage-percentage` can reserve memory headroom for post-panic diagnostics.

In the current FODC code, this crash-output path is enabled by the FODC agent, not the FODC proxy.

This is the safety net for fatal runtime failures and escaped panics.

### 3. Diagnostic Breadcrumbs

**Purpose**: Preserve the semantic path that led to a panic.

#### Core Type

```go
type Breadcrumb struct {
	Fields    map[string]string `json:"fields,omitempty"`
	Time      time.Time         `json:"time"`
	Stage     string            `json:"stage"`
	Component string            `json:"component,omitempty"`
}
```

#### Current Behavior

- `WithBreadcrumb(ctx, stage, component, fields)` returns a derived context with one additional immutable marker.
- `BreadcrumbsFromContext(ctx)` returns markers ordered oldest-to-newest.
- The implementation clones field maps so callers cannot mutate already-recorded breadcrumb data.
- Breadcrumbs are already used in several query and service paths, including FODC lifecycle collection and BanyanDB stream and measure query handling.

### 4. Deep State Serialization

**Purpose**: Persist bounded state that helps explain the crash input and runtime state.

#### Current Behavior

- `StateDumper.DumpState` is called only after a panic has been recovered.
- The runtime writes a bounded JSON dump and a bounded spew dump.
- Serialization status is recorded in `PanicRecord.StateDump`.
- Serialization failure does not prevent panic recovery or artifact creation.

#### StateDump Status

```go
type StateDumpStatus struct {
	Path      string `json:"path,omitempty"`
	Error     string `json:"error,omitempty"`
	SpewPath  string `json:"spewPath,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}
```

### 5. Artifact Writer

**Purpose**: Write crash diagnostics into a stable per-panic directory.

#### Current Layout

```text
<panic-diagnostics-dir>/
  runtime-crash-<pid>.txt
  <timestamp>-<component>-<pid>/
    panic.json
    crash.txt
    deep-dump.json        (optional)
    deep-dump.spew        (optional)
```

#### Naming and Retention

- Artifact directories are named as `<UTC timestamp>-<sanitized component>-<pid>`.
- Components are sanitized for safe filesystem names.
- Retention pruning removes the oldest artifact directories first.
- Only directories containing `panic.json` are treated as crash artifacts for pruning.

### 6. FODC Agent Collection

**Purpose**: Merge locally available crash collections into one streamable view.

#### Sources

- `InProcessPanicStore`
  - receives reports directly from `panicdiag` through the default reporter
- `DirectoryWatcher`
  - scans a crash directory using filesystem notifications plus periodic rescans
  - stores even incomplete artifacts, but flags missing required files during analysis

#### Merging

`MultiCollectionProvider` deduplicates by `Collection.ArtifactDir` so the same artifact is not reported twice when it is seen by both the in-process store and the directory watcher.

#### Collection Record

```go
type CollectionRecord struct {
	FetchedAt      time.Time            `json:"fetchedAt"`
	SourceEndpoint string               `json:"sourceEndpoint"`
	Collection     panicdiag.Collection `json:"collection"`
}
```

### 7. FODC Proxy Aggregation

**Purpose**: Collect diagnostics from connected agents on demand and expose a filtered fleet view.

#### Current Behavior

- The proxy creates the diagnostics aggregator before the gRPC service exists.
- `SetGRPCService` wires the gRPC sender after service construction.
- `CollectDiagnostics` reads `grpcService` under lock.
- If `grpcService` is still `nil` during startup, the proxy logs a warning and returns the cached snapshot instead of failing.
- On each `GET /diagnostics` request, the proxy:
  1. filters agents by `role` and optional `pod_name`
  2. sends `RequestDiagnostics(agentID)` to each matching agent
  3. waits a short fixed collection window
  4. returns the current cached snapshot

#### Cache Semantics

- Each incoming `StreamCrashDiagnosticsRequest` represents one artifact.
- The proxy cache key is `agentID::artifactDir`.
- Repeated sends update the cached record in place.
- Removing an agent removes all cached records for that agent.
- Request failures are non-fatal and are logged as agent capability or stream-availability issues.

## Crash Artifacts

### Persisted `panic.json` Example

```json
{
  "processMetadata": {
    "node": "banyand-datanode-0",
    "role": "datanode",
    "version": "dev"
  },
  "stateDump": {
    "path": "/crash/20260401T101112.000000000Z-measure-query-worker-1234/deep-dump.json",
    "spewPath": "/crash/20260401T101112.000000000Z-measure-query-worker-1234/deep-dump.spew",
    "truncated": false
  },
  "component": "measure-query-worker",
  "panicValue": "runtime error: index out of range [7] with length 4",
  "goroutineStack": "goroutine 9123 [running]:\n...",
  "occurredAt": "2026-04-01T10:11:12Z",
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
  "recovered": true
}
```

### Files in a Collection

- `panic.json` is the canonical machine-readable record.
- `crash.txt` is the human-readable summary written by the artifact writer.
- `deep-dump.json` is optional.
- `deep-dump.spew` is optional.
- A runtime-level fatal crash may also produce `runtime-crash-<pid>.txt` in the artifact root.

### Completeness Rules

- `panic.json` is required for a directory to be recognized as a crash collection by `panicdiag.ListCollections`.
- The agent-side `DirectoryWatcher` treats `panic.json` and `crash.txt` as the required files for a complete artifact.
- Incomplete artifacts can still be surfaced by the watcher, but they are logged as incomplete.

## Proxy API and Collection Semantics

### HTTP Endpoint

The proxy exposes:

- `GET /diagnostics`

Supported query parameters:

- `role`
- `pod_name`

The endpoint returns an array of aggregated records.

### Returned Record Shape

```json
{
  "fetched_at": "2026-04-20T10:00:00Z",
  "panic_record": {
    "occurred_at": "2026-04-20T09:59:30Z",
    "component": "watchdog",
    "goroutine_stack": "goroutine 100 [running]:\n...",
    "panic_value": "boom",
    "recovered": true
  },
  "agent_id": "agent-1",
  "pod_name": "banyand-datanode-0",
  "role": "datanode",
  "source_endpoint": "file:///crash",
  "artifact_dir": "20260420T095930.000000000Z-watchdog-1234",
  "files": [
    "crash.txt",
    "deep-dump.json",
    "deep-dump.spew",
    "panic.json"
  ]
}
```

### Request/Response Model

- The proxy does not continuously mirror all agent crash collections.
- Diagnostics are refreshed on demand when the HTTP endpoint is called.
- Agents respond by listing their currently known collections and streaming them one-by-one to the proxy.
- The proxy then serves a cached snapshot after waiting for a short response window.

This model avoids requiring a batch-end marker in the current proto while still giving the HTTP caller a coherent fleet snapshot.

## Deployment and Operations

### Shared Directory Model

The local crash artifact directory is still important, but it now primarily feeds the FODC agent rather than a separate sidecar uploader.

```text
+---------------------------+        +---------------------------+
| BanyanDB / FODC process   |        | FODC agent               |
|                           |        |                           |
| panicdiag writes artifacts| -----> | In-process store          |
| and runtime crash output  | local  | Directory watcher         |
|                           | dir    | gRPC crash stream client  |
+---------------------------+        +-------------+-------------+
                                                  |
                                                  v
                                        +-----------------------+
                                        | FODC proxy            |
                                        | diagnostics cache     |
                                        | GET /diagnostics      |
                                        +-----------------------+
```

### Relevant Flags

From the current implementation:

- `--panic-diagnostics-enabled`
- `--panic-diagnostics-dir`
- `--panic-diagnostics-max-artifacts`
- `--max-diagnosis-memory-usage-percentage`

These flags are part of the FODC agent's CLI surface. Agent crash collection also depends on the configured watched crash source directory when filesystem-backed collection is enabled.

### Operational Notes

- The artifact directory should be writable by the process generating artifacts.
- Retention is directory-count based, not total-byte based.
- `GOMEMLIMIT` headroom helps the process finish diagnostic work under memory pressure.
- Incomplete artifacts may appear transiently while files are still being written.
- During proxy startup, `/diagnostics` can return a cached snapshot even before the gRPC service has been wired into the aggregator.

## Testing Strategy

### Unit Tests

- `WithRecovery` captures panic value, stack trace, breadcrumbs, and optional state dump status.
- Breadcrumb helpers preserve ordering and clone field maps.
- Artifact writing creates `panic.json` and `crash.txt`.
- Crash output installation calls `debug.SetCrashOutput` when enabled.
- Directory watching detects complete and incomplete artifact directories.
- Proxy aggregation returns a cached snapshot when the gRPC service is unset.

### Integration Tests

- State dump files are surfaced in collection `Files`.
- Breadcrumbs written during recovered panics persist into `panic.json`.
- Incomplete artifact directories are still recorded by the agent watcher.
- Proxy diagnostics requests collect records from connected agents and expose them through `GET /diagnostics`.

## Appendix

### Layer Summary

| Layer | Name | Current Outcome |
|-------|------|-----------------|
| 0 | Recovery Runtime | Recover managed panics and persist `panic.json` / `crash.txt` |
| 1 | Global Crash Output | Persist fatal runtime crash output to `runtime-crash-<pid>.txt` |
| 2 | Breadcrumbs | Preserve semantic execution history in `panic.json` |
| 3 | State Dump | Persist bounded JSON and spew state snapshots |
| 4 | Agent Collection | Merge in-process and filesystem-backed crash collections |
| 5 | Proxy Aggregation | Request, cache, filter, and serve fleet-wide diagnostics |

### Expected Operator Value

- Faster root-cause analysis because crash records include stack traces, breadcrumbs, and optional state dumps.
- Better resilience because fatal runtime crash output is persisted even when recovery wrappers are bypassed.
- Fleet-level visibility because the proxy can serve crash diagnostics aggregated from connected agents through a single endpoint.
