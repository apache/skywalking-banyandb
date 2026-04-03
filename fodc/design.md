
Design a development documentation on Context-Aware Panic Diagnostics, it's a multi-layered defense-in-depth architecture for banyandb. Unhandled Panics Create Invisible Blind Spots in Production
Go runtime panics terminate BanyanDB processes without actionable forensic data.
Background goroutines silently crash, leaving no causal context.
Diagnosing root causes requires costly manual reproduction.
Standard stack traces show where the crash happened, not what state led to it.
High-throughput paths demand zero-overhead diagnostics.


Solution Overview - Five Layers of Defense
A Defense-in-Depth Architecture Captures Every Crash with Full Context
Layer 0 - WithRecovery Wrapper:
Intercepts panics in every goroutine, logs stack traces, increments Prometheus metrics
Layer 1 - Global Crash Output:
debug.SetCrashOutput registers a crash file at process bootstrap, capturing even initialization panics
Layer 2 - Diagnostic Breadcrumbs:
Lightweight markers embedded in context.Context reconstruct the causal event sequence
Layer 3 - Deep State Serialization: Named
return variables + reflection-based tools dump
complex object graphs
Layer 4 - FODC Sidecar:
External agent bundles and ships crash artifacts to SkyWalking OAP for centralized analysis

Centralized Analysis & Observability:

Layer 0: WithRecovery Wrapper -->
Layer 1: Global Crash Output -->
Layer 2: Diagnostic Breadcrumbs -->
Layer 3: Deep State Serialization -->
Layer 4: FODC Sidecar  -->
SkyWalking OAP

BanyanDB main container: It runs the main business logic, generates diagnostic data (such as crash logs crash.txt and data snapshots deep-dump.json) in case of a crash, and writes these data to the shared volume.
Shared volume (/crash): Serving as an intermediate storage between the main container and the sidecar container, it is used to store crash diagnosis files.
FODC sidecar container: Monitors changes in shared volumes, reads and analyzes generated crash data.
External notification: After the analysis is completed, the inspection agent will upload the relevant artifacts and send an alert notification via the Slack channel.

Layer 0 - WithRecovery Goroutine Wrapper
Every Goroutine Gets a Safety Perimeter via Centralized Recovery
Implemented in pkg/util or pkg/run following TiDB's high-reliability patterns
defer/recover block intercepts panics, logs the panic value, and captures a localized stack trace
Increments banyandb_panic_total Prometheus counter, labeled by component
Prevents panics from becoming "silent failures" -every crash is observable and alertable
Applied uniformly to all background workers and request-scoped goroutines

Layer 1 - Global Runtime Crash Output
debug.SetCrashOutput Ensures No Panic Escapes Unrecorded
Registered at the earliest stage of the banyand entry point captures initialization panics before any other system is ready
Uses a duplicated file descriptor that remains valid even if memory state or file management logic is corrupted
Acts as the final safety net for unhandled panics, fatal runtime errors (concurrent map writes, stack overflows)
Crash file persists on disk independently of the Go runtime's health
Complements Layer 0: where goroutine recovery handles recoverable panics, Layer 1 catches everything else
Banyand Entry Point (Start)
Register debug.SetCrashOutput
System Initialization (Early)
Runtime Errors & Fatal Panics (e.g., Stack Overflow, Map Write)
Crash Capture & Disk Persistence (Final Safety Net)
Crash File on Disk

Layer 2 - Diagnostic Breadcrumbs via
context.Context
Lightweight markers embedded in context.Context as a request progresses
Example breadcrumbs: "Merging Shard 5", "Querying Measure: service_latency"
Diagnostic handler extracts the ordered breadcrumb trail upon recovery
Negligible CPU overhead (simple pointer assignments)
Transforms cryptic stack traces into a human-readable narrative
Initial Request Start -->
Breadcrumb: "Merging Shard 5"  -->
Breadcrumb: "Querying Measure: service_latency"  -->
Breadcrumb: "Querying Measure: user_activity"  -->
Diagnostic Report: Ordered Human-Readable Narrative

1. Merging Shard 5
2. Querying Measure: service_latency
3. Querying Measure: user_activity
4. Crash Detected


Layer 3 - Deep State Serialization
Named Returns + Reflection Dump the Exact State at Crash Time
Core database functions use Named Return Variables accessible even after panic halts execution
Reflection-based tools (go-spew, godump) recursively serialize complex structures
Serialized state is written to a deep-dump file alongside the crash report
Crash Detected (Panic)
Named Returns & State Variables
Provides the "What" complement to the breadcrumb "Why”
Safety guard: Serialization is bounded by a fixed 5 MB memory limit to prevent OOM

Layer 3 - Deep State Serialization
Named Returns + Reflection Dump the Exact State at Crash Time
Core database functions use Named Return Variables accessible even after panic halts execution
Reflection-based tools (go-spew, godump) recursively serialize complex structures
Serialized state is written to a deep-dump file alongside the crash report
Provides the "What" complement to the breadcrumb "Why”
Safety guard: Serialization is bounded by a fixed 5 MB memory limit to prevent OOM

Crash Detected (Panic) --> 
Named Returns & State Variables -->
Deep-Dump File (Serialized State) and A 5 MB Bounded Limit 
Memory Usage
Serialization Safety Guard

Layer 4 - FODC Sidecar Integration
The FODC Sidecar Automates Collection and Transmission of All Crash Artifacts
FODC agent runs as a sidecar container in the same Kubernetes Pod, sharing a filesystem volume with BanyanDB
Watches for creation of new crash artifacts: crash.txt, deep-dump files
Upon detection, automatically bundles crash report + serialized state + system logs


Performance & Stability - Zero-Cost on the Hot Path
Diagnostics Are Passive by Design - Zero Throughput Impact During Normal Operation
Hot-path overhead is negligible: Breadcrumb writes are simple pointer assignments; no serialization occurs during normal execution.
Post-panic execution: Expensive reflection and state dumping only trigger after a panic - latency is irrelevant as the process is exiting.
OOM prevention: 5 MB buffer cap on deep-state serialization prevents heap exhaustion.
GOMEMLIMIT synergy: Set to 80-90% of container limit, reserving headroom for diagnostics.
Disk quota enforcement: LRU rotation on the crash/directory prevents host disk space exhaustion.


Summary - Defense-in-Depth Makes BanyanDB Production-Ready

Five Complementary Layers Transform Panic Handling from Reactive to Proactive

Layer 0 & 1: ensure no crash goes unrecorded
Layer 2 & 3: ensure every crash is fully contextualized
Layer 4: ensures every crash is automatically collected and centrally analyzed

Performance safeguards: ensure diagnostics never cause secondary failures

Result: Accelerating MTTR dramatically by knowing exactly why, what, and where

From Silent Failure → Full Forensic Insight.
