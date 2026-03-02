# KTM Metrics — Semantics & Workload Interpretation

This document defines the **semantic meaning** of kernel-level metrics collected by the
Kernel Telemetry Module (KTM) under different BanyanDB workloads.

It serves as the **authoritative interpretation guide** for:
- First Occurrence Data Capture (FODC)
- Automated analysis and reporting by LLM agents
- Self-healing and tuning recommendations

This document does **not** describe kernel attachment points or implementation details.
Those are covered separately in the KTM design document.

---

## 1. Scope and Non-Goals

### In Scope
- Interpreting kernel metrics in the context of **LSM-style read + compaction workloads**
- Distinguishing **benign background activity** from **user-visible read-path impact**
- Providing **actionable, explainable signals** for automated analysis

### Out of Scope
- Device-level I/O profiling or per-disk attribution
- SLA-grade performance accounting
- Precise block-layer root cause isolation  

SLA-grade performance accounting is explicitly out of scope because
eBPF-based sampling and histogram bucketing introduce statistical
approximation, and kernel-level telemetry cannot capture application-
or network-level queuing delays.  

KTM focuses on **user-visible impact first**, followed by kernel-side explanations.

---

## 2. Core Metrics Overview

### 2.1 Read / Pread Syscall Latency (Histogram)

**Metric Type**
- Histogram (bucketed latency)
- Collected at syscall entry/exit for `read` and `pread64`

**Semantic Meaning**
This metric represents the **time BanyanDB threads spend blocked in the read/pread syscall path**.

It is the **primary impact signal** in KTM.

**Key Rule**
> If syscall-level read/pread latency does **not** increase, the situation is **not considered an incident**, regardless of background cache or reclaim activity.

**Why Histogram**
- Captures long-tail latency (p95 / p99) reliably
- More representative of user experience than averages
- Suitable for LLM-based reasoning and reporting

---

### 2.2 fadvise Policy Actions

**Metric Type**
- Counter

**Semantic Meaning**
Records **explicit page cache eviction hints** issued by BanyanDB.

This metric represents **policy intent**, not impact.

**Interpretation Notes**
- fadvise activity alone is not an anomaly
- Must be correlated with read/pread latency to assess impact

---

### 2.3 Page Cache Add / Fill Activity

**Metric Type**
- Counter

**Semantic Meaning**
Represents pages being added to the OS page cache due to:
- Read misses
- Sequential scans
- Compaction activity

High page cache add rates are **expected** under LSM workloads.

**Note**
Page cache add activity does not necessarily imply disk I/O or cache miss.
It may increase due to readahead, sequential scans, or compaction reads,
and should be treated as a **correlated signal**, not a causal indicator,
unless accompanied by read/pread latency degradation.

---

### 2.4 Memory Reclaim and Pressure Signals

**Metrics**
- LRU shrink activity
- Direct reclaim entry events

**Semantic Meaning**
Indicates **kernel memory pressure** that may destabilize page cache residency.

These metrics act as **root-cause hints**, not incident triggers.

---

## 3. Interpretation Principles

### 3.1 Impact-First Gating

All incident detection and analysis is gated on:

> **Syscall-level read/pread latency histogram**

This refers to the combined read/pread syscall latency histograms.

Other metrics are used **only to explain why latency increased**, not to decide whether an incident occurred.

---

### 3.2 Cache Churn Is Not an Incident

High values of:
- page cache add
- reclaim
- background scans

are **normal** under LSM-style workloads and **must not** be treated as incidents unless they result in read/pread latency degradation.

---

## 4. Workload Semantics

This section defines canonical workload patterns and how KTM metrics should be interpreted.

---

> **Global Rule — Latency-Gated Evaluation**
>
> All workload patterns below are evaluated **only after syscall-level
> read/pread latency degradation has been detected** (e.g., p95/p99 bucket shift).
> Kernel signals such as page cache activity, reclaim, or fadvise **must not**
> be interpreted as incident triggers on their own.

---

### Workload 1 — Sequential Read / Background Compaction (Benign)

**Typical Signals**
- `page_cache_add ↑`
- `lru_shrink ↑` (optional)
- `read/pread syscall latency stable`

**Interpretation**
Sequential scans and compaction naturally introduce cache churn.
As long as read/pread latency remains stable, this workload is benign.

**Operational Decision**
- Do not trigger FODC
- No self-healing action required

---

### Workload 2 — High Page Cache Pressure, Foreground Sustained

**Typical Signals**
- `page_cache_add ↑`
- `lru_shrink ↑`
- occasional `direct_reclaim`
- `read/pread syscall latency stable`

**Interpretation**
System memory pressure exists, but foreground reads are not impacted.
This indicates a tight but stable operating point.

**Operational Decision**
- No incident
- Monitor trends only

---

### Workload 3 — Aggressive Cache Eviction or Reclaim Impact

**Typical Signals**
- `fadvise_calls ↑` or early reclaim activity
- `page_cache_add ↑` (repeated refills)
- `read/pread syscall latency ↑` (long-tail buckets appear)

**Interpretation**
Hot pages are evicted too aggressively, causing read amplification.
Foreground reads are directly impacted.

**Operational Decision**
- Trigger FODC
- Recommend tuning eviction thresholds or rate-limiting background activity

**Discriminator**
Eviction-driven degradation is typically characterized by:
- Elevated `fadvise` activity
- Repeated page cache refills
- Read latency degradation **without sustained compaction throughput
  or disk I/O saturation**

- **Query pattern signal** (optional): continuously scanning an extensive time range.

This pattern indicates policy-induced cache churn rather than workload contention.
These discriminator signals are typically sourced from DB-level or system-level
metrics outside KTM.

---

### Workload 4 — I/O Contention or Cold Data Access

**Typical Signals**
- `page_cache_add ↑` (due to compaction OR new data reads)
- `read/pread syscall latency ↑`
- reclaim may or may not be present

**Interpretation**
Latency degradation is caused by:
1. **Resource Contention**: Compaction threads competing with foreground reads for disk I/O.
2. **Cold Data Access**: The active working set exceeds resident memory, forcing frequent OS page cache misses (synchronous disk reads).

**Operational Decision**
- Trigger FODC
- Suggest reducing compaction concurrency
- If compaction is idle but latency remains high, consider scaling up memory (Capacity Planning).

**Discriminator**
This pattern is characterized by elevated read/pread syscall latency **without** the explicit eviction signals of W3 (fadvise) or the system-wide pressure of W5 (reclaim).
It indicates that the system is physically bound by I/O limits due to contention or capacity cache misses.

---

### Workload 5 — OS Memory Pressure–Driven Cache Drop

**Typical Signals**
- `direct_reclaim ↑`
- `lru_shrink ↑`
- `read/pread syscall latency ↑`
- `fadvise` may be absent

**Interpretation**
Cache eviction is driven by OS memory pressure rather than DB policy.
Foreground reads stall due to synchronous reclaim.

**Operational Decision**
- Trigger FODC
- Recommend adjusting memory limits or reducing background memory usage

---

## 5. Excluded Signals and Rationale

### 5.1 Page Fault Metrics

BanyanDB primarily uses `read()` with page cache access rather than mmap-based I/O.
Major and minor page faults do not reliably represent read-path stalls and are therefore excluded from impact detection.

### 5.2 Block Layer Latency

Block-layer completion context does not reliably map to BanyanDB threads in containerized environments.
Syscall-level latency already captures user-visible impact and is used as the primary signal.

Block-layer metrics may be added later as an optional enhancement.

---

## 6. Summary

KTM identifies read-path incidents by:
1. Gating on **syscall-level read/pread latency histograms**
2. Explaining impact using:
    - eviction policy actions (fadvise)
    - page cache behavior
    - memory pressure signals

This separation ensures:
- Low false positives
- Clear causality
- Actionable and explainable self-healing decisions

## 7. Decision Flow Overview
```mermaid
graph TD
    Start([Start: Metric Analysis]) --> CheckLat{Read/Pread Syscall\nLatency Increased?}

    %% Primary Gating Rule
    CheckLat -- No --> Benign[Benign State\nNo User Impact]
    CheckLat -- Yes --> Incident[Incident Detected\nTrigger FODC]

    %% Benign Analysis
    Benign --> CheckPressure{Pressure Signals\nPresent?}
    CheckPressure -- Yes --> W2[W2: Stable State]
    CheckPressure -- No --> W1[W1: Background Scan/Compaction]

    %% Incident Analysis (Root Cause)
    Incident --> CheckFadvise{High fadvise\ncalls?}
    
    %% Branch: Policy
    CheckFadvise -- Yes --> W3[W3: Policy-Driven Eviction\nAssociated with aggressive DONTNEED (policy signal)]
    
    %% Branch: Kernel/OS
    CheckFadvise -- No --> CheckReclaim{Direct Reclaim / \nLRU Shrink?}
    
    %% Branch: Pressure
    CheckReclaim -- Yes --> W5[W5: OS Memory Pressure\nCause: Sync Reclaim]
    
    %% Branch: Contention
    CheckReclaim -- No --> W4[W4: I/O Contention / Cold Read\nCause: Compaction or Working Set > RAM]

    %% Styling
    style CheckLat fill:#f9f,stroke:#333,stroke-width:2px
    style Incident fill:#f00,stroke:#333,stroke-width:2px,color:#fff
    style Benign fill:#9f9,stroke:#333,stroke-width:2px
```

---

## 8. Operational Prerequisites and Observability

- Tracefs must be mounted (usually at `/sys/kernel/tracing`) and syscall tracepoints enabled; KTM attaches via tracepoints.
- Kernel versions must expose the required tracepoints (syscalls, filemap, vmscan); there is no fentry/kprobe fallback in the current implementation.
- BTF availability is expected for CO-RE program loading; missing BTF prevents KTM from starting.
- On failure to load/attach, KTM logs an error and disables itself (see Failure Modes in the design document).
