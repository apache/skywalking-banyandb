
# Kernel Telemetry Module (KTM)

## Overview

Kernel Telemetry Module (KTM) is an optional, modular kernel observability component embedded inside the BanyanDB First Occurrence Data Collection (FODC) sidecar. The first built-in module is an eBPF-based I/O monitor ("iomonitor") that focuses on page cache behavior, fadvise() effectiveness, and memory pressure signals and their impact on BanyanDB performance. KTM is not a standalone agent or network-facing service; it runs as a sub-component of the FODC sidecar ("black box") and exposes a Go-native interface to the FlightRecorder for ingesting metrics. Collection scoping is configurable and defaults to cgroup v2.


## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Applications                   │
│  ┌──────────────┐                                       │
│  │   BanyanDB   │                                       │
│  └──────┬───────┘                                       │
└─────────┼───────────────────────────────────────────────┘
          │ Shared Pod / Node
          ▼
┌─────────────────────────────────────────────────────────┐
│           FODC Sidecar ("Black Box" Agent)              │
│  ┌────────────────────────────────────────────────────┐ │
│  │   Watchdog / FlightRecorder / KTM (iomonitor)     │ │
│  │   - KTM eBPF Loader & Manager                     │ │
│  │   - KTM Collector (Module Management)             │ │
│  │   - FlightRecorder (in-memory diagnostics store)  │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│                    Linux Kernel                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │                 eBPF Programs                      │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐          │ │
│  │  │ I/O      │  │  Cache   │  │  Memory  │          │ │
│  │  │ Monitor  │  │ Monitor  │  │ Reclaim  │          │ │
│  │  └──────────┘  └──────────┘  └──────────┘          │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

Notes:
- KTM is modular; the initial module is `iomonitor`.


## Modules

### I/O Monitor (iomonitor)
- Focus: page cache add/delete, fadvise() calls, I/O counters, and memory reclaim signals.
- Attachment points: stable tracepoints where possible; fentry/fexit preferred on newer kernels.
- Data path: kernel events -> BPF maps (monotonic counters) -> userspace collector -> exporters.
- Scoping: Fixed to the single, co-located BanyanDB process within the same container/pod.


## Metrics Model and Collection Strategy

- Counters in BPF maps are monotonic and are not cleared by the userspace collector (NoCleanup).
- Collection interval: 10 seconds by default.
- KTM writes collected metrics into the FODC FlightRecorder through a Go-native interface; the FlightRecorder is responsible for any subsequent export, persistence, or diagnostics workflows.
- Downstream systems (for example, FODC Discovery Proxy or higher-level exporters) should derive rates using `rate()`/`irate()` or equivalents; we avoid windowed counters and map resets to preserve counter semantics.
- int64 overflow is not a practical concern for our use cases; we accept long-lived monotonic growth.

Configuration surface (current):
- `collector.interval`: 10s by default.
- `collector.enable_cgroup_filter`, `collector.enable_mntns_filter`: default on when in sidecar mode; can be toggled.
- `collector.target_pid`/`collector.target_comm`: optional helpers for discovering scoping targets.
- Cleanup strategy is effectively `no_cleanup` by design intent; clear-after-read logic is deprecated for production metrics.
- Configuration is applied via the FODC sidecar; KTM does not define its own standalone process-level configuration surface.


## Scoping and Filtering

- Scoping is not optional; KTM is designed exclusively to monitor the single BanyanDB process it is co-located with in a sidecar deployment.
- The target process is identified at startup, and eBPF programs are instructed to filter events to only that process.
- Primary filtering mechanism: cgroup v2. This ensures all events originate from the correct container. PID and mount namespace filters are used as supplementary checks.
- The design intentionally avoids multi-process or node-level (DaemonSet) monitoring to keep the implementation simple and overhead minimal.

### Scoping Semantics

- The BPF maps use a single-slot structure (e.g., a BPF array map with a single entry) to store global monotonic counters for the target process.
- This approach eliminates the need for per-pid hash maps, key eviction logic, and complexities related to tracking multiple processes.
- All kernel events are filtered by the target process's identity (via its cgroup ID and PID) before any counters are updated in the BPF map.

Example (YAML):
```yaml
collector:
  interval: 10s
  modules:
    - iomonitor
  enable_cgroup_filter: true
  enable_mntns_filter: true
```


## API Surface

KTM does not expose a dedicated HTTP or gRPC API. Instead, it provides an internal Go-native interface that is consumed by the FODC sidecar:

- Go-native interfaces (implemented):
  - Register and manage KTM modules (such as `iomonitor`).
  - Configure collectors and scoping behavior.
  - Read monotonic counters from BPF maps and write the results into the FlightRecorder.

Any external APIs (HTTP or gRPC) that expose KTM-derived metrics are part of the broader FODC system (for example, Discovery Proxy or other FODC components) and are documented separately. KTM itself is not responsible for serving `/metrics` or any other network endpoints.


## Exporters

KTM itself has no direct network exporters. All metrics collected by KTM are written into the FlightRecorder inside the FODC sidecar. External consumers (such as FODC Discovery Proxy, Prometheus integrations, or BanyanDB push/pull paths) read from the FlightRecorder or other FODC-facing APIs and are specified in the FODC design documents rather than this KTM-focused document.


## Metrics Reference (selected)

Prefix: metrics are currently emitted under the `ktm_` namespace to reflect their kernel eBPF origin (e.g., `ktm_cache_misses_total`).

- I/O & Cache
  - `ktm_cache_read_attempts_total`
  - `ktm_cache_misses_total`
  - `ktm_page_cache_adds_total`
- fadvise()
  - `ktm_fadvise_calls_total`
  - `ktm_fadvise_advice_total{advice="..."}`
  - `ktm_fadvise_success_total`
- Memory
  - `ktm_memory_lru_pages_scanned_total`
  - `ktm_memory_lru_pages_reclaimed_total`
  - `ktm_memory_reclaim_efficiency_percent`
  - `ktm_memory_direct_reclaim_processes`

Semantics: all counters are monotonic; use Prometheus functions for rates/derivatives; no map clearing between scrapes.


## Safety & Overhead Boundary

KTM is strictly passive: no kernel modifications, no syscall blocking, and only bounded-size monotonic maps. Probes attach to stable tracepoints or fentry/fexit paths with kprobe fallbacks, and expected CPU overhead remains <1% under typical BanyanDB workloads.

## Security and Permissions

Loading and managing eBPF programs requires elevated privileges. The FODC sidecar process, which hosts the KTM, must run with the following Linux capabilities:
- `CAP_BPF`: Allows loading, attaching, and managing eBPF programs and maps. This is the preferred, more restrictive capability.
- `CAP_SYS_ADMIN`: A broader capability that also grants permission to perform eBPF operations. It may be required on older kernels where `CAP_BPF` is not fully supported.

The sidecar should be configured with the minimal set of capabilities required for its operation to adhere to the principle of least privilege.

## Failure Modes

KTM is designed to fail gracefully. If the eBPF programs fail to load at startup for any reason (e.g., kernel incompatibility, insufficient permissions, BTF information unavailable), the KTM module will be disabled.

In this state:
- An error will be logged to indicate that KTM could not be initialized and is therefore inactive.
- The broader FODC sidecar will continue to run in a degraded mode, ensuring that other sidecar functions remain operational.
- No KTM-related metrics will be collected or exposed.

This approach ensures that a failure within the observability module does not impact the core functionality of the BanyanDB process or its sidecar.

## Restart Semantics

On sidecar restart, BPF maps are recreated and all counters reset to zero. Downstream systems (e.g., Prometheus via FODC integrations) should treat this as a new counter lifecycle and continue deriving rates/derivatives normally.

## Kernel Attachment Points (Current)

- `ksys_fadvise64_64` → fentry/fexit (preferred) or syscall tracepoints with kprobe fallback.
- Page cache add/remove → `filemap_get_read_batch` and `mm_filemap_add_to_page_cache` tracepoints, with kprobe fallbacks.
- Memory reclaim → `mm_vmscan_lru_shrink_inactive` and `mm_vmscan_direct_reclaim_begin` tracepoints.

## Limitations

- Page cache–only perspective: direct I/O that bypasses the cache is not observed.
- Kernel-only visibility: no userspace spans, SQL parsing, or CPU profiling.