
# Kernel Telemetry Module (KTM)

## Overview

Kernel Telemetry Module (KTM) is an optional, modular kernel observability component embedded inside the BanyanDB First Occurrence Data Collection (FODC) sidecar. The first built-in module is an eBPF-based I/O monitor ("iomonitor") that focuses on page cache behavior, fadvise() effectiveness, and memory pressure signals and their impact on BanyanDB performance. KTM is not a standalone agent or network-facing service; it runs as a sub-component of the FODC sidecar ("black box") and exposes a Go-native interface to the Flight Recorder for ingesting metrics. Collection scoping defaults to the BanyanDB container’s cgroup v2, with a hardcoded comm fallback (`banyand`) for degraded mode.


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
│  │   Watchdog / Flight Recorder / KTM (iomonitor)     │ │
│  │   - KTM eBPF Loader & Manager                     │ │
│  │   - KTM Collector (Module Management)             │ │
│  │   - Flight Recorder (in-memory diagnostics store)  │ │
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
- Scoping: Fixed to the single, co-located BanyanDB process within the same container/pod, using cgroup membership first and a hardcoded comm fallback (`banyand`).


## Metrics Model and Collection Strategy

- Counters in BPF maps are monotonic and are not cleared by the userspace collector.
- Collection and push interval: 10 seconds by default.
- KTM periodically pushes collected metrics into the FODC Flight Recorder through a Go-native interface at the configured interval (default 10s). The push interval is exported through the `collector.interval` configuration option. The Flight Recorder is responsible for any subsequent export, persistence, or diagnostics workflows.
- Downstream systems derive rates (for example, Prometheus/PromQL `rate()`/`irate()`); FODC/KTM only provides raw counters and does not compute rates internally. We avoid windowed counters and map resets to preserve counter semantics.
- int64 overflow is not a practical concern for our use cases; we accept long-lived monotonic growth.
- KTM exports only raw counters; any ratios/percentages are derived upstream (see FODC operations/overview for exporter behavior).

Configuration surface (current):
- `collector.interval`: Controls the periodic push interval for metrics to Flight Recorder. Defaults to 10s.
- `collector.ebpf.cgroup_path` (optional): absolute or `/sys/fs/cgroup`-relative path to the BanyanDB cgroup v2. If not explicitly configured, KTM attempts to auto-detect the target cgroup. Only when auto-detection fails does it fall back to comm-only filtering (degraded mode with cross-pod risk).
- Cleanup strategy is monotonic counters only; downstream derives rates. KTM does not clear BPF maps during collection.
- Configuration is applied via the FODC sidecar; KTM does not define its own standalone process-level configuration surface.


## Scoping and Filtering

- Scoping is not optional; KTM is designed exclusively to monitor the single BanyanDB process it is co-located with in a sidecar deployment.
- **Kernel-side filtering**: eBPF programs always enforce `comm="banyand"` in addition to any cgroup filtering. This is hardcoded and not configurable to ensure only BanyanDB processes are monitored.
- **Filtering strategy**: 
  - Preferred mode (strict): cgroup membership + comm="banyand" (both enforced)
  - Degraded mode (comm-only): comm="banyand" only (when cgroup unavailable)
  - **Warning**: comm-only mode may mix metrics across pods if multiple processes match the comm name.
- The design intentionally avoids multi-process or node-level (DaemonSet) monitoring to keep the implementation simple and overhead minimal.

### Target Process Discovery (Pod / VM)

KTM resolves the target cgroup before enabling filters and attaching eBPF programs. If `cgroup_path` is not provided, it derives a Pod-level cgroup from `/proc/self/cgroup` (and falls back to the self cgroup for non-Kubernetes environments). Kernel-side filtering always uses `comm="banyand"` and is not configurable.

#### Kubernetes Pod (sidecar)

Preconditions:
- cgroup v2 mounted (typically at `/sys/fs/cgroup`) to enable the primary cgroup filter.
- If the cgroup path cannot be resolved at startup, KTM falls back to comm-only filtering (degraded mode).

Discovery flow (high level):
- If `cgroup_path` is explicitly configured, use it directly.
- Otherwise, derive a Pod-level cgroup from `/proc/self/cgroup`.
- Program the eBPF cgroup filter with the resolved cgroup ID.

#### VM / bare metal

Discovery flow (high level):
- If `cgroup_path` is explicitly configured, use it directly.
- Otherwise, derive a Pod-level cgroup from `/proc/self/cgroup`.
- Program the eBPF cgroup filter with the resolved cgroup ID.
- If resolution fails, fall back to comm-only filtering (degraded mode).

### Scoping Semantics

- The BPF maps use a single-slot structure (e.g., a BPF array map with a single entry) to store global monotonic counters for the target process.
- This approach eliminates the need for per-pid hash maps, key eviction logic, and complexities related to tracking multiple processes.
- **Kernel-side filtering**: All kernel events are filtered by checking `comm="banyand"` (hardcoded, non-configurable). When cgroup filtering is available, both cgroup ID and comm are enforced. When cgroup is unavailable (degraded mode), only comm filtering is used.

Example (YAML):
```yaml
collector:
  interval: 10s
  modules:
    - iomonitor
  ebpf:
    # Optional: absolute or /sys/fs/cgroup-relative path to the BanyanDB cgroup v2.
    # If not set, KTM attempts auto-detection by scanning /proc for comm="banyand".
    # Falls back to comm-only mode on failure.
    # cgroup_path: /sys/fs/cgroup/<banyandb-cgroup>
```


## API Surface

KTM does not expose a dedicated HTTP or gRPC API. Instead, it provides an internal Go-native interface that is consumed by the FODC sidecar:

- Go-native interfaces (implemented):
  - Register and manage KTM modules (such as `iomonitor`).
  - Configure collectors and scoping behavior.
  - Periodically read monotonic counters from BPF maps and push the results into the Flight Recorder at the configured interval (default 10s).

Any external APIs (HTTP or gRPC) that expose KTM-derived metrics are part of the broader FODC system (for example, Discovery Proxy or other FODC components) and are documented separately. KTM itself is not responsible for serving `/metrics` or any other network endpoints.


## Exporters

KTM itself has no direct network exporters. All metrics collected by KTM are periodically pushed into the Flight Recorder inside the FODC sidecar at the configured interval (default 10s). External consumers (such as FODC Discovery Proxy, Prometheus integrations, or BanyanDB push/pull paths) read from the Flight Recorder or other FODC-facing APIs and are specified in the FODC design documents rather than this KTM-focused document.


## Metrics Reference (selected)

Prefix: metrics are currently emitted under the `ktm_` namespace to reflect their kernel eBPF origin (e.g., `ktm_cache_misses_total`).

- I/O & Cache
  - `ktm_cache_lookups_total`
  - `ktm_cache_fills_total`
  - `ktm_cache_deletes_total`
- I/O latency (syscall-level)
  - `ktm_sys_read_latency_seconds` (histogram family: exposes `_bucket`, `_count`, `_sum`)
  - `ktm_sys_pread_latency_seconds` (histogram family: exposes `_bucket`, `_count`, `_sum`)
  - `ktm_sys_read_bytes_total`
  - `ktm_sys_pread_bytes_total`
- fadvise()
  - `ktm_fadvise_calls_total`
  - `ktm_fadvise_dontneed_total`
- Memory
  - `ktm_memory_lru_pages_scanned_total`
  - `ktm_memory_lru_pages_reclaimed_total`
  - `ktm_memory_lru_shrink_events_total`
  - `ktm_memory_direct_reclaim_begin_total`
- Status
  - `ktm_degraded` (gauge; 1 if KTM is running in comm-only degraded mode, 0 otherwise)

Semantics: all counters are monotonic; latency metrics are exported as Prometheus histograms (`_bucket`, `_count`, `_sum`); use Prometheus functions for rates/derivatives; no map clearing between scrapes. KTM does not emit ratio/percentage metrics; derive them upstream.

## Safety & Overhead Boundary

KTM is strictly passive: no kernel modifications, no syscall blocking, and only bounded-size monotonic maps. Probes attach to stable tracepoints or fentry/fexit paths with kprobe fallbacks, and expected CPU overhead remains <1% under typical BanyanDB workloads.

## Security and Permissions

Loading and managing eBPF programs requires elevated privileges. The FODC sidecar process, which hosts the KTM, must run with the following Linux capabilities:
- `CAP_BPF`: Allows loading, attaching, and managing eBPF programs and maps. This is the preferred, more restrictive capability.
- `CAP_SYS_ADMIN`: A broader capability that also grants permission to perform eBPF operations. It may be required on older kernels where `CAP_BPF` is not fully supported.

Operational prerequisites and observability: see `docs/operation/fodc/ktm_metrics.md`.

The sidecar should be configured with the minimal set of capabilities required for its operation to adhere to the principle of least privilege.

## Failure Modes

KTM is designed to fail gracefully. If the eBPF programs fail to load at startup for any reason (e.g., kernel incompatibility, insufficient permissions, BTF information unavailable), the KTM module will be disabled.

In this state:
- An error will be logged to indicate that KTM could not be initialized and is therefore inactive.
- The broader FODC sidecar will continue to run in a degraded mode, ensuring that other sidecar functions remain operational.
- No KTM-related metrics will be collected or exposed.

This approach ensures that a failure within the observability module does not impact the core functionality of the BanyanDB process or its sidecar.

## Restart Semantics

- On sidecar restart, BPF maps are recreated and all counters reset to zero. Downstream systems (e.g., Prometheus via FODC integrations) should treat this as a new counter lifecycle and continue deriving rates/derivatives normally.
- If BanyanDB restarts (PID changes), the cgroup filter continues to match as long as the container does not change; the comm fallback still matches `banyand`.
- If the pod/container is recreated (cgroup path changes), KTM re-runs target discovery on the next sidecar start, re-programs the cgroup filter, and starts counters from zero; metrics from the old container are discarded without reconciliation.
- KTM does not perform periodic health checks or re-detection. If the cgroup path changes, the sidecar/Pod must be restarted.

## Kernel Attachment Points (Current)

- `sys_enter_read`, `sys_exit_read`, `sys_enter_pread64`, `sys_exit_pread64` (syscall-level I/O latency).
- `mm_filemap_add_to_page_cache`, `filemap_get_read_batch` (page cache add/churn).
- `ksys_fadvise64_64` (fadvise policy actions; fentry/fexit preferred).
- `mm_vmscan_lru_shrink_inactive`, `mm_vmscan_direct_reclaim_begin` (memory reclaim/pressure).

## Limitations

- Page cache–only perspective: direct I/O that bypasses the cache is not observed.
- Kernel-only visibility: no userspace spans, SQL parsing, or CPU profiling.
