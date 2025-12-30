
# Kernel Telemetry Module (KTM)

## Overview

Kernel Telemetry Module (KTM) is an optional, modular kernel observability component embedded inside the BanyanDB First Occurrence Data Collection (FODC) sidecar. The first built-in module is an eBPF-based I/O monitor ("iomonitor") that focuses on page cache behavior, fadvise() effectiveness, and memory pressure signals and their impact on BanyanDB performance. KTM is not a standalone agent or network-facing service; it runs as a sub-component of the FODC sidecar ("black box") and exposes a Go-native interface to the Flight Recorder for ingesting metrics. Collection scoping defaults to the BanyanDB container’s cgroup v2, with a configurable comm fallback (default `banyand`).


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
- Scoping: Fixed to the single, co-located BanyanDB process within the same container/pod, using cgroup membership first and a configurable comm-prefix fallback (default `banyand`).


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
- `collector.ebpf.discovery_comm` (optional): comm name prefix used for userspace PID discovery in `/proc`; defaults to `banyand`. **Important**: This setting only affects userspace process discovery. Kernel-side filtering always enforces `comm="banyand"` regardless of this configuration.
- Target discovery heuristic: match `/proc/<pid>/comm` against `discovery_comm` to populate the PID cache for performance optimization. The kernel always filters by comm="banyand".
- Cleanup strategy is monotonic counters only; downstream derives rates. KTM does not clear BPF maps during collection.
- Configuration is applied via the FODC sidecar; KTM does not define its own standalone process-level configuration surface.


## Scoping and Filtering

- Scoping is not optional; KTM is designed exclusively to monitor the single BanyanDB process it is co-located with in a sidecar deployment.
- **Kernel-side filtering**: eBPF programs always enforce `comm="banyand"` in addition to any cgroup filtering. This is hardcoded and not configurable to ensure only BanyanDB processes are monitored.
- **Filtering strategy**: 
  - Preferred mode (strict): cgroup membership + comm="banyand" (both enforced)
  - Degraded mode (comm-only): comm="banyand" only (when cgroup unavailable)
  - **Warning**: comm-only mode may mix metrics across pods if multiple processes match the comm prefix.
- The design intentionally avoids multi-process or node-level (DaemonSet) monitoring to keep the implementation simple and overhead minimal.

### Target Process Discovery (Pod / VM)

KTM needs to resolve the single “target” BanyanDB process before enabling filters and attaching eBPF programs. In both Kubernetes pods and VM/bare-metal deployments, KTM uses a **process matcher** for userspace PID discovery (configurable via `discovery_comm`, default `banyand`). Kernel-side filtering always uses comm="banyand" and is not configurable.

#### Kubernetes Pod (sidecar)

Preconditions:
- The pod must be configured with `shareProcessNamespace: true` so the monitor sidecar can see the target container’s `/proc` entries.
- cgroup v2 mounted (typically at `/sys/fs/cgroup`) to enable the primary cgroup filter.
- If the target process cannot be discovered (for example, `shareProcessNamespace` is off), KTM logs a warning and keeps periodically checking; once the target process appears, KTM enables the module automatically.

Discovery flow (high level):
- Scan `/proc` for candidate processes.
- For each PID, read `/proc/<pid>/comm` and match it against the configured comm prefix (default `banyand`) (or an explicitly provided cgroup path).
- Once matched, derive the target cgroup from `/proc/<pid>/cgroup` (cgroup v2) and program the eBPF cgroup filter. The comm match remains as runtime fallback if the cgroup check does not fire.
- If no matching process is found at startup, KTM continues periodic probing and activates once it is found.

#### VM / bare metal

Discovery flow (high level):
- Scan `/proc` for candidate processes.
- Match `/proc/<pid>/comm` against the configured prefix (default `banyand`).
- Use the discovered PID to derive cgroup v2 path and program the filter; keep the comm match as runtime fallback if cgroup filtering is unavailable.
- If no matching process is found at startup, KTM continues periodic probing and activates once it is found.

### Scoping Semantics

- The BPF maps use a single-slot structure (e.g., a BPF array map with a single entry) to store global monotonic counters for the target process.
- This approach eliminates the need for per-pid hash maps, key eviction logic, and complexities related to tracking multiple processes.
- All kernel events are filtered by the target container’s cgroup ID when available; if the cgroup filter misses (for example, map not populated), a comm-prefix match (configurable; default `banyand`) is used before any counters are updated.

Example (YAML):
```yaml
collector:
  interval: 10s
  modules:
    - iomonitor
  ebpf:
    # Optional: absolute or /sys/fs/cgroup-relative path to the BanyanDB cgroup v2.
    # cgroup_path: /sys/fs/cgroup/<banyandb-cgroup>
    # Optional: comm name prefix used for process discovery and fallback filtering.
    # comm_prefix: banyand
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
- If BanyanDB restarts (PID changes), the cgroup filter continues to match as long as the container does not change; the comm fallback still matches the configured prefix (default `banyand`).
- If the pod/container is recreated (cgroup path changes), KTM re-runs target discovery, re-programs the cgroup filter, and starts counters from zero; metrics from the old container are discarded without reconciliation.
- KTM performs a lightweight health check during collection to ensure the cgroup filter is still populated; if it is missing (for example, container crash/restart), KTM re-detects and re-programs the filter automatically.

## Kernel Attachment Points (Current)

- `sys_enter_read`, `sys_exit_read`, `sys_enter_pread64`, `sys_exit_pread64` (syscall-level I/O latency).
- `mm_filemap_add_to_page_cache`, `filemap_get_read_batch` (page cache add/churn).
- `ksys_fadvise64_64` (fadvise policy actions; fentry/fexit preferred).
- `mm_vmscan_lru_shrink_inactive`, `mm_vmscan_direct_reclaim_begin` (memory reclaim/pressure).

## Limitations

- Page cache–only perspective: direct I/O that bypasses the cache is not observed.
- Kernel-only visibility: no userspace spans, SQL parsing, or CPU profiling.
