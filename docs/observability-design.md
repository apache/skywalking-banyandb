
# Observability Agent (OA)

## Overview

Observability Agent (OA) is an optional, modular observability component for Apache SkyWalking BanyanDB. The first built-in module is an eBPF-based I/O monitor ("iomonitor") that focuses on page cache behavior, fadvise() effectiveness, and memory pressure signals and their impact on BanyanDB performance. OA can be co-located with BanyanDB (sidecar-style) and enabled on demand for performance analysis. Collection runs on a 10-second cadence with monotonic counters in BPF maps and userspace; scoping is configurable and defaults to cgroup v2.


## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Applications                   │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐ │
│  │   BanyanDB   │   │  Prometheus  │───│   Grafana    │ │
│  └──────┬───────┘   └──────┬───────┘   └──────────────┘ │
└─────────┼──────────────────┼─────────────────────────────┘
          │ Native Export    │ Scrape           Query
          ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│                 Observability Agent (OA)                │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Server Layer (gRPC/HTTP)              │ │
│  ├────────────────────────────────────────────────────┤ │
│  │           Export Layer (Prometheus/BanyanDB)       │ │
│  ├────────────────────────────────────────────────────┤ │
│  │         Collector (Module Management)              │ │
│  ├────────────────────────────────────────────────────┤ │
│  │            eBPF Loader & Manager                   │ │
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
- TLS is planned but not implemented yet.
- OA is modular; the initial module is `iomonitor`.


## Modules

### I/O Monitor (iomonitor)
- Focus: page cache add/delete, fadvise() calls, I/O counters, and memory reclaim signals.
- Attachment points: stable tracepoints where possible; fentry/fexit preferred on newer kernels.
- Data path: kernel events -> BPF maps (monotonic counters) -> userspace collector -> exporters.
- Scoping: optional container/pod scoping via cgroup v2 (and mount namespace when needed).


## Metrics Model and Collection Strategy

- Counters in BPF maps are monotonic and are not cleared by the userspace collector (NoCleanup).
- Collection interval: 10 seconds by default.
- Prometheus should derive rates using `rate()`/`irate()`; we avoid windowed counters and map resets to preserve counter semantics.
- int64 overflow is not a practical concern for our use cases; we accept long-lived monotonic growth.

Configuration surface (current):
- `collector.interval`: 10s by default.
- `collector.enable_cgroup_filter`, `collector.enable_mntns_filter`: default on when in sidecar mode; can be toggled.
- `collector.target_pid`/`collector.target_comm`: optional helpers for discovering scoping targets.
- Cleanup strategy is effectively `no_cleanup` by design intent; clear-after-read logic is deprecated for production metrics.


## Scoping and Filtering

- Scoping is optional and can be enabled via configuration/environment variables (e.g., in docker-compose).
- Primary mechanism: cgroup v2 based filtering (future-friendly for DaemonSet). Mount namespace filtering can complement cgroup-based scoping when needed.
- Current deployment emphasizes sidecar co-location; cgroup v2 scoping makes it straightforward to evolve to a node-level DaemonSet in the future (not planned for now).

### Scoping Semantics
- Default scope is the BanyanDB process cgroup v2 (discovered from the target process). When cgroup data is unavailable, filtering falls back to pid/comm.
- Map keys are fixed-size enumerations (per-pid hash maps, max 8192 entries) and are cleared in the windowed collection path to avoid unbounded growth.
- Complete process scoping relies on cgroup v2; legacy cgroup layouts still work with pid/comm filtering, but scope becomes per-process rather than per-container.

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

- HTTP (implemented):
  - `GET /metrics` (Prometheus exposition)
  - `GET /health`
  - `GET /api/v1/stats`
- gRPC (implemented): `GetMetrics`, `StreamMetrics`, `GetIOStats`, `GetModuleStatus`, `GetHealth`
- gRPC (planned): `ConfigureModule`
- TLS: planned.


## Exporters

- Prometheus pull model (default): standard text format on `/metrics`.
- BanyanDB push model (optional): batch writes; group/measure naming may evolve with the OA naming; backward compatibility to be maintained during transition.


## Deployment Considerations

- Sidecar-style co-location with BanyanDB is the current focus (PID/network namespace sharing in docker-compose/k8s manifests).
- Scoping is optional and enabled via config/env (docker-compose can pass environment variables to toggle scoping filters).
- Future scale-out path uses cgroup v2 scoping to support a DaemonSet-style node-level deployment (not in current scope).


## Metrics Reference (selected)

Prefix: metrics are currently emitted under the `ebpf_` namespace to reflect their kernel eBPF origin (e.g., `ebpf_cache_misses_total`).

- I/O & Cache
  - `ebpf_cache_read_attempts_total`
  - `ebpf_cache_misses_total`
  - `ebpf_page_cache_adds_total`
- fadvise()
  - `ebpf_fadvise_calls_total`
  - `ebpf_fadvise_advice_total{advice="..."}`
  - `ebpf_fadvise_success_total`
- Memory
  - `ebpf_memory_lru_pages_scanned_total`
  - `ebpf_memory_lru_pages_reclaimed_total`
  - `ebpf_memory_reclaim_efficiency_percent`
  - `ebpf_memory_direct_reclaim_processes`

Semantics: all counters are monotonic; use Prometheus functions for rates/derivatives; no map clearing between scrapes.


## Compatibility and Transition Notes

- Product name in this document: Observability Agent (OA). The current binary, env prefixes, and paths may still use `ebpf-sidecar` naming; a staged migration will provide backward compatibility.
- Metric names starting with `ebpf_` remain stable; any unification under an OA-wide prefix would require an explicit migration plan and is out of scope here.

## Safety & Overhead Boundary

OA is strictly passive: no kernel modifications, no syscall blocking, and only bounded-size monotonic maps. Probes attach to stable tracepoints or fentry/fexit paths with kprobe fallbacks, and expected CPU overhead remains <1% under typical BanyanDB workloads.

## Restart Semantics

On agent restart, BPF maps are recreated and all counters reset to zero. Downstream systems (e.g., Prometheus) should treat this as a new counter lifecycle and continue deriving rates/derivatives normally.

## Kernel Attachment Points (Current)

- `ksys_fadvise64_64` → fentry/fexit (preferred) or syscall tracepoints with kprobe fallback.
- Page cache add/remove → `filemap_get_read_batch` and `mm_filemap_add_to_page_cache` tracepoints, with kprobe fallbacks.
- Memory reclaim → `mm_vmscan_lru_shrink_inactive` and `mm_vmscan_direct_reclaim_begin` tracepoints.

## Limitations

- Page cache–only perspective: direct I/O that bypasses the cache is not observed.
- Kernel-only visibility: no userspace spans, SQL parsing, or CPU profiling.
- Export cadence is windowed; under very high churn, inactive PIDs may be evicted once map bounds are reached.
