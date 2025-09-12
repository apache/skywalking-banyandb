# eBPF Sidecar Design Document

## 1. Executive Summary

The eBPF Sidecar Agent is a kernel-level observability component for Apache SkyWalking BanyanDB that primarily monitors **page cache miss rates** - a critical metric for database performance. High cache miss rates indicate that BanyanDB is frequently reading from disk instead of memory, directly impacting query latency and throughput. By tracking these metrics at the kernel level, we can identify performance bottlenecks and optimize storage patterns.

### Primary Goal
**Monitor and reduce page cache miss rates in BanyanDB** by providing real-time visibility into:
- Page cache hit/miss ratios during database operations
- Correlation between fadvise() hints and actual cache behavior
- Memory pressure impact on cache eviction rates

### Key Features
- **Page cache miss tracking**: Direct monitoring of cache efficiency
- **fadvise() effectiveness**: Measure if BanyanDB's cache hints work as expected
- **Zero-instrumentation monitoring**: No modifications required to BanyanDB
- **Minimal performance overhead**: Kernel-level data collection with < 1% CPU usage
- **Production-ready**: Memory-efficient design with clear-after-read strategy

## 2. Architecture Overview

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Applications                   │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐ │
│  │   BanyanDB   │   │  Prometheus  │   │   Grafana    │ │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘ │
└─────────┼──────────────────┼──────────────────┼─────────┘
          │ Native Export    │ Scrape           │ Query
          ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────────┐
│                  eBPF Sidecar Agent                     │
│  ┌────────────────────────────────────────────────────┐ │
│  │              Server Layer (gRPC/HTTP)              │ │
│  ├────────────────────────────────────────────────────┤ │
│  │           Export Layer (Push/Pull Model)           │ │
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
│  ┌────────────────────────────────────────────────────┐ │
│  │     Kernel Subsystems (VFS, MM, Block I/O)         │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Component Description

#### Server Layer
- **gRPC Server**: Provides programmatic access to metrics and control operations
- **HTTP Server**: Exposes Prometheus metrics endpoint and health checks
- **API Gateway**: Routes requests to appropriate handlers

#### Export Layer
- **Prometheus Exporter**: Formats metrics in Prometheus exposition format for pull-based collection
- **BanyanDB Native Exporter**: Pushes metrics directly to BanyanDB with batching support
- **Export Strategy**: Configurable push/pull model based on deployment requirements

#### Collector
- **Module Manager**: Manages lifecycle of eBPF monitoring modules
- **Metric Aggregator**: Collects and aggregates metrics from kernel BPF maps
- **Memory Management**: Implements clear-after-read strategy to prevent memory overflow

#### eBPF Loader
- **Program Compilation**: Uses CO-RE (Compile Once, Run Everywhere) for kernel portability
- **Verification**: Ensures eBPF programs pass kernel verifier before loading
- **Map Management**: Creates and manages BPF maps for kernel-userspace communication

## 3. eBPF Implementation Core

### 3.1 Page Cache Miss Detection Strategy

**How We Detect Cache Misses:**

The key to monitoring page cache misses is tracking when the kernel needs to fetch data from disk versus serving it from memory. We achieve this through:

1. **Read Operation Tracking**: 
   - Monitor all read syscalls to know when BanyanDB requests data
   - Track the file descriptor, offset, and size of each read

2. **Page Cache Event Correlation**:
   - `mm_filemap_add_to_page_cache`: Fired when a page is loaded from disk into cache (MISS)
   - `mm_filemap_delete_from_page_cache`: Fired when a page is evicted from cache
   - If a read triggers `add_to_page_cache`, it's a cache miss
   - If a read completes without triggering it, it's a cache hit

3. **Cache Miss Rate Calculation**:
   - Cache Miss Rate = (Pages Added to Cache / Total Read Operations) × 100
   - Track per-process to identify which BanyanDB components cause most misses

4. **fadvise() Correlation**:
   - Track `POSIX_FADV_DONTNEED`: Should increase evictions
   - Track `POSIX_FADV_WILLNEED`: Should pre-load pages to reduce future misses
   - Measure effectiveness: Do fadvise hints actually reduce miss rates?

### 3.2 Kernel Attachment Points

#### 3.2.1 Tracepoints (Stable Kernel ABI)

**Critical for Page Cache Monitoring:**
- `tracepoint/filemap/mm_filemap_add_to_page_cache` - **PRIMARY: Detects cache misses**
- `tracepoint/filemap/mm_filemap_delete_from_page_cache` - **PRIMARY: Tracks evictions**

**File I/O System Calls:**
- `tracepoint/syscalls/sys_enter_fadvise64` - Capture file advice hints
- `tracepoint/syscalls/sys_exit_fadvise64` - Track fadvise effectiveness
- `tracepoint/syscalls/sys_enter_read` - Monitor read operations
- `tracepoint/syscalls/sys_exit_read` - Correlate with cache events
- `tracepoint/syscalls/sys_enter_write` - Track write operations
- `tracepoint/syscalls/sys_exit_write` - Monitor write completion

**Memory Management:**
- `tracepoint/vmscan/mm_vmscan_lru_shrink_inactive` - LRU list scanning
- `tracepoint/vmscan/mm_vmscan_direct_reclaim_begin` - Direct reclaim start
- `tracepoint/vmscan/mm_vmscan_direct_reclaim_end` - Direct reclaim completion

#### 3.2.2 Kprobes (Dynamic Function Hooking)

Kprobes allow attaching to kernel functions when tracepoints are unavailable:

**VFS Operations:**
- `kprobe/vfs_read` and `kretprobe/vfs_read` - Virtual filesystem reads
- `kprobe/vfs_write` and `kretprobe/vfs_write` - Virtual filesystem writes

**Block I/O Layer:**
- `kprobe/submit_bio` and `kretprobe/submit_bio` - Block I/O submission

#### 3.2.3 Fentry/Fexit (Modern Fast Attachments)

For kernels 5.5+, fentry/fexit provide better performance than kprobes:

- `fentry/vfs_read` and `fexit/vfs_read` - Optimized VFS read monitoring
- `fentry/do_sys_openat2` and `fexit/do_sys_openat2` - File open operations

### 3.3 BPF CO-RE (Compile Once, Run Everywhere)

CO-RE technology enables portable eBPF programs across different kernel versions without recompilation:

**Key Benefits:**
- **Portability**: Single binary works across kernel versions 4.14 to 6.x
- **Safety**: BPF_CORE_READ macros handle struct layout changes
- **Performance**: No runtime overhead compared to direct access
- **BTF Integration**: Leverages kernel BTF (BPF Type Format) information

**Implementation Approach:**
- Use vmlinux.h generated from kernel BTF
- Access kernel structures through BPF_CORE_READ helpers
- Handle field relocations automatically
- Graceful degradation for missing fields

### 3.4 BPF Maps Architecture

The sidecar uses various BPF map types optimized for different use cases:

**Map Types and Usage:**
- **Hash Maps**: Per-process and per-thread tracking (8192 max entries)
- **Array Maps**: Global statistics storage (single entry)
- **Per-CPU Arrays**: High-frequency event counters without lock contention
- **Ring Buffers**: Event streaming (future enhancement)

**Tracking Strategy:**
- **TID-based tracking**: Thread ID used for syscall entry/exit correlation to avoid race conditions
- **PID aggregation**: Process-level statistics aggregated from thread-level data
- **Atomic operations**: Ensures data consistency in multi-threaded environments

**Data Flow:**
1. Kernel events trigger eBPF programs
2. Programs update counters in BPF maps
3. Userspace collector reads maps periodically
4. Clear-after-read prevents overflow
5. Metrics exported to monitoring systems

## 4. Module Structure and Implementation

### 4.1 Directory Structure

```
ebpf-sidecar/
├── cmd/
│   └── sidecar/
│       └── main.go                 # Application entry point, CLI flags, bootstrap
│
├── internal/                       # Private packages (not importable by external projects)
│   ├── config/
│   │   └── config.go               # Configuration parsing (YAML/env vars)
│   │
│   ├── server/
│   │   ├── grpc.go                # gRPC server implementation
│   │   ├── http.go                # HTTP server for Prometheus metrics
│   │   └── server.go              # Server lifecycle management
│   │
│   ├── collector/
│   │   ├── collector.go           # Base collector interface and manager
│   │   ├── iomonitor_module.go    # Main module for page cache monitoring
│   │   └── modules/               # Future: additional monitoring modules
│   │
│   ├── ebpf/
│   │   ├── programs/              # eBPF C source files
│   │   │   ├── iomonitor.c        # Page cache, fadvise, I/O monitoring
│   │   │   └── headers/           # Shared C headers
│   │   ├── generated/             # Auto-generated Go bindings from bpf2go
│   │   │   ├── iomonitor_x86_bpfel.go
│   │   │   └── iomonitor_arm64_bpfel.go
│   │   ├── loader.go              # eBPF program loading and verification
│   │   ├── loader_enhanced.go     # CO-RE support and BTF handling
│   │   └── kernel_compat.go       # Kernel version compatibility checks
│   │
│   ├── metrics/
│   │   ├── metrics.go             # Metric type definitions
│   │   └── store.go               # In-memory metric storage
│   │
│   └── export/
│       ├── exporter.go            # Common exporter interface
│       ├── prometheus.go          # Prometheus exposition format
│       └── banyandb.go            # Native BanyanDB client export
│
├── api/                           # Public API definitions
│   └── proto/
│       └── banyandb/
│           └── ebpf/
│               └── v1/
│                   ├── metrics.proto    # Metric message definitions
│                   └── rpc.proto        # gRPC service definitions
│
├── configs/                       # Configuration examples
│   ├── config.yaml               # Default configuration
│   └── config.banyandb.yaml      # BanyanDB export example
│
├── deployments/                   # Deployment manifests
│   ├── docker/
│   │   ├── Dockerfile            # Ubuntu-based image
│   │   └── Dockerfile.alpine     # Alpine-based image
│   ├── kubernetes/
│   │   ├── daemonset.yaml        # Node-level deployment
│   │   └── sidecar.yaml          # Sidecar container spec
│   └── docker-compose.yaml       # Local development stack
│
├── test/
│   └── integration/
│       └── ebpf_sidecar/
│           ├── iomonitor_test.go # Integration tests
│           └── testdata/         # Test fixtures
│
├── Makefile                      # Build automation
├── go.mod                        # Go dependencies
└── README.md                     # Module documentation
```

### 4.2 Component Responsibilities

#### 4.2.1 Core Components

**cmd/sidecar/main.go**
- Parse command-line flags and environment variables
- Initialize configuration
- Bootstrap all components
- Handle graceful shutdown

**internal/config/**
- Load configuration from YAML files
- Override with environment variables
- Validate configuration
- Provide config hot-reload (future)

**internal/server/**
- Manage gRPC and HTTP servers lifecycle
- Handle concurrent server operations
- Implement health checks
- Route requests to appropriate handlers

#### 4.2.2 eBPF Components

**internal/ebpf/programs/iomonitor.c**
- Attach to kernel tracepoints
- Track page cache add/delete events
- Monitor fadvise() system calls
- Store metrics in BPF maps

**internal/ebpf/loader.go**
- Load compiled eBPF bytecode
- Verify programs with kernel verifier
- Attach programs to kernel events
- Manage BPF map file descriptors

**internal/ebpf/loader_enhanced.go**
- Implement CO-RE support
- Handle BTF information
- Provide kernel compatibility layer
- Fallback mechanisms for older kernels

#### 4.2.3 Collection Components

**internal/collector/collector.go**
- Define collector interface
- Manage module lifecycle
- Coordinate metric collection intervals
- Handle errors and retries

**internal/collector/iomonitor_module.go**
- Read metrics from BPF maps
- Calculate cache hit/miss rates
- Aggregate per-process statistics
- Implement clear-after-read pattern

#### 4.2.4 Export Components

**internal/export/prometheus.go**
- Format metrics in Prometheus text format
- Implement HTTP handler for `/metrics`
- Maintain metric registry
- Handle metric families and labels

**internal/export/banyandb.go**
- Use BanyanDB Go client
- Batch metrics for efficient writes
- Handle connection management
- Implement retry logic

### 4.3 Data Flow Through Components

```
1. Kernel Event (e.g., page cache miss)
        ↓
2. eBPF Program (iomonitor.c)
        ↓
3. BPF Maps (kernel-userspace communication)
        ↓
4. Collector Module (iomonitor_module.go)
        ↓
5. Metrics Store (in-memory aggregation)
        ↓
6. Exporter (prometheus.go or banyandb.go)
        ↓
7. External System (Prometheus/BanyanDB)
```

### 4.4 Build Process

**Makefile Targets:**
- `make generate` - Generate Go bindings from eBPF C code using bpf2go
- `make build` - Compile eBPF programs and Go binary
- `make test` - Run unit tests
- `make integration-test` - Run integration tests (requires root)
- `make docker` - Build Docker images
- `make clean` - Clean build artifacts

**eBPF Compilation:**
```bash
# Generate vmlinux.h from kernel BTF
bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h

# Compile eBPF program with clang
clang -O2 -target bpf -c iomonitor.c -o iomonitor.o

# Generate Go bindings
bpf2go iomonitor iomonitor.c
```

## 5. Metrics Collection

### 5.1 Core Metrics

**Primary Page Cache Metrics (Most Important):**
- `ebpf_cache_miss_rate_percent` - **KEY METRIC**: Percentage of reads that miss cache
- `ebpf_cache_hit_rate_percent` - **KEY METRIC**: Percentage of reads served from memory
- `ebpf_pages_added_to_cache_total` - Total pages loaded from disk (indicates misses)
- `ebpf_pages_evicted_from_cache_total` - Total pages removed from cache
- `ebpf_cache_efficiency_ratio` - Ratio of hits to total I/O operations

**fadvise() Effectiveness Metrics:**
- `ebpf_fadvise_calls_total{advice="dontneed"}` - Tracks cache purge hints
- `ebpf_fadvise_calls_total{advice="willneed"}` - Tracks pre-load hints  
- `ebpf_fadvise_success_rate_percent` - Success rate of fadvise calls
- `ebpf_cache_miss_rate_after_willneed` - Miss rate after WILLNEED hints (should decrease)
- `ebpf_cache_eviction_rate_after_dontneed` - Eviction rate after DONTNEED hints (should increase)

**Supporting I/O Metrics:**
- `ebpf_io_read_operations_total` - Total read operations
- `ebpf_io_read_bytes_total` - Total bytes read
- `ebpf_io_latency_microseconds` - Read operation latency

**Memory Pressure Indicators:**
- `ebpf_memory_direct_reclaim_processes` - Processes in memory reclaim
- `ebpf_memory_reclaim_efficiency_percent` - Pages reclaimed vs scanned
- `ebpf_lru_pages_scanned_total` - LRU scanning activity

### 5.2 Collection Strategy

**Interval-based Collection:**
- Default 10-second collection interval
- Configurable based on workload
- Batch processing for efficiency

**Memory Management:**
- Clear-after-read pattern prevents map overflow
- Automatic cleanup of stale entries
- Bounded map sizes for predictable memory usage

## 6. API Design

### 6.1 gRPC API

The gRPC API provides programmatic access to eBPF metrics and control operations.

**Service Definition:**

**EBPFMetricsService:**
- Primary service for metric retrieval and monitoring
- Supports both request-response and streaming patterns
- Protocol Buffer v3 for message definitions

**Core Methods:**

1. **GetMetrics**
   - Purpose: Retrieve current metrics snapshot
   - Request: Module filter, metric type filter
   - Response: MetricSet with all matching metrics
   - Use case: Periodic polling, dashboard updates

2. **StreamMetrics**
   - Purpose: Real-time metric streaming
   - Request: Subscription parameters, update interval
   - Response: Stream of MetricSet updates
   - Use case: Live monitoring, alerting systems

3. **GetIOStats**
   - Purpose: Detailed I/O statistics
   - Request: Time range, aggregation level
   - Response: IOStats with fadvise, cache, memory stats
   - Use case: Performance analysis, troubleshooting

4. **GetModuleStatus**
   - Purpose: Check eBPF module health
   - Request: Module name (optional)
   - Response: Module status, error messages, metric counts
   - Use case: Health monitoring, diagnostics

5. **ConfigureModule**
   - Purpose: Runtime configuration changes
   - Request: Module name, configuration parameters
   - Response: Success/failure status
   - Use case: Dynamic tuning, enable/disable modules

**Message Types:**

- **Metric**: Individual metric point with name, value, type, labels, timestamp
- **MetricSet**: Collection of metrics from a module with metadata
- **IOStats**: Comprehensive I/O statistics structure
- **ModuleStatus**: Module health and operational status
- **MetricType**: Enum for counter, gauge, histogram

### 6.2 HTTP REST API

The HTTP API provides REST endpoints for web clients and monitoring systems.

**Endpoints:**

| Method | Path | Description | Response Format |
|--------|------|-------------|-----------------|
| GET | `/metrics` | Prometheus metrics | Prometheus text format |
| GET | `/health` | Health check | JSON status |
| GET | `/api/v1/stats` | Current statistics | JSON metrics |
| GET | `/api/v1/modules` | List active modules | JSON module list |
| GET | `/api/v1/modules/{name}` | Module details | JSON module info |
| POST | `/api/v1/modules/{name}/config` | Configure module | JSON result |
| GET | `/api/v1/io/stats` | I/O statistics | JSON IO stats |
| GET | `/api/v1/cache/stats` | Cache statistics | JSON cache stats |

**Response Formats:**

- Prometheus metrics: Standard text exposition format
- JSON responses: Consistent structure with status, data, error fields
- Error handling: HTTP status codes with detailed error messages

### 6.3 Integration Interfaces

**Prometheus Integration:**
- Standard `/metrics` endpoint
- Metric naming convention: `ebpf_<subsystem>_<metric>_<unit>`
- Label consistency across metrics
- Support for metric families

**BanyanDB Native Integration:**
- Direct client SDK usage
- Batch write API for efficiency
- Schema mapping from metrics to measures
- Time-series indexing optimization

## 7. Integration Points

### 7.1 Prometheus Export

**Metrics Format:**
- Standard Prometheus exposition format
- Proper metric types (counter, gauge, histogram)
- Meaningful labels for filtering
- Metric naming follows Prometheus best practices

**Scraping Configuration:**
- HTTP endpoint at `/metrics`
- Configurable port (default 8080)
- Support for service discovery
- TLS support for secure scraping

### 7.2 BanyanDB Native Export

**Direct Push Model:**
- Native BanyanDB client usage
- Batch metric submission
- Configurable flush intervals
- Automatic retry with exponential backoff

**Schema Design:**
- Dedicated measure group for eBPF metrics
- Efficient indexing for time-series queries
- Retention policies for metric data
- Tag-based filtering support

## 8. Deployment Considerations

### 8.1 Requirements

**Kernel Requirements:**
- Minimum: Linux 4.14 with CONFIG_BPF enabled
- Recommended: Linux 5.5+ for fentry/fexit support
- BTF availability for CO-RE functionality

**Security Requirements:**
- CAP_BPF and CAP_PERFMON (kernel 5.8+)
- CAP_SYS_ADMIN for older kernels
- Privileged container in Kubernetes

### 8.2 Deployment Models

**Standalone Deployment:**
- Single binary deployment
- Configuration via YAML or environment variables
- Suitable for VM or bare-metal installations

**Kubernetes Sidecar:**
- Co-located with BanyanDB pods
- Shared process namespace for monitoring
- Resource limits and requests defined

**DaemonSet Deployment:**
- Node-level monitoring
- Aggregated metrics across all pods
- Centralized collection point

## 9. Performance Considerations

### 9.1 Overhead Targets
We aim for **minimal overhead**, ensuring the sidecar remains lightweight and production-safe:
- CPU: sub-percent usage under normal load
- Memory: tens of MB resident footprint
- Latency: microsecond-level per event
- Network: negligible metric export cost

(Exact thresholds will be validated during benchmarking; numbers above are indicative, not final.)

### 9.2 Optimization Strategies
- Use per-CPU data structures to reduce contention
- Apply early filtering in kernel space
- Batch process events where possible
- Support adaptive collection intervals for efficiency

## 10. Testing Strategy

### 10.1 Testing Levels
- **Unit Tests**: Individual component testing
- **Integration Tests**: eBPF program loading and metric collection
- **Performance Tests**: Overhead measurement under various workloads
- **Compatibility Tests**: Cross-kernel version validation

### 10.2 Test Environment
- Docker containers with different kernel versions
- Workload generators for I/O patterns
- Automated test pipeline in CI/CD

## 11. Future Enhancements

Potential areas for expansion:
- Custom eBPF program loading interface
- Integration with distributed tracing systems

---

**Document Version**: 1.0  
**Status**: Draft for Review  
**Date**: 2025-09-08