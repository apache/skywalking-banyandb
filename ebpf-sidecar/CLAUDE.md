# eBPF Sidecar Agent - Development Guide

This document records the development process of the eBPF sidecar agent for SkyWalking BanyanDB and provides guidance for implementing new eBPF monitoring features.

## Project Overview

The eBPF sidecar agent provides kernel-level observability for BanyanDB operations through a standalone service that runs alongside BanyanDB instances. It offers deep insights into system calls, memory management, and I/O patterns with minimal overhead.

### Architecture Decision

**Previous Approach**: `pkg/fs/fadvisemonitor` - Embedded monitoring with limited scope
**New Approach**: Standalone sidecar service with comprehensive monitoring capabilities

### Key Design Principles

1. **Standalone Service**: Independent process with its own lifecycle
2. **API-First**: gRPC and HTTP endpoints for metrics and health checks
3. **Container-Ready**: Automatic dependency management for deployment
4. **Extensible**: Plugin architecture for new monitoring features
5. **Cross-Platform**: Support for x86_64 and ARM64 architectures

## Development Process Chronicle

### Phase 1: Architecture Design (Initial Setup)

**Problem**: Need to move from embedded fadvisemonitor to modern sidecar architecture

**Solution**: Created complete standalone service structure
```bash
/ebpf-sidecar/
├── cmd/sidecar/main.go          # CLI entry point
├── internal/
│   ├── config/                  # Configuration management
│   ├── server/                  # API servers
│   ├── collector/               # Metrics collection
│   └── ebpf/                    # eBPF programs and bindings
└── pkg/                         # Reusable packages
```

### Phase 2: eBPF Program Migration

**Challenge**: Migrate existing fadvise monitoring to new structure with better naming

**Actions Taken**:
1. **Renamed**: `fadvise.c` → `iomonitor.c` for broader scope
2. **Enhanced**: Added comprehensive system monitoring beyond just fadvise
3. **Structured**: Organized C programs in `programs/` directory

**Key eBPF Programs Implemented**:
```c
// System call monitoring
SEC("tracepoint/syscalls/sys_enter_fadvise64")
int trace_enter_fadvise64(struct trace_event_raw_sys_enter *ctx)

// Memory management tracking  
SEC("tracepoint/vmscan/mm_vmscan_lru_shrink_inactive")
int trace_lru_shrink_inactive(struct trace_event_raw_mm_vmscan_lru_shrink_inactive *ctx)

// Cache analysis
SEC("tracepoint/filemap/mm_filemap_add_to_page_cache") 
int trace_mm_filemap_add_to_page_cache(struct trace_event_raw_mm_filemap_add_to_page_cache *ctx)
```

### Phase 3: Build System Challenges

**Issue 1**: Hidden generation process using `go:generate`
**User Feedback**: "i prefer to put the generate cmd which in the loader.go into the makefile, let it be more clear and automatically"

**Solution**: Moved all generation to explicit Makefile targets
```makefile
.PHONY: ebpf-bindings
ebpf-bindings:
	@cd internal/ebpf/generated && \
		go run github.com/cilium/ebpf/cmd/bpf2go \
			-cc clang \
			-cflags "-O2 -Wall -Werror" \
			-target amd64 \
			-go-package generated \
			Iomonitor ../programs/iomonitor.c -- -I.
```

**Issue 2**: File organization chaos
**User Feedback**: "can we let them be in the generated folder and make them more tidy"

**Solution**: Separated generated files from source code
```
internal/ebpf/
├── programs/           # Source eBPF C programs
│   └── iomonitor.c
├── generated/          # Auto-generated Go bindings and objects
│   ├── iomonitor_x86_bpfel.go
│   ├── iomonitor_arm64_bpfel.go
│   └── vmlinux.h
└── loader.go          # Go integration code
```

### Phase 4: Module Dependencies Resolution

**Issue**: Separate go.mod causing dependency conflicts
**User Feedback**: "why the ebpf module need a extra go mod, can we just place it in the root folder?"

**Solution**: Integrated with root module dependency management
- Removed separate `ebpf-sidecar/go.mod`
- Used root module's existing dependencies
- Added new dependencies to root `go.mod` as needed

### Phase 5: Container Deployment Challenge

**Critical Issue**: Missing bpftool in container environments
**User Feedback**: "as our program will be run on pod, plz add downloading some required tools when didn't find bpftools in the makefile, if we lack this, the vmlinux cannot be generated"

**Solution**: Comprehensive automatic dependency installation
```makefile
.PHONY: install-bpftool-auto
install-bpftool-auto:
	@echo "Detecting Linux distribution..."
	@if [ -f /etc/debian_version ]; then \
		echo "Debian/Ubuntu detected, installing bpftools..."; \
		apt-get update && apt-get install -y linux-tools-common bpftools; \
	elif [ -f /etc/redhat-release ]; then \
		echo "RedHat/CentOS/Fedora detected, installing bpftool..."; \
		dnf install -y bpftool kernel-devel; \
	elif [ -f /etc/alpine-release ]; then \
		echo "Alpine Linux detected, installing bpftool..."; \
		apk add --no-cache bpftool linux-headers; \
	else \
		echo "Unknown distribution, compiling from source..."; \
		$(MAKE) install-bpftool-source; \
	fi
```

## Implementation Guide for New eBPF Features

### Step 1: Plan Your Monitoring Target

Before implementing, identify:
- **What to monitor**: System call, kernel function, or tracepoint
- **Data to collect**: What metrics are important
- **Update frequency**: How often data should be collected
- **Performance impact**: Keep eBPF programs efficient

### Step 2: Write eBPF C Program

Add your program to `internal/ebpf/programs/iomonitor.c`:

```c
// Define data structures
struct your_stats_t {
    __u64 counter;
    __u32 pid;
    // ... other fields
};

// Define BPF map
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, __u32);
    __type(value, struct your_stats_t);
} your_stats_map SEC(".maps");

// Implement probe function
SEC("tracepoint/category/your_event")
int trace_your_event(struct trace_event_raw_your_event *ctx) {
    // Your monitoring logic here
    return 0;
}
```

### Step 3: Update Makefile Generation

Add your new struct types to the bpf2go generation command:

```makefile
ebpf-bindings:
	@cd internal/ebpf/generated && \
		go run github.com/cilium/ebpf/cmd/bpf2go \
			# ... existing flags
			-type your_stats_t \
			Iomonitor ../programs/iomonitor.c -- -I.
```

### Step 4: Implement Go Integration

Update `internal/ebpf/loader.go`:

```go
func (l *Loader) attachYourProbe() error {
    tp, err := link.Tracepoint("category", "your_event", l.objects.TraceYourEvent, nil)
    if err != nil {
        return fmt.Errorf("attaching your_event tracepoint: %w", err)
    }
    l.links = append(l.links, tp)
    return nil
}

func (l *Loader) GetYourStats() ([]YourStats, error) {
    // Iterate over your_stats_map and collect data
}
```

### Step 5: Add to Collection System

Update `internal/collector/collector.go`:

```go
func (c *Collector) collectYourStats() error {
    stats, err := c.loader.GetYourStats()
    if err != nil {
        return err
    }
    // Process and store stats
    return nil
}
```

### Step 6: Expose via API

Add endpoints in `internal/server/` for accessing your new metrics.

### Step 7: Testing

```bash
# Generate new bindings
make generate

# Test compilation
make build

# Test with privileges (for eBPF)
sudo make test-ebpf
```

## Development Lessons Learned

### 1. User-Driven Design
- **Listen to feedback**: User preferences shaped major architectural decisions
- **Iterate quickly**: Rapid prototyping helped identify issues early
- **Transparency matters**: Explicit build processes over hidden automation

### 2. Container-First Thinking
- **Assume minimal environments**: Containers may lack development tools
- **Automate dependency management**: Don't rely on pre-installed tools
- **Multi-distribution support**: Different base images use different package managers

### 3. eBPF Best Practices
- **Keep programs simple**: Complex logic should be in userspace
- **Efficient data structures**: Use appropriate map types for your use case
- **Cross-architecture support**: Test on both x86_64 and ARM64
- **Proper cleanup**: Always implement cleanup functions for resources

### 4. Build System Design
- **Explicit over implicit**: Make generation steps visible in Makefile
- **Dependency management**: Handle missing tools gracefully
- **Cross-platform builds**: Support multiple architectures from the start

## Directory Structure Rationale

```
ebpf-sidecar/
├── Makefile                     # Build automation with dependency management
├── cmd/sidecar/main.go         # CLI entry point with Cobra framework
├── internal/                   # Private implementation packages
│   ├── config/                 # Configuration management
│   │   └── config.go          # Environment-based config with validation
│   ├── server/                 # API servers
│   │   ├── grpc.go            # gRPC server for programmatic access
│   │   └── http.go            # HTTP server for metrics and health
│   ├── collector/              # Metrics collection orchestration
│   │   ├── collector.go       # Main collection logic
│   │   └── scheduler.go       # Periodic collection scheduling
│   └── ebpf/                   # eBPF programs and Go integration
│       ├── programs/           # eBPF C source code
│       │   └── iomonitor.c    # Comprehensive system monitoring
│       ├── generated/          # Auto-generated files (git-ignored)
│       │   ├── iomonitor_x86_bpfel.go   # x86_64 Go bindings
│       │   ├── iomonitor_arm64_bpfel.go # ARM64 Go bindings
│       │   ├── *.o            # Compiled eBPF objects
│       │   └── vmlinux.h      # Kernel type definitions
│       └── loader.go          # eBPF program lifecycle management
└── pkg/                       # Reusable packages
    ├── metrics/               # Metrics type definitions
    └── export/                # Export format implementations
        ├── prometheus.go      # Prometheus format export
        └── banyandb.go       # Native BanyanDB export
```

## Future Implementation Guidelines

### For New eBPF Features:
1. **Start with C program**: Implement in `programs/iomonitor.c`
2. **Add to generation**: Update Makefile with new types
3. **Implement Go bindings**: Add collection logic in `loader.go`
4. **Integrate collection**: Update `collector.go`
5. **Expose via API**: Add endpoints in `server/`
6. **Test thoroughly**: Both unit tests and integration tests

### For Infrastructure Changes:
1. **Update Makefile**: Ensure dependency management works
2. **Test in containers**: Verify automatic installation works
3. **Check cross-platform**: Test on different architectures
4. **Update documentation**: Keep this guide current

## Testing Strategy

### Local Development:
```bash
# Install dependencies
make install-deps

# Generate eBPF bindings
make generate

# Build binary
make build

# Run tests (requires root for eBPF)
sudo make test-ebpf
```

### Container Testing:
```bash
# Build container image
make docker

# Test dependency installation
docker run --privileged -it skywalking-banyandb/ebpf-sidecar:latest make install-deps
```

### Integration Testing:
```bash
# Run with BanyanDB
./build/bin/ebpf-sidecar --config-file=configs/config.yaml
```

## Troubleshooting Common Issues

### Build Failures:
1. **Missing bpftool**: Run `make install-deps`
2. **Kernel headers missing**: Ensure linux-headers package installed
3. **Cross-compilation errors**: Check target architecture in Makefile

### Runtime Issues:
1. **Permission denied**: eBPF requires CAP_BPF/CAP_PERFMON capabilities
2. **Verifier rejection**: Simplify eBPF program logic
3. **Map lookup failures**: Check map initialization and key types

### Container Deployment:
1. **Privileged mode required**: eBPF needs privileged containers
2. **Host filesystem access**: Mount `/sys` and `/proc` if needed
3. **Kernel version compatibility**: Ensure kernel supports required features

## Performance Considerations

### eBPF Program Optimization:
- Keep instruction count low (< 1M instructions)
- Use efficient map types (per-CPU maps for high frequency)
- Minimize stack usage
- Avoid complex loops

### Go Integration:
- Batch map operations to reduce syscall overhead
- Use appropriate buffer sizes for data collection
- Implement proper rate limiting for high-frequency events

### System Impact:
- Monitor overhead using `perf` tools
- Implement circuit breakers for error conditions
- Provide configurable collection intervals

---

This document serves as both a record of our development journey and a guide for future enhancements. The architecture is designed to be extensible while maintaining performance and reliability standards suitable for production deployment.