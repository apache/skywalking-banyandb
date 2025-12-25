# eBPF Attachment Strategy and CO-RE Implementation

## Overview

This document outlines the attachment strategy for eBPF programs in the sidecar agent and explains the use of CO-RE (Compile Once, Run Everywhere) for maximum portability.

## Attachment Strategy: fentry → tracepoint → kprobe

### 1. fentry/fexit (Optimal - Kernel 5.5+)

**When to use**: Modern kernels with BTF support

**Advantages**:
- ✅ **Lowest overhead** (~40% faster than kprobes)
- ✅ **Direct argument access** - Function parameters available by name
- ✅ **Type safety** - BTF provides complete type information
- ✅ **No register manipulation** - Clean, readable code
- ✅ **BPF trampoline** - Direct call, no int3 breakpoint

**Limitations**:
- ❌ Requires kernel 5.5+ with BTF enabled
- ❌ Function must be in BTF (not all kernel functions are)
- ❌ Requires CONFIG_DEBUG_INFO_BTF=y

**Example**:
```c
SEC("fentry/ksys_fadvise64_64")
int BPF_PROG(fentry_ksys_fadvise64_64, int fd, loff_t offset, loff_t len, int advice) {
    // Direct access to arguments - clean and efficient
    if (advice == POSIX_FADV_DONTNEED) {
        // Handle DONTNEED advice
    }
    return 0;
}
```

### 2. Tracepoints (Stable - Kernel 4.7+)

**When to use**: When stable ABI is more important than minimal overhead

**Advantages**:
- ✅ **Stable ABI** - Won't break across kernel versions
- ✅ **Well-documented** - Clear event structures
- ✅ **Good performance** - Optimized static hooks
- ✅ **Widely available** - Most kernels have tracepoints enabled

**Limitations**:
- ❌ Limited to predefined events
- ❌ Can't trace arbitrary functions
- ❌ Slightly higher overhead than fentry

**Example**:
```c
SEC("tracepoint/syscalls/sys_enter_fadvise64")
int trace_enter_fadvise64(struct trace_event_raw_sys_enter *ctx) {
    // Use CO-RE for portable struct access
    int fd = BPF_CORE_READ(ctx, args[0]);
    int advice = BPF_CORE_READ(ctx, args[3]);
    return 0;
}
```

### 3. Kprobes (Universal Fallback)

**When to use**: As last resort for maximum compatibility

**Advantages**:
- ✅ **Universal support** - Works on any kernel function
- ✅ **Maximum compatibility** - Available since kernel 2.6.9
- ✅ **Dynamic** - Can attach to any exported symbol

**Limitations**:
- ❌ **Highest overhead** - Int3 breakpoint mechanism
- ❌ **Architecture-specific** - Register access varies
- ❌ **Symbol names vary** - Different across kernel versions
- ❌ **No type information** - Must cast and extract manually

**Example**:
```c
SEC("kprobe/ksys_fadvise64_64")
int kprobe_ksys_fadvise64_64(struct pt_regs *ctx) {
    // Architecture-specific register access
    int fd = (int)PT_REGS_PARM1(ctx);
    int advice = (int)PT_REGS_PARM4(ctx);
    return 0;
}
```

## CO-RE (Compile Once, Run Everywhere)

### Why CO-RE is Essential

Kernel structures can change between versions. Without CO-RE, your eBPF program might:
- Access wrong field offsets
- Read garbage data
- Cause crashes

### CO-RE Solutions

#### 1. BPF_CORE_READ Macro

**Purpose**: Safe field access with BTF relocation

```c
// Without CO-RE - breaks if struct layout changes
struct task_struct *task = (struct task_struct *)bpf_get_current_task();
pid_t pid = task->pid;  // DANGEROUS - fixed offset

// With CO-RE - adapts to struct changes
pid_t pid = BPF_CORE_READ(task, pid);  // SAFE - BTF relocates
```

#### 2. BPF_CORE_READ_INTO

**Purpose**: Read into a variable efficiently

```c
struct fadvise_args_t args = {};
BPF_CORE_READ_INTO(&args.fd, ctx, args[0]);
BPF_CORE_READ_INTO(&args.advice, ctx, args[3]);
```

#### 3. bpf_core_field_exists

**Purpose**: Check if field exists in this kernel version

```c
if (bpf_core_field_exists(task->thread_info)) {
    // Access thread_info (removed in newer kernels)
}
```

#### 4. bpf_core_type_exists

**Purpose**: Check if type exists

```c
if (bpf_core_type_exists(struct folio)) {
    // Use new folio API (kernel 5.16+)
} else {
    // Fall back to page API
}
```

### CO-RE Best Practices

1. **Always use CO-RE for kernel structures**:
```c
// Good
long ret = BPF_CORE_READ(ctx, ret);

// Bad
long ret = ctx->ret;  // Breaks on struct changes
```

2. **Use feature detection**:
```c
if (bpf_core_enum_value_exists(enum bpf_func_id, BPF_FUNC_get_attach_cookie)) {
    // Use new helper function
}
```

3. **Handle missing fields gracefully**:
```c
struct task_struct *task = (struct task_struct *)bpf_get_current_task();
pid_t tgid = 0;

// Different kernels have different field names
if (bpf_core_field_exists(task->tgid)) {
    tgid = BPF_CORE_READ(task, tgid);
} else if (bpf_core_field_exists(task->group_leader)) {
    struct task_struct *leader = BPF_CORE_READ(task, group_leader);
    tgid = BPF_CORE_READ(leader, pid);
}
```

## Implementation in Our Sidecar

### Automatic Fallback Chain

The `EnhancedLoader` implements intelligent fallback:

```go
func (l *EnhancedLoader) attachFadviseWithFallback() error {
    // 1. Try fentry/fexit first
    if l.features.HasFentry {
        if err := attachFentry(); err == nil {
            return nil // Success - using optimal path
        }
    }
    
    // 2. Fall back to tracepoints
    if err := attachTracepoint(); err == nil {
        return nil // Success - using stable path
    }
    
    // 3. Final fallback to kprobes with symbol search
    return attachKprobeWithSymbolSearch()
}
```

### Symbol Resolution for Kprobes

Different kernel versions use different function names:

```go
func GetFadviseFunctionNames(version KernelVersion) []string {
    if version.Major > 5 || (version.Major == 5 && version.Minor >= 11) {
        return []string{
            "ksys_fadvise64_64",     // Modern internal function
            "__x64_sys_fadvise64",   // x64 syscall handler
            "__do_sys_fadvise64",    // Some distributions
        }
    }
    // Older kernels
    return []string{
        "sys_fadvise64",
        "SyS_fadvise64",  // Old naming convention
    }
}
```

## Performance Comparison

| Method | Overhead | Kernel Version | Use Case |
|--------|----------|----------------|----------|
| fentry/fexit | ~20ns | 5.5+ | Production (modern) |
| Tracepoint | ~35ns | 4.7+ | Production (stable) |
| Kprobe | ~50ns | 2.6.9+ | Compatibility fallback |

## Decision Matrix

```
if (kernel >= 5.5 && BTF available && function in BTF) {
    use fentry/fexit;  // Best performance
} else if (tracepoint exists for function) {
    use tracepoint;    // Good performance, stable
} else {
    use kprobe;        // Maximum compatibility
}
```

## Testing the Strategy

### Check Current Attachment Mode

```bash
# Run the sidecar and check logs
sudo ./ebpf-sidecar 2>&1 | grep "Attached"

# Example output:
# INFO  Attached fadvise monitoring using fentry/fexit
# INFO  Attached cache monitoring using tracepoint
# INFO  Attached memory monitoring using kprobe/shrink_lruvec
```

### Force Specific Mode (for testing)

```go
// In loader configuration
type LoaderConfig struct {
    ForceKprobe     bool  // Skip fentry/tracepoint
    ForceTracepoint bool  // Skip fentry
    // Default: try all in order
}
```

## Troubleshooting

### fentry Not Working?

1. Check BTF support:
```bash
ls /sys/kernel/btf/vmlinux  # Should exist
bpftool btf dump file /sys/kernel/btf/vmlinux | grep ksys_fadvise64_64
```

2. Check kernel config:
```bash
grep CONFIG_DEBUG_INFO_BTF /boot/config-$(uname -r)
```

### Tracepoint Not Available?

```bash
# List available tracepoints
sudo cat /sys/kernel/debug/tracing/available_events | grep fadvise
```

### Kprobe Symbol Not Found?

```bash
# Check available symbols
sudo cat /proc/kallsyms | grep fadvise
```

## Future Improvements

1. **Add fprobe support** (kernel 5.18+) - Faster than kprobe, doesn't require BTF
2. **Implement multi-attach** - Attach multiple functions with one syscall
3. **Add uprobe support** - Monitor userspace functions
4. **Runtime switching** - Allow changing attachment mode without restart

## Conclusion

Our three-tier fallback strategy ensures:
- **Optimal performance** when possible (fentry)
- **Stability** when needed (tracepoint)  
- **Universal compatibility** as fallback (kprobe)

Combined with CO-RE, this provides a robust, portable eBPF monitoring solution that works across a wide range of kernel versions while automatically using the best available attachment method.