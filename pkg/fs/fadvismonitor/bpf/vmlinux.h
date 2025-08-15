// Basic vmlinux.h for eBPF programs
#ifndef __VMLINUX_H__
#define __VMLINUX_H__

// Basic kernel types
typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;

typedef signed char __s8;
typedef signed short __s16;
typedef signed int __s32;
typedef signed long long __s64;

// BPF-specific types
typedef __u16 __be16;
typedef __u32 __be32;
typedef __u64 __wsum;

// BPF map types
#define BPF_MAP_TYPE_HASH 1
#define BPF_MAP_TYPE_PERF_EVENT_ARRAY 4

// BPF flags
#define BPF_ANY 0

// Architecture-specific structures
#ifdef __x86_64__
struct pt_regs {
    unsigned long r15;
    unsigned long r14;
    unsigned long r13;
    unsigned long r12;
    unsigned long bp;
    unsigned long bx;
    unsigned long r11;
    unsigned long r10;
    unsigned long r9;
    unsigned long r8;
    unsigned long ax;
    unsigned long cx;
    unsigned long dx;
    unsigned long si;
    unsigned long di;
    unsigned long orig_ax;
    unsigned long ip;
    unsigned long cs;
    unsigned long flags;
    unsigned long sp;
    unsigned long ss;
};
#endif

#ifdef __aarch64__
struct pt_regs {
    __u64 regs[31];
    __u64 sp;
    __u64 pc;
    __u64 pstate;
    __u64 orig_x0;
    __s32 syscallno;
    __u32 unused2;
    __u64 orig_addr_limit;
    __u64 unused;
    __u64 stackframe[2];
};
#endif

// Tracepoint context structures
struct trace_event_raw_sys_enter {
    __u64 args[6];
};

struct trace_event_raw_sys_exit {
    long ret;
};

struct trace_event_raw_mm_vmscan_lru_shrink_inactive {
    __u64 nr_scanned;
    __u64 nr_reclaimed;
};

struct trace_event_raw_mm_vmscan_direct_reclaim_begin {
    unsigned int order;
    unsigned int gfp_flags;
};

#endif /* __VMLINUX_H__ */