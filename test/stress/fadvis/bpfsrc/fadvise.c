// SPDX-License-Identifier: GPL-2.0
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>

// POSIX_FADV_* constants (man 2 posix_fadvise)
#define POSIX_FADV_NORMAL     0 /* No further special treatment.  */
#define POSIX_FADV_RANDOM     1 /* Expect random page references.  */
#define POSIX_FADV_SEQUENTIAL 2 /* Expect sequential page references.  */
#define POSIX_FADV_WILLNEED   3 /* Will need these pages.  */
#define POSIX_FADV_DONTNEED   4 /* Don't need these pages.  */
#define POSIX_FADV_NOREUSE    5 /* Data will be accessed once.  */

char __license[] SEC("license") = "Dual BSD/GPL";

struct fadvise_args_t {
    int fd;
    __u64 offset;
    __u64 len;
    int advice;
};

struct fadvise_stats_t {
    __u64 total_calls;
    __u64 success_calls;
    __u64 advice_dontneed;
    __u64 advice_sequential;
    __u64 advice_normal;
    __u64 advice_random;
    __u64 advice_willneed;
    __u64 advice_noreuse;
};

// TID -> syscall 参数临时保存
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, __u64); // TID
    __type(value, struct fadvise_args_t);
} fadvise_args_map SEC(".maps");

// PID -> 累计统计信息
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, __u32); // PID
    __type(value, struct fadvise_stats_t);
} fadvise_stats_map SEC(".maps");

SEC("tracepoint/syscalls/sys_enter_fadvise64")
int trace_enter_fadvise64(struct trace_event_raw_sys_enter *ctx) {
    __u64 tid = bpf_get_current_pid_tgid();
    struct fadvise_args_t args = {};

    args.fd     = ctx->args[0];
    args.offset = ctx->args[1];
    args.len    = ctx->args[2];
    args.advice = ctx->args[3];

    bpf_map_update_elem(&fadvise_args_map, &tid, &args, BPF_ANY);

    __u32 pid = tid >> 32;
    struct fadvise_stats_t *stats = bpf_map_lookup_elem(&fadvise_stats_map, &pid);
    struct fadvise_stats_t new_stats = {};
    if (!stats) {
        stats = &new_stats;
    }

    stats->total_calls++;

    switch (args.advice) {
        case POSIX_FADV_DONTNEED:
            stats->advice_dontneed++;
            break;
        case POSIX_FADV_SEQUENTIAL:
            stats->advice_sequential++;
            break;
        case POSIX_FADV_NORMAL:
            stats->advice_normal++;
            break;
        case POSIX_FADV_RANDOM:
            stats->advice_random++;
            break;
        case POSIX_FADV_WILLNEED:
            stats->advice_willneed++;
            break;
        case POSIX_FADV_NOREUSE:
            stats->advice_noreuse++;
            break;
    }

    bpf_map_update_elem(&fadvise_stats_map, &pid, stats, BPF_ANY);

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fadvise64")
int trace_exit_fadvise64(struct trace_event_raw_sys_exit *ctx) {
    __u64 tid = bpf_get_current_pid_tgid();
    __u32 pid = tid >> 32;

    if (ctx->ret == 0) {
        struct fadvise_stats_t *stats = bpf_map_lookup_elem(&fadvise_stats_map, &pid);
        if (stats) {
            stats->success_calls++;
            bpf_map_update_elem(&fadvise_stats_map, &pid, stats, BPF_ANY);
        }
    }

    bpf_map_delete_elem(&fadvise_args_map, &tid);
    return 0;
}

struct lru_shrink_info_t {
    __u64 nr_scanned;
    __u64 nr_reclaimed;
    __u32 caller_pid;
    char caller_comm[16];
};


struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
} events SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 1);
    __type(key, __u32); // always 0
    __type(value, struct lru_shrink_info_t);
} shrink_stats_map SEC(".maps");

SEC("tracepoint/vmscan/mm_vmscan_lru_shrink_inactive")
int trace_lru_shrink_inactive(struct trace_event_raw_mm_vmscan_lru_shrink_inactive *ctx) {
    __u32 key = 0;
    struct lru_shrink_info_t info = {};

    info.nr_scanned = ctx->nr_scanned;
    info.nr_reclaimed = ctx->nr_reclaimed;
    info.caller_pid = bpf_get_current_pid_tgid() >> 32;
    bpf_get_current_comm(&info.caller_comm, sizeof(info.caller_comm));

    bpf_map_update_elem(&shrink_stats_map, &key, &info, BPF_ANY);
    return 0;
}

struct reclaim_info_t {
    __u32 pid;
    char comm[16];
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 128);
    __type(key, __u32);
    __type(value, struct reclaim_info_t);
} direct_reclaim_map SEC(".maps");

struct trace_event_raw_mm_vmscan_direct_reclaim_begin;

SEC("tracepoint/vmscan/mm_vmscan_direct_reclaim_begin")
int trace_direct_reclaim_begin(struct trace_event_raw_mm_vmscan_direct_reclaim_begin *ctx) {
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    struct reclaim_info_t info = {};
    info.pid = pid;
    bpf_get_current_comm(&info.comm, sizeof(info.comm));
    bpf_map_update_elem(&direct_reclaim_map, &pid, &info, BPF_ANY);
    return 0;
}
