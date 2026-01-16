// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "../generated/vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>
#include <stdbool.h>

// POSIX_FADV_* constants (man 2 posix_fadvise)
#define POSIX_FADV_NORMAL     0
#define POSIX_FADV_RANDOM     1
#define POSIX_FADV_SEQUENTIAL 2
#define POSIX_FADV_WILLNEED   3
#define POSIX_FADV_DONTNEED   4
#define POSIX_FADV_NOREUSE    5

#define MAX_LATENCY_BUCKETS 32

char __license[] SEC("license") = "Dual BSD/GPL";

// =========================================================================================
// Structs
// =========================================================================================

struct fadvise_args_t {
    int fd;
    __u64 offset;
    __u64 len;
    int advice;
};

struct read_latency_stats_t {
    __u64 buckets[MAX_LATENCY_BUCKETS]; // log2 histogram of latency in microseconds
    __u64 sum_latency_ns;
    __u64 count;
    __u64 read_bytes_total;
};

struct fadvise_stats_t {
    __u64 total_calls;
    __u64 advice_dontneed;
};

struct cache_stats_t {
    __u64 lookups; // mm_filemap_get_pages
    __u64 adds;    // mm_filemap_add_to_page_cache
    __u64 deletes; // mm_filemap_delete_from_page_cache
};

struct shrink_counters_t {
    __u64 nr_scanned_total;
    __u64 nr_reclaimed_total;
    __u64 events_total;
};

struct reclaim_counters_t {
    __u64 direct_reclaim_begin_total;
};

// =========================================================================================
// Maps
// =========================================================================================

// Target cgroup ID
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u64);
} config_map SEC(".maps");

// Fadvise maps
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key, __u64); // TID
    __type(value, struct fadvise_args_t);
} fadvise_args_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 8192);
    __type(key, __u32); // PID
    __type(value, struct fadvise_stats_t);
} fadvise_stats_map SEC(".maps");

// Read Latency maps
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, __u64); // TID
    __type(value, __u64); // Start Timestamp (ns)
} read_args_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 8192);
    __type(key, __u32); // PID
    __type(value, struct read_latency_stats_t);
} read_latency_stats_map SEC(".maps");

// Pread Latency maps
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10240);
    __type(key, __u64); // TID
    __type(value, __u64); // Start Timestamp (ns)
} pread_args_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 8192);
    __type(key, __u32); // PID
    __type(value, struct read_latency_stats_t);
} pread_latency_stats_map SEC(".maps");

// Memory/Cache maps
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 1);
    __type(key, __u32); // always 0
    __type(value, struct shrink_counters_t);
} shrink_stats_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 1);
    __type(key, __u32); // always 0
    __type(value, struct reclaim_counters_t);
} reclaim_counters_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_HASH);
    __uint(max_entries, 8192);
    __type(key, __u32); // PID
    __type(value, struct cache_stats_t);
} cache_stats_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
} events SEC(".maps");

// =========================================================================================
// Helpers
// =========================================================================================

static __always_inline bool comm_is_banyandb() {
    const char target[] = "banyand";
    char comm[16];
    if (bpf_get_current_comm(&comm, sizeof(comm)) != 0) {
        return false;
    }
    #pragma unroll
    for (int i = 0; i < (int)sizeof(target) - 1; i++) {
        if (comm[i] != target[i]) {
            return false;
        }
    }
    return true;
}

static __always_inline bool is_task_allowed() {
    __u64 curr_cgroup_id = bpf_get_current_cgroup_id();

    // Layer 1: Cgroup boundary (preferred correctness guarantee)
    // If cgroup filter is configured (target_id != 0), enforce it strictly.
    // If not configured (target_id == 0), fall back to comm-only filtering.
    __u32 key = 0;
    __u64 *target_id = bpf_map_lookup_elem(&config_map, &key);
    bool cgroup_enabled = (target_id && *target_id != 0);
    
    if (cgroup_enabled) {
        // Cgroup filter is active - enforce strict matching
        if (curr_cgroup_id != *target_id) {
            return false;
        }
        // Cgroup matched, continue to comm check (always required)
        
        // Layer 2: Comm verification (ALWAYS enforced)
        // This ensures kernel always filters by comm="banyand" as documented
        return comm_is_banyandb();
    } else {
        // Comm-only mode (degraded): comm check only
        // Layer 2: Comm verification (primary filter in degraded mode)
        return comm_is_banyandb();
    }
}

static __always_inline __u64 log2l(__u64 v) {
    __u64 r;
    __u64 shift;
    r = (v > 0xFFFFFFFF) << 5; v >>= r;
    shift = (v > 0xFFFF) << 4; v >>= shift; r |= shift;
    shift = (v > 0xFF) << 3; v >>= shift; r |= shift;
    shift = (v > 0xF) << 2; v >>= shift; r |= shift;
    shift = (v > 0x3) << 1; v >>= shift; r |= shift;
    r |= (v >> 1);
    return r;
}

static __always_inline int trace_enter_read() {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&read_args_map, &tid, &ts, BPF_ANY);
    return 0;
}

static __always_inline int trace_exit_read(int ret) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();
    __u64 *tsp = bpf_map_lookup_elem(&read_args_map, &tid);
    if (!tsp) {
        return 0;
    }
    
    // Only record successful reads
    if (ret >= 0) {
        __u64 delta = bpf_ktime_get_ns() - *tsp;
        __u32 pid = tid >> 32;
        
        struct read_latency_stats_t *stats = bpf_map_lookup_elem(&read_latency_stats_map, &pid);
        if (!stats) {
            struct read_latency_stats_t new_stats = {};
            bpf_map_update_elem(&read_latency_stats_map, &pid, &new_stats, BPF_ANY);
            stats = bpf_map_lookup_elem(&read_latency_stats_map, &pid);
        }
        
        if (stats) {
            stats->sum_latency_ns += delta;
            stats->count++;
            if (ret > 0) {
                stats->read_bytes_total += (__u64)ret;
            }
            
            // Log2 histogram (microseconds)
            // delta is ns. delta / 1000 is us.
            __u64 lat_us = delta / 1000;
            __u64 bucket = 0;
            if (lat_us > 0) {
                bucket = log2l(lat_us);
                if (bucket >= MAX_LATENCY_BUCKETS) {
                    bucket = MAX_LATENCY_BUCKETS - 1;
                }
            }
            stats->buckets[bucket]++;
        }
    }
    
    bpf_map_delete_elem(&read_args_map, &tid);
    return 0;
}

static __always_inline int trace_enter_pread() {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();
    __u64 ts = bpf_ktime_get_ns();
    bpf_map_update_elem(&pread_args_map, &tid, &ts, BPF_ANY);
    return 0;
}

static __always_inline int trace_exit_pread(int ret) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();
    __u64 *tsp = bpf_map_lookup_elem(&pread_args_map, &tid);
    if (!tsp) {
        return 0;
    }
    
    // Only record successful reads
    if (ret >= 0) {
        __u64 delta = bpf_ktime_get_ns() - *tsp;
        __u32 pid = tid >> 32;
        
        struct read_latency_stats_t *stats = bpf_map_lookup_elem(&pread_latency_stats_map, &pid);
        if (!stats) {
            struct read_latency_stats_t new_stats = {};
            bpf_map_update_elem(&pread_latency_stats_map, &pid, &new_stats, BPF_ANY);
            stats = bpf_map_lookup_elem(&pread_latency_stats_map, &pid);
        }
        
        if (stats) {
            stats->sum_latency_ns += delta;
            stats->count++;
            if (ret > 0) {
                stats->read_bytes_total += (__u64)ret;
            }
            
            // Log2 histogram (microseconds)
            __u64 lat_us = delta / 1000;
            __u64 bucket = 0;
            if (lat_us > 0) {
                bucket = log2l(lat_us);
                if (bucket >= MAX_LATENCY_BUCKETS) {
                    bucket = MAX_LATENCY_BUCKETS - 1;
                }
            }
            stats->buckets[bucket]++;
        }
    }
    
    bpf_map_delete_elem(&pread_args_map, &tid);
    return 0;
}

// =========================================================================================
// Tracepoints: Read / Pread Latency (stable kernel ABI)
// =========================================================================================

SEC("tracepoint/syscalls/sys_enter_read")
int trace_enter_read_tp(struct trace_event_raw_sys_enter *ctx) {
    return trace_enter_read();
}

SEC("tracepoint/syscalls/sys_exit_read")
int trace_exit_read_tp(struct trace_event_raw_sys_exit *ctx) {
    long ret = BPF_CORE_READ(ctx, ret);
    return trace_exit_read((int)ret);
}

SEC("tracepoint/syscalls/sys_enter_pread64")
int trace_enter_pread64_tp(struct trace_event_raw_sys_enter *ctx) {
    return trace_enter_pread();
}

SEC("tracepoint/syscalls/sys_exit_pread64")
int trace_exit_pread64_tp(struct trace_event_raw_sys_exit *ctx) {
    long ret = BPF_CORE_READ(ctx, ret);
    return trace_exit_pread((int)ret);
}

// =========================================================================================
// Tracepoints: Fadvise
// =========================================================================================

SEC("tracepoint/syscalls/sys_enter_fadvise64")
int trace_enter_fadvise64(struct trace_event_raw_sys_enter *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();
    struct fadvise_args_t args = {};

    args.fd     = BPF_CORE_READ(ctx, args[0]);
    args.offset = BPF_CORE_READ(ctx, args[1]);
    args.len    = BPF_CORE_READ(ctx, args[2]);
    args.advice = BPF_CORE_READ(ctx, args[3]);

    bpf_map_update_elem(&fadvise_args_map, &tid, &args, BPF_ANY);

    __u32 pid = tid >> 32;
    struct fadvise_stats_t *stats = bpf_map_lookup_elem(&fadvise_stats_map, &pid);
    if (!stats) {
        struct fadvise_stats_t new_stats = {};
        bpf_map_update_elem(&fadvise_stats_map, &pid, &new_stats, BPF_ANY);
        stats = bpf_map_lookup_elem(&fadvise_stats_map, &pid);
    }

    if (stats) {
        stats->total_calls++;
        if (args.advice == POSIX_FADV_DONTNEED) {
            stats->advice_dontneed++;
        }
    }

    return 0;
}

SEC("tracepoint/syscalls/sys_exit_fadvise64")
int trace_exit_fadvise64(struct trace_event_raw_sys_exit *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u64 tid = bpf_get_current_pid_tgid();

    bpf_map_delete_elem(&fadvise_args_map, &tid);
    return 0;
}

// =========================================================================================
// Tracepoints: Memory Pressure
// =========================================================================================

SEC("tracepoint/vmscan/mm_vmscan_lru_shrink_inactive")
int trace_lru_shrink_inactive(struct trace_event_raw_mm_vmscan_lru_shrink_inactive *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u32 key = 0;
    struct shrink_counters_t *counters = bpf_map_lookup_elem(&shrink_stats_map, &key);
    if (!counters) {
        struct shrink_counters_t zero = {};
        bpf_map_update_elem(&shrink_stats_map, &key, &zero, BPF_ANY);
        counters = bpf_map_lookup_elem(&shrink_stats_map, &key);
    }
    if (counters) {
        counters->nr_scanned_total += ctx->nr_scanned;
        counters->nr_reclaimed_total += ctx->nr_reclaimed;
        counters->events_total++;
    }
    return 0;
}

// Use a generic context pointer to avoid build warnings on kernels where the
// trace_event_raw_mm_vmscan_direct_reclaim_begin type is not exposed.
SEC("tracepoint/vmscan/mm_vmscan_direct_reclaim_begin")
int trace_direct_reclaim_begin(void *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u32 key = 0;
    struct reclaim_counters_t *counters = bpf_map_lookup_elem(&reclaim_counters_map, &key);
    if (!counters) {
        struct reclaim_counters_t zero = {};
        bpf_map_update_elem(&reclaim_counters_map, &key, &zero, BPF_ANY);
        counters = bpf_map_lookup_elem(&reclaim_counters_map, &key);
    }
    if (counters) {
        counters->direct_reclaim_begin_total++;
    }
    return 0;
}

// =========================================================================================
// Tracepoints: Page Cache Adds
// =========================================================================================

SEC("tracepoint/filemap/mm_filemap_get_pages")
int trace_mm_filemap_get_pages(void *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u32 pid = bpf_get_current_pid_tgid() >> 32;

    struct cache_stats_t *stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    if (!stats) {
        struct cache_stats_t new_stats = {};
        bpf_map_update_elem(&cache_stats_map, &pid, &new_stats, BPF_ANY);
        stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    }

    if (stats) {
        stats->lookups++;
    }

    return 0;
}

SEC("tracepoint/filemap/mm_filemap_add_to_page_cache")
int trace_mm_filemap_add_to_page_cache(void *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u32 pid = bpf_get_current_pid_tgid() >> 32;
    
    struct cache_stats_t *stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    if (!stats) {
        struct cache_stats_t new_stats = {};
        bpf_map_update_elem(&cache_stats_map, &pid, &new_stats, BPF_ANY);
        stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    }

    if (stats) {
        stats->adds++;
    }

    return 0;
}

SEC("tracepoint/filemap/mm_filemap_delete_from_page_cache")
int trace_mm_filemap_delete_from_page_cache(void *ctx) {
    if (!is_task_allowed()) {
        return 0;
    }
    __u32 pid = bpf_get_current_pid_tgid() >> 32;

    struct cache_stats_t *stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    if (!stats) {
        struct cache_stats_t new_stats = {};
        bpf_map_update_elem(&cache_stats_map, &pid, &new_stats, BPF_ANY);
        stats = bpf_map_lookup_elem(&cache_stats_map, &pid);
    }

    if (stats) {
        stats->deletes++;
    }

    return 0;
}
