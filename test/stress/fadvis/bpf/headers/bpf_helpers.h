/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */
#ifndef __BPF_HELPERS_H
#define __BPF_HELPERS_H

#define SEC(NAME) __attribute__((section(NAME), used))

#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name

enum bpf_map_type {
    BPF_MAP_TYPE_UNSPEC,
    BPF_MAP_TYPE_HASH,
    BPF_MAP_TYPE_ARRAY,
    BPF_MAP_TYPE_PERCPU_HASH,
    BPF_MAP_TYPE_PERCPU_ARRAY,
    BPF_MAP_TYPE_LRU_HASH,
    BPF_MAP_TYPE_LRU_PERCPU_HASH,
    BPF_MAP_TYPE_LPM_TRIE,
    BPF_MAP_TYPE_ARRAY_OF_MAPS,
    BPF_MAP_TYPE_HASH_OF_MAPS,
    BPF_MAP_TYPE_DEVMAP,
    BPF_MAP_TYPE_SOCKMAP,
    BPF_MAP_TYPE_CPUMAP,
    BPF_MAP_TYPE_XSKMAP,
    BPF_MAP_TYPE_SOCKHASH,
    BPF_MAP_TYPE_CGROUP_STORAGE,
    BPF_MAP_TYPE_REUSEPORT_SOCKARRAY,
    BPF_MAP_TYPE_PERCPU_CGROUP_STORAGE,
    BPF_MAP_TYPE_QUEUE,
    BPF_MAP_TYPE_STACK,
    BPF_MAP_TYPE_SK_STORAGE,
    BPF_MAP_TYPE_DEVMAP_HASH,
    BPF_MAP_TYPE_STRUCT_OPS,
    BPF_MAP_TYPE_RINGBUF,
    BPF_MAP_TYPE_INODE_STORAGE,
    BPF_MAP_TYPE_TASK_STORAGE,
};

enum {
    BPF_ANY     = 0,
    BPF_NOEXIST = 1,
    BPF_EXIST   = 2,
};

/* BPF helper functions */
static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static int (*bpf_map_update_elem)(void *map, const void *key, const void *value, __u64 flags) = (void *)2;
static int (*bpf_map_delete_elem)(void *map, const void *key) = (void *)3;
static int (*bpf_probe_read)(void *dst, __u32 size, const void *unsafe_ptr) = (void *)4;
static __u64 (*bpf_ktime_get_ns)(void) = (void *)5;
static int (*bpf_trace_printk)(const char *fmt, __u32 fmt_size, ...) = (void *)6;
static __u32 (*bpf_get_current_pid_tgid)(void) = (void *)14;
static __u32 (*bpf_get_current_uid_gid)(void) = (void *)15;
static int (*bpf_perf_event_output)(void *ctx, void *map, __u64 flags, void *data, __u64 size) = (void *)25;

/* BPF_TRACE_PRINTK macro for debug */
#define bpf_printk(fmt, ...)                                   \
({                                                              \
    char ____fmt[] = fmt;                                       \
    bpf_trace_printk(____fmt, sizeof(____fmt), ##__VA_ARGS__); \
})

#endif /* __BPF_HELPERS_H */