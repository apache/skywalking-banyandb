// SPDX-License-Identifier: GPL-2.0
#include "vmlinux.h"
#include "bpf_helpers.h"
#include "bpf_tracing.h"
#include "bpf_core_read.h"

char LICENSE[] SEC("license") = "Dual BSD/GPL";

// PID -> 成功调用次数
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 8192);
    __type(key,   __u32);
    __type(value, __u64);
} fadvise_calls SEC(".maps");

SEC("tracepoint/syscalls/sys_exit_fadvise64")
int trace_fadvise_exit(struct trace_event_raw_syscalls_sys_exit_fadvise64 *ctx) {
    if (ctx->ret != 0) {
        return 0;
    }
    u64 tid = bpf_get_current_pid_tgid();
    u32 pid = tid >> 32;
    __u64 init = 1, *cnt;

    cnt = bpf_map_lookup_elem(&fadvise_calls, &pid);
    if (cnt) {
        __sync_fetch_and_add(cnt, 1);
    } else {
        bpf_map_update_elem(&fadvise_calls, &pid, &init, BPF_ANY);
    }
    return 0;
}