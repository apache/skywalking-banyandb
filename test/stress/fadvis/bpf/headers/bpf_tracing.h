/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */
#ifndef __BPF_TRACING_H
#define __BPF_TRACING_H

#include "bpf_helpers.h"

/* 定义常见的系统调用追踪事件结构 */
struct trace_event_raw_sys_enter {
    unsigned long long args[6];
};

#endif /* __BPF_TRACING_H */