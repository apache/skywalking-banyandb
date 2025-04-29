/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */
#ifndef __BPF_CORE_READ_H
#define __BPF_CORE_READ_H

#include "bpf_helpers.h"

/* 支持 CO-RE（编译一次，到处运行）的BPF辅助宏 */
#define BPF_CORE_READ(dst, src, field)                              \
    do {                                                            \
        dst = src->field;                                           \
    } while (0)

#define BPF_CORE_READ_INTO(dst, src, field)                        \
    do {                                                           \
        dst = src->field;                                          \
    } while (0)

#endif /* __BPF_CORE_READ_H */