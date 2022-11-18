/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

typedef uint8_t         u8;
typedef int8_t          s8;
typedef uint16_t        u16;
typedef int16_t         s16;
typedef uint32_t        u32;
typedef int32_t         s32;
typedef uint64_t        u64;
typedef uint64_t        s64;

#ifndef __le16
#define __le16          uint16_t
#endif
#ifndef __le32
#define __le32          uint32_t
#endif
#ifndef __le64
#define __le64          uint64_t
#endif
#ifndef __be16
#define __be16          uint16_t
#endif
#ifndef __be32
#define __be32          uint32_t
#endif
#ifndef __be64
#define __be64          uint64_t
#endif

#define barrier() asm volatile("" ::: "memory")

static inline void
ICE_PCI_REG_WRITE(volatile void *reg, uint32_t value)
{
	barrier();
	*(volatile uint32_t *)reg = rte_cpu_to_le_32(value);
}
