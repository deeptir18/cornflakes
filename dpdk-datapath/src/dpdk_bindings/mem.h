#ifndef INLINED_MEM_H_
#define INLINED_MEM_H_
/* 
 * These headers are taken from shenango: https://github.com/shenango/shenango/blob/16ea43895a37cb4c04d5065f1073cc452f0bc00c/inc/base/mem.h 
 */
enum {
	PGSHIFT_4KB = 12,
	PGSHIFT_2MB = 21,
	PGSHIFT_1GB = 30,
};

enum {
	PGSIZE_4KB = (1 << PGSHIFT_4KB), /* 4096 bytes */
	PGSIZE_2MB = (1 << PGSHIFT_2MB), /* 2097152 bytes */
	PGSIZE_1GB = (1 << PGSHIFT_1GB), /* 1073741824 bytes */
};

#define PGMASK_4KB	(PGSIZE_4KB - 1)
#define PGMASK_2MB	(PGSIZE_2MB - 1)
#define PGMASK_1GB	(PGSIZE_1GB - 1)

/* page numbers */
#define PGN_4KB(la)	(((uintptr_t)(la)) >> PGSHIFT_4KB)
#define PGN_2MB(la)	(((uintptr_t)(la)) >> PGSHIFT_2MB)
#define PGN_1GB(la)	(((uintptr_t)(la)) >> PGSHIFT_1GB)

#define PGOFF_4KB(la)	(((uintptr_t)(la)) & PGMASK_4KB)
#define PGOFF_2MB(la)	(((uintptr_t)(la)) & PGMASK_2MB)
#define PGOFF_1GB(la)	(((uintptr_t)(la)) & PGMASK_1GB)

#define PGADDR_4KB(la)	(((uintptr_t)(la)) & ~((uintptr_t)PGMASK_4KB))
#define PGADDR_2MB(la)	(((uintptr_t)(la)) & ~((uintptr_t)PGMASK_2MB))
#define PGADDR_1GB(la)	(((uintptr_t)(la)) & ~((uintptr_t)PGMASK_1GB))

#define PAGEMAP_PGN_MASK	0x7fffffffffffffULL
#define PAGEMAP_FLAG_PRESENT	(1ULL << 63)
#define PAGEMAP_FLAG_SWAPPED	(1ULL << 62)
#define PAGEMAP_FLAG_FILE	(1ULL << 61)

struct tx_pktmbuf_priv {
    uint32_t lkey;
    uint16_t lkey_present;
    uint16_t refers_to_another;
};


#endif
