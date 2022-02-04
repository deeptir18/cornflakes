/*
 * mempool.h - a simple, preallocated pool of memory
 */

#pragma once

#include <base/stddef.h>
#include <base/debug.h>

struct mempool {
    void **free_items; /* Array of pointers to free items. */
    size_t allocated; /* Number of allocated items. */
    size_t capacity; /* Total capacity of memory pool. */
    void *buf; /* Actual contiguous region of backing data. */
    size_t len; /* Total region length. */
    size_t pgsize; /* Page size. Using larger pages leads to TLB efficiency. */
    size_t item_len; /* Length of mempool items. Must be aligned to page size. */
    int32_t lkey; /* Lkey for the memory region backed by mempool. -1 if not registered. */
};

inline int is_allocated(struct mempool *mempool) {
    if (mempool->buf != NULL) {
        return 1;
    } else {
        return 0;
    }
}

inline int is_registered(struct mempool *mempool) {
    if (mempool->lkey != -1) {
        return 1;
    } else {
        return 0;
    }
}

inline void clear_mempool(struct mempool *mempool) {
    mempool->free_items = NULL;
    mempool->allocated = 0;
    mempool->capacity = 0;
    mempool->buf = NULL;
    mempool->len = 0;
    mempool->pgsize = 0;
    mempool->item_len = 0;
    mempool->lkey = -1;
}

#ifdef DEBUG
extern void __mempool_alloc_debug_check(struct mempool *m, void *item);
extern void __mempool_free_debug_check(struct mempool *m, void *item);
#else /* DEBUG */
static inline void __mempool_alloc_debug_check(struct mempool *m, void *item) {}
static inline void __mempool_free_debug_check(struct mempool *m, void *item) {}
#endif /* DEBUG */


/* Registers mempool to include lkey information. */
static inline void register_mempool(struct mempool *mempool, uint32_t lkey) {
    mempool->lkey = (int32_t)lkey;
}

/* Removes lkey information from the mempool.*/
static inline void deregister_mempool(struct mempool *mempool) {
    mempool->lkey = -1;
}

/**
 * mempool_alloc - allocates an item from the pool
 * @m: the memory pool to allocate from
 *
 * Returns an item, or NULL if the pool is empty.
 */
static inline void *mempool_alloc(struct mempool *m)
{
	void *item;
	if (unlikely(m->allocated >= m->capacity))
		return NULL;
	item = m->free_items[m->allocated++];
	__mempool_alloc_debug_check(m, item);
	return item;
}

/* 
 * mempool_free - returns an item to the pool
 * @m: the memory pool the item was allocated from
 * @item: the item to return
 * */
static inline void mempool_free(struct mempool *m, void *item) {
	__mempool_free_debug_check(m, item);
    if (m->allocated == 0) {
        NETPERF_WARN("Freeing item %p item back into mempool %p with mem allocated 0.\n", m, item);
        return;
    }
    m->free_items[--m->allocated] = item;
    NETPERF_ASSERT(m->allocated <= m->capacity, "Overflow in mempool"); /* ensure no overflow */
}

extern int mempool_create(struct mempool *m,
                            size_t len,
                            size_t pgsize,
                            size_t item_len);

extern void mempool_destroy(struct mempool *m);

