/*
 * mempool.h - a simple, preallocated pool of memory
 */

#pragma once

#include <base/stddef.h>
#include <base/debug.h>
#include <base/types.h>
#include <base/mem.h>

struct custom_ice_mempool {
    void **free_items; /* Array of pointers to free items. */
    uint8_t *ref_counts; /* Array of reference counts for each item in the memory pool */
    physaddr_t *phys_paddrs; /* Array of physical page addresses. */
    size_t allocated; /* Number of allocated items. */
    size_t capacity; /* Total capacity of memory pool. */
    void *buf; /* Actual contiguous region of backing data. */
    size_t len; /* Total region length. */
    size_t pgsize; /* Page size. Using larger pages leads to TLB efficiency. */
    size_t item_len; /* Length of mempool items. Must be aligned to page size. */
    size_t log_item_len; /* Log of the item len*/
    size_t num_pages; /* Number of pages */
    uint32_t use_atomic_ops; /* If true, use atomic updates */
};

int custom_ice_is_allocated(struct custom_ice_mempool *mempool);

void custom_ice_clear_mempool(struct custom_ice_mempool *mempool);

#ifdef DEBUG
extern void __custom_ice_mempool_alloc_debug_check(struct custom_ice_mempool *m, void *item);
extern void __custom_ice_mempool_free_debug_check(struct custom_ice_mempool *m, void *item);
#else /* DEBUG */
static inline void __custom_ice_mempool_alloc_debug_check(struct custom_ice_mempool *m, void *item) {}
static inline void __custom_ice_mempool_free_debug_check(struct custom_ice_mempool *m, void *item) {}
#endif /* DEBUG */


/**
 * mempool_find_index - Finds the allocated index of an item from the pool.
 * @m: the memory pool
 * @item: the object to find the index of.
 *
 * Returns index of item; -1 if item not within bounds of pool.
 */

int custom_ice_mempool_find_index(struct custom_ice_mempool *m, void *item);

/**
 * mempool_alloc - allocates an item from the pool
 * @m: the memory pool to allocate from
 *
 * Returns an item, or NULL if the pool is empty.
 */
void *custom_ice_mempool_alloc(struct custom_ice_mempool *m);

static inline void *custom_ice_mempool_alloc_by_idx(struct custom_ice_mempool *m, size_t idx)
{
    void *item;
    if (unlikely(m->allocated >= m->capacity)) {
        return NULL;
    }
    if (idx >= m->capacity) {
        return NULL;
    }
    item = m->free_items[idx];
    m->free_items[idx] = NULL;
    m->allocated++;
	__custom_ice_mempool_alloc_debug_check(m, item);
    return item;
}

/* 
 * mempool_free - returns an item to the pool
 * @m: the memory pool the item was allocated from
 * @item: the item to return
 * */
void custom_ice_mempool_free(struct custom_ice_mempool *m, void *item);

static inline void custom_ice_mempool_free_by_idx(struct custom_ice_mempool *m, void *item, size_t idx) {
	__custom_ice_mempool_free_debug_check(m, item);
    if (m->allocated == 0) {
        NETPERF_WARN("Freeing item %p item back into mempool %p with mem allocated 0.\n", m, item);
        return;
    }
    NETPERF_ASSERT(idx <= m->capacity, "Idx out of bounds");
    m->free_items[idx] = item;
    m->allocated--;
    NETPERF_ASSERT(m->allocated <= m->capacity, "Overflow in mempool"); /* Ensure no overflow */

}

/* Pins the memory backing the mempool by calling mlock. */
int custom_ice_mempool_pin(struct custom_ice_mempool *m);

/* Unpins the memory backing the mempool by calling munlock. */
int custom_ice_mempool_unpin(struct custom_ice_mempool *m);

extern int custom_ice_mempool_create(struct custom_ice_mempool *m,
                            size_t len,
                            size_t pgsize,
                            size_t item_len,
                            uint32_t use_atomic_ops);

extern void custom_ice_mempool_destroy(struct custom_ice_mempool *m);

/* Decrement reference count or return buffer to mempool. */
int custom_ice_refcnt_update_or_free(struct custom_ice_mempool *mempool,
        void *buf, 
        size_t refcnt_index, 
        int8_t change);

/* Get physical address associated with buffer. */
physaddr_t custom_ice_get_dma_addr(struct custom_ice_mempool *mempool,
        void *buf,
        size_t refcnt_index,
        size_t offset);

