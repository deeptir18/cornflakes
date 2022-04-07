/*
 * mempool.h - a simple, preallocated pool of memory
 */

#pragma once

#include <base/stddef.h>
#include <base/debug.h>

struct custom_mlx5_mempool {
    void **free_items; /* Array of pointers to free items. */
    size_t allocated; /* Number of allocated items. */
    size_t capacity; /* Total capacity of memory pool. */
    void *buf; /* Actual contiguous region of backing data. */
    size_t len; /* Total region length. */
    size_t pgsize; /* Page size. Using larger pages leads to TLB efficiency. */
    size_t item_len; /* Length of mempool items. Must be aligned to page size. */
    int32_t lkey; /* Lkey for the memory region backed by mempool. -1 if not registered. */
};

inline int custom_mlx5_is_allocated(struct custom_mlx5_mempool *mempool) {
    if (mempool->buf != NULL) {
        return 1;
    } else {
        return 0;
    }
}

inline int custom_mlx5_is_registered(struct custom_mlx5_mempool *mempool) {
    if (mempool->lkey != -1) {
        return 1;
    } else {
        return 0;
    }
}

inline void custom_mlx5_clear_mempool(struct custom_mlx5_mempool *mempool) {
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
extern void __custom_mlx5_mempool_alloc_debug_check(struct custom_mlx5_mempool *m, void *item);
extern void __custom_mlx5_mempool_free_debug_check(struct custom_mlx5_mempool *m, void *item);
#else /* DEBUG */
static inline void __custom_mlx5_mempool_alloc_debug_check(struct custom_mlx5_mempool *m, void *item) {}
static inline void __custom_mlx5_mempool_free_debug_check(struct custom_mlx5_mempool *m, void *item) {}
#endif /* DEBUG */


/* Registers mempool to include lkey information. */
static inline void custom_mlx5_register_mempool(struct custom_mlx5_mempool *mempool, uint32_t lkey) {
    mempool->lkey = (int32_t)lkey;
}

/* Removes lkey information from the mempool.*/
static inline void custom_mlx5_deregister_mempool(struct custom_mlx5_mempool *mempool) {
    mempool->lkey = -1;
}

/**
 * mempool_find_index - Finds the allocated index of an item from the pool.
 * @m: the memory pool
 * @item: the object to find the index of.
 *
 * Returns index of item; -1 if item not within bounds of pool.
 */
static inline int custom_mlx5_mempool_find_index(struct custom_mlx5_mempool *m, void *item) {
    if ((char *)item < (char *)m->buf && (char *)item >= ((char *)m->buf + m->len)) {
        return -1;
    }

    // TODO: check that item is aligned?
    return (int)((char *)item - (char *)m->buf) / m->item_len;
}

/**
 * mempool_alloc - allocates an item from the pool
 * @m: the memory pool to allocate from
 *
 * Returns an item, or NULL if the pool is empty.
 */
static inline void *custom_mlx5_mempool_alloc(struct custom_mlx5_mempool *m)
{
	void *item;
	if (unlikely(m->allocated >= m->capacity))
		return NULL;
	item = m->free_items[m->allocated++];
	__custom_mlx5_mempool_alloc_debug_check(m, item);
	return item;
}

static inline void *custom_mlx5_mempool_alloc_by_idx(struct custom_mlx5_mempool *m, size_t idx)
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
	__custom_mlx5_mempool_alloc_debug_check(m, item);
    return item;
}

/* 
 * mempool_free - returns an item to the pool
 * @m: the memory pool the item was allocated from
 * @item: the item to return
 * */
static inline void custom_mlx5_mempool_free(struct custom_mlx5_mempool *m, void *item) {
	__custom_mlx5_mempool_free_debug_check(m, item);
    if (m->allocated == 0) {
        NETPERF_WARN("Freeing item %p item back into mempool %p with mem allocated 0.\n", m, item);
        return;
    }
    m->free_items[--m->allocated] = item;
    NETPERF_ASSERT(m->allocated <= m->capacity, "Overflow in mempool"); /* ensure no overflow */
}

static inline void custom_mlx5_mempool_free_by_idx(struct custom_mlx5_mempool *m, void *item, size_t idx) {
	__custom_mlx5_mempool_free_debug_check(m, item);
    if (m->allocated == 0) {
        NETPERF_WARN("Freeing item %p item back into mempool %p with mem allocated 0.\n", m, item);
        return;
    }
    NETPERF_ASSERT(idx <= m->capacity, "Idx out of bounds");
    m->free_items[idx] = item;
    m->allocated--;
    NETPERF_ASSERT(m->allocated <= m->capacity, "Overflow in mempool"); /* Ensure no overflow */

}

extern int custom_mlx5_mempool_create(struct custom_mlx5_mempool *m,
                            size_t len,
                            size_t pgsize,
                            size_t item_len);

extern void custom_mlx5_mempool_destroy(struct custom_mlx5_mempool *m);

