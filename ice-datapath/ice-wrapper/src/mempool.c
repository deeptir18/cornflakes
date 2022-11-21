/*
 * mempool.c - a simple, preallocated, virtually contiguous pool of memory
 * Mostly taken from caladan: https://github.com/shenango/caladan/blob/068f30e0d1d63ee745b3f03a5e2b7be560222fc2/base/mempool.c
 * For convenience with DMA operations, items are not allowed to straddle page
 * boundaries.
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>

#include <base/debug.h>
#include <base/mempool.h>
#include <base/mem.h>
#include <sys/mman.h>

#ifdef DEBUG

static void custom_ice_mempool_common_check(struct custom_ice_mempool *m, void *item)
{
	uintptr_t pos = (uintptr_t)item;
	uintptr_t start = (uintptr_t)m->buf;

	/* is the item within the bounds of the pool */
	assert(pos >= start && pos < start + m->len);

	/* is the item properly aligned */
	assert((start & (m->pgsize - 1)) % m->item_len == 0);
}

void __custom_ice_mempool_alloc_debug_check(struct custom_ice_mempool *m, void *item)
{
	custom_ice_mempool_common_check(m, item);

	/* poison the item */
	memset(item, 0xAB, m->item_len);
}

void __custom_ice_mempool_free_debug_check(struct custom_ice_mempool *m, void *item)
{
	custom_ice_mempool_common_check(m, item);

	/* poison the item */
	memset(item, 0xCD, m->item_len);
}

#endif /* DEBUG */

void *custom_ice_mempool_alloc(struct custom_ice_mempool *m)
{
	void *item;
	if (unlikely(m->allocated >= m->capacity))
		return NULL;
	item = m->free_items[m->allocated++];
	__custom_ice_mempool_alloc_debug_check(m, item);
	return item;
}

int custom_ice_mempool_find_index(struct custom_ice_mempool *m, void *item) {
    // todo: MAKE THIS PANIC ON TRUE?
    /*if ((char *)item < (char *)m->buf && (char *)item >= ((char *)m->buf + m->len)) {
        return -1;
    }*/
    //NETPERF_DEBUG("Log item len: %lu, item: %p, mempool buf: %p, dif: %lu, returned index: %d", m->log_item_len, item, m->buf, (char *)item - (char *)m->buf, (int)(((char *)item - (char *)m->buf) >> m->log_item_len));
    return (int)(((char *)item - (char *)m->buf) >> m->log_item_len);
}

int custom_ice_is_allocated(struct custom_ice_mempool *mempool) {
    if (mempool->buf != NULL) {
        return 1;
    } else {
        return 0;
    }
}

void custom_ice_mempool_free(struct custom_ice_mempool *m, void *item) {
	__custom_ice_mempool_free_debug_check(m, item);
    if (m->allocated == 0) {
        NETPERF_WARN("Freeing item %p item back into mempool %p with mem allocated 0.\n", item, m);
        return;
    }
    //NETPERF_INFO("Freeing item %p back into mempool %p with allocated %lu", item, m, m->allocated);
    m->free_items[--m->allocated] = item;
    NETPERF_ASSERT(m->allocated <= m->capacity, "Overflow in mempool"); /* ensure no overflow */
}


int custom_ice_is_registered(struct custom_ice_mempool *mempool) {
    if (mempool->lkey != -1) {
        return 1;
    } else {
        return 0;
    }
}

void custom_ice_clear_mempool(struct custom_ice_mempool *mempool) {
    mempool->free_items = NULL;
    mempool->allocated = 0;
    mempool->capacity = 0;
    mempool->buf = NULL;
    mempool->len = 0;
    mempool->pgsize = 0;
    mempool->item_len = 0;
    mempool->lkey = -1;
}

static int custom_ice_mempool_populate(struct custom_ice_mempool *m, void *buf, size_t len,
			    size_t pgsize, size_t item_len)
{
	size_t items_per_page = pgsize / item_len;
	size_t nr_pages = len / pgsize;
	int i, j;

	m->free_items = calloc(nr_pages * items_per_page, sizeof(void *));
	if (!m->free_items) {
            NETPERF_DEBUG("Calloc didn't allocate free items list.");
		return -ENOMEM;
    }
    m->ref_counts = calloc(nr_pages * items_per_page, sizeof(uint8_t));
    if (!m->ref_counts) {
        NETPERF_DEBUG("Calloc didn't allocate ref counts list.");
        return -ENOMEM;
    }

	for (i = 0; i < nr_pages; i++) {
		for (j = 0; j < items_per_page; j++) {
			m->free_items[m->capacity++] =
				(char *)buf + pgsize * i + item_len * j;
            m->ref_counts[i * j] = 0;
		}
	}

    NETPERF_DEBUG("Allocated Items per page: %u, item_len: %zu, # pages: %u, LEN: %zu, current capacity: %zu", (unsigned)items_per_page, m->item_len, (unsigned)nr_pages, len, m->capacity);
	return 0;
}

/**
 * mempool_create - initializes a memory pool
 * @m: the memory pool to initialize
 * @len: the length of the buffer region managed by the pool
 * @pgsize: the size of the pages in the buffer region (must be uniform)
 * @item_len: the length of each item in the pool
 */
int custom_ice_mempool_create(struct custom_ice_mempool *m, size_t len,
		   size_t pgsize, size_t item_len)
{
	if (item_len == 0 || !is_power_of_two(pgsize) || len % pgsize != 0) {
        NETPERF_WARN("Invalid params to create mempool.");
		return -EINVAL;
	}

    void *buf = custom_ice_mem_map_anom(NULL, len, pgsize, 0);
    if (buf ==  NULL) {
        NETPERF_WARN("mem_map_anom failed: resulting buffer is null: len %lu, pgsize %lu", len, pgsize);
        return -EINVAL;
    }

    if (buf == MAP_FAILED) {
        NETPERF_WARN("mem_map_anom failed: resulting buffer is 0xffffffffffffffff; len %lu, pgsize %lu num_pages %lu", len, pgsize, len / pgsize);
        return -EINVAL;
    }

    
	m->allocated = 0;
	m->buf = buf;
	m->len = len;
	m->pgsize = pgsize;
    m->num_pages = (len / pgsize);
	m->item_len = item_len;
    m->log_item_len = (size_t)(log2((float)item_len));

	return custom_ice_mempool_populate(m, buf, len, pgsize, item_len);
}

/**
 * mempool_destroy - tears down a memory pool
 * @m: the memory pool to tear down.
 * Note: if the memory pool was registered, remember to unregister the region
 * with the NIC.
 */
void custom_ice_mempool_destroy(struct custom_ice_mempool *m)
{
	free(m->free_items);
    munmap(m->buf, m->len);
}
