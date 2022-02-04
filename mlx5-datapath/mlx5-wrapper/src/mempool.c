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

#include <base/debug.h>
#include <base/mempool.h>
#include <base/mem.h>
#include <sys/mman.h>

#ifdef DEBUG

static void mempool_common_check(struct mempool *m, void *item)
{
	uintptr_t pos = (uintptr_t)item;
	uintptr_t start = (uintptr_t)m->buf;

	/* is the item within the bounds of the pool */
	assert(pos >= start && pos < start + m->len);

	/* is the item properly aligned */
	assert((start & (m->pgsize - 1)) % m->item_len == 0);
}

void __mempool_alloc_debug_check(struct mempool *m, void *item)
{
	mempool_common_check(m, item);

	/* poison the item */
	memset(item, 0xAB, m->item_len);
}

void __mempool_free_debug_check(struct mempool *m, void *item)
{
	mempool_common_check(m, item);

	/* poison the item */
	memset(item, 0xCD, m->item_len);
}

#endif /* DEBUG */

static int mempool_populate(struct mempool *m, void *buf, size_t len,
			    size_t pgsize, size_t item_len)
{
	size_t items_per_page = pgsize / item_len;
	size_t nr_pages = len / pgsize;
	int i, j;

    NETPERF_DEBUG("Items per page: %u, # pages: %u", (unsigned)items_per_page, (unsigned)nr_pages);
	m->free_items = calloc(nr_pages * items_per_page, sizeof(void *));
	if (!m->free_items) {
        NETPERF_DEBUG("Calloc didn't allocate free items list.");
		return -ENOMEM;
    }

	for (i = 0; i < nr_pages; i++) {
		for (j = 0; j < items_per_page; j++) {
			m->free_items[m->capacity++] =
				(char *)buf + pgsize * i + item_len * j;
		}
	}

	return 0;
}

/**
 * mempool_create - initializes a memory pool
 * @m: the memory pool to initialize
 * @len: the length of the buffer region managed by the pool
 * @pgsize: the size of the pages in the buffer region (must be uniform)
 * @item_len: the length of each item in the pool
 */
int mempool_create(struct mempool *m, size_t len,
		   size_t pgsize, size_t item_len)
{
	if (item_len == 0 || !is_power_of_two(pgsize) || len % pgsize != 0) {
        NETPERF_DEBUG("Invalid params to create mempool.");
		return -EINVAL;
	}

    void *buf = mem_map_anom(NULL, len, pgsize, 0);
    if (buf ==  NULL) {
        NETPERF_DEBUG("mem_map_anom failed: resulting buffer is null.");
        return -EINVAL;
    }

    
	m->allocated = 0;
	m->buf = buf;
	m->len = len;
	m->pgsize = pgsize;
	m->item_len = item_len;

	return mempool_populate(m, buf, len, pgsize, item_len);
}

/**
 * mempool_destroy - tears down a memory pool
 * @m: the memory pool to tear down.
 * Note: if the memory pool was registered, remember to unregister the region
 * with the NIC.
 */
void mempool_destroy(struct mempool *m)
{
	free(m->free_items);
    munmap(m->buf, m->len);
}
