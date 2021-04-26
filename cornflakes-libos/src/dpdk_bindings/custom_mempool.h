#ifndef CUSTOM_MEMPOOL_H_
#define CUSTOM_MEMPOOL_H_
#include <rte_mempool.h>

struct completion_stack {
	uint32_t size;
	uint32_t len;
	void *objs[];
};

void free_referred_mbuf(void *buf);

int custom_extbuf_enqueue(struct rte_mempool *mp, void * const *obj_table, unsigned n);

int custom_extbuf_dequeue(struct rte_mempool *mp, void **obj_table, unsigned n);

unsigned custom_extbuf_get_count(const struct rte_mempool *mp);

int custom_extbuf_alloc(struct rte_mempool *mp);

void custom_extbuf_free(struct rte_mempool *mp);

#endif
