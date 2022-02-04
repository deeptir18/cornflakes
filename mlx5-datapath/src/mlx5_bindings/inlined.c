#include <base/mempool.h>
#include <base/mbuf.h>
#include <base/rte_memcpy.h>
#include <mlx5/mlx5.h>

static inline void *alloc_buf_(struct mempool *mempool) {
    return (void *)(mempool_alloc(mempool));
}

static inline struct mbuf *alloc_metadata_(struct mempool *mempool) {
    return (struct mbuf *)(mempool_alloc(mempool));
}

static inline void init_metadata_(struct mbuf *m, void *buf, struct mempool *data_mempool, struct mempool *metadata_mempool, size_t data_len, size_t offset) {
    mbuf_clear(m);
    m->buf_addr = buf;
    m->data_mempool = data_mempool;
    m->data_buf_len = data_mempool->item_len;
    m->lkey = data_mempool->lkey;
    m->metadata_mempool = metadata_mempool;
    m->data_len = data_len;
    m->offset = offset;
}

static inline struct mempool *get_data_mempool_(struct registered_mempool *mempool) {
    return (struct mempool *)(&(mempool->data_mempool));
}

static inline struct mempool *get_metadata_mempool_(struct registered_mempool *mempool) {
    return (struct mempool *)(&(mempool->metadata_mempool));
}

static inline void *mbuf_offset_ptr_(struct mbuf *mbuf, size_t off) {
    return (void *)mbuf_offset_ptr(mbuf, off);
}

static inline uint16_t mbuf_refcnt_read_(struct mbuf *mbuf) {
    return mbuf_refcnt_read(mbuf);
}

static inline void mbuf_refcnt_update_or_free_(struct mbuf *mbuf, int16_t change) {
    mbuf_refcnt_update_or_free(mbuf, change);
}

static inline void mbuf_free_(struct mbuf *mbuf) {
    mbuf_free(mbuf);
}

static inline struct mbuf *mbuf_at_index_(struct mempool *mempool, size_t index) {
    return (struct mbuf *)((char *)mempool->buf + index * mempool->item_len);
}
