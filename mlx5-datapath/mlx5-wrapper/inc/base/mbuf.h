/*
 * mbuf.h - buffer management for network packets
 *
 * TODO: Maybe consider adding refcounts to mbuf's. Let's wait until this turns
 * out to be necessary.
 */

#pragma once

#include <limits.h>
#include <string.h>

#include <base/stddef.h>
#include <base/mempool.h>
#include <base/rte_memcpy.h>
#include <base/debug.h>

/* Mbuf data structure. Should be 64 byte aligned. */
struct __attribute__((packed, aligned(1))) custom_mlx5_mbuf {
    void *buf_addr; /* Data address of this mbuf */
    uint16_t refcnt; /* Reference count. Currently only supports single threaded code. */
    uint16_t nb_segs; /* Number of segments (if this is the head segment) in linked list.*/
    union {
        uint16_t tx_flags; /* Flags for transmission */
        uint16_t rss_hash; /* RSS result for received packets. */
    };
    uint16_t data_buf_len; /* Length of the data buffer. */
    uint16_t offset; /* Buffer offset (if any) from the beginning of the buffer. */
    uint16_t data_len; /* Length of data in this mbuf. */
    uint32_t pkt_len; /* Length of data across entire packet. */
    uint32_t num_wqes; /* Number of wqes this transmission used. Filled in at transmission. */
    uint32_t lkey; /* Lkey for mr region the data pointed by this mbuf points to. */
    struct custom_mlx5_mbuf *next; /* Pointer to the next mbuf in the list */
    struct custom_mlx5_mempool *metadata_mempool; /* Pointer to mempool where metadata was allocated */
    struct custom_mlx5_mempool *data_mempool; /* Pointer to mempool where data was allocated. If NULL, freeing back to metadata pool frees data. */
    struct custom_mlx5_mbuf *indirect_mbuf_ptr; /* This mbuf could just be "attached" to another mbuf, with a different offset. If null, not attached. */
};

#define custom_mlx5_mbuf_offset(mbuf, off, t) \
    (t)(custom_mlx5_mbuf_offset_ptr(mbuf, off))

static inline unsigned char *custom_mlx5_mbuf_offset_ptr(const struct custom_mlx5_mbuf *m, size_t off) {
    return (unsigned char *)m->buf_addr + m->offset + off;
}

static inline void custom_mlx5_mbuf_clear(struct custom_mlx5_mbuf *m) {
    m->buf_addr = NULL;
    m->refcnt = 0;
    m->nb_segs = 0;
    m->tx_flags = 0;
    m->data_buf_len = 0;
    m->offset = 0;
    m->data_len = 0;
    m->pkt_len = 0;
    m->num_wqes = 0;
    m->lkey = -1;
    m->next = NULL;
    m->metadata_mempool = NULL;
    m->data_mempool = NULL;
    m->indirect_mbuf_ptr = NULL;
}

/* Initializes an mbuf to attach to another mbuf. */
static inline void custom_mlx5_mbuf_init_external(struct custom_mlx5_mbuf *m,
                                        struct custom_mlx5_mempool *metadata_mempool,
                                        struct custom_mlx5_mbuf *indirect_mbuf,
                                        uint16_t offset,
                                        uint16_t len) {
    custom_mlx5_rte_memcpy((char *)m, (char *)indirect_mbuf, sizeof(struct custom_mlx5_mbuf));
    m->metadata_mempool = metadata_mempool;
    m->data_mempool = NULL;
    m->indirect_mbuf_ptr = indirect_mbuf;
    m->offset = offset;
    m->data_len = len;
    m->next = NULL;
}

/* Initializes an mbuf. */
static inline void custom_mlx5_mbuf_init(struct custom_mlx5_mbuf *m, 
                                void *data,  
                                struct custom_mlx5_mempool *metadata_mempool,
                                struct custom_mlx5_mempool *data_mempool) {
    custom_mlx5_mbuf_clear(m);
    m->buf_addr = data;
    m->metadata_mempool = metadata_mempool;
    if (data_mempool != NULL) {
        m->data_mempool = data_mempool;
        m->data_buf_len = data_mempool->item_len;
        m->lkey = data_mempool->lkey;
    }
}

/* Attaches the mbuf to an external buffer. */
static inline void custom_mlx5_mbuf_attach_external(struct custom_mlx5_mbuf *m,
                                            void *data,
                                            uint16_t data_offset,
                                            uint16_t data_len,
                                            struct custom_mlx5_mempool *data_mempool) {
    m->data_mempool = data_mempool;
    m->buf_addr = data;
    m->offset = data_offset;
    m->data_len = data_len;
    m->data_buf_len = data_mempool->item_len;
}

/* Returns the mbuf to the given memory pool(s). */    
static inline void custom_mlx5_mbuf_free(struct custom_mlx5_mbuf *m) {
    // Only free back to the data pool if:
    // 1. There is a data pointer.
    // 2. This is NOT an indirect mbuf.
    if (m->data_mempool != NULL && m->indirect_mbuf_ptr == NULL) {
        // calculate the index this mbuf is in the mempool
        int index = custom_mlx5_mempool_find_index(m->data_mempool, m->buf_addr);
        custom_mlx5_mempool_free(m->data_mempool, m->buf_addr);
        custom_mlx5_mempool_free_by_idx(m->metadata_mempool, (void *)m, (size_t)index);
        custom_mlx5_mempool_free(m->metadata_mempool, (void *)m);
    } else {
        custom_mlx5_mempool_free(m->metadata_mempool, (void *)m);
    }
}

static inline uint16_t custom_mlx5_mbuf_refcnt_read(struct custom_mlx5_mbuf *m) {
    if (m->indirect_mbuf_ptr != NULL) {
        return m->indirect_mbuf_ptr->refcnt;
    } else {
        return m->refcnt;
    }
}

/* Updates the ref count of the given mbuf. */
static inline void custom_mlx5_mbuf_refcnt_update(struct custom_mlx5_mbuf *m, int16_t change) {
    NETPERF_ASSERT(((int16_t)m->refcnt + change) >= 0, "Refcnt negative");
    m->refcnt += change;
}

/* Updates ref count of given mbuf and frees if refcnt has reached 0. */
static inline void custom_mlx5_direct_mbuf_refcnt_update_or_free(struct custom_mlx5_mbuf *m, int16_t change) {
    custom_mlx5_mbuf_refcnt_update(m, change);
    if (m->refcnt == 0) {
        custom_mlx5_mbuf_free(m);
    }
}

/* Updates ref count of mbuf and frees mbuf if ref count has reached 0.
 * For indirect mbufs, works on indirect pointer.
 * */
static inline void custom_mlx5_mbuf_refcnt_update_or_free(struct custom_mlx5_mbuf *m, int16_t change) {
    if (m->indirect_mbuf_ptr != NULL) {
        // update refcnt of underlying mbuf
        custom_mlx5_direct_mbuf_refcnt_update_or_free(m->indirect_mbuf_ptr, change);
        custom_mlx5_mbuf_free(m);
    } else {
        custom_mlx5_direct_mbuf_refcnt_update_or_free(m, change);
    }
}
