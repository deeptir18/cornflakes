/*
 * mbuf - contains implementation around custom_ice_mbuf data structures
 * */
#include <limits.h>
#include <string.h>

#include <base/debug.h>
#include <base/mempool.h>
#include <base/stddef.h>
#include <base/mbuf.h>

void custom_ice_mbuf_clear(struct custom_ice_mbuf *m) {
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

void custom_ice_mbuf_init_external(struct custom_ice_mbuf *m,
                                        struct custom_ice_mempool *metadata_mempool,
                                        struct custom_ice_mbuf *indirect_mbuf,
                                        uint16_t offset,
                                        uint16_t len) {
    rte_memcpy((char *)m, (char *)indirect_mbuf, sizeof(struct custom_ice_mbuf));
    m->metadata_mempool = metadata_mempool;
    m->data_mempool = NULL;
    m->indirect_mbuf_ptr = indirect_mbuf;
    m->offset = offset;
    m->data_len = len;
    m->next = NULL;
}

/* Initializes an mbuf. */
void custom_ice_mbuf_init(struct custom_ice_mbuf *m, 
                                void *data,  
                                struct custom_ice_mempool *metadata_mempool,
                                struct custom_ice_mempool *data_mempool) {
    custom_ice_mbuf_clear(m);
    m->buf_addr = data;
    m->metadata_mempool = metadata_mempool;
    if (data_mempool != NULL) {
        m->data_mempool = data_mempool;
        m->data_buf_len = data_mempool->item_len;
    }
}

/* Attaches the mbuf to an external buffer. */
void custom_ice_mbuf_attach_external(struct custom_ice_mbuf *m,
                                            void *data,
                                            uint16_t data_offset,
                                            uint16_t data_len,
                                            struct custom_ice_mempool *data_mempool) {
    m->data_mempool = data_mempool;
    m->buf_addr = data;
    m->offset = data_offset;
    m->data_len = data_len;
    m->data_buf_len = data_mempool->item_len;
}

void custom_ice_mbuf_free(struct custom_ice_mbuf *m) {
    // Only free back to the data pool if:
    // 1. There is a data pointer.
    // 2. This is NOT an indirect mbuf.
    if (m->data_mempool != NULL && m->indirect_mbuf_ptr == NULL) {
        custom_ice_direct_mbuf_free(m);
    } else {
        custom_ice_mempool_free(m->metadata_mempool, (void *)m);
    }
}

uint16_t custom_ice_mbuf_refcnt_read(struct custom_ice_mbuf *m) {
    if (m->indirect_mbuf_ptr != NULL) {
        return m->indirect_mbuf_ptr->refcnt;
    } else {
        return m->refcnt;
    }
}

uint16_t custom_ice_mbuf_refcnt_update_or_free(struct custom_ice_mbuf *m, int16_t change) {
    NETPERF_INFO("In refcnt update with mbuf %p, change %d", m, change);
    if (m->indirect_mbuf_ptr != NULL) {
        // update refcnt of underlying mbuf
        uint16_t ret = custom_ice_direct_mbuf_refcnt_update_or_free(m->indirect_mbuf_ptr, change);
        // free the mbuf struct back to its mempool
        custom_ice_mempool_free(m->metadata_mempool, (void *)m);
        return ret;
    } else {
        return custom_ice_direct_mbuf_refcnt_update_or_free(m, change);
    }
}
