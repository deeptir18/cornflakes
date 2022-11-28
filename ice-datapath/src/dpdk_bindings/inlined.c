#include <rte_mbuf.h>
#include <rte_errno.h>

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

void rte_pktmbuf_refcnt_update_or_free_(struct rte_mbuf *packet,int16_t val) {
    //printf("[rte_mbuf_refcnt_update_or_free_] Changing refcnt of mbuf %p by val %d; currently %d\n", packet, val, rte_mbuf_refcnt_read(packet));
    rte_mbuf_refcnt_update(packet, val);
    uint16_t cur_rc = rte_mbuf_refcnt_read(packet);
    if (cur_rc == 0) {
        rte_pktmbuf_free(packet);
    }
    return;
}

void rte_pktmbuf_refcnt_set_(struct rte_mbuf *packet, uint16_t val) {
    rte_mbuf_refcnt_set(packet, val);
}

int rte_errno_() {
    return rte_errno;
}

