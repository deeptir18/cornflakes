#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_errno.h>
#include <rte_cycles.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <ctype.h>
#include <stdint.h>

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

struct rte_mbuf* rte_pktmbuf_alloc_(struct rte_mempool *mp) {
    return rte_pktmbuf_alloc(mp);
}

uint16_t rte_eth_tx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
    return rte_eth_tx_burst(port_id, queue_id, tx_pkts, nb_pkts);
}

uint16_t rte_eth_rx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **rx_pkts, const uint16_t nb_pkts) {
    uint16_t ret = rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts);
    return ret;
}

int rte_errno_() {
    return rte_errno;
}

uint64_t rte_get_timer_cycles_() {
    return rte_get_timer_cycles();
}

uint64_t rte_get_timer_hz_() {
    return rte_get_timer_hz();
}

void rte_pktmbuf_attach_extbuf_(struct rte_mbuf *m, void *buf_addr, rte_iova_t buf_iova, uint16_t buf_len, struct rte_mbuf_ext_shared_info *shinfo) {
    rte_pktmbuf_attach_extbuf(m, buf_addr, buf_iova, buf_len, shinfo);
}

void general_free_cb_(void  *addr, void *opaque) {}
