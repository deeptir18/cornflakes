#include <rte_mbuf.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_ip.h>

void flip_headers_(struct rte_mbuf *mbuf ) {
	struct rte_ether_hdr *ptr_mac_hdr;
	struct rte_ether_addr src_addr;
	struct rte_ipv4_hdr *ptr_ipv4_hdr;
	uint32_t src_ip_addr;
	struct rte_udp_hdr *rte_udp_hdr;
	uint16_t tmp_port;
    
    /* swap src and dst ether addresses */
    ptr_mac_hdr = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
    rte_ether_addr_copy(&ptr_mac_hdr->src_addr, &src_addr);
	rte_ether_addr_copy(&ptr_mac_hdr->dst_addr, &ptr_mac_hdr->src_addr);
	rte_ether_addr_copy(&src_addr, &ptr_mac_hdr->dst_addr);


	/* swap src and dst IP addresses */
	ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(mbuf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
	src_ip_addr = ptr_ipv4_hdr->src_addr;
	ptr_ipv4_hdr->src_addr = ptr_ipv4_hdr->dst_addr;
	ptr_ipv4_hdr->dst_addr = src_ip_addr;
    ptr_ipv4_hdr->hdr_checksum = 0;
    ptr_ipv4_hdr->hdr_checksum = rte_ipv4_cksum(ptr_ipv4_hdr);

	/* swap UDP ports */
	rte_udp_hdr = rte_pktmbuf_mtod_offset(mbuf, struct rte_udp_hdr *,
                                            RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
	tmp_port = rte_udp_hdr->src_port;
	rte_udp_hdr->src_port = rte_udp_hdr->dst_port;
	rte_udp_hdr->dst_port = tmp_port;
    rte_udp_hdr->dgram_cksum = 0;
    rte_udp_hdr->dgram_cksum = rte_ipv4_udptcp_cksum(ptr_ipv4_hdr, (void *)rte_udp_hdr);

    mbuf->l2_len = RTE_ETHER_HDR_LEN;
	mbuf->l3_len = sizeof(struct rte_ipv4_hdr);
}

uint16_t rte_eth_rx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **rx_pkts, const uint16_t nb_pkts) {
    uint16_t ret = rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts);
    return ret;
}

uint16_t rte_eth_tx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
    /*for (uint16_t i = 0; i < nb_pkts; i++) {
        struct rte_mbuf *first_mbuf = tx_pkts[i];
        printf("First packet addr: %p\n", first_mbuf);
        printf("[rte_eth_tx_burst_] first mbuf num segs: %u\n", first_mbuf->nb_segs);
        printf("[rte_eth_tx_burst_] first mbuf data_len: %u, pkt_len: %u\n", first_mbuf->data_len, first_mbuf->pkt_len);
        printf("[rte_eth_tx_burst_] first mbuf next is null: %d\n", (first_mbuf->next == NULL));
        struct rte_mbuf *cur_pkt = first_mbuf;
        for (uint16_t j = 1; j < first_mbuf->nb_segs; j++) {
            cur_pkt = cur_pkt->next;
            printf("[rte_eth_tx_burst_] mbuf # %u, addr: %p\n", (unsigned)j, cur_pkt);
            printf("[rte_eth_tx_burst_]  mbuf # %u data_len: %u, pkt_len: %u\n", (unsigned)j, cur_pkt->data_len, cur_pkt->pkt_len);
            printf("[rte_eth_tx_burst_] mbuf # %u next is null: %d\n", (unsigned)j, (cur_pkt->next == NULL));
        }
        uint8_t *p = rte_pktmbuf_mtod(first_mbuf, uint8_t *);
        struct rte_ether_hdr * const eth_hdr = (struct rte_ether_hdr *)(p);
        struct rte_ipv4_hdr *const ipv4 = (struct rte_ipv4_hdr *)(p + sizeof(struct rte_ether_hdr));
        struct rte_udp_hdr *const udp = (struct rte_udp_hdr *)(p + sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr));
        uint32_t *id_ptr = (uint32_t *)(p + sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
        if (eth_hdr->ether_type != ntohs(RTE_ETHER_TYPE_IPV4)) {
            printf("[rte_eth_tx_burst_] Ether type is not RTE_ETHER_TYPE_IPV4\n");
        }
        printf("[rte_eth_tx_burst_] Src MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
            " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->src_addr.addr_bytes[0], eth_hdr->src_addr.addr_bytes[1],
            eth_hdr->src_addr.addr_bytes[2], eth_hdr->src_addr.addr_bytes[3],
            eth_hdr->src_addr.addr_bytes[4], eth_hdr->src_addr.addr_bytes[5]);
        printf("[rte_eth_tx_burst_] Dst MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
            " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->dst_addr.addr_bytes[0], eth_hdr->dst_addr.addr_bytes[1],
            eth_hdr->dst_addr.addr_bytes[2], eth_hdr->dst_addr.addr_bytes[3],
            eth_hdr->dst_addr.addr_bytes[4], eth_hdr->dst_addr.addr_bytes[5]);
        printf("[rte_eth_tx_burst_] Queue: %u, Scp IP: %u, dst IP: %u, checksum: %u, udp data len: %u, ID: %u\n", queue_id, ipv4->src_addr, ipv4->dst_addr, ipv4->hdr_checksum, ntohs(udp->dgram_len), *id_ptr);
    }*/
    return rte_eth_tx_burst(port_id, queue_id, tx_pkts, nb_pkts);
}

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

void rte_pktmbuf_refcnt_update_or_free_(struct rte_mbuf *packet,int16_t val) {
    uint16_t cur_rc = rte_mbuf_refcnt_read(packet);
    if ((cur_rc + val) == 0) {
        rte_pktmbuf_free(packet);
    } else {
        rte_mbuf_refcnt_update(packet, val);
    }
    return;
}

void rte_pktmbuf_refcnt_set_(struct rte_mbuf *packet, uint16_t val) {
    rte_mbuf_refcnt_set(packet, val);
}

int rte_errno_() {
    return rte_errno;
}

