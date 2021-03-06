#include <ctype.h>
#include <inttypes.h>
#include <rte_cycles.h>
#include <rte_dev.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_malloc.h>
#include <rte_mbuf.h>
#include <rte_memcpy.h>
#include <rte_udp.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define IP_DEFTTL  64   /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN  0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)
#define RX_PACKET_LEN 9216

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

struct rte_mbuf* rte_pktmbuf_alloc_(struct rte_mempool *mp) {
    return rte_pktmbuf_alloc(mp);
}

uint16_t rte_eth_tx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
    /*struct rte_mbuf *first_mbuf = tx_pkts[0];
    printf("[rte_eth_tx_burst_] mbuf num segs: %u\n", first_mbuf->nb_segs);
    printf("[rte_eth_tx_burst_] mbuf data_len: %u, pkt_len: %u\n", first_mbuf->data_len, first_mbuf->pkt_len);
    printf("[rte_eth_tx_burst_] mbuf next is null: %d\n", (first_mbuf->next == NULL));
    uint8_t *p = rte_pktmbuf_mtod(first_mbuf, uint8_t *);
    struct rte_ether_hdr * const eth_hdr = (struct rte_ether_hdr *)(p);
    if (eth_hdr->ether_type != ntohs(RTE_ETHER_TYPE_IPV4)) {
        printf("[rte_eth_tx_burst_] Ether type is not RTE_ETHER_TYPE_IPV4\n");
    }
    printf("[rte_eth_tx_burst_] Src MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
            " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->s_addr.addr_bytes[0], eth_hdr->s_addr.addr_bytes[1],
            eth_hdr->s_addr.addr_bytes[2], eth_hdr->s_addr.addr_bytes[3],
            eth_hdr->s_addr.addr_bytes[4], eth_hdr->s_addr.addr_bytes[5]);
    printf("[rte_eth_tx_burst_] Dst MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
            " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->d_addr.addr_bytes[0], eth_hdr->d_addr.addr_bytes[1],
            eth_hdr->d_addr.addr_bytes[2], eth_hdr->d_addr.addr_bytes[3],
            eth_hdr->d_addr.addr_bytes[4], eth_hdr->d_addr.addr_bytes[5]);*/
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

void rte_memcpy_(void *dst, const void *src, size_t n) {
    rte_memcpy(dst, src, n);
}

int rte_dev_dma_map_(uint16_t device_id, void * addr, uint64_t iova, size_t len) {
    struct rte_eth_dev *dev = &rte_eth_devices[device_id];
    return rte_dev_dma_map(dev->device, addr, iova, len);
}

int rte_dev_dma_unmap_(uint16_t device_id, void *addr, uint64_t iova, size_t len) {
    struct rte_eth_dev *dev = &rte_eth_devices[device_id];
    return rte_dev_dma_unmap(dev->device, addr, iova, len);
}

void custom_init_(struct rte_mempool *mp, void *opaque_arg, void *m, unsigned i) {
    struct rte_mbuf *pkt = (struct rte_mbuf *)(m);
    uint8_t *p = rte_pktmbuf_mtod(pkt, uint8_t *);
    // p += 46;
    char *s = (char *)(p);
    memset(s, 'a', 1024);
}

uint32_t make_ip_(uint8_t a, uint8_t b, uint8_t c, uint8_t d) {
    return (((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d);
}

size_t fill_in_packet_header_(struct rte_mbuf *mbuf, struct rte_ether_addr *my_eth, struct rte_ether_addr *dst_eth, uint32_t my_ip, uint32_t dst_ip, uint16_t udp_port, size_t message_size) {
    size_t header_size = 0;
    uint8_t *ptr = rte_pktmbuf_mtod(mbuf, uint8_t *);
    struct rte_ether_hdr *eth_hdr = (struct rte_ether_hdr *)ptr;
    rte_ether_addr_copy(my_eth, &eth_hdr->s_addr);
    rte_ether_addr_copy(dst_eth, &eth_hdr->d_addr);
    eth_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

    ptr += sizeof(*eth_hdr);
    header_size += sizeof(*eth_hdr);

    /* add in ipv4 header*/
    struct rte_ipv4_hdr *ipv4_hdr = (struct rte_ipv4_hdr *)ptr;
    ipv4_hdr->version_ihl = IP_VHL_DEF;
    ipv4_hdr->type_of_service = 0;
    ipv4_hdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + message_size);
    ipv4_hdr->packet_id = 0;
    ipv4_hdr->fragment_offset = 0;
    ipv4_hdr->time_to_live = IP_DEFTTL;
    ipv4_hdr->next_proto_id = IPPROTO_UDP;
    ipv4_hdr->src_addr = rte_cpu_to_be_32(my_ip);
    ipv4_hdr->dst_addr = rte_cpu_to_be_32(dst_ip);
    /* offload checksum computation in hardware */
    ipv4_hdr->hdr_checksum = 0;
    header_size += sizeof(*ipv4_hdr);
    ptr += sizeof(*ipv4_hdr);

    /* add in udp header */
    struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)ptr;
    udp_hdr->src_port = rte_cpu_to_be_16(udp_port);
    udp_hdr->dst_port = rte_cpu_to_be_16(udp_port);
    udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + message_size);
    udp_hdr->dgram_cksum = 0;
    ptr += sizeof(*udp_hdr);
    header_size += sizeof(*udp_hdr);

    mbuf->l2_len = RTE_ETHER_HDR_LEN;
    mbuf->l3_len = sizeof(struct rte_ipv4_hdr);
    mbuf->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
    return header_size;
}

bool parse_packet_(struct rte_mbuf *mbuf, size_t *payload_len, const struct rte_ether_addr *our_eth, uint32_t our_ip) {
    const struct rte_ether_addr ether_broadcast = {
        .addr_bytes = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
    };
    size_t header_size = 0;
    uint8_t *ptr = rte_pktmbuf_mtod(mbuf, uint8_t *);
    struct rte_ether_hdr *eth_hdr = (struct rte_ether_hdr *)ptr;
    
    ptr += sizeof(*eth_hdr);
    header_size += sizeof(*eth_hdr);
    uint16_t eth_type = ntohs(eth_hdr->ether_type);
    if (!rte_is_same_ether_addr(our_eth, &eth_hdr->d_addr) && !rte_is_same_ether_addr(&ether_broadcast, &eth_hdr->d_addr)) {
        printf("Bad MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->d_addr.addr_bytes[0], eth_hdr->d_addr.addr_bytes[1],
			eth_hdr->d_addr.addr_bytes[2], eth_hdr->d_addr.addr_bytes[3],
			eth_hdr->d_addr.addr_bytes[4], eth_hdr->d_addr.addr_bytes[5]);
        return false;
    }
    if (RTE_ETHER_TYPE_IPV4 != eth_type) {
        printf("Bad ether type");
        return false;
    }

    // check the IP header
    struct rte_ipv4_hdr *const ip_hdr = (struct rte_ipv4_hdr *)(ptr);
    ptr += sizeof(*ip_hdr);
    header_size += sizeof(*ip_hdr);

    // In network byte order.
    if (ip_hdr->dst_addr != rte_cpu_to_be_32(our_ip)) {
        printf("Bad dst ip addr; expected: %u, got: %u, our_ip: %u\n", (unsigned)(ip_hdr->dst_addr), (unsigned)(rte_cpu_to_be_32(our_ip)), (unsigned)(our_ip));
        return false;    
    }

    if (IPPROTO_UDP != ip_hdr->next_proto_id) {
        printf("Bad next proto_id\n");
        return false;
    }
    
    struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)(ptr);
    ptr += sizeof(*udp_hdr);
    header_size += sizeof(*udp_hdr);

    *payload_len = mbuf->pkt_len - header_size;
    // printf("[parse_packet_] Received packet with %u pkt_len, %u data_Len, %u header_size, set payload_len to %u\n", (unsigned)mbuf->pkt_len, (unsigned)mbuf->data_len, (unsigned)header_size, (unsigned)*payload_len);
    return true;
}

void switch_headers_(struct rte_mbuf *rx_buf, struct rte_mbuf *tx_buf, size_t payload_length) {
    /* swap src and dst ether addresses */
    struct rte_ether_hdr *rx_ptr_mac_hdr = rte_pktmbuf_mtod(rx_buf, struct rte_ether_hdr *);
    struct rte_ether_hdr *tx_ptr_mac_hdr = rte_pktmbuf_mtod(tx_buf, struct rte_ether_hdr *);
    rte_ether_addr_copy(&rx_ptr_mac_hdr->s_addr, &tx_ptr_mac_hdr->d_addr);
	rte_ether_addr_copy(&rx_ptr_mac_hdr->d_addr, &tx_ptr_mac_hdr->s_addr);
    tx_ptr_mac_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

    /* swap src and dst ip addresses */
    struct rte_ipv4_hdr *rx_ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
    struct rte_ipv4_hdr *tx_ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(tx_buf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
    tx_ptr_ipv4_hdr->src_addr = rx_ptr_ipv4_hdr->dst_addr;
    tx_ptr_ipv4_hdr->dst_addr = rx_ptr_ipv4_hdr->src_addr;

    tx_ptr_ipv4_hdr->hdr_checksum = 0;
    tx_ptr_ipv4_hdr->version_ihl = IP_VHL_DEF;
    tx_ptr_ipv4_hdr->type_of_service = 0;
    tx_ptr_ipv4_hdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + payload_length);
    tx_ptr_ipv4_hdr->packet_id = 0;
    tx_ptr_ipv4_hdr->fragment_offset = 0;
    tx_ptr_ipv4_hdr->time_to_live = IP_DEFTTL;
    tx_ptr_ipv4_hdr->next_proto_id = IPPROTO_UDP;
    /* offload checksum computation in hardware */
    tx_ptr_ipv4_hdr->hdr_checksum = 0;

    /* Swap UDP ports */
    struct rte_udp_hdr *rx_rte_udp_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
    struct rte_udp_hdr *tx_rte_udp_hdr = rte_pktmbuf_mtod_offset(tx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
    tx_rte_udp_hdr->src_port = rx_rte_udp_hdr->dst_port;
    tx_rte_udp_hdr->dst_port = rx_rte_udp_hdr->src_port;
    tx_rte_udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_length);
    tx_rte_udp_hdr->dgram_cksum = 0;

    /* Set packet metadata */
    tx_buf->l2_len = RTE_ETHER_HDR_LEN;
    tx_buf->l3_len = sizeof(struct rte_ipv4_hdr);
    tx_buf->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
}

struct rte_mbuf_ext_shared_info *shinfo_init_(void *addr, uint16_t *buf_len) {
    return rte_pktmbuf_ext_shinfo_init_helper(addr, buf_len, general_free_cb_, NULL);
}

void eth_dev_configure_(uint16_t port_id, uint16_t rx_rings, uint16_t tx_rings) {
    uint16_t mtu;
    struct rte_eth_dev_info dev_info = {};
    rte_eth_dev_info_get(port_id, &dev_info);
    rte_eth_dev_set_mtu(port_id, RX_PACKET_LEN);
    rte_eth_dev_get_mtu(port_id, &mtu);
    fprintf(stderr, "Dev info MTU:%u\n", mtu);
    struct rte_eth_conf port_conf = {};
    port_conf.rxmode.max_rx_pkt_len = RX_PACKET_LEN;

    port_conf.rxmode.offloads = DEV_RX_OFFLOAD_JUMBO_FRAME | DEV_RX_OFFLOAD_TIMESTAMP;
    port_conf.txmode.offloads = DEV_TX_OFFLOAD_MULTI_SEGS | DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_UDP_CKSUM;
    port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;

    rte_eth_dev_configure(port_id, rx_rings, tx_rings, &port_conf);
}

int loop_in_c_(uint16_t port,
               const struct rte_ether_addr * my_eth,
               uint32_t my_ip,
               struct rte_mbuf **rx_bufs,
               struct rte_mbuf **tx_bufs, 
               struct rte_mbuf **secondary_mbufs, 
               struct rte_mempool *mbuf_pool, 
               struct rte_mempool *header_mbuf_pool, 
               struct rte_mempool *extbuf_mempool, 
               size_t num_mbufs, 
               size_t split_payload, 
               bool zero_copy,
               bool use_external, 
               struct rte_mbuf_ext_shared_info *shinfo, 
               void *ext_mem_addr) {
    uint32_t burst_size = 32;

    while (true) {
        uint16_t num_received = rte_eth_rx_burst(port, 0, rx_bufs, burst_size);
        size_t num_valid = 0;
        for (size_t i = 0; i < (size_t)num_received; i++) {
            size_t n_to_tx = i;
            // check if packet is valid and what payload size is
            size_t payload_length = 0;
            if (!parse_packet_(rx_bufs[n_to_tx], &payload_length, my_eth, my_ip)) {
                rte_pktmbuf_free(rx_bufs[n_to_tx]);
                continue;
            }
            num_valid++;
            size_t header_size = rx_bufs[n_to_tx]->pkt_len - payload_length;
            
            if (!use_external) {
                if (num_mbufs == 2) {
                    tx_bufs[n_to_tx] = rte_pktmbuf_alloc(header_mbuf_pool);
                    secondary_mbufs[n_to_tx] = rte_pktmbuf_alloc(mbuf_pool);
                    if (tx_bufs[n_to_tx] == NULL || secondary_mbufs[n_to_tx] == NULL) {
                        printf("[loop_in_c_]: Not able to alloc tx_bufs[%u] or secondary_mbufs[%u]\n", (unsigned)i, (unsigned)i);
                    }
                } else if (num_mbufs == 1) {
                    tx_bufs[n_to_tx] = rte_pktmbuf_alloc(mbuf_pool);
                    printf("[loop_in_c_]: Not able to alloc tx_bufs[%u]\n", (unsigned)i);
                } else {
                    if (tx_bufs[n_to_tx] == NULL || secondary_mbufs[n_to_tx] == NULL) { 
                        printf("[loop_in_c_]: Num mbufs cannot be anything other than 2 or 1: %u\n", (unsigned)num_mbufs);
                    }
                    exit(1);
                }
            } else {
                if (num_mbufs == 2) {
                    tx_bufs[n_to_tx] = rte_pktmbuf_alloc(mbuf_pool);
                    secondary_mbufs[n_to_tx] = rte_pktmbuf_alloc(extbuf_mempool);
                    if (tx_bufs[n_to_tx] == NULL || secondary_mbufs[n_to_tx] == NULL) {
                        printf("[loop_in_c_]: Not able to alloc tx_bufs[%u] or extbuf_mempool[%u]\n", (unsigned)i, (unsigned)i);
                    }
                    rte_pktmbuf_attach_extbuf(secondary_mbufs[n_to_tx], ext_mem_addr, 0, payload_length + header_size, shinfo);
                } else {
                    printf("[loop_in_c_]: For external memory, loop_in_c_ only supports two mbufs.\n");
                }
            }
            struct rte_mbuf *tx_buf = tx_bufs[n_to_tx];
            struct rte_mbuf *rx_buf = rx_bufs[n_to_tx];
            struct rte_mbuf *secondary_tx = secondary_mbufs[n_to_tx];

            // switch headers and timestamps
            switch_headers_(rx_buf, tx_buf, payload_length);
            char *timestamp_rx = rte_pktmbuf_mtod_offset(rx_buf, char *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
            char *timestamp_tx = rte_pktmbuf_mtod_offset(tx_buf, char *, sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
            rte_memcpy(timestamp_tx, timestamp_rx, 8);

            // add in mbuf metadata
            if (num_mbufs == 2) {
                tx_buf->next = secondary_tx;
                tx_buf->data_len = rx_buf->pkt_len - payload_length + 8 + split_payload;
                tx_buf->pkt_len = rx_buf->pkt_len;
                secondary_tx->data_len  = payload_length - 8 - split_payload;
                tx_buf->nb_segs = 2;
            } else {
                tx_buf->pkt_len = rx_buf->pkt_len;
                tx_buf->data_len = rx_buf->data_len;
                tx_buf->next = NULL;
                tx_buf->nb_segs = 1;
            }

            rte_pktmbuf_free(rx_bufs[n_to_tx]);
        }
        if (num_valid > 0) {
            uint16_t nb_recv = 0;
            while ((size_t)nb_recv < num_valid) {
                nb_recv = rte_eth_tx_burst(port, 0, tx_bufs, num_valid);
            }
        }
    }
    return 0;
}

void copy_payload_(struct rte_mbuf *src_mbuf,
                   size_t src_offset, 
                   struct rte_mbuf *dst_mbuf,
                   size_t dst_offset,
                   size_t len) {
    char *rx_slice = rte_pktmbuf_mtod_offset(src_mbuf, char *, src_offset);
    char *tx_slice = rte_pktmbuf_mtod_offset(dst_mbuf, char *, dst_offset);
    rte_memcpy(tx_slice, rx_slice, len);
}


