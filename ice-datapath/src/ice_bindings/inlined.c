#include <net/ethernet.h>
#include <net/ip.h>
#include <net/udp.h>
#include <rte_memcpy.h>
#include <rte_ethdev.h>
#define IP_DEFTTL  64   /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN  0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)
#define RX_PACKET_LEN 9216
#define MBUF_HEADER_SIZE 64
#define MBUF_PRIV_SIZE 8
#define PGSIZE_2MB (1 <<  21)
#define MAX_CORES 6

struct tx_pktmbuf_priv {
    uint32_t lkey;
    uint16_t lkey_present;
    uint16_t refers_to_another;
};
struct tx_pktmbuf_priv *tx_pktmbuf_get_priv(struct rte_mbuf *buf) {
    	struct tx_pktmbuf_priv *priv = (struct tx_pktmbuf_priv *)(((char *)buf)
			+ sizeof(struct rte_mbuf));
        //printf("[tx_pktmbuf_get_priv] addr of mbuf: %p, addr: of priv: %p\n", buf, priv);
        // printf("[tx_pktmbuf_get_priv] priv lkey: %u, priv lkey present: %u, priv lkey refers to another: %u, size of struct: %u\n", (unsigned)priv->lkey, (unsigned)priv->lkey_present, (unsigned)priv->refers_to_another, (unsigned)sizeof(struct tx_pktmbuf_priv));
        return priv;
}

static uint8_t sym_rss_key[] = {
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
};

void custom_init_priv_(struct rte_mempool *mp, void *opaque_arg, void *m, unsigned m_idx) {
    struct rte_mbuf *buf = m;
    struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(buf);
    memset(data, 0, sizeof(*data));
}

void eth_dev_configure_ice_(uint16_t port_id, uint16_t rx_rings, uint16_t tx_rings) {
    uint16_t mtu;
    struct rte_eth_dev_info dev_info = {};
    rte_eth_dev_info_get(port_id, &dev_info);
    rte_eth_dev_set_mtu(port_id, RX_PACKET_LEN);
    rte_eth_dev_get_mtu(port_id, &mtu);
    fprintf(stderr, "Dev info MTU:%u\n", mtu);
    struct rte_eth_conf port_conf = {};
    port_conf.rxmode.max_lro_pkt_size = RX_PACKET_LEN;

    //port_conf.rxmode.offloads = RTE_ETH_RX_OFFLOAD_IPV4_CKSUM;
    //port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS | RTE_ETH_MQ_RX_RSS_FLAG;
    //port_conf.rx_adv_conf.rss_conf.rss_key = sym_rss_key;
    //port_conf.rx_adv_conf.rss_conf.rss_key_len = 40;
    //port_conf.rx_adv_conf.rss_conf.rss_hf =  RTE_ETH_RSS_NONFRAG_IPV4_UDP;
    port_conf.txmode.offloads = RTE_ETH_TX_OFFLOAD_IPV4_CKSUM | RTE_ETH_TX_OFFLOAD_UDP_CKSUM;
    port_conf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;

    printf("port_id: %u, rx_rings; %u, tx_rings: %u\n", port_id, rx_rings, tx_rings);
    rte_eth_dev_configure(port_id, rx_rings, tx_rings, &port_conf);
}


void custom_ice_fill_in_hdrs_(void *buffer, const void *hdr, uint32_t id, size_t data_len) {
    char *dst_ptr = buffer;
    const char *src_ptr = hdr;
    // copy ethernet header
    rte_memcpy(dst_ptr, src_ptr, sizeof(struct eth_hdr));
    dst_ptr += sizeof(struct eth_hdr);
    src_ptr += sizeof(struct eth_hdr);

    // copy in the ipv4 header and reset the the data length and checksum
    rte_memcpy(dst_ptr, src_ptr, sizeof(struct ip_hdr));
    struct ip_hdr *ip = (struct ip_hdr *)dst_ptr;
    dst_ptr += sizeof(struct ip_hdr);
    src_ptr += sizeof(struct ip_hdr);

     rte_memcpy(dst_ptr, src_ptr, sizeof(struct udp_hdr));
     struct udp_hdr *udp = (struct udp_hdr *)dst_ptr;
     dst_ptr += sizeof(struct udp_hdr);

    *((uint32_t *)dst_ptr) = id;

    ip->len = htons(sizeof(struct ip_hdr) + sizeof(struct udp_hdr) + 4 + data_len);
    ip->chksum = 0;
    ip->chksum = custom_ice_get_chksum(ip);
    
    udp->len = htons(sizeof(struct udp_hdr) + 4 + data_len);
    udp->chksum = 0;
    udp->chksum = custom_ice_get_chksum(udp);

}

