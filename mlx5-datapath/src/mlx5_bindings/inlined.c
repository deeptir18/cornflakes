#include <base/mempool.h>
#include <base/mbuf.h>
#include <base/rte_memcpy.h>
#include <base/time.h>
#include <mlx5/mlx5.h>
#include <mlx5/mlx5_runtime.h>
#include <net/ethernet.h>
#include <net/ip.h>
#include <net/udp.h>

void free_mbuf_(struct mbuf *metadata_mbuf) {
    mbuf_free(metadata_mbuf);
}

uint64_t ns_to_cycles_(uint64_t a) {
    return ns_to_cycles(a);
}

uint64_t cycles_to_ns_(uint64_t a) {
    return cycles_to_us(a);
}

uint64_t current_cycles_() {
    return microcycles();
}

void *alloc_data_buf_(struct registered_mempool *mempool) {
    return (void *)(mempool_alloc(&(mempool->data_mempool)));
}

struct mbuf *alloc_metadata_(struct registered_mempool *mempool, void *data_buf) {
    int index = mempool_find_index(&(mempool->data_mempool), data_buf);
    if (index == -1) {
        return NULL;
    } else {
        return (struct mbuf *)(mempool_alloc_by_idx(&(mempool->metadata_mempool), (size_t)index));
    }
    
}

void init_metadata_(struct mbuf *m, void *buf, struct mempool *data_mempool, struct mempool *metadata_mempool, size_t data_len, size_t offset) {
    mbuf_clear(m);
    m->buf_addr = buf;
    m->data_mempool = data_mempool;
    m->data_buf_len = data_mempool->item_len;
    m->lkey = data_mempool->lkey;
    m->metadata_mempool = metadata_mempool;
    m->data_len = data_len;
    m->offset = offset;
}

struct mempool *get_data_mempool_(struct registered_mempool *mempool) {
    return (struct mempool *)(&(mempool->data_mempool));
}

struct mempool *get_metadata_mempool_(struct registered_mempool *mempool) {
    return (struct mempool *)(&(mempool->metadata_mempool));
}

void *mbuf_offset_ptr_(struct mbuf *mbuf, size_t off) {
    return (void *)mbuf_offset_ptr(mbuf, off);
}

uint16_t mbuf_refcnt_read_(struct mbuf *mbuf) {
    return mbuf_refcnt_read(mbuf);
}

void mbuf_refcnt_update_or_free_(struct mbuf *mbuf, int16_t change) {
    mbuf_refcnt_update_or_free(mbuf, change);
}

void mbuf_free_(struct mbuf *mbuf) {
    mbuf_free(mbuf);
}

void mempool_free_(void *item, struct mempool *mempool) {
    mempool_free(mempool, item);
}

struct mbuf *mbuf_at_index_(struct mempool *mempool, size_t index) {
    return (struct mbuf *)((char *)mempool->buf + index * mempool->item_len);
}

void mlx5_rte_memcpy_(void *dst, const void *src, size_t n) {
    rte_memcpy(dst, src, n);
}

void fill_in_hdrs_(void *buffer, const void *hdr, uint32_t id, size_t data_len) {
    char *dst_ptr = buffer;
    const char *src_ptr = hdr;
    // copy ethernet header
    memcpy(dst_ptr, src_ptr, sizeof(struct eth_hdr));
    dst_ptr += sizeof(struct eth_hdr);
    src_ptr += sizeof(struct eth_hdr);

    // copy in the ipv4 header and reset the the data length and checksum
    memcpy(dst_ptr, src_ptr, sizeof(struct ip_hdr));
    struct ip_hdr *ip = (struct ip_hdr *)dst_ptr;
    dst_ptr += sizeof(struct ip_hdr);
    src_ptr += sizeof(struct ip_hdr);

     memcpy(dst_ptr, src_ptr, sizeof(struct udp_hdr));
     struct udp_hdr *udp = (struct udp_hdr *)dst_ptr;
     dst_ptr += sizeof(struct udp_hdr);

    *(uint32_t *)dst_ptr = id;

    ip->len = data_len;
    ip->chksum = 0;
    ip->chksum = get_chksum(ip);

     udp->len = data_len;
     udp->chksum = 0;
     udp->chksum = get_chksum(udp);

}

struct transmission_info *completion_start_(struct mlx5_per_thread_context *context) {
    return completion_start(&context->txq);
}

struct mlx5_wqe_data_seg *dpseg_start_(struct mlx5_per_thread_context *context, size_t inline_off) {
    return dpseg_start(&context->txq, inline_off);
}

void flip_headers_mlx5_(struct mbuf *metadata_mbuf) {
    // flips all headers in this packet's data
    struct eth_hdr *eth = mbuf_offset(metadata_mbuf, 0, struct eth_hdr *);
    struct ip_hdr *ip = mbuf_offset(metadata_mbuf, sizeof(struct eth_hdr), struct ip_hdr *);
    struct udp_hdr *udp = mbuf_offset(metadata_mbuf, sizeof(struct eth_hdr) + sizeof(struct ip_hdr), struct udp_hdr *);
    
    struct eth_addr tmp;
    mlx5_rte_memcpy(&tmp, &eth->dhost, sizeof(struct eth_addr));
    mlx5_rte_memcpy(&eth->dhost, &eth->shost, sizeof(struct eth_addr));
    mlx5_rte_memcpy(&eth->shost, &tmp, sizeof(struct eth_addr));
    
    uint32_t tmp_ip = ip->daddr;
    ip->daddr = ip->saddr;
    ip->saddr = tmp_ip;
    ip->chksum = 0;
    ip->chksum = get_chksum(ip);

    uint16_t tmp_udp = udp->dst_port;
    udp->dst_port = udp->src_port;
    udp->src_port = tmp_udp;
    udp->chksum = 0;
    udp->chksum = get_chksum(udp);
}
