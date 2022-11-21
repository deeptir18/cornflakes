#include <base/mempool.h>
#include <base/mbuf.h>
#include <base/rte_memcpy.h>
#include <base/time.h>
#include <mlx5/mlx5.h>
#include <mlx5/mlx5_runtime.h>
#include <net/ethernet.h>
#include <net/ip.h>
#include <net/udp.h>

void custom_mlx5_free_mbuf_(struct custom_mlx5_mbuf *metadata_mbuf) {
    custom_mlx5_mbuf_free(metadata_mbuf);
}

uint64_t ns_to_cycles_(uint64_t a) {
    return custom_mlx5_ns_to_cycles(a);
}

uint64_t cycles_to_ns_(uint64_t a) {
    return custom_mlx5_cycles_to_us(a);
}

uint64_t current_cycles_() {
    return custom_mlx5_microcycles();
}

void *alloc_data_buf_(struct registered_mempool *mempool) {
    return (void *)(custom_mlx5_mempool_alloc(&(mempool->data_mempool)));
}

struct custom_mlx5_mempool *get_data_mempool_(struct registered_mempool *mempool) {
    return (struct custom_mlx5_mempool *)(&(mempool->data_mempool));
}

void *custom_mlx5_mbuf_offset_ptr_(struct custom_mlx5_mbuf *mbuf, size_t off) {
    return (void *)custom_mlx5_mbuf_offset_ptr(mbuf, off);
}

struct custom_mlx5_mbuf *custom_mlx5_mbuf_at_index_(struct custom_mlx5_mempool *mempool, size_t index) {
    return (struct custom_mlx5_mbuf *)((char *)mempool->buf + (index << mempool->log_item_len));
}

void mlx5_rte_memcpy_(void *dst, const void *src, size_t n) {
    custom_mlx5_rte_memcpy(dst, src, n);
}

void custom_mlx5_fill_in_hdrs_(void *buffer, const void *hdr, uint32_t id, size_t data_len) {
    char *dst_ptr = buffer;
    const char *src_ptr = hdr;
    // copy ethernet header
    custom_mlx5_rte_memcpy(dst_ptr, src_ptr, sizeof(struct eth_hdr));
    dst_ptr += sizeof(struct eth_hdr);
    src_ptr += sizeof(struct eth_hdr);

    // copy in the ipv4 header and reset the the data length and checksum
    custom_mlx5_rte_memcpy(dst_ptr, src_ptr, sizeof(struct ip_hdr));
    struct ip_hdr *ip = (struct ip_hdr *)dst_ptr;
    dst_ptr += sizeof(struct ip_hdr);
    src_ptr += sizeof(struct ip_hdr);

     custom_mlx5_rte_memcpy(dst_ptr, src_ptr, sizeof(struct udp_hdr));
     struct udp_hdr *udp = (struct udp_hdr *)dst_ptr;
     dst_ptr += sizeof(struct udp_hdr);

    *((uint32_t *)dst_ptr) = id;

    ip->len = htons(sizeof(struct ip_hdr) + sizeof(struct udp_hdr) + 4 + data_len);
    ip->chksum = 0;
    ip->chksum = custom_mlx5_get_chksum(ip);
    
    udp->len = htons(sizeof(struct udp_hdr) + 4 + data_len);
    udp->chksum = 0;
    udp->chksum = custom_mlx5_get_chksum(udp);

}

struct custom_mlx5_transmission_info *custom_mlx5_completion_start_(struct custom_mlx5_per_thread_context *context) {
    return custom_mlx5_completion_start(&context->txq);
}

struct mlx5_wqe_data_seg *custom_mlx5_dpseg_start_(struct custom_mlx5_per_thread_context *context, size_t inline_off) {
    return custom_mlx5_dpseg_start(&context->txq, inline_off);
}

void flip_headers_mlx5_(void *data) {
    // flips all headers in this packet's data
    struct eth_hdr *eth = (struct eth_hdr *)data;
    struct ip_hdr *ip = (struct ip_hdr *)((char *)data + sizeof(struct eth_hdr));
    struct udp_hdr *udp = (struct udp_hdr *)((char *)data + sizeof(struct eth_hdr) + sizeof(struct ip_hdr));
    //uint32_t *id = (uint32_t *)((char *)data + sizeof(struct eth_hdr) + sizeof(struct ip_hdr) + sizeof(struct udp_hdr)); 
    //printf("[flip_headers_] id: %u, data addr: %p\n", *id, (char *)data);
    
     //printf("[flip_headers_] src eth before flip: %2x:%2x:%2x:%2x:%2x:%2x\n", eth->shost.addr[0], eth->shost.addr[1], eth->shost.addr[2], eth->shost.addr[3], eth->shost.addr[4], eth->shost.addr[5]);
     //printf("[flip_headers_] dst eth before flip: %2x:%2x:%2x:%2x:%2x:%2x\n", eth->dhost.addr[0], eth->dhost.addr[1], eth->dhost.addr[2], eth->dhost.addr[3], eth->dhost.addr[4], eth->dhost.addr[5]);
    struct eth_addr tmp;
    custom_mlx5_rte_memcpy(&tmp, &eth->dhost, sizeof(struct eth_addr));
    custom_mlx5_rte_memcpy(&eth->dhost, &eth->shost, sizeof(struct eth_addr));
    custom_mlx5_rte_memcpy(&eth->shost, &tmp, sizeof(struct eth_addr));
    
    uint32_t tmp_ip = ip->daddr;
    ip->daddr = ip->saddr;
    ip->saddr = tmp_ip;
    ip->chksum = 0;
    ip->chksum = custom_mlx5_get_chksum(ip);

    uint16_t tmp_udp = udp->dst_port;
    udp->dst_port = udp->src_port;
    udp->src_port = tmp_udp;
    udp->chksum = 0;
    udp->chksum = custom_mlx5_get_chksum(udp);
     //printf("[flip_headers_] src eth after flip: %2x:%2x:%2x:%2x:%2x:%2x\n", eth->shost.addr[0], eth->shost.addr[1], eth->shost.addr[2], eth->shost.addr[3], eth->shost.addr[4], eth->shost.addr[5]);
     //printf("[flip_headers_] dst eth after flip: %2x:%2x:%2x:%2x:%2x:%2x\n", eth->dhost.addr[0], eth->dhost.addr[1], eth->dhost.addr[2], eth->dhost.addr[3], eth->dhost.addr[4], eth->dhost.addr[5]);
}
