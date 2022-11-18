#include <net/ethernet.h>
#include <net/ip.h>
#include <net/udp.h>
#include <base/rte_memcpy.h>

void custom_ice_fill_in_hdrs_(void *buffer, const void *hdr, uint32_t id, size_t data_len) {
    char *dst_ptr = buffer;
    const char *src_ptr = hdr;
    // copy ethernet header
    custom_ice_rte_memcpy(dst_ptr, src_ptr, sizeof(struct eth_hdr));
    dst_ptr += sizeof(struct eth_hdr);
    src_ptr += sizeof(struct eth_hdr);

    // copy in the ipv4 header and reset the the data length and checksum
    custom_ice_rte_memcpy(dst_ptr, src_ptr, sizeof(struct ip_hdr));
    struct ip_hdr *ip = (struct ip_hdr *)dst_ptr;
    dst_ptr += sizeof(struct ip_hdr);
    src_ptr += sizeof(struct ip_hdr);

     custom_ice_rte_memcpy(dst_ptr, src_ptr, sizeof(struct udp_hdr));
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

