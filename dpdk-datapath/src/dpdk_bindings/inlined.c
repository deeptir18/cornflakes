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
#include <mem.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <rte_mempool.h>
#include <rte_flow.h>
#include <custom_mempool.h>
#include <rte_thash.h>
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

typedef unsigned long physaddr_t;
typedef unsigned long virtaddr_t;

#define IP_DEFTTL  64   /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN  0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)
#define RX_PACKET_LEN 9216
#define MBUF_HEADER_SIZE 64
#define MBUF_PRIV_SIZE 8
#define PGSIZE_2MB (1 <<  21)
#define MAX_CORES 6

static uint8_t sym_rss_key[] = {
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
};

static struct rte_flow_attr attr = { .ingress = 1 };
static struct rte_flow_item patterns[MAX_CORES][4];
static struct rte_flow_action actions[MAX_CORES][2];
static struct rte_flow_item_eth eths[MAX_CORES];
static struct rte_flow_item_ipv4 ipv4s[MAX_CORES];
static struct rte_flow_item_udp udps[MAX_CORES];
static struct rte_flow_action_queue queues[MAX_CORES];
static struct rte_flow *flows[MAX_CORES];
static struct rte_flow_error errors[MAX_CORES];

inline void free_referred_mbuf(void *buf) {
    struct rte_mbuf *mbuf = (struct rte_mbuf *)(buf);
    struct tx_pktmbuf_priv *priv_data = (struct tx_pktmbuf_priv *)(((char *)buf) + sizeof(struct rte_mbuf));
    //printf("[free_refered_mbuf_] about to check if refers to anther\n");
    if (priv_data->refers_to_another == 1) {
        //printf("[free_refered_mbuf_] refers to another = 1\n");
        // get the mbuf this refers to
        // decrease the ref count of that mbuf
        // buf_addr in mbuf is (128 + sizeof(priv)) away from the base address
        struct rte_mbuf *ref_mbuf = (struct rte_mbuf *)((char *)(mbuf->buf_addr) - (RTE_PKTMBUF_HEADROOM + sizeof(struct tx_pktmbuf_priv)));
        //printf("[free_refered_mbuf_] Original extbuf buffer: %p; buf_addr: %p, Pointer of referred mbuf: %p; \n", buf, mbuf->buf_addr, ref_mbuf);
        uint16_t ref_cnt = rte_mbuf_refcnt_read(ref_mbuf);
        //printf("[free_refered_mbuf_] Original extbuf buffer: %p; Pointer of referred mbuf: %p; refcnt of reffered buf: %u\n", buf, ref_mbuf, (unsigned)ref_cnt);
        if (ref_cnt == 0 || ref_cnt == 1) {
            //printf("Freeing mbuf %p\n", ref_mbuf);
            rte_pktmbuf_free(ref_mbuf);
        } else {
        //printf("[free_refered_mbuf_] Original extbuf buffer: %p; Pointer of referred mbuf: %p; refcnt being set to: %u\n", buf, ref_mbuf, (unsigned)ref_cnt - 1);
            rte_mbuf_refcnt_set(ref_mbuf, ref_cnt - 1);
        }
    }
}

int custom_extbuf_obj_free(void * const *obj_table, unsigned n) {
    unsigned long i;
	for (i = 0; i < n; i++)
        free_referred_mbuf(obj_table[i]);
    return 0;
}

/* Largely taken from shenango: https://github.com/shenango/shenango/blob/master/iokernel/mempool_completion.c */
int custom_extbuf_enqueue(struct rte_mempool *mp, void * const *obj_table, unsigned n) {
    unsigned long i;
	struct completion_stack *s = mp->pool_data;
    //printf("[custom_extbuf_enqueue] Enqueueing %u packets; mp has %u; mp name: %s\n", (unsigned)n, (unsigned) s->len, mp->name);

	if (unlikely(s->len + n > s->size))
		return -ENOBUFS;

	for (i = 0; i < n; i++)
		s->objs[s->len + i] = obj_table[i];

    s->len += n;
    return 0;
}

/* Largely taken from shenango: https://github.com/shenango/shenango/blob/master/iokernel/mempool_completion.c */
int custom_extbuf_dequeue(struct rte_mempool *mp, void **obj_table, unsigned n) {
    unsigned long i, j;
	struct completion_stack *s = mp->pool_data;
	if (unlikely(n > s->len)) {
        //printf("[custom_extbuf_dequeue] Returning ENOBUFS\n");
		return -ENOBUFS;
    }

	s->len -= n;
	for (i = 0, j = s->len; i < n; i++, j++)
		obj_table[i] = s->objs[j];

    return 0;
}

/* Taken from shenango: https://github.com/shenango/shenango/blob/master/iokernel/mempool_completion.c */
unsigned custom_extbuf_get_count(const struct rte_mempool *mp) {
    struct completion_stack *s = mp->pool_data;
	return s->len;
}

/* Taken from shenango: https://github.com/shenango/shenango/blob/master/iokernel/mempool_completion.c */
int custom_extbuf_alloc(struct rte_mempool *mp) {
    struct completion_stack *s;
	unsigned n = mp->size;
	int size = sizeof(*s) + (n + 16) * sizeof(void *);
	s = rte_zmalloc_socket(mp->name, size, RTE_CACHE_LINE_SIZE, mp->socket_id);
	if (!s) {
        printf("[custom_extbuf_alloc] Could not allocate stack for extbuf mempool\n");
		return -ENOMEM;
	}

	s->len = 0;
	s->size = n;
	mp->pool_data = s;
	return 0;
}

/* Largely taken from shenango: https://github.com/shenango/shenango/blob/master/iokernel/mempool_completion.c */
void custom_extbuf_free(struct rte_mempool *mp) {
    rte_free(mp->pool_data);
}
static struct rte_mempool_ops custom_ops = {
        .name = "external",
        .alloc = custom_extbuf_alloc,
        .free = custom_extbuf_free,
        .enqueue = custom_extbuf_enqueue,
        .dequeue = custom_extbuf_dequeue,
        .get_count = custom_extbuf_get_count,
        .obj_free = custom_extbuf_obj_free,
};

void munmap_huge_(void *addr, size_t pgsize, size_t num_pages) {
    munmap(addr, pgsize * num_pages);
}

// Taken from shenango
// Maps virtual addresses to physical addresses
// Relies on the fact that huge pages are by default pinned
int mem_lookup_page_phys_addrs_(void *addr,
                               size_t len,
                               size_t pgsize,
                               physaddr_t *paddrs) {
    printf("[mem_lookup_page_phys_addrs_] Len: %u, paddrs: %p, pgsize: %u; addr %p\n", (unsigned)len, paddrs, (unsigned)pgsize, addr);
    uintptr_t pos;
	uint64_t tmp;
	int fd, i = 0, ret = 0;

	/*
	 * 4 KB pages could be swapped out by the kernel, so it is not
	 * safe to get a machine address. If we later decide to support
	 * 4KB pages, then we need to mlock() the page first.
	 */
	if (pgsize == PGSIZE_4KB)
		return -EINVAL;

	fd = open("/proc/self/pagemap", O_RDONLY);
	if (fd < 0)
		return -EIO;

	for (pos = (uintptr_t)addr; pos < (uintptr_t)addr + len;
	     pos += pgsize) {
		if (lseek(fd, pos / PGSIZE_4KB * sizeof(uint64_t), SEEK_SET) ==
		    (off_t)-1) {
            printf("[mem_lookup_page_phys_addrs_] failing in lseek.\n");
			ret = -EIO;
			goto out;
		}

		if (read(fd, &tmp, sizeof(uint64_t)) <= 0) {
			ret = -EIO;
            printf("[mem_lookup_page_phys_addrs_] failing in read.\n");
			goto out;
		}


		if (!(tmp & PAGEMAP_FLAG_PRESENT)) {
			ret = -ENODEV;
            printf("[mem_lookup_page_phys_addrs_] failing in pagemap flag present.\n");
			goto out;
		}

		paddrs[i++] = (tmp & PAGEMAP_PGN_MASK) * PGSIZE_4KB;
	}

out:
	close(fd);
	return ret;
}


int mmap_huge_(size_t num_pages, void **ext_mem_addr, physaddr_t *paddrs) {
    int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB;
    size_t pgsize = PGSIZE_2MB;
    void * addr = mmap(NULL, pgsize * num_pages, PROT_READ | PROT_WRITE, flags, -1, 0);
    if (addr == MAP_FAILED) {
        printf("[mmap_huge_] Failed to mmap memory\n");
        return 1;
    }
    // need to write to the pages to ensure they're actually mapped.
    memset((char *)addr, 'D', pgsize * num_pages);
    printf("[mmap_huge_]: pagesize: %u, num_pages: %u, length: %u, addr: %p\n", (unsigned)PGSIZE_2MB, (unsigned)num_pages, (unsigned)(num_pages * PGSIZE_2MB), (void *)addr);

    int ret = mem_lookup_page_phys_addrs_(addr, pgsize * num_pages, pgsize, paddrs);
    if (ret != 0) {
        printf("[mmap_huge_]: mem_lookup_page_phys_addrs_ call failed: addr %p, len %u, pgsize %u, paddrs: %p\n", (void *)addr, (unsigned)(pgsize * num_pages), (unsigned)pgsize, (void *)paddrs);
        return 1;
    }
    *ext_mem_addr = addr;
    return 0;
}

struct tx_pktmbuf_priv *tx_pktmbuf_get_priv(struct rte_mbuf *buf) {
    	struct tx_pktmbuf_priv *priv = (struct tx_pktmbuf_priv *)(((char *)buf)
			+ sizeof(struct rte_mbuf));
        //printf("[tx_pktmbuf_get_priv] addr of mbuf: %p, addr: of priv: %p\n", buf, priv);
        // printf("[tx_pktmbuf_get_priv] priv lkey: %u, priv lkey present: %u, priv lkey refers to another: %u, size of struct: %u\n", (unsigned)priv->lkey, (unsigned)priv->lkey_present, (unsigned)priv->refers_to_another, (unsigned)sizeof(struct tx_pktmbuf_priv));
        return priv;
}

// registers a custom mempool for the external mbuf pool
// the custom free function must check whether the mbuf points to another mbuf
// And decrement the ref counter of that mbuf
int register_custom_extbuf_ops_() {
    int ret = rte_mempool_register_ops(&custom_ops);
    if (ret < 0) {
        return 1;
    }
    return 0;
}

int set_custom_extbuf_ops_(struct rte_mempool *mempool) {
    return rte_mempool_set_ops_byname(mempool, "external",  NULL);
}

int rte_mempool_count_(struct rte_mempool *mp) {
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);
	return ops->get_count(mp);
}

void rte_pktmbuf_refcnt_update_or_free_(struct rte_mbuf *packet, int16_t val) {
    //printf("[rte_mbuf_refcnt_update_] Changing refcnt of mbuf %p by val %d; currently %d\n", packet, val, rte_mbuf_refcnt_read(packet));
    uint16_t cur_rc = rte_mbuf_refcnt_read(packet);
    if (((int16_t)cur_rc + val ) <= 0) {
        //printf("[rte_pktmbuf_refcnt_update_or_free_] Freeing packet %p\n", packet);
        rte_pktmbuf_free(packet);
        return;
    } else {
        //rte_mbuf_refcnt_update(packet, cur_rc + val);
        rte_mbuf_refcnt_set(packet, cur_rc + val);
    }
    //printf("[rte_mbuf_refcnt_update_] Refcnt is now %d\n", rte_mbuf_refcnt_read(packet));
}

void rte_pktmbuf_refcnt_set_(struct rte_mbuf *packet, uint16_t val) {
    //printf("[rte_pktmbuf_refcnt_set_] Setting refcnt of mbuf %p to val %u\n", packet, (unsigned)val);
    rte_mbuf_refcnt_set(packet, val);
}

uint16_t rte_pktmbuf_refcnt_get_(struct rte_mbuf *packet) {
    return rte_mbuf_refcnt_read(packet);
}

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

struct rte_mbuf* rte_pktmbuf_alloc_(struct rte_mempool *mp) {
    return rte_pktmbuf_alloc(mp);
}

/* Sets ipv4 and udp checksums in the packet */
void set_checksums_(struct rte_mbuf *pkt) {
    struct rte_ipv4_hdr *ipv4 = rte_pktmbuf_mtod_offset(pkt, struct rte_ipv4_hdr *, sizeof(struct rte_ether_hdr));
    ipv4->hdr_checksum = 0;
    ipv4->hdr_checksum = rte_ipv4_cksum(ipv4);
    struct rte_udp_hdr *udp = rte_pktmbuf_mtod_offset(pkt, struct rte_udp_hdr *,
                                                        sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr));
    udp->dgram_cksum = rte_cpu_to_be_16(rte_raw_cksum((void *)udp, sizeof(struct rte_udp_hdr)));
    printf("Set ipv4 checksum as %u, udp as %u\n", ipv4->hdr_checksum, udp->dgram_cksum);
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
            eth_hdr->s_addr.addr_bytes[0], eth_hdr->s_addr.addr_bytes[1],
            eth_hdr->s_addr.addr_bytes[2], eth_hdr->s_addr.addr_bytes[3],
            eth_hdr->s_addr.addr_bytes[4], eth_hdr->s_addr.addr_bytes[5]);
        printf("[rte_eth_tx_burst_] Dst MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
            " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->d_addr.addr_bytes[0], eth_hdr->d_addr.addr_bytes[1],
            eth_hdr->d_addr.addr_bytes[2], eth_hdr->d_addr.addr_bytes[3],
            eth_hdr->d_addr.addr_bytes[4], eth_hdr->d_addr.addr_bytes[5]);
        printf("[rte_eth_tx_burst_] Queue: %u, Scp IP: %u, dst IP: %u, checksum: %u, udp data len: %u, ID: %u\n", queue_id, ipv4->src_addr, ipv4->dst_addr, ipv4->hdr_checksum, ntohs(udp->dgram_len), *id_ptr);
    }*/
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

void custom_init_priv_(struct rte_mempool *mp, void *opaque_arg, void *m, unsigned m_idx) {
    struct rte_mbuf *buf = m;
    struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(buf);
    memset(data, 0, sizeof(*data));
}

void set_lkey_(struct rte_mbuf *packet, uint32_t key) {
    struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(packet);
    data->lkey = key;
    data->lkey_present = 1;
}

void set_lkey_not_present_(struct rte_mbuf *packet) {
    struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(packet);
    data->lkey = 0;
    data->lkey_present = 0;
}

void set_refers_to_another_(struct rte_mbuf *packet, uint16_t val) {
    struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(packet);
    data->refers_to_another = val;

}

uint32_t make_ip_(uint8_t a, uint8_t b, uint8_t c, uint8_t d) {
    return (((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d);
}

size_t fill_in_packet_header_(struct rte_mbuf *mbuf, struct rte_ether_addr *my_eth, struct rte_ether_addr *dst_eth, uint32_t my_ip, uint32_t dst_ip, uint16_t client_port, uint16_t server_port, size_t message_size) {
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
    udp_hdr->src_port = rte_cpu_to_be_16(client_port);
    udp_hdr->dst_port = rte_cpu_to_be_16(server_port);
    udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + message_size);
    udp_hdr->dgram_cksum = 0;
    ptr += sizeof(*udp_hdr);
    header_size += sizeof(*udp_hdr);
    printf("[write hdrs dpdk] dgram len for udp: %u", rte_be_to_cpu_16(udp_hdr->dgram_len));

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
    struct rte_ipv4_hdr *const ip_hdr = (struct rte_ipv4_hdr *)(ptr);
    ptr += sizeof(*ip_hdr);
    header_size += sizeof(*ip_hdr);
    struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)(ptr);
    ptr += sizeof(*udp_hdr);
    header_size += sizeof(*udp_hdr);

    //printf("[parse_packet__] Received packet: ip cksum: %u; udp checksum, %u\n", ip_hdr->hdr_checksum, udp_hdr->dgram_cksum);
    

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
        //printf("Bad ether type");
        return false;
    }

    // In network byte order.
    if (ip_hdr->dst_addr != rte_cpu_to_be_32(our_ip)) {
        //printf("Bad dst ip addr; got: %u, expected: %u, our_ip in lE: %u\n", (unsigned)(ip_hdr->dst_addr), (unsigned)(rte_cpu_to_be_32(our_ip)), (unsigned)(our_ip));
        return false;    
    }

    if (IPPROTO_UDP != ip_hdr->next_proto_id) {
        printf("Bad next proto_id\n");
        return false;
    }

    *payload_len = mbuf->pkt_len - header_size;
    // printf("[parse_packet_] Received packet with %u pkt_len, %u data_Len, %u header_size, set payload_len to %u\n", (unsigned)mbuf->pkt_len, (unsigned)mbuf->data_len, (unsigned)header_size, (unsigned)*payload_len);
    return true;
}

uint32_t read_pkt_id_(struct rte_mbuf *mbuf) {
    uint32_t *id_slice;
    id_slice = rte_pktmbuf_mtod_offset(mbuf, uint32_t *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
    //printf("[read_pkt_id] mbuf %p, id pointer %p\n", mbuf, id_slice);
    return ntohl(*id_slice);
}

void flip_headers_(struct rte_mbuf *mbuf, uint32_t id) {
	struct rte_ether_hdr *ptr_mac_hdr;
	struct rte_ether_addr src_addr;
	struct rte_ipv4_hdr *ptr_ipv4_hdr;
	uint32_t src_ip_addr;
	struct rte_udp_hdr *rte_udp_hdr;
	uint16_t tmp_port;
    uint32_t *id_ptr;
    
    /* swap src and dst ether addresses */
    ptr_mac_hdr = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
    rte_ether_addr_copy(&ptr_mac_hdr->s_addr, &src_addr);
	rte_ether_addr_copy(&ptr_mac_hdr->d_addr, &ptr_mac_hdr->s_addr);
	rte_ether_addr_copy(&src_addr, &ptr_mac_hdr->d_addr);


	/* swap src and dst IP addresses */
	ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(mbuf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
	src_ip_addr = ptr_ipv4_hdr->src_addr;
	ptr_ipv4_hdr->src_addr = ptr_ipv4_hdr->dst_addr;
	ptr_ipv4_hdr->dst_addr = src_ip_addr;

	/* swap UDP ports */
	rte_udp_hdr = rte_pktmbuf_mtod_offset(mbuf, struct rte_udp_hdr *,
                                            RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
	tmp_port = rte_udp_hdr->src_port;
	rte_udp_hdr->src_port = rte_udp_hdr->dst_port;
	rte_udp_hdr->dst_port = tmp_port;

	/* enable computation of IPv4 checksum in hardware */
    ptr_ipv4_hdr->hdr_checksum = 0;
    mbuf->l2_len = RTE_ETHER_HDR_LEN;
	mbuf->l3_len = sizeof(struct rte_ipv4_hdr);
    mbuf->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;

    id_ptr = rte_pktmbuf_mtod_offset(mbuf, uint32_t *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
    *id_ptr = htonl(id);

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
    //tx_ptr_ipv4_hdr->hdr_checksum = rx_ptr_ipv4_hdr->hdr_checksum;

    /* Swap UDP ports */
    struct rte_udp_hdr *rx_rte_udp_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
    struct rte_udp_hdr *tx_rte_udp_hdr = rte_pktmbuf_mtod_offset(tx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
    tx_rte_udp_hdr->src_port = rx_rte_udp_hdr->dst_port;
    tx_rte_udp_hdr->dst_port = rx_rte_udp_hdr->src_port;
    tx_rte_udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_length);
    //tx_rte_udp_hdr->dgram_cksum = rx_rte_udp_hdr->dgram_cksum;

    /* Set packet metadata */
    tx_buf->l2_len = RTE_ETHER_HDR_LEN;
    tx_buf->l3_len = sizeof(struct rte_ipv4_hdr);
    tx_buf->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
}

struct rte_mbuf_ext_shared_info *shinfo_init_(void *addr, uint16_t *buf_len) {
    return rte_pktmbuf_ext_shinfo_init_helper(addr, buf_len, general_free_cb_, NULL);
}

void destroy_flow_rules_(uint16_t dpdk_port) {
    rte_flow_flush(dpdk_port, &errors[0]);
}

void add_flow_rule_(uint16_t dpdk_port,
                        struct rte_ether_addr *dest_eth,
                        uint32_t dst_ip, 
                        uint16_t dst_udp_port, 
                        uint16_t queue_id) {
    printf("Adding flow rule for queue %u udp port %u\n", queue_id, dst_udp_port);
    // set the queue
    queues[queue_id].index = queue_id;
    /* setting the eth to pass only packets to this eth addr */
    //rte_memcpy(&eths[queue_id].dst, dest_eth, RTE_ETHER_HDR_LEN);
    patterns[queue_id][0].type = RTE_FLOW_ITEM_TYPE_ETH;
    patterns[queue_id][0].spec = &eths[queue_id];

    /* set the dst ipv4 packet to the required value */
    ipv4s[queue_id].hdr.dst_addr = htonl(dst_ip);
    patterns[queue_id][1].type = RTE_FLOW_ITEM_TYPE_IPV4;
    patterns[queue_id][1].spec = &ipv4s[queue_id];

    udps[queue_id].hdr.dst_port = htonl(dst_udp_port);
    patterns[queue_id][2].type = RTE_FLOW_ITEM_TYPE_UDP;
    patterns[queue_id][2].spec = &udps[queue_id];

    /* end the pattern array */
    patterns[queue_id][3].type = RTE_FLOW_ITEM_TYPE_END;

    /* create the queue action */
    actions[queue_id][0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
    actions[queue_id][0].conf = &queues[queue_id];
    actions[queue_id][1].type = RTE_FLOW_ACTION_TYPE_END;

    /* validate and create the flow rule */
    if (!rte_flow_validate(dpdk_port, &attr, patterns[queue_id], actions[queue_id], &errors[queue_id])) {
        flows[queue_id] = rte_flow_create(dpdk_port, &attr, patterns[queue_id], actions[queue_id], &errors[queue_id]);
    } else {
        printf("Flow rule for queue %u not validated: %s\n", queue_id, strerror(rte_errno_()));
    }
}

/**
 * compute_flow_affinity - compute rss hash for incoming packets
 * @local_port: the local port number
 * @remote_port: the remote port
 * @local_ip: local ip (in host-order)
 * @remote_ip: remote ip (in host-order)
 * @num_queues: total number of queues
 *
 * Returns the 32 bit hash mod maxks
 *
 * copied from dpdk/lib/librte_hash/rte_thash.h
 */
uint32_t compute_flow_affinity_(uint32_t local_ip, 
                                        uint32_t remote_ip, 
                                        uint16_t local_port, 
                                        uint16_t remote_port, 
                                        size_t num_queues)
{
	const uint8_t *rss_key = (uint8_t *)sym_rss_key;

	uint32_t input_tuple[] = {
        remote_ip, local_ip, local_port | remote_port << 16
	};
    
    uint32_t ret = rte_softrss((uint32_t *)&input_tuple, ARRAY_SIZE(input_tuple),
         (const uint8_t *)rss_key);
	return ret % (uint32_t)num_queues;
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

    port_conf.rxmode.offloads = DEV_RX_OFFLOAD_JUMBO_FRAME | DEV_RX_OFFLOAD_TIMESTAMP | DEV_RX_OFFLOAD_IPV4_CKSUM;
    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS | ETH_MQ_RX_RSS_FLAG;
    port_conf.rx_adv_conf.rss_conf.rss_key = sym_rss_key;
    port_conf.rx_adv_conf.rss_conf.rss_key_len = 40;
    port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_UDP | ETH_RSS_IP;
    port_conf.txmode.offloads = DEV_TX_OFFLOAD_MULTI_SEGS | DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_UDP_CKSUM;
    port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;

    printf("port_id: %u, rx_rings; %u, tx_rings: %u\n", port_id, rx_rings, tx_rings);
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

void fill_in_hdrs_dpdk_(void *buffer, const void *hdr, uint32_t id, size_t data_len) {
    char *dst_ptr = buffer;
    const char *src_ptr = hdr;
    // copy ethernet header
    rte_memcpy(dst_ptr, src_ptr, sizeof(struct rte_ether_hdr));
    dst_ptr += sizeof(struct rte_ether_hdr);
    src_ptr += sizeof(struct rte_ether_hdr);

    // copy in the ipv4 header and reset the the data length and checksum
    rte_memcpy(dst_ptr, src_ptr, sizeof(struct rte_ipv4_hdr));
    struct rte_ipv4_hdr *ip = (struct rte_ipv4_hdr *)dst_ptr;
    dst_ptr += sizeof(struct rte_ipv4_hdr);
    src_ptr += sizeof(struct rte_ipv4_hdr);

     rte_memcpy(dst_ptr, src_ptr, sizeof(struct rte_udp_hdr));
     struct rte_udp_hdr *udp = (struct rte_udp_hdr *)dst_ptr;
     dst_ptr += sizeof(struct rte_udp_hdr);

    *((uint32_t *)dst_ptr) = id;

    ip->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) +  4 + data_len);
    ip->hdr_checksum = 0;
    ip->hdr_checksum = rte_ipv4_cksum(ip);
    
    udp->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + 4 + data_len);
    //printf("[fill_in_hdrs_dpdk_] dpdk udp header length 1, data_len: %u: %lu\n", rte_be_to_cpu_16(udp->dgram_len), data_len);
    udp->dgram_cksum = 0;
    udp->dgram_cksum = rte_cpu_to_be_16(rte_raw_cksum((void *)udp, sizeof(struct rte_udp_hdr)));
}
