/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */
#include "mem.h"
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/queue.h>
#include <assert.h>
#include <limits.h>
#include <ctype.h>
#include <getopt.h>
#include <sys/mman.h>
#include "fcntl.h"
#include <sys/types.h>
#include <unistd.h>
//#include <numaif.h>

#include <rte_memory.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_debug.h>
#include <rte_common.h>
#include <rte_cycles.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_memcpy.h>
#include <rte_errno.h>
#include <rte_udp.h>
#include <rte_ethdev.h>
#include <rte_arp.h>
#include <rte_memzone.h>
#include <rte_malloc.h>
//#include <rte_pmd_mlx5.h>

#define NNUMA 2
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

/******************************************/
/******************************************/
typedef unsigned long physaddr_t;
typedef unsigned long virtaddr_t;
/******************************************/
/******************************************/
#define MAX_ITERATIONS 3000000
static char* latency_log = NULL;
static int has_latency_log = 0;

typedef struct Latency_Dist_t
{
    uint64_t min, max;
    uint64_t latency_sum;
    uint64_t total_count;
    float moving_avg;
    uint64_t latencies[MAX_ITERATIONS];
} Latency_Dist_t;

struct tx_pktmbuf_priv
{
    int32_t lkey;
    int32_t field2; // needs to be atleast 8 bytes large
};

struct ClientRequest
{
    uint64_t timestamp;
    uint64_t packet_id;
    uint64_t segment_size;
    uint64_t num_segments;
    uint64_t segment_offsets[32]; // maximum number of segments we'd be asking for (within array_size)
};

static void add_latency(Latency_Dist_t *dist, uint64_t latency) {
    dist->latencies[dist->total_count] = latency;
    dist->total_count++;
    if (dist->total_count > MAX_ITERATIONS) {
        printf("[add_latency] Adding too much to total_count\n");
    }
    dist->latency_sum += latency;
    if (latency < dist->min) {
        dist->min = latency;
    }

    if (latency > dist->max) {
        dist->max = latency;
    }

    // calculate moving avg
    dist->moving_avg = dist->moving_avg * ((float)(dist->total_count - 1)/(float)dist->total_count) + ((float)(latency) / (float)(dist->total_count));
}
int cmpfunc(const void * a, const void *b) {
    const uint64_t *a_ptr = (const uint64_t *)a;
    const uint64_t *b_ptr = (const uint64_t *)b;
    return (int)(*a_ptr - *b_ptr);
}

static void dump_latencies(Latency_Dist_t *dist, float total_time, size_t message_size, float rate_gbps) {
    printf("Trying to dump latencies on client side with message size: %u\n", (unsigned)message_size);
    // sort the latencies
    uint64_t *arr = malloc(dist->total_count * sizeof(uint64_t));
    if (arr == NULL) {
        printf("Not able to allocate array to sort latencies\n");
        exit(1);
    }
    for (size_t i = 0; i < dist->total_count; i++) {
        arr[i] = dist->latencies[i];
    }
    
    if (!has_latency_log) {
        qsort(arr, dist->total_count, sizeof(uint64_t), cmpfunc);
        uint64_t avg_latency = (dist->latency_sum) / (dist->total_count);
        uint64_t median = arr[(size_t)((double)dist->total_count * 0.50)];
        uint64_t p99 = arr[(size_t)((double)dist->total_count * 0.99)];
        uint64_t p999 = arr[(size_t)((double)dist->total_count * 0.999)];
    
        float achieved_rate = (float)(dist->total_count) / (float)total_time * (float)(message_size) * 8.0 / (float)1e9;
        float percent_rate = achieved_rate / rate_gbps;
        printf("Stats:\n\t- Min latency: %u ns\n\t- Max latency: %u ns\n\t- Avg latency: %" PRIu64 " ns", (unsigned)dist->min, (unsigned)dist->max, avg_latency);
        printf("\n\t- Median latency: %u ns\n\t- p99 latency: %u ns\n\t- p999 latency: %u ns", (unsigned)median, (unsigned)p99, (unsigned)p999);
        printf("\n\t- Achieved Goodput: %0.4f Gbps ( %0.4f %% ) \n", achieved_rate, percent_rate);
    } else {
        FILE *fp = fopen(latency_log, "w");
        for (int i = 0; i < dist->total_count; i++) {
            fprintf(fp, "%lu\n", dist->latencies[i]);
        }
        fclose(fp);
    }
    free((void *)arr);
    printf("Finished reporting function\n");
}

typedef struct Packet_Map_t
{
    uint64_t rtts[MAX_ITERATIONS * 32];
    uint32_t ids[MAX_ITERATIONS * 32]; // unique IDs sent in each packet
    size_t total_count;
    uint32_t *sent_ids;
    uint64_t *grouped_rtts;
} Packet_Map_t;

static void add_latency_to_map(Packet_Map_t *map, uint64_t rtt, uint32_t id) {
    map->rtts[map->total_count] = rtt;
    map->ids[map->total_count] = id;
    map->total_count++;
    if (map->total_count > (MAX_ITERATIONS * 32)) {
        printf("[add_latency_to_map] ERROR: Overflow in Packet map");
    }
}

static void free_pktmap(Packet_Map_t *map) {
    if (map->sent_ids != NULL) {
        free(map->sent_ids);
    }

    if (map->grouped_rtts != NULL) {
        free(map->grouped_rtts);
    }
}

static void calculate_latencies(Packet_Map_t *map, Latency_Dist_t *dist, size_t num_sent, size_t num_per_bucket) {
    map->sent_ids = (uint32_t *)(malloc(sizeof(uint32_t) * num_sent));
    map->grouped_rtts = (uint64_t *)(malloc(sizeof(uint64_t) * num_sent * num_per_bucket));
    if (map->grouped_rtts == NULL || map->sent_ids == NULL) {
        printf("Error allocating grouped rtts and sent_ids.\n");
        exit(1);
    }

    for (size_t r = 0; r < num_sent; r++) {
        map->sent_ids[r] = 0;
        for (size_t c = 0; c < num_per_bucket; c++) {
            *(map->grouped_rtts + r*num_per_bucket + c) = 0;
        }
    }

    for (size_t i = 0;  i < map->total_count; i++) {
        uint32_t id = map->ids[i];
        uint64_t rtt = map->rtts[i];
        map->sent_ids[id] = 1;
        for (size_t c = 0; c < num_per_bucket; c++) {
            size_t r = (size_t)id;
            if (*(map->grouped_rtts + r*num_per_bucket + c) == 0) {
                *(map->grouped_rtts + r*num_per_bucket + c) = rtt;
                break;
            }
        }
    }

    int num_valid = 0;
    for (size_t r = 0; r < num_sent; r++) {
        if (map->sent_ids[r] == 0) {
            continue;
        }
        uint64_t max_rtt = 0;
        for (size_t c = 0; c < num_per_bucket; c++) {
            uint64_t rtt = *(map->grouped_rtts + r*num_per_bucket + c);
            if (rtt != 0) {
                if (rtt > max_rtt) {
                    max_rtt = rtt;
                }
            }
        }
        if (max_rtt > 0) {
            num_valid++;
            add_latency(dist, max_rtt);
        }
    }
    printf("\nNum fully received: %d, packets per bucket: %u, total_count: %u.\n", num_valid, (unsigned)num_per_bucket, (unsigned)map->total_count);
}


typedef void (*netperf_onfail_t)(int error_arg,
      const char *expr_arg, const char *funcn_arg, const char *filen_arg,
      int lineno_arg);

static void default_onfail(int error_arg,
      const char *expr_arg, const char *fnn_arg, const char *filen_arg,
      int lineno_arg);

static netperf_onfail_t current_onfail = &default_onfail;

static void netperf_panic(const char *why_arg, const char *filen_arg, int lineno_arg) {
    if (NULL == why_arg) {
        why_arg = "*unspecified*";
    }

    if (NULL == filen_arg) {
        filen_arg = "*unspecified*";
    }

    /* there's really no point in checking the return code of fprintf().
     * if it fails, i don't have a backup plan for informing the
     * operator. */
    fprintf(stderr, "*** panic in line %d of `%s`: %s\n", lineno_arg, filen_arg, why_arg);
    abort();
}
#define NETPERF_PANIC(Why) netperf_panic((Why), __FILE__, __LINE__)

static void netperf_fail(int error_arg, const char *expr_arg,
      const char *fnn_arg, const char *filen_arg, int lineno_arg) {
   current_onfail(error_arg, expr_arg, fnn_arg, filen_arg,
         lineno_arg);
}

static void default_onfail(int error_arg, const char *expr_arg,
   const char *fnn_arg, const char *filen_arg, int lineno_arg) {
    int n = -1;

    if (0 == error_arg) {
        NETPERF_PANIC("attempt to fail with a success code.");
    }

    /* to my knowledge, Windows doesn't support providing the function name,
     * so i need to tolerate a NULL value for fnn_arg. */
    const char *err_msg = NULL;
    if (error_arg > 0) {
        err_msg = strerror(error_arg);
    } else {
        err_msg = "error message is undefined";
    }

    if (NULL == fnn_arg) {
        n = fprintf(stderr, "FAIL (%d => %s) at %s, line %d: %s\n", error_arg, err_msg,
                filen_arg, lineno_arg, expr_arg);
        if (n < 1) {
            NETPERF_PANIC("fprintf() failed.");
        }
    } else {
        n = fprintf(stderr, "FAIL (%d => %s) in %s, at %s, line %d: %s\n", error_arg, err_msg,
                fnn_arg, filen_arg, lineno_arg, expr_arg);
        if (n < 1) {
            NETPERF_PANIC("fprintf() failed.");
        }
   }
}

#define NETPERF_UNLIKELY(Cond) __builtin_expect((Cond), 0)
#define NETPERF_LIKELY(Cond) __builtin_expect((Cond), 1)
#define MAKE_IP_ADDR(a, b, c, d)			\
	(((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d)

#define NETPERF_TRUE2(Error, Condition, ErrorCache) \
    do { \
        const int ErrorCache = (Error); \
        if (NETPERF_UNLIKELY(!(Condition))) { \
            netperf_fail(ErrorCache, #Condition, NULL, __FILE__, __LINE__);  \
            return ErrorCache; \
        } \
   } while (0)

#define NETPERF_TRUE(Error, Condition) \
    NETPERF_TRUE2(Error, Condition, NETPERF_TRUE_errorCache)

#define NETPERF_ZERO(Error, Value) NETPERF_TRUE((Error), 0 == (Value))
#define NETPERF_NONZERO(Error, Value) NETPERF_TRUE((Error), 0 != (Value))
#define NETPERF_NULL(Error, Value) NETPERF_TRUE((Error), NULL == (Value))
#define NETPERF_NOTNULL(Error, Value) NETPERF_TRUE((Error), NULL != (Value
/******************************************/
/******************************************/
/* DPDK CONSTANTS */
#define RX_RING_SIZE 2048
#define TX_RING_SIZE 2048

#define NUM_MBUFS 8191
#define MBUF_CACHE_SIZE 250
#define UDP_MAX_PAYLOAD 1472
#define BURST_SIZE 32
#define MAX_SCATTERS 64
#define IP_DEFTTL  64   /* from RFC 1340. */
#define IP_VERSION 0x40
#define IP_HDRLEN  0x05 /* default IP header length == five 32-bits words. */
#define IP_VHL_DEF (IP_VERSION | IP_HDRLEN)
#define MBUF_BUF_SIZE RTE_ETHER_MAX_JUMBO_FRAME_LEN + RTE_PKTMBUF_HEADROOM
#define RX_PACKET_LEN 9216
#define MAX_SEGMENT_SIZE 8192
//#define RX_PACKET_LEN 18432
/*
 * RX and TX Prefetch, Host, and Write-back threshold values should be
 * carefully set for optimal performance. Consult the network
 * controller's datasheet and supporting DPDK documentation for guidance
 * on how these parameters should be set.
 */
#define RX_PTHRESH          8 /**< Default values of RX prefetch threshold reg. */
#define RX_HTHRESH          8 /**< Default values of RX host threshold reg. */
#define RX_WTHRESH          0 /**< Default values of RX write-back threshold reg. */

/*
 * These default values are optimized for use with the Intel(R) 82599 10 GbE
 * Controller and the DPDK ixgbe PMD. Consider using other values for other
 * network controllers and/or network drivers.
 */
#define TX_PTHRESH          0 /**< Default values of TX prefetch threshold reg. */
#define TX_HTHRESH          0  /**< Default values of TX host threshold reg. */
#define TX_WTHRESH          0  /**< Default values of TX write-back threshold reg. */

/*
 * Configurable number of RX/TX ring descriptors
 */
#define RTE_TEST_RX_DESC_DEFAULT    128
#define RTE_TEST_TX_DESC_DEFAULT    128

#define FULL_MAX 0xFFFFFFFF
#define EMPTY_MAX 0x0
#define PAGE_SIZE 4096
/******************************************/
/******************************************/
/*Static Variables*/
enum {
    MODE_UDP_CLIENT = 0,
    MODE_UDP_SERVER,
    MODE_MEMCPY_BENCH
};

const struct rte_ether_addr ether_broadcast = {
    .addr_bytes = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
};
struct rte_ether_addr server_mac = {
    .addr_bytes = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
};
static uint16_t dpdk_nbports;
static uint8_t mode;
static uint8_t memory_mode;
static size_t num_mbufs = 1;
static uint32_t my_ip;
static uint32_t server_ip;
static uint32_t segment_size = 256;
static uint32_t seconds = 1;
static uint32_t rate = 500000; // in packets / second
static uint32_t intersend_time;
static unsigned int client_port = 12345;
static unsigned int server_port = 12345;
static struct rte_mempool *tx_mbuf_pool;
static struct rte_mempool *rx_mbuf_pool;
static struct rte_mempool *extbuf_pool;
static uint16_t our_dpdk_port_id;
static struct rte_ether_addr my_eth;
static Latency_Dist_t latency_dist = { .min = LONG_MAX, .max = 0, .total_count = 0, .latency_sum = 0 };
static Packet_Map_t packet_map = {.total_count = 0, .grouped_rtts = NULL, .sent_ids = NULL };
static uint64_t clock_offset = 0;
static int zero_copy_mode = 1;
static int with_read_mode = 0;
static void *payload_to_copy = NULL;
static rte_iova_t payload_iova = 0;
size_t array_size = 10000; // default array size
static struct ClientRequest *client_requests = NULL; // array of client requests
static void *client_request_payloads = NULL;

// static unsigned int num_queues = 1;
/******************************************/
/******************************************/

// generates the client requests (which contain random offsets within array
// size)
static void initialize_client_requests() {
    // first: fill in the client requests
    size_t num_requests = (size_t)((float)seconds * rate * 1.20);
    //printf("Total num requests: %u\n", (unsigned)num_requests);
    client_requests = malloc(sizeof(struct ClientRequest) * num_requests);
    if (client_requests == NULL) {
        printf("Failed to malloc client requests array\n");
        exit(1);
    } else {
        printf("Initialized client requests at %p\n", client_requests);
    }
    struct ClientRequest *current_req = (struct ClientRequest*)client_requests;
    size_t num_segments_within_region = (array_size - segment_size) / 64;
    for (size_t iter = 0; iter < num_requests; iter++) {
        //printf("Iter: %u, current_req: %p\n", (unsigned)iter, current_req);
        current_req->timestamp = 0;
        current_req->packet_id = (uint64_t)iter;
        current_req->segment_size = segment_size;
        current_req->num_segments = num_mbufs;
        for (size_t i = 0; i < (size_t)num_mbufs; i++) {
            // make sure that the random offset region doesn't bleed outside the
            // array
            size_t random_segment = (size_t)(rand() % (num_segments_within_region));
            uint64_t random_offset = (uint64_t)random_segment * 64; // always 64 bit aligned
            current_req->segment_offsets[i] = random_offset;
            //printf("For packet # %u, setting idx %u segment rando as %u\n", (unsigned)iter, (unsigned)i, (unsigned)random_offset);
        }
        current_req++;
    }

    
    // second: fill in the client "payload" data (including the headers)
    size_t payload_size = 32 + (8 * num_mbufs);
    size_t full_payload_size = sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + payload_size;
    client_request_payloads = malloc(full_payload_size * num_requests);
    if (client_request_payloads == NULL) {
        printf("Failed to malloc client requests payload\n");
        exit(1);
    }

    char *payload = (char *)(client_request_payloads);
    current_req = (struct ClientRequest *)client_requests;
    for (size_t iter = 0; iter < num_requests; iter++) {
        //printf("Iter: %u, Payload: %p, current_req: %p\n", (unsigned)iter, payload, current_req);
        size_t header_size = 0;
        struct rte_ether_hdr *eth_hdr = (struct rte_ether_hdr *)payload;
        header_size += sizeof(struct rte_ether_hdr);

        struct rte_ipv4_hdr *ipv4_hdr = (struct rte_ipv4_hdr *)(payload + header_size);
        header_size += sizeof(struct rte_ipv4_hdr);

        struct rte_udp_hdr *udp_hdr = (struct rte_udp_hdr *)(payload + header_size);
        header_size += sizeof(struct rte_udp_hdr);

        uint64_t *timestamp_ptr = (uint64_t *)(payload + header_size);
        uint64_t *id_ptr = (uint64_t *)(payload + header_size + 8);
        uint64_t *segment_size_ptr = (uint64_t *)(payload + header_size + 16);
        uint64_t *num_segments_ptr = (uint64_t *)(payload + header_size + 24);

        uint64_t *offsets_ptr = (uint64_t *)(payload + header_size + 32);

        // fill in the header
        rte_ether_addr_copy(&my_eth, &eth_hdr->s_addr);
        rte_ether_addr_copy(&server_mac, &eth_hdr->d_addr);
        eth_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

        /* add in ipv4 header*/
        ipv4_hdr->version_ihl = IP_VHL_DEF;
        ipv4_hdr->type_of_service = 0;
        ipv4_hdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + payload_size);
        ipv4_hdr->packet_id = 0;
        ipv4_hdr->fragment_offset = 0;
        ipv4_hdr->time_to_live = IP_DEFTTL;
        ipv4_hdr->next_proto_id = IPPROTO_UDP;
        ipv4_hdr->src_addr = rte_cpu_to_be_32(my_ip);
        ipv4_hdr->dst_addr = rte_cpu_to_be_32(server_ip);
        /* offload checksum computation in hardware */
        ipv4_hdr->hdr_checksum = 0;

        /* add in UDP hdr*/
        udp_hdr->src_port = rte_cpu_to_be_16(client_port);
        udp_hdr->dst_port = rte_cpu_to_be_16(server_port);
        udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_size);
        udp_hdr->dgram_cksum = 0;

        // add request metadata
        *timestamp_ptr =  current_req->timestamp;
        *id_ptr =  current_req->packet_id;
        *segment_size_ptr = current_req->segment_size;
        *num_segments_ptr = current_req->num_segments;

        // fill in offsets
        for (uint64_t i = 0; i < current_req->num_segments; i++) {
            offsets_ptr[i] = current_req->segment_offsets[i];
        }

        payload += full_payload_size;
        current_req++;
    }

    free((void *)client_requests);

}

static int str_to_mac(const char *s, struct rte_ether_addr *mac_out) {
    assert(RTE_ETHER_ADDR_LEN == 6);
    unsigned int values[RTE_ETHER_ADDR_LEN];
    int ret = sscanf(s, "%2x:%2x:%2x:%2x:%2x:%2x%*c", &values[0], &values[1], &values[2], &values[3],&values[4], &values[5]);
    if (6 != ret) {
        printf("Scan of mac addr %s was not 6, but length %d\n", s, ret);
        return EINVAL;
    }

    for (size_t i = 0; i < RTE_ETHER_ADDR_LEN; ++i) {
        mac_out->addr_bytes[i] = (uint8_t)(values[i]);
    }
    return 0;
}

static int str_to_ip(const char *str, uint32_t *addr)
{
	uint8_t a, b, c, d;
	if(sscanf(str, "%hhu.%hhu.%hhu.%hhu", &a, &b, &c, &d) != 4) {
		return -EINVAL;
	}

	*addr = MAKE_IP_ADDR(a, b, c, d);
	return 0;
}

static int str_to_long(const char *str, long *val)
{
	char *endptr;

	*val = strtol(str, &endptr, 10);
	if (endptr == str || (*endptr != '\0' && *endptr != '\n') ||
	    ((*val == LONG_MIN || *val == LONG_MAX) && errno == ERANGE))
		return -EINVAL;
	return 0;
}

static void print_usage(void) {
    printf("To run client: netperf <EAL_INIT> -- --mode=CLIENT --ip=<CLIENT_IP> --server_ip=<SERVER_IP> --server_mac=<SERVER_MAC> --port=<PORT> --time=<TIME_SECONDS> --segment_size<SEGMENT_SIZE_BYTES> --rate<RATE_PKTS_PER_S> --array_size=<ARRAY_SIZE> --num_segments=<NUM_SEGMENTS>.\n");
    printf("To run server: netperf <EAL_INIT> -- --mode=SERVER --ip=<SERVER_IP>  --num_mbufs=<INT> <--with_copy>\n");
}

static inline struct tx_pktmbuf_priv *tx_pktmbuf_get_priv(struct rte_mbuf *buf)
{
	return (struct tx_pktmbuf_priv *)(((char *)buf)
			+ sizeof(struct rte_mbuf));
}

static void custom_init_priv(struct rte_mempool *mp __attribute__((unused)), void *opaque_arg __attribute__((unused)), void *m, unsigned i __attribute__((unused))) {
    struct rte_mbuf *buf = m;
	struct tx_pktmbuf_priv *data = tx_pktmbuf_get_priv(buf);
	memset(data, 0, sizeof(*data));
    data->lkey = -1;
    struct rte_mbuf *pkt = (struct rte_mbuf *)(m);
}

static void custom_pkt_init_with_header(struct rte_mempool *mp __attribute__((unused)), void *opaque_arg __attribute__((unused)), void *m, unsigned i __attribute__((unused))) {
    struct rte_mbuf *pkt = (struct rte_mbuf *)(m);
    uint8_t *p = rte_pktmbuf_mtod(pkt, uint8_t *);
    char *s = (char *)(p);
    memset(s, 0, 46);
    p += 46;
    s = (char *)(p);
    memset(s, 'a', 8950);
}

static void custom_pkt_init_whole(struct rte_mempool *mp __attribute__((unused)), void *opaque_arg __attribute__((unused)), void *m, unsigned i __attribute__((unused))) {
    struct rte_mbuf *pkt = (struct rte_mbuf *)(m);
    uint8_t *p = rte_pktmbuf_mtod(pkt, uint8_t *);
    char *s = (char *)(p);
    memset(s, 'a', 9000);
}

static int parse_args(int argc, char *argv[]) {
    long tmp;
    int has_server_ip = 0;
    int has_port = 0;
    int has_segment_size = 0;
    int has_server_mac = 0;
    int has_seconds = 0;
    int has_rate = 0;
    int opt = 0;
    int has_memory = 0;

    static struct option long_options[] = {
        {"mode",      required_argument,       0,  'm' },
        {"ip",      required_argument,       0,  'i' },
        {"server_ip", optional_argument,       0,  's' },
        {"log", optional_argument, 0, 'l'},
        {"port", optional_argument, 0,  'p' },
        {"server_mac",   optional_argument, 0,  'c' },
        {"segment_size",   optional_argument, 0,  'z' },
        {"time",   optional_argument, 0,  't' },
        {"rate",   optional_argument, 0,  'r' },
        {"num_mbufs", optional_argument, 0, 'n'},
        {"array_size", optional_argument, 0, 'a'},
        {"with_copy", no_argument, 0, 'k'},
        {"with_read", no_argument, 0, 'b'},
        {0,           0,                 0,  0   }
    };
    int long_index = 0;
    while ((opt = getopt_long(argc, argv,"m:i:s:l:p:c:z:t:r:n:a:b:k:",
                   long_options, &long_index )) != -1) {
        switch (opt) {
            case 'm':
                if (!strcmp(optarg, "CLIENT")) {
                    mode = MODE_UDP_CLIENT;
                } else if (!strcmp(optarg, "SERVER")) {
                    mode = MODE_UDP_SERVER;
                } else if (!strcmp(optarg, "MEMCPY")) {
                    mode = MODE_MEMCPY_BENCH;
                } else {
                    printf("mode should be SERVER or CLIENT or MEMCPY\n");
                    return -EINVAL;
                }
                break;
            case 'i':
                str_to_ip(optarg, &my_ip);
                break;
            case 's':
                has_server_ip = 1;
                str_to_ip(optarg, &server_ip);
                break;
            case 'l':
                has_latency_log = 1;
                latency_log = (char *)malloc(strlen(optarg));
                strcpy(latency_log, optarg);
                break;
            case 'p':
                has_port = 1;
                if (sscanf(optarg, "%u", &client_port) != 1) {
                    return -EINVAL;
                }
                server_port = client_port;
                break;
            case 'c':
                has_server_mac = 1;
                if (str_to_mac(optarg, &server_mac) != 0) {
                    printf("Failed to convert %s to mac address\n", optarg);
                    return -EINVAL;
                }
                break;
            case 'z':
                has_segment_size = 1;
                str_to_long(optarg, &tmp);
                segment_size = tmp;
                break;
            case 't':
                has_seconds = 1;
                str_to_long(optarg, &tmp);
                seconds = tmp;
                break;
            case 'r':
                has_rate = 1;
                str_to_long(optarg, &tmp);
                rate = tmp;
                intersend_time = 1e9 / rate;
                break;
            case 'n':
                str_to_long(optarg, &tmp);
                num_mbufs = (size_t)(tmp);
                break;
            case 'a':
                str_to_long(optarg, &tmp);
                array_size = (size_t)(tmp);
                break;
            case 'k':
                printf("Setting zero copy mode off.\n");
                zero_copy_mode = 0;
                break;
            case 'b':
                with_read_mode = 1;
                break;
            default: print_usage();
                 exit(EXIT_FAILURE);
        }
    }
    
    if (mode == MODE_MEMCPY_BENCH) {
        // just initialize payload to copy
        payload_to_copy = rte_malloc("region", array_size, 0);
        if (payload_to_copy == NULL) {
            printf("Failed to malloc region\n");
            exit(1);
        }
        return 0;
    }

    if (mode == MODE_UDP_CLIENT) {
        if (!has_server_ip) {
            printf("Server ip, -s, --server_ip=, required.\n");
            exit(EXIT_FAILURE);
        }
        if (!has_server_mac) {
            printf("Server mac, -c, --server_mac=,required.\n");
            exit(EXIT_FAILURE);
        }

        // check we have enough space to store all the times.
        if ((1e9 * seconds)/intersend_time > MAX_ITERATIONS) {
            printf("Provided rate: %u in %u seconds implies more than %u packets sent. Please change the MAX_ITERATIONS constant and recompile (how many latencies are stored).\n", (unsigned)(rate), (unsigned)seconds, (unsigned)MAX_ITERATIONS);
           exit(EXIT_FAILURE); 
        }

        if (!has_port || !has_seconds || !has_rate || !has_segment_size) {
            printf("If options for --time, --rate, or --segment_size aren't provided, defaults will be used.\n");
        }
        float rate_gbps = (float)rate * (float)(segment_size * num_mbufs) * 8.0 / (float)1e9;
        printf("Running with:\n\t- port: %u\n\t- time: %u seconds\n\t- segment_size: %u bytes\n\t- rate: %u pkts/sec (%u ns inter-packet send time)\n\t- rate_gbps: %0.4f Gbps\n\t- num_mbufs: %u segments\n\t- array_size: %u\n", (unsigned)client_port, (unsigned)seconds, (unsigned)segment_size, (unsigned)rate, (unsigned)intersend_time, rate_gbps, (unsigned)num_mbufs, (unsigned)array_size);
    } else {
        // init a separate mbuf pool to external mbufs
        extbuf_pool = rte_pktmbuf_pool_create(
                                "extbuf_pool",
                                NUM_MBUFS * dpdk_nbports,
                                MBUF_CACHE_SIZE,
                                sizeof(struct tx_pktmbuf_priv),
                                sizeof(struct tx_pktmbuf_priv) + sizeof(struct rte_mbuf),
                                rte_socket_id());
        if (extbuf_pool == NULL) {
            printf("Failed to initialize linked data mempool\n");
        }
        if (rte_mempool_obj_iter(
                extbuf_pool,
                &rte_pktmbuf_init,
                NULL) != (NUM_MBUFS * dpdk_nbports)) {
            printf("Failed to run pktmbuf_init on extbuf mempool\n");
            return 1;
        }
        if (rte_mempool_obj_iter(
                extbuf_pool,
                &custom_init_priv,
                NULL) != (NUM_MBUFS * dpdk_nbports)) {
            printf("Failed to run custom_init_priv on extbuf mempool\n");
            return 1;
        }

        // for both zero-copy and scatter-gather, init the region where payloads
        // will be sent from
        payload_to_copy = rte_malloc("region", array_size, 0);
        if (payload_to_copy == NULL) {
            printf("Failed to malloc region\n");
            exit(1);
        }
        memset(payload_to_copy, 0xAB, array_size);
        // find the IO address of this buffer
        payload_iova = rte_malloc_virt2iova(payload_to_copy);
        if (payload_iova == RTE_BAD_IOVA) {
            printf("Failed to initialize and get IO address for memory region\n");
            exit(1);
        }

        printf("Successfully allocated payload to copy\n");   
    }
    return 0;
}

static int print_link_status(FILE *f, uint16_t port_id, const struct rte_eth_link *link) {

    struct rte_eth_link link2 = {};
    if (NULL == link) {
        rte_eth_link_get_nowait(port_id, &link2);
        link = &link2;
    }
    if (ETH_LINK_UP == link->link_status) {
        const char * const duplex = ETH_LINK_FULL_DUPLEX == link->link_duplex ?  "full" : "half";
        fprintf(f, "Port %d Link Up - speed %u " "Mbps - %s-duplex\n", port_id, link->link_speed, duplex);
    } else {
        printf("Port %d Link Down\n", port_id);
    }

    return 0;
}

static int wait_for_link_status_up(uint16_t port_id) {
    NETPERF_TRUE(ERANGE, rte_eth_dev_is_valid_port(port_id));

    const size_t sleep_duration_ms = 100;
    const size_t retry_count = 90;

    struct rte_eth_link link = {};
    for (size_t i = 0; i < retry_count; ++i) {
        rte_eth_link_get_nowait(port_id, &link);
        if (ETH_LINK_UP == link.link_status) {
            print_link_status(stderr, port_id, &link);
            return 0;
        }

        rte_delay_ms(sleep_duration_ms);
    }
    print_link_status(stderr, port_id, &link);

    return ECONNREFUSED;

}

static int init_dpdk_port(uint16_t port_id, struct rte_mempool *mbuf_pool) {
    printf("Initializing port %u\n", (unsigned)(port_id));
    NETPERF_TRUE(ERANGE, rte_eth_dev_is_valid_port(port_id)); 
    const uint16_t rx_rings = 1;
    const uint16_t tx_rings = 1;
    const uint16_t nb_rxd = RX_RING_SIZE;
    const uint16_t nb_txd = TX_RING_SIZE;
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
    //    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS;
    //    port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_IP | dev_info.flow_type_rss_offloads;
    port_conf.txmode.mq_mode = ETH_MQ_TX_NONE;

    struct rte_eth_rxconf rx_conf = {};
    rx_conf.rx_thresh.pthresh = RX_PTHRESH;
    rx_conf.rx_thresh.hthresh = RX_HTHRESH;
    rx_conf.rx_thresh.wthresh = RX_WTHRESH;
    rx_conf.rx_free_thresh = 32;

    struct rte_eth_txconf tx_conf = {};
    tx_conf.tx_thresh.pthresh = TX_PTHRESH;
    tx_conf.tx_thresh.hthresh = TX_HTHRESH;
    tx_conf.tx_thresh.wthresh = TX_WTHRESH;

    // configure the ethernet device.
    rte_eth_dev_configure(port_id, rx_rings, tx_rings, &port_conf);

    // todo: what does this do?
    /*
    retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
    if (retval != 0) {
        return retval;
    }
    */

    // todo: this call fails and i don't understand why.
    int socket_id = rte_eth_dev_socket_id(port_id);

    // allocate and set up 1 RX queue per Ethernet port.
    for (uint16_t i = 0; i < rx_rings; ++i) {
        rte_eth_rx_queue_setup(port_id, i, nb_rxd, socket_id, &rx_conf, mbuf_pool);
    }

    // allocate and set up 1 TX queue per Ethernet port.
    for (uint16_t i = 0; i < tx_rings; ++i) {
        rte_eth_tx_queue_setup(port_id, i, nb_txd, socket_id, &tx_conf);
    }

    // start the ethernet port.
    int dev_start_ret = rte_eth_dev_start(port_id);
    if (dev_start_ret != 0) {
        printf("Failed to start ethernet for prot %u\n", (unsigned)port_id);
    }

    //NETPERF_OK(rte_eth_promiscuous_enable(port_id));

    // disable the rx/tx flow control
    // todo: why?
    struct rte_eth_fc_conf fc_conf = {};
    rte_eth_dev_flow_ctrl_get(port_id, &fc_conf);
    fc_conf.mode = RTE_FC_NONE;
    rte_eth_dev_flow_ctrl_set(port_id, &fc_conf);
    wait_for_link_status_up(port_id);

   return 0;
}

static int dpdk_init(int argc, char **argv) {
    
    // initialize Environment Abstraction Layer
    // our arguments: "-c", "0xff", "-n", "4", "-w", "0000:37:00.0","--proc-type=auto"
    int args_parsed = rte_eal_init(argc, argv);
    if (args_parsed < 0) {
        rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    }

    // initialize ports
    const uint16_t nbports = rte_eth_dev_count_avail();
    dpdk_nbports = nbports;
    if (nbports <= 0) {
       rte_exit(EXIT_FAILURE, "No ports available\n"); 
    }
    fprintf(stderr, "DPDK reports that %d ports (interfaces) are available.\n", nbports);

    // create a pool of memory for ring buffers
    tx_mbuf_pool = rte_pktmbuf_pool_create(
                        "tx_mbuf_pool",
                        NUM_MBUFS * dpdk_nbports,
                        MBUF_CACHE_SIZE,
                        sizeof(struct tx_pktmbuf_priv),
                        MBUF_BUF_SIZE,
                        rte_socket_id());
    if (tx_mbuf_pool == NULL) {
        printf("Failed to initialize tx mempool\n");
    }
    if (rte_mempool_obj_iter(
        tx_mbuf_pool,
        &custom_init_priv,
        NULL) != (NUM_MBUFS * dpdk_nbports)) {
            return 1;
    }
    if (rte_mempool_obj_iter(
        tx_mbuf_pool,
        &custom_pkt_init_with_header,
        NULL) != (NUM_MBUFS * dpdk_nbports)) {
            return 1;
    }
    rx_mbuf_pool = rte_pktmbuf_pool_create(
                        "rx_mbuf_pool",
                        NUM_MBUFS * dpdk_nbports,
                        MBUF_CACHE_SIZE,
                        sizeof(struct tx_pktmbuf_priv),
                        MBUF_BUF_SIZE * 2,
                        rte_socket_id());
    if (rx_mbuf_pool == NULL) {
        return 1;
        printf("Failed to initialize rx mempool\n");
    }
    if (rte_mempool_obj_iter(
        rx_mbuf_pool,
        &custom_init_priv,
        NULL) != (NUM_MBUFS * dpdk_nbports)) {
            return 1;
    }
    if (rte_mempool_obj_iter(
        rx_mbuf_pool,
        &custom_pkt_init_with_header,
        NULL) != (NUM_MBUFS * dpdk_nbports)) {
            return 1;
    }
    // initialize all ports
    uint16_t i = 0;
    uint16_t port_id = 0;
    RTE_ETH_FOREACH_DEV(i) {
        port_id = i;
        if (init_dpdk_port(i, rx_mbuf_pool) != 0) {
            rte_exit(EXIT_FAILURE, "Failed to initialize port %u\n", (unsigned) port_id);
        }
    }
    our_dpdk_port_id = port_id;
    rte_eth_macaddr_get(our_dpdk_port_id, &my_eth);
    printf("Port %u MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
			(unsigned)our_dpdk_port_id,
			my_eth.addr_bytes[0], my_eth.addr_bytes[1],
			my_eth.addr_bytes[2], my_eth.addr_bytes[3],
			my_eth.addr_bytes[4], my_eth.addr_bytes[5]);


    if (rte_lcore_count() > 1) {
        printf("\nWARNING: Too many lcores enabled. Only 1 used.\n");
    }
    
    printf("Finished eal parsing\n");
    return args_parsed;
}

static uint64_t raw_time(void) {
    struct timespec tstart={0,0};
    clock_gettime(CLOCK_MONOTONIC, &tstart);
    uint64_t t = (uint64_t)(tstart.tv_sec*1.0e9 + tstart.tv_nsec);
    return t;

}

static uint64_t time_now(uint64_t offset) {
    return raw_time() - offset;
}

static int parse_packet(struct sockaddr_in *src,
                        struct sockaddr_in *dst,
                        void **payload,
                        size_t *payload_len,
                        struct rte_mbuf *pkt)
{
    // packet layout order is (from outside -> in):
    // ether_hdr
    // ipv4_hdr
    // udp_hdr
    // client timestamp
    uint8_t *p = rte_pktmbuf_mtod(pkt, uint8_t *);
    size_t header = 0;

    // check the ethernet header
    struct rte_ether_hdr * const eth_hdr = (struct rte_ether_hdr *)(p);
    p += sizeof(*eth_hdr);
    header += sizeof(*eth_hdr);
    uint16_t eth_type = ntohs(eth_hdr->ether_type);
    struct rte_ether_addr mac_addr = {};

    rte_eth_macaddr_get(our_dpdk_port_id, &mac_addr);
    if (!rte_is_same_ether_addr(&mac_addr, &eth_hdr->d_addr) && !rte_is_same_ether_addr(&ether_broadcast, &eth_hdr->d_addr)) {
        printf("Bad MAC: %02" PRIx8 " %02" PRIx8 " %02" PRIx8
			   " %02" PRIx8 " %02" PRIx8 " %02" PRIx8 "\n",
            eth_hdr->d_addr.addr_bytes[0], eth_hdr->d_addr.addr_bytes[1],
			eth_hdr->d_addr.addr_bytes[2], eth_hdr->d_addr.addr_bytes[3],
			eth_hdr->d_addr.addr_bytes[4], eth_hdr->d_addr.addr_bytes[5]);
        return 1;
    }
    if (RTE_ETHER_TYPE_IPV4 != eth_type) {
        printf("Bad ether type.\n");
        return 1;
    }

    // check the IP header
    struct rte_ipv4_hdr *const ip_hdr = (struct rte_ipv4_hdr *)(p);
    p += sizeof(*ip_hdr);
    header += sizeof(*ip_hdr);

    // In network byte order.
    in_addr_t ipv4_src_addr = ip_hdr->src_addr;
    in_addr_t ipv4_dst_addr = ip_hdr->dst_addr;

    if (IPPROTO_UDP != ip_hdr->next_proto_id) {
        printf("Bad next proto_id\n");
        return 1;
    }
    
    src->sin_addr.s_addr = ipv4_src_addr;
    dst->sin_addr.s_addr = ipv4_dst_addr;

    // check udp header
    struct rte_udp_hdr * const udp_hdr = (struct rte_udp_hdr *)(p);
    p += sizeof(*udp_hdr);
    header += sizeof(*udp_hdr);

    // In network byte order.
    in_port_t udp_src_port = udp_hdr->src_port;
    in_port_t udp_dst_port = udp_hdr->dst_port;

    src->sin_port = udp_src_port;
    dst->sin_port = udp_dst_port;
    src->sin_family = AF_INET;
    dst->sin_family = AF_INET;
    
    *payload_len = pkt->pkt_len - header;
    *payload = (void *)p;
    return 0;

}

static uint16_t rte_eth_tx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
    return rte_eth_tx_burst(port_id, queue_id, tx_pkts, nb_pkts);
}

void rte_pktmbuf_free_(struct rte_mbuf *packet) {
    rte_pktmbuf_free(packet);
}

struct rte_mbuf* rte_pktmbuf_alloc_(struct rte_mempool *mp) {
    return rte_pktmbuf_alloc(mp);
}

uint16_t rte_eth_rx_burst_(uint16_t port_id, uint16_t queue_id, struct rte_mbuf **rx_pkts, const uint16_t nb_pkts) {
    uint16_t ret = rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts);
    return ret;
}

uint64_t rte_get_timer_cycles_() {
    return rte_get_timer_cycles();
}

uint64_t rte_get_timer_hz_() {
    return rte_get_timer_hz();
}

int calculate_total_pkts_required(uint32_t seg_size, uint32_t nb_segs) {
    if (seg_size > MAX_SEGMENT_SIZE) {
        if (nb_segs != 1) {
            printf("Cannot handle if segment size > MAX and nb_segs != 1.\n");
            exit(1);
        }
        int total_pkts_required = (int)(ceil((double)seg_size / (double)MAX_SEGMENT_SIZE));
        return total_pkts_required;
    } else {
        uint32_t pkts_per_segment = (uint32_t)MAX_SEGMENT_SIZE / seg_size;
        int total_pkts_required = (int)(ceil((double)nb_segs / (double)pkts_per_segment));
        return total_pkts_required;
    }
}

static int do_client(void) {
    printf("Starting do client\n");
    // initialize all the packets
    initialize_client_requests();
    clock_offset = raw_time();
    uint64_t start_time, end_time;
    struct rte_mbuf *pkts[BURST_SIZE];
    struct rte_mbuf *pkt;
    // char *buf_ptr;
    struct rte_ether_hdr *eth_hdr;
    struct rte_ipv4_hdr *ipv4_hdr;
    struct rte_udp_hdr *udp_hdr;
    uint16_t nb_rx;
    uint64_t reqs = 0;
    uint64_t cycle_wait = intersend_time * rte_get_timer_hz() / (1e9);
    size_t payload_size = 32;
    float rate_gbps = (float)rate * (float)(segment_size * num_mbufs) * 8.0 / (float)1e9;
    
    // TODO: add in scaffolding for timing/printing out quick statistics
    start_time = rte_get_timer_cycles();
    int outstanding = 0;
    uint64_t id = 0;
    int total_pkts_required = calculate_total_pkts_required(segment_size, num_mbufs);
    size_t header_size = sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr);
    size_t full_payload_size = header_size + 32 + (8 * num_mbufs);
    while (rte_get_timer_cycles_() < start_time + seconds * rte_get_timer_hz_()) {
        // send a packet
        pkt = rte_pktmbuf_alloc_(tx_mbuf_pool);
        if (pkt == NULL) {
            printf("Error allocating tx mbuf\n");
            return -EINVAL;
        }
        char *payload_ptr = (char *)(client_request_payloads) + reqs * full_payload_size;
        //printf("Sending packet # %u, payload ptr being copied in: %p\n", (unsigned)reqs, payload_ptr);
        size_t payload_length = 32;

        uint8_t *ptr = rte_pktmbuf_mtod(pkt, uint8_t *);
        uint64_t *timestamp_ptr = rte_pktmbuf_mtod_offset(pkt, uint64_t *, header_size);
        rte_memcpy((char *)ptr, (char *)payload_ptr, full_payload_size);
        
        /* record timestamp in the payload itself*/
        uint64_t send_time = time_now(clock_offset);
        *timestamp_ptr = send_time;

        pkt->l2_len = RTE_ETHER_HDR_LEN;
        pkt->l3_len = sizeof(struct rte_ipv4_hdr);
        pkt->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
        pkt->data_len = full_payload_size;
        pkt->pkt_len = full_payload_size;
        pkt->nb_segs = 1;

        if (with_read_mode) {
            pkt->data_len = num_mbufs * segment_size;
            pkt->pkt_len = num_mbufs * segment_size;
        }

        int pkts_sent = 0;

        while (pkts_sent < 1) {
            pkts_sent = rte_eth_tx_burst(our_dpdk_port_id, 0, &pkt, 1);
        }
        outstanding += total_pkts_required;
        uint64_t last_sent = rte_get_timer_cycles();
        //printf("Sent packet at %u, %u send_time, %d is outstanding, intersend is %u\n", (unsigned)last_sent, (unsigned)send_time, outstanding, (unsigned)intersend_time);

        /* now poll on receiving packets */
        nb_rx = 0;
        reqs += 1;
        while ((outstanding > 0)) {
            nb_rx = rte_eth_rx_burst_(our_dpdk_port_id, 0, pkts, BURST_SIZE);
            if (nb_rx == 0) {
                if (rte_get_timer_cycles() > (last_sent + cycle_wait)) {
                    break;
                }
                continue;
            }

            for (int i = 0; i < nb_rx; i++) {
                struct sockaddr_in src, dst;
                void *payload = NULL;
                size_t payload_length = 0;
                int valid = parse_packet(&src, &dst, &payload, &payload_length, pkts[i]);
                if (valid == 0) {
                    /* parse the timestamp and record it */
                    uint64_t now = (uint64_t)time_now(clock_offset);
                    uint64_t then = (*(uint64_t *)payload);
                    uint64_t id = (*(uint64_t *)(payload + 8));
                    //printf("Got a packet at time now: %u from %u, with payload_length: %u (with header), id: %u\n", (unsigned)(now), (unsigned)then, (unsigned)payload_length + 42, (unsigned)id);
                    add_latency_to_map(&packet_map, now - then, id);
                    rte_pktmbuf_free_(pkts[i]);
                    outstanding--;
                } else {
                    rte_pktmbuf_free_(pkts[i]);
                }
            }
        }
        while (((last_sent + cycle_wait) >= rte_get_timer_cycles())) {
            continue;
        }
    }
    end_time = rte_get_timer_cycles();
    float total_time = (float) (end_time - start_time) / rte_get_timer_hz(); 
    printf("\nRan for %f seconds, sent %"PRIu64" packets.\n",
			total_time, reqs);
    calculate_latencies(&packet_map, &latency_dist, reqs, total_pkts_required);
    dump_latencies(&latency_dist, total_time, num_mbufs * segment_size, rate_gbps);
    free_pktmap(&packet_map);
    printf("Freed packet map; about to free client requests payloads at %p\n", client_request_payloads);
    free(client_request_payloads);
    printf("Freed client request payloads\n");
    return 0;
}

static void swap_headers(struct rte_mbuf *tx_buf, struct rte_mbuf *rx_buf, size_t payload_length) {
    struct rte_ether_hdr *rx_ptr_mac_hdr;
    struct rte_ipv4_hdr *rx_ptr_ipv4_hdr;
    struct rte_udp_hdr *rx_rte_udp_hdr;
    struct rte_ether_hdr *tx_ptr_mac_hdr;
    struct rte_ipv4_hdr *tx_ptr_ipv4_hdr;
    struct rte_udp_hdr *tx_rte_udp_hdr;
    uint64_t *tx_buf_id_ptr;
    uint64_t *rx_buf_id_ptr;

   /* swap src and dst ether addresses */
   rx_ptr_mac_hdr = rte_pktmbuf_mtod(rx_buf, struct rte_ether_hdr *);
   tx_ptr_mac_hdr = rte_pktmbuf_mtod(tx_buf, struct rte_ether_hdr *);
   rte_ether_addr_copy(&rx_ptr_mac_hdr->s_addr, &tx_ptr_mac_hdr->d_addr);
   rte_ether_addr_copy(&rx_ptr_mac_hdr->d_addr, &tx_ptr_mac_hdr->s_addr);
   tx_ptr_mac_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

   /* swap src and dst ip addresses */
   rx_ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
   tx_ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(tx_buf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
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
   rx_rte_udp_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
   tx_rte_udp_hdr = rte_pktmbuf_mtod_offset(tx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
   tx_rte_udp_hdr->src_port = rx_rte_udp_hdr->dst_port;
   tx_rte_udp_hdr->dst_port = rx_rte_udp_hdr->src_port;
   tx_rte_udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_length);
   tx_rte_udp_hdr->dgram_cksum = 0;

   /* Set packet id and timestamp */
   tx_buf_id_ptr = rte_pktmbuf_mtod_offset(tx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN);
   rx_buf_id_ptr = rte_pktmbuf_mtod_offset(rx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN);
   *tx_buf_id_ptr = *rx_buf_id_ptr;

   tx_buf_id_ptr = rte_pktmbuf_mtod_offset(tx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN + 8);
   rx_buf_id_ptr = rte_pktmbuf_mtod_offset(rx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN + 8);
   *tx_buf_id_ptr = *rx_buf_id_ptr;
}

void swap_headers_into_static_payload(struct rte_mbuf *rx_buf, size_t payload_length, size_t offset) {
    void *payload_ptr = (void *)((char *)payload_to_copy + offset); // will be a 64 byte offset
    struct rte_ether_hdr *rx_ptr_mac_hdr;
    struct rte_ipv4_hdr *rx_ptr_ipv4_hdr;
    struct rte_udp_hdr *rx_rte_udp_hdr;
    struct rte_ether_hdr *tx_ptr_mac_hdr;
    struct rte_ipv4_hdr *tx_ptr_ipv4_hdr;
    struct rte_udp_hdr *tx_rte_udp_hdr;
    uint64_t *tx_buf_id_ptr;
    uint64_t *rx_buf_id_ptr;

   /* swap src and dst ether addresses */
   rx_ptr_mac_hdr = rte_pktmbuf_mtod(rx_buf, struct rte_ether_hdr *);
   tx_ptr_mac_hdr = (struct rte_ether_hdr *)(payload_ptr);
   rte_ether_addr_copy(&rx_ptr_mac_hdr->s_addr, &tx_ptr_mac_hdr->d_addr);
   rte_ether_addr_copy(&rx_ptr_mac_hdr->d_addr, &tx_ptr_mac_hdr->s_addr);
   tx_ptr_mac_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

   /* swap src and dst ip addresses */
   rx_ptr_ipv4_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_ipv4_hdr *, RTE_ETHER_HDR_LEN);
   tx_ptr_ipv4_hdr = (struct rte_ipv4_hdr *)((char *)payload_ptr + RTE_ETHER_HDR_LEN);
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
   rx_rte_udp_hdr = rte_pktmbuf_mtod_offset(rx_buf, struct rte_udp_hdr *, RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
   tx_rte_udp_hdr = (struct rte_udp_hdr *)((char *)payload_ptr + RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr));
   tx_rte_udp_hdr->src_port = rx_rte_udp_hdr->dst_port;
   tx_rte_udp_hdr->dst_port = rx_rte_udp_hdr->src_port;
   tx_rte_udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_length);
   tx_rte_udp_hdr->dgram_cksum = 0;

   /* Set packet id and timestamp */
   tx_buf_id_ptr = (uint64_t *)((char *)payload_ptr + RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
   rx_buf_id_ptr = rte_pktmbuf_mtod_offset(rx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN);
   *tx_buf_id_ptr = *rx_buf_id_ptr;

   tx_buf_id_ptr = (uint64_t *)((char *)payload_ptr + RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + 8);
   rx_buf_id_ptr = rte_pktmbuf_mtod_offset(rx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN + 8);
   *tx_buf_id_ptr = *rx_buf_id_ptr;
}


static int do_server(void) {
    struct rte_mbuf *rx_bufs[BURST_SIZE];
    // for fake GSO, need to transmit up to MAX_SCATTERS separate packets
    // potentially
    // TODO: optimization where if only 1 packet per request is needed, burst
    // all the ones that came at once
    struct rte_mbuf *tx_bufs[MAX_SCATTERS][MAX_SCATTERS];
    struct rte_mbuf *rx_buf;
    struct rte_mbuf *tx_buf;
    uint64_t offsets[MAX_SCATTERS];
    uint8_t queue = 0; // our application only uses one queue
    
    uint16_t nb_rx, n_to_tx, nb_tx, i;
    size_t nb_req = 0;

    /* Run until the application is quit or killed. */
    for (;;) {
        nb_rx = rte_eth_rx_burst(our_dpdk_port_id, queue, rx_bufs, BURST_SIZE);
        if (nb_rx == 0) {
            continue;
        }
        n_to_tx = 0;
        for (i = 0; i < nb_rx; i++) {
            struct sockaddr_in src, dst;
            void *payload = NULL;
            size_t payload_length = 0;
            int valid = parse_packet(&src, &dst, &payload, &payload_length, rx_bufs[i]);
            struct rte_mbuf* secondary = NULL;
            
            if (valid == 0) {
                //printf("Received a valid packet\n");
                rx_buf = rx_bufs[i];
                size_t header_size = rx_buf->pkt_len - payload_length;
                uint32_t segment_length = (uint32_t)(*(uint64_t *)((char *)payload + 16));
                uint16_t nb_segs = (uint16_t)(*(uint64_t *)((char *)payload + 24));
                // fill in the offsets
                for (uint16_t seg = 0; seg < nb_segs; seg++) {
                    offsets[(size_t)seg] = *((uint64_t *)((char *)payload + 32 + (8 * (size_t)seg)));
                }

                nb_req += 1;

                int total_pkts_required = calculate_total_pkts_required(segment_length, (uint32_t)nb_segs);
                uint32_t payload_left = segment_length * nb_segs;
                //printf("Total packets required for %u segment_size and %u nb_segs is %d; total_payload: %u\n", (unsigned)segment_length, (unsigned)nb_segs, total_pkts_required, (unsigned)payload_left);
                
                size_t seg_offset = 0;
                for (int pkt = 0; pkt < total_pkts_required; pkt++) {
                    struct rte_mbuf *prev = NULL;
                    uint32_t actual_segment_size = MIN(payload_left, MIN(segment_length, MAX_SEGMENT_SIZE));
                    int nb_segs_segment = (MIN(payload_left, (uint32_t)MAX_SEGMENT_SIZE)) / actual_segment_size;
                    uint32_t pkt_len = actual_segment_size * (uint32_t)nb_segs_segment;

                    if (zero_copy_mode) {
                        for (int seg = 0; seg < nb_segs_segment; seg++) {
                            void *payload_at_offset = (void *)((char *)payload_to_copy + offsets[seg_offset]);
                            // TODO: is this cast to iova correct here?
                            rte_iova_t iova_at_offset = (rte_iova_t)((char *)payload_iova + offsets[seg_offset]);
                            tx_bufs[seg][pkt] = rte_pktmbuf_alloc(extbuf_pool); // allocate an external buffer for each segment
                            if (tx_bufs[seg][pkt] == NULL) {
                                printf("Failed to allocate tx buf burst # %i, seg %i\n", i, seg);
                                exit(1);
                            }

                            tx_bufs[seg][pkt]->buf_addr = payload_at_offset;
                            tx_bufs[seg][pkt]->buf_iova = iova_at_offset;
                            tx_bufs[seg][pkt]->data_off = 0;
                            
                            //printf("\t->Allocated packet segment %d, for pkt %d\n", seg, pkt);
                            if (seg == 0) {
                                // copy in the packet header to the first segment
                                // for fake GSO segmentation.
                                // this does copy into the payload, but we're
                                // assuming that there's either 1 client per
                                // core (hash based on source ethernet header)
                                // or only 1 client is sending
                                swap_headers_into_static_payload(rx_buf, pkt_len - (RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr)), (size_t)(offsets[seg_offset]));
                                //printf("Setting pkt len as %u\n", pkt_len);
                                tx_bufs[seg][pkt]->pkt_len = pkt_len;
                                tx_bufs[seg][pkt]->nb_segs = nb_segs_segment;
                                tx_bufs[seg][pkt]->l2_len = RTE_ETHER_HDR_LEN;
                                tx_bufs[seg][pkt]->l3_len = sizeof(struct rte_ipv4_hdr);
                                tx_bufs[seg][pkt]->l4_len = sizeof(struct rte_udp_hdr);
                                tx_bufs[seg][pkt]->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
                            }
                            if (prev != NULL) {
                               prev->next = tx_bufs[seg][pkt];
                            }
                            //printf("\t->Pkt # %d, segment # %d, data_len: %u\n", pkt, seg, (unsigned)actual_segment_size);
                            tx_bufs[seg][pkt]->data_len = (uint16_t)actual_segment_size;
                            prev = tx_bufs[seg][pkt];
                        }
                    } else {
                        tx_bufs[0][pkt] = rte_pktmbuf_alloc(tx_mbuf_pool);
                        if (tx_bufs[0][pkt] == NULL) {
                            printf("Failed to allocate tx buf burst # %i, seg %i\n", i, 0);
                            exit(1);
                        }
                        swap_headers(tx_bufs[0][pkt],
                                     rx_buf, 
                                     (size_t)(pkt_len) - 
                                     (RTE_ETHER_HDR_LEN + 
                                        sizeof(struct rte_ipv4_hdr) + 
                                        sizeof(struct rte_udp_hdr)));
                        tx_bufs[0][pkt]->pkt_len = pkt_len;
                        tx_bufs[0][pkt]->data_len = pkt_len;
                        tx_bufs[0][pkt]->nb_segs = 1;
                        tx_bufs[0][pkt]->l2_len = RTE_ETHER_HDR_LEN;
                        tx_bufs[0][pkt]->l3_len = sizeof(struct rte_ipv4_hdr);
                        tx_bufs[0][pkt]->l4_len = sizeof(struct rte_udp_hdr);
                        tx_bufs[0][pkt]->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
                        char *payload_ptr = rte_pktmbuf_mtod_offset(tx_bufs[0][pkt], char *, header_size + 16);
                        size_t amt_to_copy_left = pkt_len;
                        for (int seg = 0; seg < (size_t)(nb_segs); seg++) {
                            void *copy_pointer = (void *)((char *)payload_to_copy + offsets[seg_offset]);
                            // specifically DON'T copy less for the copy-out
                            // solution
                            size_t amt_to_copy = MIN(amt_to_copy_left, actual_segment_size);
                            rte_memcpy(payload_ptr, copy_pointer, amt_to_copy);
                            payload_ptr += (size_t)amt_to_copy;
                            amt_to_copy_left -= amt_to_copy;
                        }
                    tx_bufs[0][pkt]->next = NULL;
                    }
                    // at the end of finishing each packet
                    // increment segment offset and amount of total payload left
                    seg_offset += 1;
                    payload_left -= pkt_len;
                }
                assert(payload_left == 0);
                //printf("Past construction, about to burst\n");

                // burst all of the packets for this response
                nb_tx = rte_eth_tx_burst_(our_dpdk_port_id, queue, tx_bufs[0], total_pkts_required);
                if (nb_tx != total_pkts_required) {
                    printf("error: could not transmit all %u pkts, transmitted %u\n", total_pkts_required, nb_tx);
                }
                rte_pktmbuf_free(rx_buf);
            }
        }
    }
    return 0;
}

static void sig_handler(int signum){
    // free any resources
    rte_free(payload_to_copy);

    if (has_latency_log) {
        free(latency_log);
    }
}

static int do_memcpy_bench() {
    // initialize pointers within
    // number of segments: divide the array_size by the segment_size
    if (array_size % segment_size != 0) {
        printf("Segment size %u is not aligned to array size %u\n", (unsigned)segment_size, (unsigned)array_size);
        exit(1);
    }

    if (segment_size % 64 != 0) {
        printf("Segment size %u not divisible by 64.\n", (unsigned)segment_size);
    }
    size_t len = (size_t)(array_size / segment_size);
    size_t* indices = malloc(sizeof(size_t) * len);
    if (indices == NULL) {
        printf("Failed to allocate indices.\n");
        exit(1);
    }

    for (size_t i = 0; i < len; i++) {
        indices[i] = i;
    }

    for (size_t i = 0; i < len -1; i++) {
        size_t j = i + (rand() % (len - i));
        if (i != j) {
            size_t tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }
    }

    void *tmp = malloc(segment_size);
    if (tmp == NULL) {
        printf("Was not able to allocate tmp array to store segment size\n");
        exit(1);
    }

    if (!zero_copy_mode) {
        // fill in the whole memory with integers
        uint64_t max_int = MAX_ITERATIONS;
        uint64_t cur_int = 0;
        uint64_t *cur_ptr = (uint64_t *)payload_to_copy;
        for (size_t i = 0; i < (array_size / 8); i++) {
            *cur_ptr = cur_int;
            cur_ptr++;
            cur_int = (cur_int + 1) % MAX_ITERATIONS;
        }
    }

    // now fill memory with pointer references (offsets)
    for (size_t i = 1; i < len; i++) {
        size_t prev_off = (i - 1) * segment_size; 
        size_t *prev_ptr = (size_t *)((char *)payload_to_copy + prev_off);
        *prev_ptr = (size_t)(indices[i] * segment_size);
    }

    //  fill last and first
    uint64_t *last_off = (uint64_t *)((char *)payload_to_copy + (len - 1) * segment_size);
    *last_off = (uint64_t)(indices[0] * segment_size);
    
    // do the benchmark
    size_t original_count = MAX(MAX_ITERATIONS, len * 10);
    size_t count = original_count;
    clock_offset = raw_time();
    uint64_t start = time_now(clock_offset);
    // start at the first pointer
    size_t cur_off = 0;
    while (count > 0) {
        // current thing to copy
        char *cur_ptr = (char *)payload_to_copy + (cur_off);
        if (!zero_copy_mode) {
            rte_memcpy(tmp, cur_ptr, segment_size);
            cur_off = *(size_t *)cur_ptr;
        } else {
            cur_off = *(size_t *)cur_ptr;
            // read every 64 byte chunk in this
            size_t tmp = 0;
            for (int i = 1; i < segment_size / 64; i += 64) {
                size_t *ptr = (size_t *)(cur_ptr + i * 64);
                tmp = *ptr;
            }
        }
        count--;
    }

    uint64_t end = time_now(clock_offset);
    uint64_t time_taken = end - start;
    uint64_t avg = (uint64_t)((float)(time_taken) / (float)original_count);
    free(tmp);
    free(indices);
    rte_free(payload_to_copy);

    if (zero_copy_mode) {
        printf("for array size of %u, did %u %u-byte segment reads in %u ns; %u avg.\n",
            (unsigned)array_size,
            (unsigned)original_count,
            (unsigned)segment_size,
            (unsigned)time_taken,
            (unsigned)(avg));
        printf("%lu,%u,False,%lu\n", array_size, segment_size, avg);
        return 0;
    } else {
        printf("for array size of %u, segment size of %u, did %u memcpys in %u ns; %u avg.\n",
            (unsigned)array_size,
            (unsigned)segment_size,
            (unsigned)original_count,
            (unsigned)time_taken,
            (unsigned)(avg));
        printf("%lu,%u,True,%lu\n", array_size, segment_size, avg);
        return 0;
    }
}

int
main(int argc, char **argv)
{
	int ret;
    int args_parsed = dpdk_init(argc, argv);
    argc -= args_parsed;
    argv += args_parsed;

    // initialize our arguments
    ret = parse_args(argc, argv);
    if (ret != 0) {
        return ret;
    }

    if (mode == MODE_MEMCPY_BENCH) {
        return do_memcpy_bench();
    }

    //signal(SIGINT, sig_handler); // Register signal handler
    if (mode == MODE_UDP_CLIENT) {
        return do_client();
    } else {
        do_server();
    }


    printf("Reached end of program execution\n");

	return 0;
}
