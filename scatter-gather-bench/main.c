/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */
#define _GNU_SOURCE
#include "mem.h"
#include "debug.h"
#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <math.h>
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
#include <pthread.h>
#include <sched.h>
#include "rand_exp.h"
#include "thread_info.h"
#include "stats.h"

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
#include <rte_thash.h>

/* Replace a string.
 * Taken from: https://www.geeksforgeeks.org/c-program-replace-word-text-another-given-word/
 * Function to replace a string with another
 * string
 */
char* replaceWord(const char* s, const char* oldW,
                  const char* newW)
{
    char* result;
    int i, cnt = 0;
    int newWlen = strlen(newW);
    int oldWlen = strlen(oldW);

    // Counting the number of times old word
    // occur in the string
    for (i = 0; s[i] != '\0'; i++) {
        if (strstr(&s[i], oldW) == &s[i]) {
            cnt++;

            // Jumping to index after the old word.
            i += oldWlen - 1;
        }
    }

    // Making new string of enough length
    result = (char*)malloc(i + cnt * (newWlen - oldWlen) + 1);

    i = 0;
    while (*s) {
        // compare the substring with the result
        if (strstr(s, oldW) == s) {
            strcpy(&result[i], newW);
            i += newWlen;
            s += oldWlen;
        }
        else
            result[i++] = *s++;
    }

    result[i] = '\0';
    return result;
}
static uint8_t sym_rss_key[] = {
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
};
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

#define NNUMA 2
#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

/******************************************/
/******************************************/
typedef unsigned long physaddr_t;
typedef unsigned long virtaddr_t;
/******************************************/
/******************************************/
static __thread int thread_id = 0;
#define MAX_ITERATIONS 8000000
static char* latency_log = NULL;
static char *threads_log = NULL;
static int has_threads_log = 0;
static int has_latency_log = 0;

struct tx_pktmbuf_priv
{
    int32_t lkey;
    int32_t field2; // needs to be atleast 8 bytes large
};

typedef struct OutgoingHeader
{
    struct rte_ether_hdr eth;
    struct rte_ipv4_hdr ipv4;
    struct rte_udp_hdr udp;
} __attribute__((packed)) OutgoingHeader;


typedef struct ClientRequest
{
    uint64_t timestamp; // this is within the request struct, but not sent
    uint64_t packet_id;
    uint64_t segment_offsets[32]; // maximum number of segments we'd be asking for (within array_size)
} __attribute__((packed)) ClientRequest;

static void add_latency(Latency_Dist_t *dist, uint64_t latency) {
    static __thread int thread_id;
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

static void dump_debug_latencies(Latency_Dist_t *dist) {
    static __thread int thread_id;
    // sort the latencies
    NETPERF_DEBUG("Dumping latencies: ct: %lu", dist->total_count);
    uint64_t *arr = malloc(dist->total_count * sizeof(uint64_t));
    NETPERF_DEBUG("Allocated array");
    if (arr == NULL) {
        printf("Not able to allocate array to sort latencies\n");
        exit(1);
    }
    for (size_t i = 0; i < dist->total_count; i++) {
        arr[i] = dist->latencies[i];
    }
    NETPERF_DEBUG("Filled array");
    qsort(arr, dist->total_count, sizeof(uint64_t), cmpfunc);
	uint64_t avg_latency = (dist->latency_sum) / (dist->total_count);
    uint64_t median = arr[(size_t)((double)dist->total_count * 0.50)];
    uint64_t p99 = arr[(size_t)((double)dist->total_count * 0.99)];
    uint64_t p999 = arr[(size_t)((double)dist->total_count * 0.999)];

    printf("Stats:\n\t- Min latency: %u ns\n\t- Max latency: %u ns\n\t- Avg latency: %" PRIu64 " ns", (unsigned)dist->min, (unsigned)dist->max, avg_latency);
    printf("\n\t- Median latency: %u ns\n\t- p99 latency: %u ns\n\t- p999 latency: %u ns\n", (unsigned)median, (unsigned)p99, (unsigned)p999);
    free(arr);
}

static void dump_latencies(Latency_Dist_t *dist, float total_time, size_t message_size, float rate_gbps, float rate_pps, size_t client_id, Summary_Statistics_t *statistics) {
    static __thread int thread_id;
    statistics->recved = dist->total_count;
    printf("Trying to dump latencies on client side with message size for client %lu: %lu\n", message_size, client_id);
    if (dist->total_count == 0) {
        NETPERF_WARN("No packets received");
        return;
    }
    // sort the latencies
    uint64_t *arr = malloc(dist->total_count * sizeof(uint64_t));
    if (arr == NULL) {
        printf("Not able to allocate array to sort latencies\n");
        exit(1);
    }
    for (size_t i = 0; i < dist->total_count; i++) {
        arr[i] = dist->latencies[i];
    }
    
    qsort(arr, dist->total_count, sizeof(uint64_t), cmpfunc);
    uint64_t avg_latency = (dist->latency_sum) / (dist->total_count);
    uint64_t median = arr[(size_t)((double)dist->total_count * 0.50)];
    uint64_t p99 = arr[(size_t)((double)dist->total_count * 0.99)];
    uint64_t p999 = arr[(size_t)((double)dist->total_count * 0.999)];
    
    float achieved_rate_gbps = (float)(dist->total_count) / (float)total_time * (float)(message_size) * 8.0 / (float)1e9;
    float percent_rate = achieved_rate_gbps / rate_gbps;
    if (has_latency_log) {
        write_latency_log(latency_log, dist, client_id);
    }
    free((void *)arr);
    statistics->min = dist->min;
    statistics->max = dist->max;
    statistics->avg = avg_latency;
    statistics->median = median;
    statistics->p99 = p99;
    statistics->p999 = p999;
    statistics->offered_rate_gbps = rate_gbps;
    statistics->offered_rate_pps = rate_pps;
    statistics->achieved_rate_gbps = achieved_rate_gbps;
    statistics->achieved_rate_pps = (float)(dist->total_count) / (float)total_time;
    statistics->percent_rate = percent_rate;
    return;
}

typedef struct Packet_Map_t
{
    uint64_t *rtts;
    uint32_t *ids; // unique ids sent in each packet
    size_t total_count;
    uint32_t *sent_ids;
    uint64_t *grouped_rtts;
} Packet_Map_t;

static void add_latency_to_map(Packet_Map_t *map, uint64_t rtt, uint32_t id) {
    static __thread int thread_id;
    map->rtts[map->total_count] = rtt;
    map->ids[map->total_count] = id;
    map->total_count++;
    if (map->total_count > (MAX_ITERATIONS * 32)) {
        printf("[add_latency_to_map] ERROR: Overflow in Packet map");
    }
}

static void alloc_pktmap(Packet_Map_t *map, Latency_Dist_t *latency_dist) {
    static __thread int thread_id;
    latency_dist->latencies = malloc(MAX_ITERATIONS * sizeof(uint64_t));
    latency_dist->min = LONG_MAX;
    if (latency_dist->latencies == NULL) {
        NETPERF_WARN("Latency dist latencies null");
        exit(1);
    }

    map->rtts = malloc(MAX_ITERATIONS * sizeof(uint64_t) * 32);
    if (map->rtts == NULL) {
        NETPERF_WARN("Packet map RTTs latencies null");
        exit(1);
    }
    
    map->ids = malloc(MAX_ITERATIONS * sizeof(uint32_t) * 32);
    if (map->ids == NULL) {
        NETPERF_WARN("Packet map Ids latencies null");
        exit(1);
    }
}

static void free_pktmap(Packet_Map_t *map, Latency_Dist_t *latency_dist) {
    static __thread int thread_id;
    // free latency dist fields
    if (latency_dist->latencies != NULL) {
        free(latency_dist->latencies);
    }

    if (map->ids != NULL) {
        free(map->ids);
    }
    if (map->rtts != NULL) {
        free(map->rtts);
    }
    if (map->sent_ids != NULL) {
        free(map->sent_ids);
    }

    if (map->grouped_rtts != NULL) {
        free(map->grouped_rtts);
    }
}

static void calculate_latencies(Packet_Map_t *map, Latency_Dist_t *dist, size_t num_sent, size_t num_per_bucket) {
    static __thread int thread_id;
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

#define MAKE_IP_ADDR(a, b, c, d)			\
	(((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d)

/******************************************/
// MACROS
#define POINTER_SIZE (sizeof(uint64_t))
#define get_next_ptr(mem, idx) \
    *(get_client_ptr(mem, idx))
#define get_client_ptr(mem, idx) ((uint64_t *)((char *)mem + ((idx) * POINTER_SIZE)))
#define get_server_region(memory, idx, segment_size) ((void *)((char *)memory + ((idx) * (segment_size))))
#define get_client_req(client_reqs, idx) (ClientRequest *)((char *)client_reqs + (size_t)(idx) * (sizeof(ClientRequest)))
#define read_u64(ptr, offset) *((uint64_t *)((char *)ptr + (offset) * sizeof(uint64_t)))
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
#define MAX_THREADS 8
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
static size_t num_client_threads = 1; // number of client threads sending packets (automatically figures out port / IP to use)
static size_t client_id = 0; // out of all the separate clients sending, which client am I
static uint64_t random_seed = 0; // random seed to generate the  payload with
static pthread_t threads[MAX_THREADS];
static cpu_set_t cpusets[MAX_THREADS];
static uint16_t dpdk_nbports;
static uint8_t mode;
static uint8_t memory_mode;
static size_t num_mbufs = 1;
static uint32_t client_ip;
static uint8_t client_ip_octets[4];
static uint32_t server_ip;
static uint8_t server_ip_octets[4];
static uint32_t segment_size = 256;
static uint32_t seconds = 1;
static uint32_t rate = 500000; // in packets / second
static uint32_t intersend_time;
static unsigned int client_port = 12345;
static unsigned int server_port = 12345;
static struct rte_mempool *tx_mbuf_pools[MAX_THREADS];
static struct rte_mempool *rx_mbuf_pools[MAX_THREADS];
static struct rte_mempool *extbuf_pools[MAX_THREADS];
static uint16_t our_dpdk_port_id;
static struct rte_ether_addr my_eth;
static Latency_Dist_t latency_dists[MAX_THREADS];
static Packet_Map_t packet_maps[MAX_THREADS];
static uint64_t clock_offset = 0;
static int zero_copy_mode = 1;
static void *payload_to_copy = NULL;
static rte_iova_t payload_iova = 0;
size_t array_size = 10000; // default array size
static struct ClientRequest *client_requests[MAX_THREADS];
static struct OutgoingHeader outgoing_headers[MAX_THREADS];
static struct Summary_Statistics_t summary_statistics[MAX_THREADS];
static char *tx_pool_names[MAX_THREADS] = {"tx_pool_0", "tx_pool_1", "tx_pool_2", "tx_pool_3", "tx_pool_4", "tx_pool_5", "tx_pool_6", "tx_pool_7"};
static char *rx_pool_names[MAX_THREADS] = {"rx_pool_0", "rx_pool_1", "rx_pool_2", "rx_pool_3", "rx_pool_4", "rx_pool_5", "rx_pool_6", "rx_pool_7"};

#ifdef __TIMERS__
static Latency_Dist_t server_processing_dist = { .min = LONG_MAX, .max = 0, .total_count = 0, .latency_sum = 0 };
static Latency_Dist_t server_send_dist = { .min = LONG_MAX, .max = 0, .total_count = 0, .latency_sum = 0 };
static Latency_Dist_t server_construction_dist = { .min = LONG_MAX, .max = 0, .total_count = 0, .latency_sum = 0 };
#endif

// static unsigned int num_queues = 1;
/******************************************/
/******************************************/

static int initialize_pointer_chasing_at_client(uint64_t **pointer_segments,
                                            size_t array_size, 
                                            size_t segment_size,
                                            uint64_t random_seed) {
    srand(random_seed);
    if (array_size % segment_size != 0) {
        printf("Segment size %u not aligned to array size %u", (unsigned)segment_size, (unsigned)array_size);
        return -EINVAL;
    }

    size_t len = (size_t)(array_size / segment_size);
    uint64_t *indices = malloc(sizeof(uint64_t) * len);
    if (indices == NULL) {
        printf("Failed to allocate indices to initialize pointer chasing.");
        return -ENOMEM;
    }
    
    for (uint64_t i = 0; i < len; i++) {
        indices[i] = i;
    }

    for (uint64_t i = 0; i < len -1; i++) {
        uint64_t j = i + ((uint64_t)rand() % (len - i));
        if (i != j) {
            uint64_t tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }
    }

    for (uint64_t i = 0; i < len; i++) {
    }
    void *pointers = malloc(sizeof(uint64_t) * len);
    if (pointers == NULL) {
        printf("Failed to allocate pointers to chase.");
        return -ENOMEM;
    }

    for (size_t i = 1; i < len; i++) {
        uint64_t *ptr = get_client_ptr(pointers, i - 1);
        NETPERF_ASSERT(((char *)ptr - (char *)pointers) == POINTER_SIZE * (i - 1), "Ptr not in right place");
        *ptr = indices[i];
    }

    *(get_client_ptr(pointers, len - 1)) = indices[0];
    *pointer_segments = pointers;
    free(indices);
    return 0;
}

static void initialize_outgoing_header(OutgoingHeader *header,
                                        struct rte_ether_addr *src_addr,
                                        struct rte_ether_addr *dst_addr,
                                        uint32_t src_ip,
                                        uint32_t dst_ip,
                                        uint16_t src_port,
                                        uint16_t dst_port,
                                        size_t payload_size) {
    static __thread int thread_id;
    struct rte_ether_hdr *eth_hdr = &header->eth;
    struct rte_ipv4_hdr *ipv4_hdr = &header->ipv4;
    struct rte_udp_hdr *udp_hdr = &header->udp;

    // fill in the header
    rte_ether_addr_copy(src_addr, &eth_hdr->s_addr);
    rte_ether_addr_copy(dst_addr, &eth_hdr->d_addr);
    eth_hdr->ether_type = htons(RTE_ETHER_TYPE_IPV4);

    /* add in ipv4 header*/
    ipv4_hdr->version_ihl = IP_VHL_DEF;
    ipv4_hdr->type_of_service = 0;
    ipv4_hdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + payload_size);
    ipv4_hdr->packet_id = 0;
    ipv4_hdr->fragment_offset = 0;
    ipv4_hdr->time_to_live = IP_DEFTTL;
    ipv4_hdr->next_proto_id = IPPROTO_UDP;
    ipv4_hdr->src_addr = rte_cpu_to_be_32(src_ip);
    ipv4_hdr->dst_addr = rte_cpu_to_be_32(dst_ip);
    /* offload checksum computation in hardware */
    ipv4_hdr->hdr_checksum = 0;

    /* add in UDP hdr*/
    udp_hdr->src_port = rte_cpu_to_be_16(src_port);
    udp_hdr->dst_port = rte_cpu_to_be_16(dst_port);
    udp_hdr->dgram_len = rte_cpu_to_be_16(sizeof(struct rte_udp_hdr) + payload_size);
    udp_hdr->dgram_cksum = 0;
}

// initialize common parts of client requests
static void initialize_client_requests_common(size_t num_total_clients) {
    size_t num_requests = (size_t)((float)seconds * rate * 1.20);
	uint64_t *indices = NULL;
	if (initialize_pointer_chasing_at_client(&indices, array_size, segment_size, time(0)) != 0) {
		printf("Failed to initialize pointer chasing at client\n");
		exit(1);
	}

    for (size_t client_id = 0; client_id < num_total_clients; client_id++) {
        client_requests[client_id] = malloc(sizeof(struct ClientRequest) * num_requests);
        if (client_requests == NULL) {
            printf("Failed to malloc client requests array\n");
            exit(1);
        } else {
            printf("Initialized client requests at %p\n", client_requests);
        }
        struct ClientRequest *current_req = (struct ClientRequest*)client_requests[client_id];
        size_t num_segments_within_region = (array_size - segment_size) / 64;
        uint64_t cur_region_idx = indices[0]; // TODO: start them all at an offset from each other?
        for (size_t iter = 0; iter < num_requests; iter++) {
            current_req->timestamp = 0; // TODO: fill this in
            current_req->packet_id = (uint64_t)iter;
            for (size_t i = 0; i < (size_t)num_mbufs; i++) {
                //NETPERF_DEBUG("[Client %lu], req %lu, ,seg # %lu, index: %lu", client_id, iter, i, cur_region_idx);
                current_req->segment_offsets[i] = cur_region_idx;
                cur_region_idx = get_next_ptr(indices, cur_region_idx);
            }
        current_req++;
        }

        
    }
    free(indices);
}

// initialize specific part of client header (per thread)
static void initialize_client_header(uint32_t ip, uint16_t port, size_t client_id) {
    size_t client_payload_size = sizeof(uint64_t) * (2 + (size_t)num_mbufs);
    size_t num_requests = (size_t)((float)seconds * rate * 1.20);
    // initialize timestamps for each thread
    uint64_t *timestamps = malloc(sizeof(uint64_t) * num_requests);
    NETPERF_INFO("Starting to sample distr");
    sample_exp_distribution((double)intersend_time, num_requests, timestamps);
    NETPERF_INFO("Finished sample distr");
    ClientRequest *current_req = (struct ClientRequest*)client_requests[client_id];
    for (size_t iter = 0; iter < num_requests; iter++) {
        current_req->timestamp = timestamps[iter];
        NETPERF_DEBUG("[Client %lu] Timestamp to send packet: %lu", client_id, current_req->timestamp);
        current_req++;
    }
    NETPERF_INFO("Finished copying timestamps");
    free(timestamps);
    initialize_outgoing_header(&outgoing_headers[client_id],
                                &my_eth,
                                &server_mac,
                                ip,
                                server_ip,
                                port,
                                server_port,
                                client_payload_size);
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


static int str_to_ip(const char *str, uint32_t *addr, uint8_t *a, uint8_t *b, uint8_t *c, uint8_t *d)
{
	if(sscanf(str, "%hhu.%hhu.%hhu.%hhu", a, b, c, d) != 4) {
		return -EINVAL;
	}

	*addr = MAKE_IP_ADDR(*a, *b, *c, *d);
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
    printf("To run server: netperf <EAL_INIT> -- --mode=SERVER --ip=<SERVER_IP>  --num_segments=<INT> <--with_copy>\n");
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
    static __thread int thread_id;
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
        {"client_ip",      required_argument,       0,  'i' },
        {"server_ip", optional_argument,       0,  's' },
        {"log", optional_argument, 0, 'l'},
        {"port", optional_argument, 0,  'p' },
        {"server_mac",   optional_argument, 0,  'c' },
        {"segment_size",   optional_argument, 0,  'z' },
        {"time",   optional_argument, 0,  't' },
        {"rate",   optional_argument, 0,  'r' },
        {"num_segments", optional_argument, 0, 'n'},
        {"array_size", optional_argument, 0, 'a'},
        {"client_threads", optional_argument, 0, 'b'},
        {"threads_log", optional_argument, 0, 'q'},
        {"client_id", optional_argument, 0, 'x'},
        {"with_copy", no_argument, 0, 'k'},
        {0,           0,                 0,  0   }
    };
    int long_index = 0;
    while ((opt = getopt_long(argc, argv,"m:i:s:l:p:c:z:t:r:n:a:k:b:x:q:",
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
                str_to_ip(optarg, &client_ip, &client_ip_octets[0], &client_ip_octets[1], &client_ip_octets[2], &client_ip_octets[3]);
                break;
            case 's':
                has_server_ip = 1;
                str_to_ip(optarg, &server_ip, &server_ip_octets[0], &server_ip_octets[1], &server_ip_octets[2], &server_ip_octets[3]);
                break;
            case 'l':
                has_latency_log = 1;
                latency_log = (char *)malloc(strlen(optarg));
                strcpy(latency_log, optarg);
                break;
            case 'q':
                has_threads_log = 1;
                threads_log = (char *)malloc(strlen(optarg));
                strcpy(threads_log, optarg);
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
            case 'b': // num client threads
                str_to_long(optarg, &tmp);
                num_client_threads = (size_t)tmp;
                // check it is a power of 2
                if (num_client_threads & (num_client_threads - 1) != 0) {
                    NETPERF_ERROR("num_client_threads arg must be a power of 2. Instead: %lu", num_client_threads);
                    exit(1);
                }
                break;
            case 'x': // client id
                str_to_long(optarg, &tmp);
                client_id = (size_t)tmp;
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
        struct rte_mempool *extbuf_pool = rte_pktmbuf_pool_create(
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
        extbuf_pools[0] = extbuf_pool;

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
  	NETPERF_IS_ONE(rte_eth_dev_is_valid_port(port_id), "Invalid port num");

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

static int init_dpdk_port(uint16_t port_id, struct rte_mempool **rx_mbuf_pools, size_t num_queues) {
    static __thread int thread_id;
    printf("Initializing port %u\n", (unsigned)(port_id));
  	NETPERF_IS_ONE(rte_eth_dev_is_valid_port(port_id), "Invalid port num");
    const uint16_t rx_rings = num_queues;
    const uint16_t tx_rings = num_queues;
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
    port_conf.rxmode.mq_mode = ETH_MQ_RX_RSS | ETH_MQ_RX_RSS_FLAG;
    port_conf.rx_adv_conf.rss_conf.rss_key = sym_rss_key;
    port_conf.rx_adv_conf.rss_conf.rss_key_len = 40;
    port_conf.rx_adv_conf.rss_conf.rss_hf = ETH_RSS_UDP | ETH_RSS_IP;
    port_conf.txmode.offloads = DEV_TX_OFFLOAD_MULTI_SEGS | DEV_TX_OFFLOAD_IPV4_CKSUM | DEV_TX_OFFLOAD_UDP_CKSUM;
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

    // todo: this call fails and i don't understand why.
    int socket_id = rte_eth_dev_socket_id(port_id);

    // allocate and set up num_queues RX queue per Ethernet port.
    for (uint16_t i = 0; i < rx_rings; ++i) {
        rte_eth_rx_queue_setup(port_id, i, nb_rxd, socket_id, &rx_conf, rx_mbuf_pools[i]);
    }

    // allocate and set up num_queues TX queue per Ethernet port.
    for (uint16_t i = 0; i < tx_rings; ++i) {
        NETPERF_DEBUG("Initializing tx queue %u", i);
        rte_eth_tx_queue_setup(port_id, i, nb_txd, socket_id, &tx_conf);
    }

    // start the ethernet port.
    int dev_start_ret = rte_eth_dev_start(port_id);
    if (dev_start_ret != 0) {
        printf("Failed to start ethernet for prot %u\n", (unsigned)port_id);
    }

    // disable the rx/tx flow control
    struct rte_eth_fc_conf fc_conf = {};
    rte_eth_dev_flow_ctrl_get(port_id, &fc_conf);
    fc_conf.mode = RTE_FC_NONE;
    rte_eth_dev_flow_ctrl_set(port_id, &fc_conf);
    wait_for_link_status_up(port_id);

   return 0;
}

static int global_init(size_t num_queues) {
    static __thread int thread_id;
    NETPERF_INFO("Running global dpdk init for %lu threads", num_queues);
    // initialize ports
    const uint16_t nbports = rte_eth_dev_count_avail();
    dpdk_nbports = nbports;
    if (nbports <= 0) {
       rte_exit(EXIT_FAILURE, "No ports available\n"); 
    }
    fprintf(stderr, "DPDK reports that %d ports (interfaces) are available.\n", nbports);

    // for each "queue", initialize a tx and rx pool.
    // attach rx pool to the queue.
    // create a pool of memory for ring buffers
    for (size_t i = 0; i < num_queues; i++) {
        NETPERF_INFO("Initializing tx pool %s", tx_pool_names[i]);
        struct rte_mempool *tx_mbuf_pool = rte_pktmbuf_pool_create(
                        tx_pool_names[i],
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
        tx_mbuf_pools[i] = tx_mbuf_pool;

        // rx mbuf pool
        struct rte_mempool *rx_mbuf_pool = rte_pktmbuf_pool_create(
                        rx_pool_names[i],
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
        rx_mbuf_pools[i] = rx_mbuf_pool;
    } 
    // initialize all ports
    uint16_t i = 0;
    uint16_t port_id = 0;
    RTE_ETH_FOREACH_DEV(i) {
        port_id = i;
        if (init_dpdk_port(i, rx_mbuf_pools, num_queues) != 0) {
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
    return 0;
}

static int dpdk_eal_init(int argc, char **argv) {
    
    // initialize Environment Abstraction Layer
    // our arguments: "-c", "0xff", "-n", "4", "-w", "0000:37:00.0","--proc-type=auto"
    int args_parsed = rte_eal_init(argc, argv);
    if (args_parsed < 0) {
        rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    }
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
                        struct rte_mbuf *pkt,
                        size_t client_id)
{
    static __thread int thread_id;
    thread_id = client_id;
    NETPERF_DEBUG("Received packet, checking if valid; pkt_len %u", pkt->pkt_len);
    // packet layout order is (from outside -> in):
    // ether_hdr
    // ipv4_hdr
    // udp_hdr
    // request id
    uint8_t *p = rte_pktmbuf_mtod(pkt, uint8_t *);
    char *cur_byte = p;
    size_t header = 0;

    // check the ethernet header
    struct rte_ether_hdr * const eth_hdr = (struct rte_ether_hdr *)(p);
    // print all the bytes in the ethernet header:
    /*printf("eth header: ");
    for (size_t i = 0; i < sizeof(struct rte_ether_hdr); i++) {
        printf("%x ", *cur_byte);
        if ((i+1) % 4 == 0) {
            printf("   ");
        }
        cur_byte++;
    }
    printf("\n");*/
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
    /*printf("ip header: ");
    for (size_t i = 0; i < sizeof(struct rte_ipv4_hdr); i++) {
        printf("%x", *cur_byte);
        if ((i+1) % 4 == 0) {
            printf(" ");
        }
        cur_byte++;
    }
    printf("\n");*/


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
    
    /*printf("udp header: ");
    for (size_t i = 0; i < sizeof(struct rte_udp_hdr); i++) {
        printf("%x", *cur_byte);
        if ((i+1) % 4 == 0) {
            printf(" ");
        }
        cur_byte++;
    }
    printf("\n");*/
    // In network byte order.
    in_port_t udp_src_port = udp_hdr->src_port;
    in_port_t udp_dst_port = udp_hdr->dst_port;
    NETPERF_DEBUG("Received packet with ip checksum of %u, to dst ip %u, udp checksum of %u", ntohs(ip_hdr->hdr_checksum), ntohl(ip_hdr->dst_addr), ntohs(udp_hdr->dgram_cksum));

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
    static __thread int thread_id;
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
static uint32_t compute_flow_affinity(uint32_t local_ip,
                                        uint32_t remote_ip,
                                        uint16_t local_port,
                                        uint16_t remote_port,
                                        size_t num_queues)
{
    static __thread int thread_id;
	const uint8_t *rss_key = (uint8_t *)sym_rss_key;

	uint32_t input_tuple[] = {
        //local_ip, remote_ip, local_port | remote_port << 16
        remote_ip, local_ip, remote_port | local_port << 16
	};

    uint32_t ret = rte_softrss((uint32_t *)&input_tuple, ARRAY_SIZE(input_tuple),
         (const uint8_t *)rss_key);
	return ret % (uint32_t)num_queues;
}

void find_ip_and_pair(uint16_t queue_id,
                        size_t num_queues,
                        uint32_t remote_ip,
                        uint16_t remote_port,
                        uint8_t start_ip[4],
                        uint16_t start_port,
                        uint32_t *ip,
                        uint16_t *port) {
    static __thread int thread_id;
    while(compute_flow_affinity(MAKE_IP_ADDR(start_ip[0], start_ip[1], start_ip[2], start_ip[3]), 
                                remote_ip,
                                start_port,
                                remote_port,
                                num_queues) != (uint32_t)queue_id) {
        start_ip[3] += 1;
    }
    *ip = MAKE_IP_ADDR(start_ip[0], start_ip[1], start_ip[2], start_ip[3]);
    *port = start_port;
}

static void * do_client(void *client) {
    static __thread int thread_id;
    thread_id = (size_t)client;
    size_t client_id = (size_t)client;
    // find the IP and pair for this client to send from so received RSS brings
    // back packets to this queue
    uint8_t starting_octets[4] = {client_ip_octets[0], client_ip_octets[1], client_ip_octets[2], client_ip_octets[3]};
    uint32_t our_client_ip = client_ip;
    uint16_t our_client_port = client_port;
    find_ip_and_pair((uint16_t)client_id,
                      num_client_threads,
                      server_ip, 
                      server_port, 
                      starting_octets, 
                      client_port,
                      &our_client_ip, 
                      &our_client_port);
    NETPERF_INFO("Starting do client on client thread %lu; client ip: %u, client port: %u", client_id, our_client_ip, our_client_port);
    
    // initialize latency histogram for this thread
    alloc_pktmap(&packet_maps[client_id], &latency_dists[client_id]);
    
    // initialize all the packets for this thread
    initialize_client_header(our_client_ip, our_client_port, client_id);
    clock_offset = raw_time();
    uint64_t start_time, end_time, last_sent, wait_time, deficit_cycles, next;
    struct rte_mbuf *pkts[BURST_SIZE];
    struct rte_mbuf *pkt;
    uint16_t nb_rx;
    uint64_t reqs = 0;
    size_t payload_size = 32;
    float rate_gbps = (float)rate * (float)(segment_size * num_mbufs) * 8.0 / (float)1e9;

    ClientRequest *current_request = client_requests[client_id];
    
    // TODO: add in scaffolding for timing/printing out quick statistics
    start_time = rte_get_timer_cycles();
    next = start_time;
    wait_time = 0;
    last_sent = start_time;
    int outstanding = 0;
    uint64_t id = 0;
    int total_pkts_required = calculate_total_pkts_required(segment_size, num_mbufs);
    size_t header_size = sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr);
    // todo: ends up getting padded to be 60 or something
    size_t client_payload_size = sizeof(uint64_t) * (1 + num_mbufs);
    
    while (rte_get_timer_cycles_() < (start_time + seconds * rte_get_timer_hz_())) {
        NETPERF_DEBUG("Past cycle waiting, about to send");
        // allocate packet
        pkt = rte_pktmbuf_alloc_(tx_mbuf_pools[client_id]);
        if (pkt == NULL) {
            NETPERF_WARN("Error allocating tx mbuf.");
            return (void *)-EINVAL;
        }
        uint8_t *ptr = rte_pktmbuf_mtod(pkt, uint8_t *);
        current_request->timestamp = time_now(clock_offset);
        rte_memcpy((char *)ptr, (char *)&outgoing_headers[client_id], sizeof(OutgoingHeader));
        ptr += header_size;
        rte_memcpy((char *)ptr, (char *)current_request + sizeof(uint64_t), client_payload_size);
        
        /* extra dpdk metadata */

        pkt->l2_len = RTE_ETHER_HDR_LEN;
        pkt->l3_len = sizeof(struct rte_ipv4_hdr);
        pkt->ol_flags = PKT_TX_IP_CKSUM | PKT_TX_IPV4;
        pkt->data_len = header_size + client_payload_size;
        pkt->pkt_len = pkt->data_len;
        pkt->nb_segs = 1;

        int pkts_sent = 0;

        while (pkts_sent < 1) {
            pkts_sent = rte_eth_tx_burst(our_dpdk_port_id, (uint16_t)client_id, &pkt, 1);
        }
        outstanding += total_pkts_required;
        last_sent = rte_get_timer_cycles();
        deficit_cycles = last_sent - next;
        current_request++;
        wait_time = current_request->timestamp * rte_get_timer_hz() / 1e9;
        NETPERF_DEBUG("Wait time: %lu, deficit: %lu", wait_time, deficit_cycles);
        if (deficit_cycles > wait_time) {
            wait_time = 0;
            next = last_sent;
        } else {
            wait_time -= deficit_cycles;
            next = last_sent + wait_time;
        }
        NETPERF_DEBUG("Sent packet at %u, %u send_time, %d is outstanding, wait time is %lu", (unsigned)last_sent, (unsigned)time_now(clock_offset), outstanding, wait_time);

        /* now poll on receiving packets */
        nb_rx = 0;
        reqs += 1;
        while (rte_get_timer_cycles_() < (last_sent + wait_time)) {
            if (rte_get_timer_cycles_() > (start_time + seconds * rte_get_timer_hz_())) {
                break;
            }
            nb_rx = rte_eth_rx_burst_(our_dpdk_port_id, (uint16_t)client_id, pkts, BURST_SIZE);
            if (nb_rx == 0) {
                if (rte_get_timer_cycles() > (last_sent + wait_time)) {
                    break;
                }
                continue;
            }

            for (int i = 0; i < nb_rx; i++) {
                struct sockaddr_in src, dst;
                void *payload = NULL;
                size_t payload_length = 0;
                int valid = parse_packet(&src, &dst, &payload, &payload_length, pkts[i], client_id);
                if (valid == 0) {
                    /* parse the timestamp and record it */
                    uint64_t now = (uint64_t)time_now(clock_offset);
                    uint64_t id = (*(uint64_t *)payload);
                    ClientRequest *recvd_req = (ClientRequest *)((char *)client_requests[client_id] + (sizeof(ClientRequest) * (size_t)(id)));
                    uint64_t then = recvd_req->timestamp;
                    NETPERF_DEBUG("Received a packet at time now: %u from %u, with payload_length: %u (without header), id: %u.", (unsigned)(now), (unsigned)then, (unsigned)payload_length - 8, (unsigned)id);
                    add_latency_to_map(&packet_maps[client_id], now - then, id);
                    rte_pktmbuf_free_(pkts[i]);
                    outstanding--;
                } else {
                    rte_pktmbuf_free_(pkts[i]);
                }
            }
        }
    }
    end_time = rte_get_timer_cycles();
    float total_time = (float) (end_time - start_time) / rte_get_timer_hz(); 
    printf("\nRan for %f seconds, sent %"PRIu64" packets.\n",
			total_time, reqs);
    calculate_latencies(&packet_maps[client_id], &latency_dists[client_id], reqs, total_pkts_required);
    summary_statistics[client_id].id = client_id;
    summary_statistics[client_id].sent = reqs;
    summary_statistics[client_id].runtime = total_time;
    summary_statistics[client_id].retries = 0;
    dump_latencies(&latency_dists[client_id], total_time, num_mbufs * segment_size, rate_gbps, rate, client_id, &summary_statistics[client_id]);
    free_pktmap(&packet_maps[client_id], &latency_dists[client_id]);
    free(client_requests[client_id]);
    NETPERF_DEBUG("Freed client request payloads\n");
    return (void *)0;
} 


static void swap_headers(struct rte_mbuf *tx_buf, struct rte_mbuf *rx_buf, size_t payload_length) {
    static __thread int thread_id;
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
   NETPERF_DEBUG("Swapped header: %lu", *rx_buf_id_ptr);

}

void swap_headers_into_static_payload(struct rte_mbuf *rx_buf, size_t payload_length, size_t offset) {
    static __thread int thread_id;
    void *payload_ptr = (void *)(get_server_region(payload_to_copy, offset, segment_size));
    NETPERF_DEBUG("Start of payload to copy: %p", payload_ptr);
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

   /* Set packet id */
   tx_buf_id_ptr = (uint64_t *)((char *)payload_ptr + RTE_ETHER_HDR_LEN + sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr));
   rx_buf_id_ptr = rte_pktmbuf_mtod_offset(rx_buf, uint64_t *, sizeof(struct rte_udp_hdr) + sizeof(struct rte_ipv4_hdr) + RTE_ETHER_HDR_LEN);
   *tx_buf_id_ptr = *rx_buf_id_ptr;
   NETPERF_DEBUG("Packet Id: %lu", *rx_buf_id_ptr);

}


static int do_server(void) {
    static __thread int thread_id;
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
    clock_offset = raw_time();
    
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
            NETPERF_DEBUG("Checking received packet");
            // check processing loop for each packet
#ifdef __TIMERS__
            uint64_t start_processing = time_now(clock_offset);
#endif
            struct sockaddr_in src, dst;
            void *payload = NULL;
            size_t payload_length = 0;
            int valid = parse_packet(&src, &dst, &payload, &payload_length, rx_bufs[i], 0);
            struct rte_mbuf* secondary = NULL;
            
            if (valid == 0) {
                NETPERF_DEBUG("Received a valid packet.");
                rx_buf = rx_bufs[i];
                size_t header_size = rx_buf->pkt_len - payload_length;
                uint32_t segment_length = segment_size;
                uint16_t nb_segs = num_mbufs;
                char *payload_ptr = payload + sizeof(uint64_t) * 2;
                // fill in the offsets
                for (uint16_t seg = 0; seg < nb_segs; seg++) {
                    offsets[(size_t)seg] = *((uint64_t *)(payload_ptr + (sizeof(uint64_t) * (size_t)seg)));
                    NETPERF_DEBUG("Seg %u is %lu", seg, offsets[(size_t)seg]);
                }

                nb_req += 1;

                int total_pkts_required = calculate_total_pkts_required(segment_length, (uint32_t)nb_segs);
                uint32_t payload_left = segment_length * nb_segs;
                NETPERF_DEBUG("Total packets required for %u segment_size and %u nb_segs is %d; total_payload: %u.", (unsigned)segment_length, (unsigned)nb_segs, total_pkts_required, (unsigned)payload_left);
                
                size_t seg_offset = 0;
                for (int pkt = 0; pkt < total_pkts_required; pkt++) {
                    struct rte_mbuf *prev = NULL;
                    uint32_t actual_segment_size = MIN(payload_left, MIN(segment_length, MAX_SEGMENT_SIZE));
                    int nb_segs_segment = (MIN(payload_left, (uint32_t)MAX_SEGMENT_SIZE)) / actual_segment_size;
                    uint32_t pkt_len = actual_segment_size * (uint32_t)nb_segs_segment;

                    if (zero_copy_mode) {
                        for (int seg = 0; seg < nb_segs_segment; seg++) {
                            void *payload_at_offset = (void *)(get_server_region(payload_to_copy, offsets[seg_offset], segment_size));
                            rte_iova_t iova_at_offset = (rte_iova_t)(get_server_region(payload_iova, offsets[seg_offset], segment_size));
                            tx_bufs[seg][pkt] = rte_pktmbuf_alloc(extbuf_pools[0]); // allocate an external buffer for each segment
                            if (tx_bufs[seg][pkt] == NULL) {
                                printf("Failed to allocate tx buf burst # %i, seg %i.", i, seg);
                                exit(1);
                            }

                            tx_bufs[seg][pkt]->buf_addr = payload_at_offset;
                            tx_bufs[seg][pkt]->buf_iova = iova_at_offset;
                            tx_bufs[seg][pkt]->data_off = 0;
                            
                            NETPERF_DEBUG("\t->Allocated packet segment %d, for pkt %d", seg, pkt);
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
                            NETPERF_DEBUG("\t->Pkt # %d, segment # %d, data_len: %u.", pkt, seg, (unsigned)actual_segment_size);
                            tx_bufs[seg][pkt]->data_len = (uint16_t)actual_segment_size;
                            prev = tx_bufs[seg][pkt];
                        }
                    } else {
                        tx_bufs[0][pkt] = rte_pktmbuf_alloc(tx_mbuf_pools[0]);
                        if (tx_bufs[0][pkt] == NULL) {
                            NETPERF_WARN("Failed to allocate tx buf burst # %i, seg %i.", i, 0);
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
                            void *copy_pointer = (void *)(get_server_region(payload_to_copy, offsets[seg_offset], segment_size));
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
                
                // burst all of the packets for this response
#ifdef __TIMERS__
                uint64_t end_construct = time_now(clock_offset);
                add_latency(&server_construction_dist, end_construct - start_processing);
                uint64_t start_send = time_now(clock_offset);
#endif
                nb_tx = rte_eth_tx_burst_(our_dpdk_port_id, queue, tx_bufs[0], total_pkts_required);
                if (nb_tx != total_pkts_required) {
                    NETPERF_DEBUG("error: could not transmit all %u pkts, transmitted %u.", total_pkts_required, nb_tx);
                }
#ifdef __TIMERS__
                uint64_t end_send = time_now(clock_offset);
                add_latency(&server_send_dist, end_send - start_send);
#endif
                rte_pktmbuf_free(rx_buf);
#ifdef __TIMERS__
                uint64_t end_time = time_now(clock_offset);
                add_latency(&server_processing_dist, end_time - start_processing);
#endif
            }
        }
    }
    return 0;
}

static void sig_handler(int signum){
    static __thread int thread_id;
    // free any resources
    rte_free(payload_to_copy);

    if (has_latency_log) {
        free(latency_log);
    }

    // if debug timers were turned on, dump them
#ifdef __TIMERS__
	if (mode == MODE_UDP_SERVER) {
        NETPERF_INFO("----");
        NETPERF_INFO("server request processing timers: ");
        dump_debug_latencies(&server_processing_dist);
        NETPERF_INFO("----");
        NETPERF_INFO("server send processing timers: ");
        dump_debug_latencies(&server_send_dist);
        NETPERF_INFO("----");
        NETPERF_INFO("server send construction timers: ");
        dump_debug_latencies(&server_construction_dist);
        NETPERF_INFO("----");
    }
#endif   
    exit(0);
}

static int do_memcpy_bench() {
    static __thread int thread_id;
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

/* Dumps and summarizes all statistics */
static void dump_total_statistics(size_t num_clients) {
    static __thread int thread_id;
    Summary_Statistics_t summary_stats = { .min = 0, .max = 0, .avg = 0, .median = 0, .p99 = 0, .p999 = 0, .achieved_rate_gbps = 0, .offered_rate_gbps = 0, .percent_rate = 0 };
    Summary_Statistics_t *summary = &summary_stats;
    for (size_t clients = 0; clients < num_clients; clients++) {
        Summary_Statistics_t *stats = &summary_statistics[clients];
        summary->min += stats->min;
        summary->max += stats->max;
        summary->avg += stats->avg;
        summary->median += stats->median;
        summary->p99 += stats->p99;
        summary->p999 += stats->p999;
        summary->achieved_rate_gbps += stats->achieved_rate_gbps;
        summary->offered_rate_gbps += stats->offered_rate_gbps;
        printf("Client %lu Stats:\n\t- Min latency: %lu ns\n\t- Max latency: %lu ns\n\t- Avg latency: %" PRIu64 " ns\n\t- Median latency: %lu ns\n\t- p99 latency: %lu ns\n\t- p999 latency: %lu ns\n\t- Achieved Goodput: %0.4f Gbps ( %0.4f %% ) \n", clients, stats->min, stats->max, stats->avg, stats->median, stats->p99, stats->p999, stats->achieved_rate_gbps, stats->percent_rate);
    }

    summary->min = summary->min / num_clients;
    summary->max = summary->max / num_clients;
    summary->avg = summary->avg / num_clients;
    summary->median = summary->median / num_clients;
    summary->p99 = summary->p99 / num_clients;
    summary->p999 = summary->p999 / num_clients;
    summary->percent_rate = summary->achieved_rate_gbps / summary->offered_rate_gbps;
    printf("Summary Average Stats:\n\t- Min latency: %lu ns\n\t- Max latency: %lu ns\n\t- Avg latency: %" PRIu64 " ns\n\t- Median latency: %lu ns\n\t- p99 latency: %lu ns\n\t- p999 latency: %lu ns\n\t- Achieved Goodput: %0.4f Gbps ( %0.4f %% ) \n", summary->min, summary->max, summary->avg, summary->median, summary->p99, summary->p999, summary->achieved_rate_gbps, summary->percent_rate);
    return;
}

/* Takes care of per thread initialization for dpdk as well (per thread rx and
 * tx memory pools.*/
static int spawn_client_threads() {
    // do global thread init
    int ret;
    initialize_client_requests_common(num_client_threads);
    static __thread int thread_id;
    int thread_results[MAX_THREADS];

    // start a thread for each client 
    for (size_t i = 0; i < num_client_threads; i++) {
        thread_results[i] = pthread_create(&threads[i], NULL, do_client, (void *)i);
        // set affinity for this thread
        CPU_ZERO(&cpusets[i]);
        CPU_SET(i + 1, &cpusets[i]);
        ret = pthread_setaffinity_np(threads[i], sizeof(cpu_set_t), &cpusets[i]);
        if (ret != 0) {
            NETPERF_WARN("Unable to set cpu affinity for thread %lu", i);
        }
    }

    for (size_t i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);
        NETPERF_INFO("Thread %lu returns: %d", i, thread_results[i]);
    }

    // at the end, print out all the client statistics
    dump_total_statistics(num_client_threads);

    // print out all the threads info to the thread filename
    if (has_threads_log) {
        write_threads_info(threads_log, num_client_threads, summary_statistics);
        free(threads_log);
    }
    return 0;
}

int
main(int argc, char **argv)
{
	int ret;
    int args_parsed = dpdk_eal_init(argc, argv);
    argc -= args_parsed;
    argv += args_parsed;

    // initialize our arguments
    ret = parse_args(argc, argv);
    if (ret != 0) {
        return ret;
    }

    ret = global_init(num_client_threads);
    if (ret != 0) {
        NETPERF_WARN("Something wrong with dpdk global thread init: %s", strerror(ret));
    }

    if (mode == MODE_MEMCPY_BENCH) {
        return do_memcpy_bench();
    }

    //signal(SIGINT, sig_handler); // Register signal handler
    if (mode == MODE_UDP_CLIENT) {
        return spawn_client_threads();
    } else {
        // set up signal handler
        if (signal(SIGINT, sig_handler) == SIG_ERR) {
            NETPERF_WARN("Can't catch SIGINT");
        }
        do_server();
    }

	return 0;
}
