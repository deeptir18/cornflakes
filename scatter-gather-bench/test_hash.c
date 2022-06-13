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
#include <arpa/inet.h>

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


/* From caladan: inc/base/byteorder.h */
static inline uint32_t __bswap32(uint32_t val)
{
#ifdef HAS_BUILTIN_BSWAP
	return __builtin_bswap32(val);
#else
	return (((val & 0x000000ffUL) << 24) |
		((val & 0x0000ff00UL) << 8) |
		((val & 0x00ff0000UL) >> 8) |
		((val & 0xff000000UL) >> 24));
#endif
}

#define cpu_to_be32(x)	(__bswap32(x))
#define hton32(x) (cpu_to_be32(x))

static uint8_t sym_rss_key[] = {
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
};
#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

#define MAKE_IP_ADDR(a, b, c, d)			\
	(((uint32_t) a << 24) | ((uint32_t) b << 16) |	\
	 ((uint32_t) c << 8) | (uint32_t) d)

static uint32_t client_ip;
static uint8_t client_ip_octets[4];
static uint32_t server_ip;
static uint8_t server_ip_octets[4];
static unsigned int client_port = 50000;
static unsigned int server_port = 50000;
struct rte_ether_addr server_mac = {
    .addr_bytes = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
};

static int str_to_ip(const char *str, uint32_t *addr, uint8_t *a, uint8_t *b, uint8_t *c, uint8_t *d)
{
	if(sscanf(str, "%hhu.%hhu.%hhu.%hhu", a, b, c, d) != 4) {
		return -EINVAL;
	}

	*addr = MAKE_IP_ADDR(*a, *b, *c, *d);
	return 0;
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

static int parse_args(int argc, char *argv[]) {
  static __thread int thread_id;
    long tmp;
    int has_server_ip = 0;
    int has_port = 0;
    int has_server_mac = 0;

    int opt = 0;
    static struct option long_options[] = {
        {"client_ip",      required_argument,       0,  'i' },
        {"server_ip", optional_argument,       0,  's' },
        {"server_port", optional_argument, 0,  'p' },
        {"client_port", optional_argument, 0,  'y' },
        {"server_mac",   optional_argument, 0,  'c' },
        {0,           0,                 0,  0   }
    };
    int long_index = 0;
    while ((opt = getopt_long(argc, argv,"i:s:p:c:x:y",
                   long_options, &long_index )) != -1) {
        switch (opt) {
            case 'i':
                str_to_ip(optarg, &client_ip, &client_ip_octets[0], &client_ip_octets[1], &client_ip_octets[2], &client_ip_octets[3]);
                break;
            case 's':
                has_server_ip = 1;
                str_to_ip(optarg, &server_ip, &server_ip_octets[0], &server_ip_octets[1], &server_ip_octets[2], &server_ip_octets[3]);
                break;
            case 'p':
                if (sscanf(optarg, "%u", &server_port) != 1) {
                    return -EINVAL;
                }
                break;
            case 'c':
                has_server_mac = 1;
                if (str_to_mac(optarg, &server_mac) != 0) {
                    printf("Failed to convert %s to mac address\n", optarg);
                    return -EINVAL;
                }
                break;
	    case 'y':
	      client_port = atoi(optarg);
	      break;
            default: 
                 exit(EXIT_FAILURE);
        }
    }
    return 0;
}



/**
 * compute_flow_affinity - compute rss hash for incoming packets
 * @local_port: the local port number
 * @remote_port: the remote port
 * @local_ip: local ip (in host-order)
 * @remote_ip: remote ip (in host-order)
 * @num_queues: total number of queues
 *
 * Returns the 32 bit hash mod num_queues
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

    uint32_t i, j, map, ret = 0, input_tuple[] = {
      remote_ip, local_ip, local_port | remote_port << 16
    };

    /* From caladan - implementation of rte_softrss */
    for (j = 0; j < ARRAY_SIZE(input_tuple); j++) {
      for (map = input_tuple[j]; map;	map &= (map - 1)) {
	i = (uint32_t)__builtin_ctz(map);
	ret ^= hton32(((const uint32_t *)rss_key)[j]) << (31 - i) |
	  (uint32_t)((uint64_t)(hton32(((const uint32_t *)rss_key)[j + 1])) >>
		     (i + 1));
      }
    }
    
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
    while(true) {
      uint32_t queue =
	compute_flow_affinity(MAKE_IP_ADDR(start_ip[0],
					   start_ip[1],
					   start_ip[2],
					   start_ip[3]), 
			      remote_ip,
			      start_port,
			      remote_port,
			      num_queues);
      if (queue == queue_id) {
	break;
      }
      
      start_port += 1;
    }
    *ip = MAKE_IP_ADDR(start_ip[0], start_ip[1], start_ip[2], start_ip[3]);
    *port = start_port;
}

int main(int argc, char** argv) {
  int ret;
  // initialize our arguments
  ret = parse_args(argc, argv);
  if (ret != 0) {
    return ret;
  }

  uint8_t starting_octets[4] = {client_ip_octets[0], client_ip_octets[1], client_ip_octets[2], client_ip_octets[3]};
  
  size_t num_queues = 8;
  for ( uint16_t queue_id = 0; queue_id < num_queues; queue_id++ ) {
    uint32_t our_client_ip = client_ip;
    uint16_t our_client_port = client_port;
    find_ip_and_pair(queue_id, num_queues,
		     server_ip, server_port,
		     starting_octets,
		     client_port,
		     &our_client_ip, 
		     &our_client_port);

    printf("%u\n", our_client_port);
  }
  
  return 0;
}
