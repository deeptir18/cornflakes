#pragma once
#include <base/mempool.h>
#include <ice_rxtx.h>
#include <base/types.h>

struct custom_ice_tx_entry {
    // pointer to mempool
    struct custom_ice_mempool *mempool;
    // reference count of buffer in that mempool
    uint16_t refcnt;
    // index of next entry in ring (to safely do wraparound)
    uint16_t next_id;
    // index of last entry for this packet completion
    uint16_t last_id;
};

struct custom_ice_global_context {
    size_t num_threads;
    void *thread_contexts;
};

struct custom_ice_per_thread_context {
    // thread/queue id
    size_t thread_id;
    // pointer back to global context
    struct custom_ice_global_context *global_context;
    // pointer to DPDK per-thread context
    struct ice_tx_queue *tx_queue;
    // custom ring to store completion information
    struct custom_ice_tx_entry *pending_transmissions;
};

/* Gets per thread context associated with id */
struct custom_ice_per_thread_context *custom_ice_get_per_thread_context(
        struct custom_ice_global_context *context,
        size_t idx);

/* Clears state in per thread context. */
void custom_ice_clear_per_thread_context(struct custom_ice_global_context *context, size_t idx);

/* Size of global context struct */
size_t custom_ice_get_global_context_size();

/* Size of per_thread_context struct */
size_t custom_ice_get_per_thread_context_size(size_t num_threads);

/* Size of mempool struct */
size_t custom_ice_get_mempool_size();

/* Get raw threads pointer */
void *custom_ice_get_raw_threads_ptr(struct custom_ice_global_context *global_context);

/* Allocates global context. */
void custom_ice_init_global_context(size_t num_threads, unsigned char *global_context_ptr, unsigned char *per_thread_info);

/* Error string */
const char *custom_ice_err_to_str(int no);

/* Initialize tx_queue information in per-thread context.
 * Automatically accesses &rte_eth_devices[port_id]*/
int custom_ice_init_tx_queues(
        struct custom_ice_global_context *context,
        size_t dpdk_port_id,
        int dpdk_socket_id);

/* Tearsdown state in the ice per thread context. 
 * Includes:
 * */
int custom_ice_teardown(struct custom_ice_per_thread_context *per_thread_context);

size_t get_current_tx_id(struct custom_ice_per_thread_context *per_thread_context);

size_t get_last_tx_id_needed(
        struct custom_ice_per_thread_context *per_thread_context,
        size_t num_needed);

size_t custom_ice_post_data_segment(
    struct custom_ice_per_thread_context *per_thread_context,
    physaddr_t dma_addr, 
    size_t len, 
    uint16_t tx_id,
    uint16_t last_id);

int finish_single_transmission(struct custom_ice_per_thread_context *per_thread_context,
        uint16_t last_id);

void post_queued_segments(struct custom_ice_per_thread_context *per_thread_context, uint16_t tx_id);

size_t custom_ice_get_txd_avail(struct custom_ice_per_thread_context *per_thread_context);

int custom_ice_tx_cleanup(struct custom_ice_per_thread_context *per_thread_context);
