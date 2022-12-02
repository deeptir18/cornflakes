#include <base/mempool.h>
#include <custom_ice/ice_state.h>
#include <custom_ice/ice_pci.h>
#include <rte_ethdev.h>
#include <ethdev_driver.h>
#include <rte_malloc.h>

/* Gets per thread context associated with id */
struct custom_ice_per_thread_context *custom_ice_get_per_thread_context(
        struct custom_ice_global_context *context,
        size_t idx) {
    NETPERF_ASSERT(idx < context->num_threads,
            "Accessing thread greater than allocated.");
    return (struct custom_ice_per_thread_context *)((char *)context->thread_contexts + idx * custom_ice_get_per_thread_context_size(1));

}

/* Clears state in per thread context. */
void custom_ice_clear_per_thread_context(struct custom_ice_global_context *context, size_t idx) {
    struct custom_ice_per_thread_context *tcontext = custom_ice_get_per_thread_context(context, idx);
    tcontext->thread_id = 0;
    tcontext->global_context = NULL;
    tcontext->tx_queue = NULL;
    tcontext->pending_transmissions = NULL;
}

/* Size of global context struct */
size_t custom_ice_get_global_context_size() {
    return sizeof(struct custom_ice_global_context);
}

/* Size of per_thread_context struct */
size_t custom_ice_get_per_thread_context_size(size_t num_threads) {
    return num_threads * sizeof(struct custom_ice_per_thread_context);
}

/* Size of mempool struct */
size_t custom_ice_get_mempool_size() {
    return sizeof(struct custom_ice_mempool);
}

/* Get raw threads pointer */
void *custom_ice_get_raw_threads_ptr(struct custom_ice_global_context *global_context) {
    return global_context->thread_contexts;
}

/* Allocates global context. */
void custom_ice_init_global_context(size_t num_threads, unsigned char *global_context_ptr, unsigned char *per_thread_info) {
    struct custom_ice_global_context *context = (struct custom_ice_global_context *)global_context_ptr;
    context->num_threads = num_threads;
    context->thread_contexts = per_thread_info;
    for (size_t i = 0; i < num_threads; i++) {
        custom_ice_clear_per_thread_context(context, i);
        struct custom_ice_per_thread_context *t_context = custom_ice_get_per_thread_context(context, i);
        t_context->thread_id = i;
        t_context->global_context = context;
    }
}

const char *custom_ice_err_to_str(int no) {
    return strerror(no);
}

/* Initialize tx_queue information in per-thread context.
 * Automatically accesses &rte_eth_devices[0]*/
int custom_ice_init_tx_queues(
        struct custom_ice_global_context *context,
        size_t dpdk_port_id,
        int dpdk_socket_id) {
    struct rte_eth_dev *dev = &rte_eth_devices[dpdk_port_id];
    struct rte_eth_dev_data *data = dev->data;
    for (size_t thread_id = 0; thread_id < context->num_threads; thread_id++) {
        struct custom_ice_per_thread_context *t_context = custom_ice_get_per_thread_context(context, thread_id);
        struct ice_tx_queue *tx_queue = data->tx_queues[thread_id];
        t_context->tx_queue = tx_queue;
        t_context->pending_transmissions = 
            rte_zmalloc_socket(NULL,
                                sizeof(struct custom_ice_tx_entry) * tx_queue->nb_tx_desc,
                                RTE_CACHE_LINE_SIZE,
                                dpdk_socket_id);
        if (t_context->pending_transmissions == NULL) {
            NETPERF_WARN("Allocated pending transmissions null.");
            return -1;
        }
        for (uint16_t tx_desc = 0; tx_desc < tx_queue->nb_tx_desc; tx_desc ++) {
            uint16_t next_id = tx_desc + 1;
            if (next_id == tx_queue->nb_tx_desc) {
                next_id = 0;
            }
            t_context->pending_transmissions[tx_desc].next_id = next_id;
        }   
    }
    return 0;
}

/* Tearsdown state in the ice per thread context. 
 * Includes:
 * freeing the custom tx completion ring.
 * */
int custom_ice_teardown(struct custom_ice_per_thread_context *per_thread_context) {
    rte_free(per_thread_context->pending_transmissions);
    return 0;
}

int get_current_tx_id(struct custom_ice_per_thread_context *per_thread_context) {
}
int get_last_tx_id_needed(
        struct custom_ice_per_thread_context *per_thread_context,
        size_t num_needed) {
}
int custom_ice_post_data_segment(
        struct custom_ice_per_thread_context *per_thread_context,
        physaddr_t dma_addr,
        size_t len,
        uint16_t *tx_id,
        uint16_t last_id) {
    volatile struct ice_tx_desc *tx_ring;
    volatile struct ice_tx_desc *txd;
    struct custom_ice_tx_entry *completion_entry;

    if (*tx_id == last_id) {
        // write EOP bit
        // do rs thresh stuff
    }
    
    // TODO: access tx_queue
    *tx_id = completion_entry->next_id;
}

int finish_single_transmission(struct custom_ice_per_thread_context *per_thread_context,
        uint16_t last_id) {
    // locally updates the tx_id in tx queue struct
    // optionally do this rs thresh stuff
    // https://github.com/deeptir18/ice_netperf/blob/main/ice/ice_rxtx.c#L668
    // stuff here
}
int post_queued_segments(struct custom_ice_per_thread_context *per_thread_context, uint16_t tx_id) {
    // WRITE TO TAIL REGISTER 
    ICE_PCI_REG_WRITE(per_thread_context->tx_queue->qtx_tail, tx_id);
}





