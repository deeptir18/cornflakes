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

size_t get_current_tx_id(struct custom_ice_per_thread_context *per_thread_context) {
    return per_thread_context->tx_queue->tx_tail;
}

size_t get_last_tx_id_needed(
        struct custom_ice_per_thread_context *per_thread_context,
        size_t num_needed) {
    uint16_t nb_txd = per_thread_context->tx_queue->nb_tx_desc;
    uint16_t nxt_txd = per_thread_context->tx_queue->tx_tail;
    return (nxt_txd + num_needed - 1) % nb_txd;
}

size_t custom_ice_post_data_segment(
        struct custom_ice_per_thread_context *per_thread_context,
        void* buf_addr,
	struct custom_ice_mempool *mempool,
	size_t refcnt_index,
        size_t len,
        uint16_t tx_id,
        uint16_t last_id) {
    struct ice_tx_queue *txq;
    volatile struct ice_tx_desc *tx_ring;
    volatile struct ice_tx_desc *txd;
    struct custom_ice_tx_entry *completion_entry;
    uint32_t td_cmd = 0;
    uint32_t td_offset = 0;
    uint32_t td_tag = 0;
    uint64_t dma_addr = 0;

    // update tx_queue metadata
    txq = per_thread_context->tx_queue;
    txq->nb_tx_used++;
    txq->nb_tx_free--;
    
    if (tx_id == last_id) {
        // write EOP bit
        td_cmd |= ICE_TX_DESC_CMD_EOP;
        // do rs thresh stuff
        if (txq->nb_tx_used >= txq->tx_rs_thresh) {
	    td_cmd |= ICE_TX_DESC_CMD_RS;
	    /* Update txq RS bit counters */
	    txq->nb_tx_used = 0;
	}
    }

    // queue onto tx ring
    tx_ring = txq->tx_ring;
    txd = &tx_ring[tx_id];
    dma_addr = custom_ice_get_dma_addr(mempool, buf_addr, refcnt_index, 0);
    txd->buf_addr = rte_cpu_to_le_64(dma_addr);  // little endian
    txd->cmd_type_offset_bsz = rte_cpu_to_le_64(ICE_TX_DESC_DTYPE_DATA |
				((uint64_t)td_cmd << ICE_TXD_QW1_CMD_S) |
				((uint64_t)td_offset << ICE_TXD_QW1_OFFSET_S) |
				((uint64_t)len << ICE_TXD_QW1_TX_BUF_SZ_S) |
				((uint64_t)td_tag << ICE_TXD_QW1_L2TAG1_S));
    
    // fill in completion entry
    completion_entry = &per_thread_context->pending_transmissions[tx_id];
    completion_entry->data = buf_addr;
    completion_entry->refcnt = refcnt_index;
    completion_entry->mempool = mempool;
    completion_entry->last_id = last_id;

    // return next id
    return completion_entry->next_id;
}

int finish_single_transmission(struct custom_ice_per_thread_context *per_thread_context,
        uint16_t last_id) {
    struct custom_ice_tx_entry *completion_entry = &per_thread_context->pending_transmissions[last_id];
    uint16_t next_tx_id = completion_entry->next_id;
    // locally updates the tx_id in tx queue struct
    per_thread_context->tx_queue->tx_tail = next_tx_id;
    if (last_id == 0) {
        printf("last_id: %hu, nb_tx_desc: %hu\n", last_id, per_thread_context->tx_queue->nb_tx_desc);
    }
    return 0;
}

void post_queued_segments(struct custom_ice_per_thread_context *per_thread_context, uint16_t tx_id) {
    struct custom_ice_tx_entry *completion_entry = &per_thread_context->pending_transmissions[tx_id];
    uint16_t next_tx_id = completion_entry->next_id;
    // WRITE TO TAIL REGISTER 
    ICE_PCI_REG_WRITE(per_thread_context->tx_queue->qtx_tail, next_tx_id);
}

size_t custom_ice_get_txd_avail(struct custom_ice_per_thread_context *per_thread_context) {
    return per_thread_context->tx_queue->nb_tx_free;
}

int custom_ice_tx_cleanup(struct custom_ice_per_thread_context *per_thread_context) {
	struct ice_tx_queue *txq = per_thread_context->tx_queue;
	struct custom_ice_tx_entry *completion_ring = per_thread_context->pending_transmissions;
	volatile struct ice_tx_desc *txd = txq->tx_ring;
	uint16_t last_desc_cleaned = txq->last_desc_cleaned;
	uint16_t nb_tx_desc = txq->nb_tx_desc;
	uint16_t desc_to_clean_to;
	uint16_t nb_tx_to_clean;
	struct custom_ice_tx_entry *completion;

	/* Determine the last descriptor needing to be cleaned */
	desc_to_clean_to = (uint16_t)(last_desc_cleaned + txq->tx_rs_thresh);
	if (desc_to_clean_to >= nb_tx_desc)
		desc_to_clean_to = (uint16_t)(desc_to_clean_to - nb_tx_desc);

	/* Check to make sure the last descriptor to clean is done */
	desc_to_clean_to = completion_ring[desc_to_clean_to].last_id;
	if (!(txd[desc_to_clean_to].cmd_type_offset_bsz &
	    rte_cpu_to_le_64(ICE_TX_DESC_DTYPE_DESC_DONE))) {
		NETPERF_DEBUG("TX descriptor %4u is not done "
				"(port=%d queue=%d) value=0x%"PRIx64"\n",
				desc_to_clean_to,
				txq->port_id, txq->queue_id,
				txd[desc_to_clean_to].cmd_type_offset_bsz);
		/* Failed to clean any descriptors */
		return -1;
	}

	/* Figure out how many descriptors will be cleaned */
	if (last_desc_cleaned > desc_to_clean_to)
		nb_tx_to_clean = (uint16_t)((nb_tx_desc - last_desc_cleaned) +
					    desc_to_clean_to);
	else
		nb_tx_to_clean = (uint16_t)(desc_to_clean_to -
					    last_desc_cleaned);

	for (uint16_t i = 1; i <= nb_tx_to_clean; i++) {
	    completion = &completion_ring[(last_desc_cleaned + i) % nb_tx_desc];
	    custom_ice_refcnt_update_or_free(completion->mempool, completion->data, 
			    completion->refcnt, -1); 
	}	

	/* The last descriptor to clean is done, so that means all the
	 * descriptors from the last descriptor that was cleaned
	 * up to the last descriptor with the RS bit set
	 * are done. Only reset the threshold descriptor.
	 */
	txd[desc_to_clean_to].cmd_type_offset_bsz = 0;

	/* Update the txq to reflect the last descriptor that was cleaned */
	txq->last_desc_cleaned = desc_to_clean_to;
	txq->nb_tx_free = (uint16_t)(txq->nb_tx_free + nb_tx_to_clean);

	return 0;
}



