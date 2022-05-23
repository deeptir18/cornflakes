#pragma once
#include <mlx5/mlx5.h>
#include <mlx5/mlx5_init.h>
#include <mlx5/mlx5_runtime.h>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <net/ethernet.h>
#include <net/ip.h>
#include <net/udp.h>



/* 
 * Check the number of octowords required for a particular transmission.
 * Args:
 * @inline_len: size_t - Amount of data to be inlined.
 * @num_segs: size_t - Number of data segments to write in,
 * */
size_t custom_mlx5_num_octowords(size_t inline_len, size_t num_segs);

/* 
 * Returns current number of wqes available.
 * @per_thread_context - per thread context.
 * */
size_t custom_mlx5_num_wqes_available(struct custom_mlx5_per_thread_context *per_thread_context);

/* 
 * Check the number of wqes required for a particular transmission.
 * Args:
 * @num_octowords - Number of 16 byte segments needed by this transmission.
 * */
size_t custom_mlx5_num_wqes_required(size_t num_octowords);

/* 
 * Check if this amount of inlined data and dpsegs can be transmitted.
 * Args:
 * @v: struct mlx5_txq * - Transmission queue pointer to transmit on.
 * @num_wqes_required - Number of wqes required for the transmission,
 *
 * Returns:
 * 1 if enough descriptors are available.
 * 0 if not enough descriptors are available.
 * */
int custom_mlx5_tx_descriptors_available(struct custom_mlx5_per_thread_context *per_thread_context, size_t num_wqes_required);

/* 
 * Process completion - processes completion for specific work request.
 * Args:
 * @wqe_idx: index into ring buffer for completion.
 * @v: transmission queue.
 * */
void custom_mlx5_process_completion(uint16_t wqe_idx, struct custom_mlx5_txq *v);

/*
 * Process completions - processes any transmission completions.
 * Will reduce reference count and/or free underlying mbufs within
 * completed transmission.
 * Args:
 * @per_thread_context: Mlx5 per thread context
 * @budget: Maximum number of completions to process.
 *
 * Returns:
 * Number of processed completions.
 * */
int custom_mlx5_process_completions(struct custom_mlx5_per_thread_context *per_thread_context,
                                unsigned int budget);

/* 
 * mlx5_gather_rx - Gathers received packets so far into given mbuf array.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @ms - Array of mbuf pointers to put in received packets.
 * @budget - Maximum number of received packets to process.
 *
 * Returns:
 * Number of packets received.
 * */
int custom_mlx5_gather_rx(struct custom_mlx5_per_thread_context *per_thread_context,
                    struct custom_mlx5_mbuf **ms,
                    unsigned int budget);
                    

/* 
 * Refills the rxqueue by allocating new buffers.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @rx_cnt - Number of packets to reallocate / fill
 * @rx_mempool - Receive metadata and data mempool to allocate from.
 *
 * Returns:
 * 0 on success, error if error ocurred.
 * */
int custom_mlx5_refill_rxqueue(struct custom_mlx5_per_thread_context *per_thread_context, size_t rx_cnt, struct registered_mempool *rx_mempool);

/* 
 * Starts the next transmission by writing in the header segment.
 * Args:
 * @per_thread_context: Mlx5 per thread context
 * @num_octowords - Number of octowords required for a particular transmission,
 * @num_wqes - Number of wqes required to transmit this inline length and number
 * @of segments.
 * @inline_len - Amount of data to inline in this segment
 * @num_segs - Number of non-contiguous segments to transmit
 * @tx_flags - Flags to set in cs_flags field in ethernet segment
 *
 * Assumes:
 * Caller has checked if there are available wqes on the ring buffer
 * available. 
 *
 * Returns:
 * Pointer to the ctrl segment on success, NULL if anything went wrong, with
 * errno set.
 *
 * */
struct mlx5_wqe_ctrl_seg *custom_mlx5_fill_in_hdr_segment(struct custom_mlx5_per_thread_context *per_thread_context,
                            size_t num_octowords,
                            size_t num_wqes,
                            size_t inline_len,
                            size_t num_segs,
                            int tx_flags);

/* 
 * For current work request being filled in,
 * get offset into inline data inline_off in.
 * Assumes current work request is at index:
 * v->sq_head.
 * Arguments:
 * @v: transmission queue
 * @inline_off - offset into inline data to calculate
 * @round_to_16 - rounds the address to the next offset of 16 (where a dpseg can
 * start).
 * Returns:
 * Pointer to end of inline data (which could be wrapped around to the front
 * of the ring buffer).
 * */
char *custom_mlx5_work_request_inline_off(struct custom_mlx5_txq *v, size_t inline_off, bool round_to_16);

/* 
 * For current segment being transmitted, return start of data segments pointer.
 * Arguments:
 * @v - transmission queue
 * @inline_size - Amount of data that has been inlined.
 *
 * Returns:
 * Pointer to first data segment for this transmission.
 * */
inline struct mlx5_wqe_data_seg *custom_mlx5_dpseg_start(struct custom_mlx5_txq *v, size_t inline_off) {
    return (struct mlx5_wqe_data_seg *)(custom_mlx5_work_request_inline_off(v, inline_off, 1));
}

/* 
 * For current segment being transmitted, return the SECOND transmission_info
 * pointer, e.g., where data for the first segment being transmitted would be
 * recorded.
 * Arguments:
 * @v - transmission queue
 *
 * Returns:
 * Pointer to second transmission info struct.
 * */
inline struct custom_mlx5_transmission_info *custom_mlx5_completion_start(struct custom_mlx5_txq *v) {
    struct custom_mlx5_transmission_info *current_completion_info = custom_mlx5_get_completion_segment(v, custom_mlx5_current_segment(v));
    NETPERF_DEBUG("Current completion info: %p", current_completion_info); 
    return custom_mlx5_incr_transmission_info(v, current_completion_info);
}

/*
 * Check if copying into ring buffer causes a crossover back to the beginning.
 * */
int check_inline_copy_crossover(struct custom_mlx5_per_thread_context *per_thread_context, char *inline_offset_ptr, size_t copy_len);

/* 
 * copy_inline_data_from_offset - copies inline data into the segment currently being
 * constructed from inline offset ptr.
 * arguments:
 * @per_thread_context: mlx5 per thread context
 * @inline_offset_ptr - offset into inline data (inlined data already copied)
 * @src - source buffer to copy from
 * @copy_len - amount of data to copy.
 *
 * returns:
 * amount of data copied. truncates to minimum of (inline_size -
 * inline_offset, copy_len)
 * */
size_t custom_mlx5_copy_inline_data_from_offset_wraparound(struct custom_mlx5_per_thread_context *per_thread_context, char *inline_offset_ptr, const char *src, size_t copy_len);

 /* copy_inline_data - copies inline data into the segment currently being
 * constructed from inline offset.
 * arguments:
 * @per_thread_context: mlx5 per thread context
 * @inline_offset - amount of already inlined data
 * @src - source buffer to copy from
 * @copy_len - amount of data to copy.
 * @inline_size - Total inline size
 *
 * returns:
 * amount of data copied. truncates to minimum of (inline_size -
 * inline_offset, copy_len)
 * */
size_t custom_mlx5_copy_inline_data(struct custom_mlx5_per_thread_context *per_thread_context, size_t inline_offset, const char *src, size_t copy_len, size_t inline_size);
/* 
 * Inline and write in ethernet header.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @eth - ethernet header to inline
 * @total_inline_size - total amount of data being inlined in this transmission
 * */
void custom_mlx5_inline_eth_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct eth_hdr *eth, size_t total_inline_size);

/* 
 * Inline and write in ipv4 header.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @ipv4 - Ipv4 header to inline. ASSUMES CHECKSUM HAS BEEN WRITTEN IN.
 * @payload_size - Size of the packet payload, not including networking (ip and
 * udp) headers.
 * @total_inline_size - total amount of data being inlined in this traAnsmission
 *
 * Note this function is NOT threadsafe. It temporarily modifies the data inside
 * ip_hdr in order to calculate the checksum.
 * */
void custom_mlx5_inline_ipv4_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct ip_hdr *ipv4, size_t payload_size, size_t total_inline_size);

/* 
 * Inline and write udp header.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @udp - Udp header to inline.
 * @payload_size - Size of packet payload, not including networking (udp)
 * headers.
 *
 * Note this function is NOT threadsafe. It temporarily modifies the data inside
 * udp_hdr in order to calculate the checksum.
 * */
void custom_mlx5_inline_udp_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct udp_hdr *udp, size_t payload_size, size_t total_inline_size);

/* Inlines packet header after ethernet, ip and udp headers.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @packet_id - packet id to inline
 * */
void custom_mlx5_inline_packet_id(struct custom_mlx5_per_thread_context *per_thread_context,  uint32_t packet_id);

/*
 * add_dpseg - Adds next dpseg into this transmission.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @dpseg - Pointer to the dpseg.
 * @m - mbuf to add as dpseg.
 * @data_off - data offset into mbuf.
 * @data_len - size of data to reference inside mbuf.
 *
 * Returns:
 * Pointer to next dpseg to add to.
 * */
struct mlx5_wqe_data_seg *custom_mlx5_add_dpseg(struct custom_mlx5_per_thread_context *per_thread_context,
                struct mlx5_wqe_data_seg *dpseg,
                struct custom_mlx5_mbuf *m, 
                size_t data_off,
                size_t data_len);


/* 
 * Records completion info in completion ring buffer.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @transmission info - current completion info struct,
 * @m - mbuf to record.
 *
 * Returns:
 * location to record next transmission info.
 * */
struct custom_mlx5_transmission_info *custom_mlx5_add_completion_info(struct custom_mlx5_per_thread_context *per_thread_context,
                struct custom_mlx5_transmission_info *transmission_info,
                struct custom_mlx5_mbuf *m);

/* 
 * finish_one_transmission - Finishes a single transmission.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @inline_len - Amount of data to be inlined.
 * @num_segs - Number of data segments.
 * */
int custom_mlx5_finish_single_transmission(struct custom_mlx5_per_thread_context *per_thread_context,
                                size_t num_wqes);

/* 
 * post_transmissions - Rings doorbell and posts new transmissions for the nic
 * to transmit.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @first_ctrl - Control segment of the first transmission. Possibly required
 * for bluefield register.
 * */
int custom_mlx5_post_transmissions(struct custom_mlx5_per_thread_context *per_thread_context, 
                        struct mlx5_wqe_ctrl_seg *first_ctrl);
                        

