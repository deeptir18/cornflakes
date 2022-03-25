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
 * Check the number of wqes required for a particular transmission.
 * Args:
 * @inline_len: size_t - Amount of data to be inlined.
 * @num_segs: size_t - Number of data segments to write in,
 * */
size_t num_wqes_required(size_t inline_len, size_t num_segs);

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
int tx_descriptors_available(struct mlx5_per_thread_context *per_thread_context, size_t num_wqes_required);

/* 
 * Process completion - processes completion for specific work request.
 * Args:
 * @wqe_idx: index into ring buffer for completion.
 * @v: transmission queue.
 * */
void process_completion(uint16_t wqe_idx, struct mlx5_txq *v);

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
int mlx5_process_completions(struct mlx5_per_thread_context *per_thread_context,
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
int mlx5_gather_rx(struct mlx5_per_thread_context *per_thread_context,
                    struct mbuf **ms,
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
int mlx5_refill_rxqueue(struct mlx5_per_thread_context *per_thread_context, size_t rx_cnt, struct registered_mempool *rx_mempool);

/* 
 * Starts the next transmission by writing in the header segment.
 * Args:
 * @per_thread_context: Mlx5 per thread context
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
struct mlx5_wqe_ctrl_seg *fill_in_hdr_segment(struct mlx5_per_thread_context *per_thread_context,
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
inline char *work_request_inline_off(struct mlx5_txq *v, size_t inline_off, bool round_to_16) {
    uint32_t current_idx = current_segment(v);
    struct mlx5_wqe_eth_seg *eseg = (struct mlx5_wqe_eth_seg *)((char *)get_work_request(v, current_idx) + sizeof(struct mlx5_wqe_ctrl_seg));
    char *end_ptr = work_requests_end(v);

    char *current_segment_ptr = (char *)eseg + offsetof(struct mlx5_wqe_eth_seg, inline_hdr_start);
    // wrap around to front of ring buffer
    if ((current_segment_ptr + inline_off) >= end_ptr) {
        size_t second_segment = inline_off - (end_ptr - current_segment_ptr);
        current_segment_ptr = (char *)v->tx_qp_dv.sq.buf;
        if (round_to_16) {
            current_segment_ptr += (second_segment + 15) & 0xf;
        } else {
            current_segment_ptr += second_segment;
        }
    } else {
        char *end_inline = current_segment_ptr + inline_off;
        // wrap around to front of ring buffer.
        if (((end_ptr - end_inline) <= 15) && round_to_16) {
            current_segment_ptr = v->tx_qp_dv.sq.buf;
        } else {
            if (inline_off <= 2) {
                if (round_to_16) {
                    current_segment_ptr += 2;
                } else {
                    current_segment_ptr += inline_off;
                }
            } else {
                current_segment_ptr += 2;
                if (round_to_16) {
                    current_segment_ptr += (inline_off - 2 + 15) & 0xf;
                } else {
                    current_segment_ptr += (inline_off - 2);
                }
            }
        }
    }

    return current_segment_ptr;
}

/* 
 * For current segment being transmitted, return start of data segments pointer.
 * Arguments:
 * @v - transmission queue
 * @inline_size - Amount of data that has been inlined.
 *
 * Returns:
 * Pointer to first data segment for this transmission.
 * */
inline struct mlx5_wqe_data_seg *dpseg_start(struct mlx5_txq *v, size_t inline_off) {
    return (struct mlx5_wqe_data_seg *)(work_request_inline_off(v, inline_off, 1));
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
inline struct transmission_info *completion_start(struct mlx5_txq *v) {
    struct transmission_info *current_completion_info = get_completion_segment(v, current_segment(v));
    return incr_transmission_info(v, current_completion_info);
}

/* 
 * copy_inline_data - Copies inline data into the segment currently being
 * constructed.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @inline_offset - offset into inline data (inlined data already copied)
 * @src - Source buffer to copy from
 * @copy_len - Amount of data to copy.
 * @inline_size - Amount of total inlined size
 *
 * Returns:
 * Amount of data copied. Truncates to minimum of (inline_size -
 * inline_offset, copy_len)
 * */
size_t copy_inline_data(struct mlx5_per_thread_context *per_thread_context, size_t inline_offset, const char *src, size_t copy_len, size_t inline_size);

/* 
 * Inline and write in ethernet header.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @eth - ethernet header to inline
 * @total_inline_size - total amount of data being inlined in this transmission
 * */
void inline_eth_hdr(struct mlx5_per_thread_context *per_thread_context, struct eth_hdr *eth, size_t total_inline_size);

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
void inline_ipv4_hdr(struct mlx5_per_thread_context *per_thread_context, struct ip_hdr *ipv4, size_t payload_size, size_t total_inline_size);

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
void inline_udp_hdr(struct mlx5_per_thread_context *per_thread_context, struct udp_hdr *udp, size_t payload_size, size_t total_inline_size);

/* Inlines packet header after ethernet, ip and udp headers.
 * Arguments:
 * @per_thread_context: Mlx5 per thread context
 * @packet_id - packet id to inline
 * */
void inline_packet_id(struct mlx5_per_thread_context *per_thread_context,  uint32_t packet_id);

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
struct mlx5_wqe_data_seg *add_dpseg(struct mlx5_per_thread_context *per_thread_context,
                struct mlx5_wqe_data_seg *dpseg,
                struct mbuf *m, 
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
struct transmission_info *add_completion_info(struct mlx5_per_thread_context *per_thread_context,
                struct transmission_info *transmission_info,
                struct mbuf *m);

/* 
 * finish_one_transmission - Finishes a single transmission.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @inline_len - Amount of data to be inlined.
 * @num_segs - Number of data segments.
 * */
int finish_single_transmission(struct mlx5_per_thread_context *per_thread_context,
                                size_t num_wqes);

/* 
 * post_transmissions - Rings doorbell and posts new transmissions for the nic
 * to transmit.
 * Arguments:
 * @per_thread_context: mlx5 per thread context
 * @first_ctrl - Control segment of the first transmission. Possibly required
 * for bluefield register.
 * */
int post_transmissions(struct mlx5_per_thread_context *per_thread_context, 
                        struct mlx5_wqe_ctrl_seg *first_ctrl);
                        
