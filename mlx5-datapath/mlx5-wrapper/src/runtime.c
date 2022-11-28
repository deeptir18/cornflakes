#include <inttypes.h>
#include <math.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>

#include <asm/ops.h>
#include <base/debug.h>
#include <base/time.h>
#include <base/mem.h>
#include <base/compiler.h>
#include <base/mempool.h>
#include <base/mbuf.h>
#include <base/pci.h>
#include <base/stddef.h>
#include <mlx5/mlx5.h>
#include <mlx5/mlx5_init.h>
#include <mlx5/mlx5_runtime.h>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <util/mmio.h>
#include <util/udma_barrier.h>

void custom_mlx5_process_completion(uint16_t wqe_idx, struct custom_mlx5_txq *v) {
    struct custom_mlx5_transmission_info *transmission = custom_mlx5_get_completion_segment(v, wqe_idx);
    struct custom_mlx5_transmission_info *curr = transmission;
    NETPERF_DEBUG("Transmission %p, num_wqes: %u, num_mbufs: %u", transmission, transmission->info.metadata.num_wqes, transmission->info.metadata.num_mbufs);
    for (uint64_t i = 0; i < transmission->info.metadata.num_mbufs; i++) {
        curr = custom_mlx5_incr_transmission_info(v, curr);
        NETPERF_DEBUG("Processing completion entry %p with data addr %p", curr, curr->info.data);
        // find ref count index
        size_t refcnt_index = (size_t)(custom_mlx5_mempool_find_index(&(curr->mempool->data_mempool), (void *)curr->info.data));
        // reduce refcnt or free data
        NETPERF_DEBUG("Index %lu, Processing completion for mempool %p, data %p, refcnt_index %lu; cur refcnt: %u, cur allocated: %lu", i, curr->mempool, curr->info.data, refcnt_index, curr->mempool->data_mempool.ref_counts[refcnt_index], curr->mempool->data_mempool.allocated);
        custom_mlx5_refcnt_update_or_free(curr->mempool, curr->info.data, refcnt_index, -1);
    }
    // advance the sq_head
    v->true_cq_head += transmission->info.metadata.num_wqes;
    NETPERF_DEBUG("Incrementing true cq head to %u", v->true_cq_head);
}

int custom_mlx5_process_completions(struct custom_mlx5_per_thread_context *per_thread_context,
                                unsigned int budget) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;

    if (custom_mlx5_nr_inflight_tx(v) < (SQ_CLEAN_THRESH)) {
        NETPERF_DEBUG("IN FLIGHT: %u", custom_mlx5_nr_inflight_tx(v));
        return 0;
    }
    unsigned int compl_cnt;
    uint8_t opcode;
    uint16_t wqe_idx;

    struct mlx5dv_cq *cq = &v->tx_cq_dv;
    struct mlx5_cqe64 *cqe = cq->buf;
    struct mlx5_cqe64 *cqes = cq->buf;

    for (compl_cnt = 0; compl_cnt < budget; compl_cnt++, v->cq_head++) {
        NETPERF_DEBUG("Polling for completion on cqe: %u, true_cq_head: %u, true idx: %u", v->cq_head, v->true_cq_head, v->cq_head & (cq->cqe_cnt - 1));
        cqe = &cqes[v->cq_head & (cq->cqe_cnt - 1)];
        opcode = custom_mlx5_cqe_status(cqe, cq->cqe_cnt, v->cq_head);
        if (opcode == MLX5_CQE_INVALID) {
            NETPERF_DEBUG("Invalid cqe for cqe # %u, syndrome: %d", v->cq_head, custom_mlx5_get_error_syndrome(cqe));
            break;
        }

        NETPERF_PANIC_ON_TRUE(opcode != MLX5_CQE_REQ, "Wrong opcode, cqe_format: %d, cqe_format equals 0x3: %d, opcode: %d, wqe counter: %d, syndrome: %d", 
                custom_mlx5_get_cqe_format(cqe), 
                custom_mlx5_get_cqe_format(cqe) == 0x3, 
                custom_mlx5_get_cqe_opcode(cqe), 
                be16toh(cqe->wqe_counter), 
                custom_mlx5_get_error_syndrome(cqe));

        wqe_idx = be16toh(cqe->wqe_counter) & (v->tx_qp_dv.sq.wqe_cnt - 1);
        NETPERF_DEBUG("Got completion on wqe idx: %u; unwrapped cqe wqe idx: %u, wqe ct: %u, cqe cnt: %u", wqe_idx, be16toh(cqe->wqe_counter), v->tx_qp_dv.sq.wqe_cnt, cq->cqe_cnt);
        // process completion information for this transmission
        custom_mlx5_process_completion(wqe_idx, v);
    }

    cq->dbrec[0] = htobe32(v->cq_head & 0xffffff);
    return 0;
}

int custom_mlx5_gather_rx(struct custom_mlx5_per_thread_context *per_thread_context,
                    struct recv_mbuf_info *recv_mbufs,
                    unsigned int budget) {
    struct registered_mempool *rx_mempool = per_thread_context->rx_mempool;
    struct custom_mlx5_rxq *v = &per_thread_context->rxq;
    uint8_t opcode;
    uint16_t wqe_idx;
    int rx_cnt;
    int ret;
	
	struct mlx5dv_rwq *wq = &v->rx_wq_dv;
	struct mlx5dv_cq *cq = &v->rx_cq_dv;

	struct mlx5_cqe64 *cqe, *cqes = cq->buf;

    struct recv_mbuf_info *recv_info = recv_mbufs;
	for (rx_cnt = 0; rx_cnt < budget; rx_cnt++, v->consumer_idx++) {
		cqe = &cqes[v->consumer_idx & (cq->cqe_cnt - 1)];
		opcode = custom_mlx5_cqe_status(cqe, cq->cqe_cnt, v->consumer_idx);

		if (opcode == MLX5_CQE_INVALID)
			break;

		if (unlikely(opcode != MLX5_CQE_RESP_SEND)) {
			NETPERF_PANIC("got opcode %d", opcode);
			exit(1);
		}

        // TODO: log what was dropped
        v->rx_hw_drop += be32toh(cqe->sop_drop_qpn) >> 24;
        if ((be32toh(cqe->sop_drop_qpn) >> 24) > 0) {
            NETPERF_DEBUG("Logging rx drop: %u", be32toh(cqe->sop_drop_qpn) >> 24);
        }
		//STAT(RX_HW_DROP) += be32toh(cqe->sop_drop_qpn) >> 24;
        NETPERF_DEBUG("Past drop check");

		NETPERF_PANIC_ON_TRUE(custom_mlx5_get_cqe_format(cqe) == 0x3i, "Wrong cqe format"); // not compressed
		wqe_idx = be16toh(cqe->wqe_counter) & (wq->wqe_cnt - 1);
        struct custom_mlx5_rx_buffer *rx_buf_info = custom_mlx5_get_rx_buffers_segment(v, wqe_idx);
        NETPERF_DEBUG("Got rx_buf_info: %p", rx_buf_info);
        recv_info->buf_addr = rx_buf_info->buf_addr;
        recv_info->mempool = rx_buf_info->mempool;
        recv_info->ref_count_index = custom_mlx5_mempool_find_index(&rx_mempool->data_mempool, rx_buf_info->buf_addr);
        recv_info->pkt_len = be32toh(cqe->byte_cnt);
        recv_info->rss_hash = custom_mlx5_get_rss_result(cqe);
        NETPERF_DEBUG("Result of find index for %p data and mempool buf %p: %d; rx_buf_info ptr; %p", rx_buf_info->buf_addr, rx_mempool->data_mempool.buf, custom_mlx5_mempool_find_index(&rx_mempool->data_mempool, rx_buf_info->buf_addr), rx_buf_info);
        NETPERF_DEBUG("Received packet with: addr %p, mempool %p, ref_count_index: %lu, pkt_len: %u", rx_buf_info->buf_addr, rx_buf_info->mempool, recv_info->ref_count_index, recv_info->pkt_len);
        recv_info += 1;
	}

	if (unlikely(!rx_cnt))
		return rx_cnt;

	cq->dbrec[0] = htobe32(v->consumer_idx & 0xffffff);
    NETPERF_DEBUG("Received %u, refilling, consumer idx is %u", rx_cnt, v->consumer_idx);
	ret = custom_mlx5_refill_rxqueue(per_thread_context, rx_cnt, rx_mempool);
    if (ret != 0) {
        NETPERF_WARN("Error filling rxqueue");
        return ret;
    }

    NETPERF_DEBUG("Returning %u", rx_cnt);
	return rx_cnt;
}
                    

int custom_mlx5_refill_rxqueue(struct custom_mlx5_per_thread_context *per_thread_context,
                            size_t rx_cnt, 
                            struct registered_mempool *rx_mempool) {
    struct custom_mlx5_rxq *v = &per_thread_context->rxq;
    unsigned int i;
    uint32_t index;
    struct mlx5_wqe_data_seg *seg;

    struct mlx5dv_rwq *wq = &v->rx_wq_dv;

    NETPERF_ASSERT(wraps_lte(rx_cnt + v->wq_head, v->consumer_idx + wq->wqe_cnt), "Wraparound assertion failed");
    
    for (i = 0; i < rx_cnt; i++) {
        // allocate data mbuf
        void *data = custom_mlx5_mempool_alloc(&rx_mempool->data_mempool);
        if (data == NULL) {
            NETPERF_WARN("Could not allocate data mbuf to refill rx queue; allocated in rx_mempool: %lu, capacity in rx_mempool: %lu", rx_mempool->data_mempool.allocated, rx_mempool->data_mempool.capacity);
            return -ENOMEM;
        }
        struct ibv_mr *mr = rx_mempool->mr;
        index = POW2MOD(v->wq_head, wq->wqe_cnt);
        seg = wq->buf + (index << v->rx_wq_log_stride);
        seg->lkey = htobe32(mr->lkey);
        seg->byte_count = htobe32(rx_mempool->data_mempool.item_len);
        seg->addr = htobe64((unsigned long)data);

        struct custom_mlx5_rx_buffer *rx_ring_entry = custom_mlx5_get_rx_buffers_segment(v, index);
        rx_ring_entry->buf_addr = data;
        rx_ring_entry->mempool = rx_mempool;
        v->wq_head++;
    }

    udma_to_device_barrier();
    wq->dbrec[0] = htobe32(v->wq_head & 0xffff); 
    NETPERF_DEBUG("After refill, allocated in rx mempool is: %lu, capacity: %lu", rx_mempool->data_mempool.allocated, rx_mempool->data_mempool.capacity);
    return 0;
}

size_t custom_mlx5_num_octowords(size_t inline_len, size_t num_segs) {
    size_t num_hdr_segs = sizeof(struct mlx5_wqe_ctrl_seg) / 16
                            + (offsetof(struct mlx5_wqe_eth_seg, inline_hdr)) / 16;
    if (inline_len > 2) {
        num_hdr_segs += ((inline_len - 2) + 15) / 16;
    }
    size_t num_dpsegs = (sizeof(struct mlx5_wqe_data_seg) * num_segs) / 16;
    return num_hdr_segs + num_dpsegs;
}

size_t custom_mlx5_num_wqes_required(size_t num_octowords) {
    return (num_octowords + 3) / 4;
}

size_t custom_mlx5_num_wqes_available(struct custom_mlx5_per_thread_context *per_thread_context) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    return (v->tx_qp_dv.sq.wqe_cnt - custom_mlx5_nr_inflight_tx(v));
}

int custom_mlx5_tx_descriptors_available(struct custom_mlx5_per_thread_context *per_thread_context,
                                size_t num_wqes_required) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    return unlikely((v->tx_qp_dv.sq.wqe_cnt - custom_mlx5_nr_inflight_tx(v)) >= num_wqes_required);
}

struct mlx5_wqe_ctrl_seg *custom_mlx5_fill_in_hdr_segment(struct custom_mlx5_per_thread_context *per_thread_context,
                                                size_t num_octowords,
                                                size_t num_wqes,
                                                size_t inline_len,
                                                size_t num_segs,
                                                int tx_flags) {

    struct custom_mlx5_txq *v = &per_thread_context->txq;
    struct mlx5_wqe_ctrl_seg *ctrl;
    struct mlx5_wqe_eth_seg *eseg;
    char *current_segment_ptr;
    struct custom_mlx5_transmission_info *current_completion_info;
    
    uint32_t current_idx = custom_mlx5_current_segment(v);
    NETPERF_DEBUG("Current wqe idx for header segment: %u", current_idx);
    if (!custom_mlx5_tx_descriptors_available(per_thread_context, num_wqes)) {
        errno = -ENOMEM;
        return NULL;
    }
    
    current_segment_ptr = custom_mlx5_get_work_request(v, current_idx);
    NETPERF_DEBUG("Base of wqe ring buffer: %p, current segment: %p", v->tx_qp_dv.sq.buf, (char *)current_segment_ptr);
    current_completion_info = custom_mlx5_get_completion_segment(v, current_idx);
    NETPERF_DEBUG("Base of completions ring buffer: %p, first: %p, current completion segment: %p", v->pending_transmissions, custom_mlx5_get_completion_segment(v, 0), current_completion_info);
    current_completion_info->info.metadata.num_wqes = (uint32_t)num_wqes;
    current_completion_info->info.metadata.num_mbufs = (uint32_t)num_segs;

    ctrl = (struct mlx5_wqe_ctrl_seg *)current_segment_ptr;
    NETPERF_DEBUG("Into ctrl %p, writing in num_wqes %lu, octowords %lu, v->sq_head %u", ctrl, num_wqes, num_octowords, v->sq_head);
    *(uint32_t *)(current_segment_ptr + 8) = 0;
    ctrl->imm = 0;
    ctrl->fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    ctrl->qpn_ds = htobe32( (num_octowords) | (v->tx_qp->qp_num << 8) );
    ctrl->opmod_idx_opcode = htobe32( ((v->sq_head & 0xffff) << 8) | MLX5_OPCODE_SEND);
    current_segment_ptr += sizeof(*ctrl);
    
    eseg = (struct mlx5_wqe_eth_seg *)current_segment_ptr;
    NETPERF_DEBUG("Eseg addr: %p, inline_len: %lu", current_segment_ptr, inline_len);
    memset(eseg, 0, sizeof(struct mlx5_wqe_eth_seg));
    eseg->cs_flags = tx_flags;
    eseg->inline_hdr_sz = htobe16(inline_len);
    
    return ctrl;
}

char *custom_mlx5_work_request_inline_off(struct custom_mlx5_txq *v, size_t inline_off, bool round_to_16) {
    NETPERF_DEBUG("Trying to get inline of off %lu, round to 16: %d", inline_off, round_to_16);
    uint32_t current_idx = custom_mlx5_current_segment(v);
    NETPERF_DEBUG("Current wqe idx: %u", current_idx);
    struct mlx5_wqe_eth_seg *eseg = (struct mlx5_wqe_eth_seg *)((char *)custom_mlx5_get_work_request(v, current_idx) + sizeof(struct mlx5_wqe_ctrl_seg));
    NETPERF_DEBUG("eseg addr: %p", eseg);
    char *end_ptr = custom_mlx5_work_requests_end(v);
    NETPERF_DEBUG("End ptr: %p", end_ptr);

    char *current_segment_ptr = (char *)eseg + offsetof(struct mlx5_wqe_eth_seg, inline_hdr_start);
    // wrap around to front of ring buffer
    if ((current_segment_ptr + inline_off) >= end_ptr) {
        size_t second_segment = inline_off - (end_ptr - current_segment_ptr);
        current_segment_ptr = (char *)v->tx_qp_dv.sq.buf;
        if (round_to_16) {
            current_segment_ptr += ((second_segment + 15) & ~0xf);
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
                    NETPERF_DEBUG("In setting where we must round to 16, and inline off is <= 2");
                    current_segment_ptr += 2;
                } else {
                    current_segment_ptr += inline_off;
                }
            } else {
                current_segment_ptr += 2;
                if (round_to_16) {
                    NETPERF_DEBUG("Current: %p, inline_off: %lu, incr: %lu", current_segment_ptr, inline_off, ((inline_off - 2 + 15) & ~0xf));
                    current_segment_ptr += ((inline_off - 2 + 15) & ~0xf);
                    NETPERF_DEBUG("Current: %p, inline_off: %lu", current_segment_ptr, inline_off);
                } else {
                    current_segment_ptr += (inline_off - 2);
                }
            }
        }
    }

    NETPERF_DEBUG("Returning current segment ptr %p", current_segment_ptr);
    return current_segment_ptr;
}

int check_inline_copy_crossover(struct custom_mlx5_per_thread_context *per_thread_context, char *inline_offset_ptr, size_t copy_len) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *end_ptr = custom_mlx5_work_requests_end(v);
    if ((char *)end_ptr < (inline_offset_ptr + copy_len)) {
        // cross over
        return 1;
    } else {
        return 0;
    }
}

size_t custom_mlx5_copy_inline_data_from_offset_wraparound(struct custom_mlx5_per_thread_context *per_thread_context, char *inline_offset_ptr, const char *src, size_t copy_len) {
    NETPERF_DEBUG("Copying inline data at %p", inline_offset_ptr);
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *end_ptr = custom_mlx5_work_requests_end(v);
    size_t first_half = (char *)end_ptr - inline_offset_ptr;
    custom_mlx5_rte_memcpy(inline_offset_ptr, src, first_half);
    custom_mlx5_rte_memcpy((char *)v->tx_qp_dv.sq.buf, src + first_half, copy_len - first_half);
    return copy_len;
}

size_t custom_mlx5_copy_inline_data(struct custom_mlx5_per_thread_context *per_thread_context, size_t inline_offset, const char *src, size_t copy_len, size_t inline_size) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *end_ptr = custom_mlx5_work_requests_end(v);

    // check for out of bounds
    if ((inline_offset + copy_len) > inline_size) {
        if (inline_offset < inline_size) {
            copy_len = inline_size - inline_offset;
        } else {
            return 0;
        }
    }
    char *inline_offset_ptr = custom_mlx5_work_request_inline_off(v, inline_offset, 0);
    NETPERF_DEBUG("Copying inline data at %p", inline_offset_ptr);

    if ((inline_offset_ptr + copy_len) > end_ptr) {
        size_t first_half = (char *)end_ptr - inline_offset_ptr;
        custom_mlx5_rte_memcpy(inline_offset_ptr, src, first_half);
        custom_mlx5_rte_memcpy((char *)v->tx_qp_dv.sq.buf, src + first_half, copy_len - first_half);
    } else {
        custom_mlx5_rte_memcpy(inline_offset_ptr, src, copy_len);
    }

    return copy_len; 
}

void custom_mlx5_inline_eth_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct eth_hdr *eth, size_t total_inline_size) {
    NETPERF_DEBUG("Inlining eth hdr");
    custom_mlx5_copy_inline_data(per_thread_context, 0, (char *)eth, sizeof(struct eth_hdr), total_inline_size);
}

void custom_mlx5_inline_ipv4_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct ip_hdr *ipv4, size_t payload_size, size_t total_inline_size) {
    NETPERF_DEBUG("Inlining ipv4 header");
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *inline_offset_ptr = custom_mlx5_work_request_inline_off(v, sizeof(struct eth_hdr), 0);
    if (check_inline_copy_crossover(per_thread_context, inline_offset_ptr, sizeof(struct ip_hdr)) == 1) {
        NETPERF_DEBUG("In case for wraparound copy");
        struct ip_hdr tmp;
        custom_mlx5_rte_memcpy(&tmp, (char *)ipv4, sizeof(struct ip_hdr));
        (&tmp)->len = htons(sizeof(struct ip_hdr) + sizeof(struct udp_hdr) + 4 + payload_size);
        (&tmp)->chksum = 0;
        (&tmp)->chksum = custom_mlx5_get_chksum(&tmp);
        custom_mlx5_copy_inline_data_from_offset_wraparound(per_thread_context, inline_offset_ptr, (char *)(&tmp), sizeof(struct ip_hdr));
    } else {
        // copy ipv4 into ring buffer directly, calculate checksum
        custom_mlx5_rte_memcpy(inline_offset_ptr, (char *)ipv4, sizeof(struct ip_hdr));
        struct ip_hdr *new_ipv4 = (struct ip_hdr *)inline_offset_ptr;
        new_ipv4->chksum = 0;
        new_ipv4->len = htons(sizeof(struct ip_hdr) + sizeof(struct udp_hdr) + 4 + payload_size);
        new_ipv4->chksum = custom_mlx5_get_chksum(new_ipv4);
    }
}

void custom_mlx5_inline_udp_hdr(struct custom_mlx5_per_thread_context *per_thread_context, const struct udp_hdr *udp, size_t payload_size, size_t total_inline_size) {
    NETPERF_DEBUG("Inlining udp header");
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *inline_offset_ptr = custom_mlx5_work_request_inline_off(v, sizeof(struct eth_hdr) + sizeof(struct ip_hdr), 0);
    if (check_inline_copy_crossover(per_thread_context, inline_offset_ptr, sizeof(struct udp_hdr)) == 1) {
        struct udp_hdr tmp;
        custom_mlx5_rte_memcpy(&tmp, (char *)udp, sizeof(struct udp_hdr));
        (&tmp)->len = htons(sizeof(struct udp_hdr) + 4 + payload_size);
        (&tmp)->chksum = 0;
        (&tmp)->chksum = custom_mlx5_get_chksum(&tmp);
        custom_mlx5_copy_inline_data_from_offset_wraparound(per_thread_context, inline_offset_ptr, (char *)(&tmp), sizeof(struct udp_hdr));
    } else {
        // copy udp into ring buffer directly, calculate checksum
        custom_mlx5_rte_memcpy(inline_offset_ptr, (char *)udp, sizeof(struct udp_hdr));
        struct udp_hdr *new_udp = (struct udp_hdr *)inline_offset_ptr;
        new_udp->chksum = 0;
        new_udp->len = htons(sizeof(struct udp_hdr) + 4 + payload_size);
        new_udp->chksum = custom_mlx5_get_chksum(new_udp);
    }
}

void custom_mlx5_inline_packet_id(struct custom_mlx5_per_thread_context *per_thread_context, uint32_t packet_id) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    char *cur_inline_off = custom_mlx5_work_request_inline_off(v, sizeof(struct eth_hdr) + sizeof(struct ip_hdr) + sizeof(struct udp_hdr), 0);
    // can be written in little endian order
    *(uint32_t *)cur_inline_off = packet_id;
}

struct mlx5_wqe_data_seg *custom_mlx5_advance_dpseg(
        struct custom_mlx5_per_thread_context *per_thread_context, 
        struct mlx5_wqe_data_seg *dpseg) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    return custom_mlx5_incr_dpseg(v, dpseg);
}
struct custom_mlx5_transmission_info *custom_mlx5_advance_completion_info(
        struct custom_mlx5_per_thread_context *per_thread_context,
        struct custom_mlx5_transmission_info *transmission_info) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    return custom_mlx5_incr_transmission_info(v, transmission_info);
}
struct mlx5_wqe_data_seg *custom_mlx5_add_dpseg(struct custom_mlx5_per_thread_context *per_thread_context,
                struct mlx5_wqe_data_seg *dpseg,
                void *data,
                struct registered_mempool *mempool,
                size_t data_off,
                size_t data_len) {
    NETPERF_DEBUG("Dpseg being filled in: %p, data_len: %lu, addr: %p, lkey: %u", dpseg, data_len, data, mempool->mr->lkey);
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    dpseg->byte_count = htobe32(data_len);
    dpseg->addr = htobe64((uint64_t)((char *)data + data_off));
    dpseg->lkey = htobe32(mempool->mr->lkey);
    return custom_mlx5_incr_dpseg(v, dpseg);
}

struct custom_mlx5_transmission_info *custom_mlx5_add_completion_info(struct custom_mlx5_per_thread_context *per_thread_context,
            struct custom_mlx5_transmission_info *transmission_info,
            void *data,
            struct registered_mempool *mempool) {
    NETPERF_DEBUG("Transmission info being filled in: %p", transmission_info);
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    transmission_info->info.data = data;
    transmission_info->mempool = mempool;
    return custom_mlx5_incr_transmission_info(v, transmission_info);
}

int custom_mlx5_finish_single_transmission(struct custom_mlx5_per_thread_context *per_thread_context,
                                size_t num_wqes) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    v->sq_head += num_wqes;
    NETPERF_DEBUG("Incrementing sq head to %u", v->sq_head);
    return 0;
}

int custom_mlx5_post_transmissions(struct custom_mlx5_per_thread_context *per_thread_context,
                        struct mlx5_wqe_ctrl_seg *first_ctrl) {
    struct custom_mlx5_txq *v = &per_thread_context->txq;
    NETPERF_DEBUG("Ringing doorbell with ctrl %p, sq_head is %u", first_ctrl, v->sq_head);
    // post next unused wqe to doorbell
    udma_to_device_barrier();
    v->tx_qp_dv.dbrec[MLX5_SND_DBR] = htobe32(v->sq_head & 0xffff);

    NETPERF_DEBUG("blueflame register size: %u", v->tx_qp_dv.bf.size);
    // blue field doorbell
    mmio_wc_start();
    mmio_write64_be(v->tx_qp_dv.bf.reg, *(__be64 *)first_ctrl);
    mmio_flush_writes();
    
    return 0;
}

