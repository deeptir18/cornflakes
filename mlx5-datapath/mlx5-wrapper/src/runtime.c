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

void process_completion(uint16_t wqe_idx, struct mlx5_txq *v) {
    struct transmission_info *transmission = get_completion_segment(v, wqe_idx);
    struct transmission_info *curr = transmission;
    for (uint64_t i = 0; i < transmission->info.metadata.num_mbufs; i++) {
        curr = incr_transmission_info(v, curr);
        // reduce the ref count on this mbuf
        mbuf_refcnt_update_or_free(curr->info.mbuf, -1);
    }
    // advance the sq_head
    v->true_cq_head += transmission->info.metadata.num_wqes;
}

int mlx5_process_completions(struct mlx5_txq *v,
                                unsigned int budget) {
    unsigned int compl_cnt;
    uint8_t opcode;
    uint16_t wqe_idx;

    struct mlx5dv_cq *cq = &v->tx_cq_dv;
    struct mlx5_cqe64 *cqe = cq->buf;
    struct mlx5_cqe64 *cqes = cq->buf;

    for (compl_cnt = 0; compl_cnt < budget; compl_cnt++, v->cq_head++) {
        cqe = &cqes[v->cq_head & (cq->cqe_cnt - 1)];
        opcode = cqe_status(cqe, cq->cqe_cnt, v->cq_head);
        if (opcode == MLX5_CQE_INVALID) {
            NETPERF_DEBUG_RATE_LIMITED("Invalid cqe for cqe # %u", v->cq_head);
            break;
        }

        PANIC_ON_TRUE(opcode != MLX5_CQE_REQ, "Wrong opcode, cqe_format: %d, cqe_format equals 0x3: %d, opcode: %d, wqe counter: %d, syndrome: %d", 
                mlx5_get_cqe_format(cqe), 
                mlx5_get_cqe_format(cqe) == 0x3, 
                mlx5_get_cqe_opcode(cqe), 
                be16toh(cqe->wqe_counter), 
                get_error_syndrome(cqe));

        wqe_idx = be16toh(cqe->wqe_counter) & (v->tx_qp_dv.sq.wqe_cnt - 1);
        // process completion information for this transmission
        process_completion(wqe_idx, v);
    }

    cq->dbrec[0] = htobe32(v->cq_head & 0xffffff);
    return compl_cnt;
}

int mlx5_gather_rx(struct mlx5_rxq *v,
                    struct mbuf **ms,
                    unsigned int budget,
                    struct registered_mempool *rx_mempool) {
    uint8_t opcode;
    uint16_t wqe_idx;
    int rx_cnt;
    int ret;
	
	struct mlx5dv_rwq *wq = &v->rx_wq_dv;
	struct mlx5dv_cq *cq = &v->rx_cq_dv;

	struct mlx5_cqe64 *cqe, *cqes = cq->buf;
	struct mbuf *m;

	for (rx_cnt = 0; rx_cnt < budget; rx_cnt++, v->consumer_idx++) {
		cqe = &cqes[v->consumer_idx & (cq->cqe_cnt - 1)];
		opcode = cqe_status(cqe, cq->cqe_cnt, v->consumer_idx);

		if (opcode == MLX5_CQE_INVALID)
			break;

		if (unlikely(opcode != MLX5_CQE_RESP_SEND)) {
			NETPERF_PANIC("got opcode %02X", opcode);
			exit(1);
		}

        // TODO: log what was dropped
        v->rx_hw_drop += be32toh(cqe->sop_drop_qpn) >> 24;
		//STAT(RX_HW_DROP) += be32toh(cqe->sop_drop_qpn) >> 24;

		PANIC_ON_TRUE(mlx5_get_cqe_format(cqe) == 0x3i, "Wrong cqe format"); // not compressed
		wqe_idx = be16toh(cqe->wqe_counter) & (wq->wqe_cnt - 1);
		m = v->buffers[wqe_idx];
        // set length to be actual data length received
        // TODO: necessary to reset the buf address here?
        m->data_len = be32toh(cqe->byte_cnt);
        m->rss_hash = mlx5_get_rss_result(cqe);
		ms[rx_cnt] = m;
	}

	if (unlikely(!rx_cnt))
		return rx_cnt;

	cq->dbrec[0] = htobe32(v->consumer_idx & 0xffffff);
	ret = mlx5_refill_rxqueue(v, rx_cnt, rx_mempool);
    if (ret != 0) {
        NETPERF_WARN("Error filling rxqueue");
        return ret;
    }

	return rx_cnt;
}
                    

struct mlx5_wqe_ctrl_seg *fill_in_hdr_segment(struct mlx5_txq *v,
                                                size_t inline_len,
                                                size_t num_segs,
                                                int tx_flags) {
    struct mlx5_wqe_ctrl_seg *ctrl;
    struct mlx5_wqe_eth_seg *eseg;
    char *current_segment_ptr;
    struct transmission_info *current_completion_info;
    
    uint32_t current_idx = current_segment(v);
    if (!tx_descriptors_available(v, inline_len, num_segs)) {
        errno = -ENOMEM;
        return NULL;
    }
    
    current_segment_ptr = get_work_request(v, current_idx);
    current_completion_info = get_completion_segment(v, current_idx);
    size_t num_wqes = num_wqes_required(inline_len, num_segs);
    current_completion_info->info.metadata.num_wqes = (uint32_t)num_wqes;
    current_completion_info->info.metadata.num_mbufs = (uint32_t)num_segs;

    ctrl = (struct mlx5_wqe_ctrl_seg *)current_segment_ptr;
    *(uint32_t *)(current_segment_ptr + 8) = 0;
    ctrl->imm = 0;
    ctrl->fm_ce_se = MLX5_WQE_CTRL_CQ_UPDATE;
    ctrl->qpn_ds = htobe32((num_wqes_required(inline_len, num_segs)) | (v->tx_qp->qp_num << 8) );
    ctrl->opmod_idx_opcode = htobe32( ((v->sq_head * 0xffff) << 8) | MLX5_OPCODE_SEND);
    current_segment_ptr += sizeof(ctrl);
    
    eseg = (struct mlx5_wqe_eth_seg *)current_segment_ptr;
    memset(eseg, 0, sizeof(struct mlx5_wqe_eth_seg));
    eseg->cs_flags = tx_flags;
    eseg->inline_hdr_sz = htobe16(inline_len);
    
    return ctrl;
}

size_t copy_inline_data(struct mlx5_txq *v, size_t inline_offset, char *src, size_t copy_len, size_t inline_size) {
    char *end_ptr = work_requests_end(v);
    // check for out of bounds
    if ((inline_offset + copy_len) > inline_size) {
        if (inline_offset < inline_size) {
            copy_len = inline_size - inline_offset;
        } else {
            return 0;
        }
    }
    char *inline_offset_ptr = work_request_inline_off(v, inline_offset, 0);

    // break the memcpy around the ring buffer
    if ((char *)end_ptr > (inline_offset_ptr + copy_len)) {
        size_t first_half = (char *)end_ptr - inline_offset_ptr;
        memcpy(inline_offset_ptr, src, first_half);
        memcpy((char *)v->tx_qp_dv.sq.buf, src + first_half, copy_len - first_half);
    } else {
        memcpy(inline_offset_ptr, src, copy_len);
    }
    
    return copy_len; 
}

struct mlx5_wqe_data_seg *add_dpseg(struct mlx5_txq *v,
                struct mlx5_wqe_data_seg *dpseg,
                struct mbuf *m, 
                size_t data_off,
                size_t data_len) {
    dpseg->byte_count = htobe32(data_len);
    dpseg->addr = htobe64(mbuf_offset(m, data_off, uint64_t));
    dpseg->lkey = htobe32(m->lkey);
    return incr_dpseg(v, dpseg);
}

struct transmission_info *add_completion_info(struct mlx5_txq *v,
            struct transmission_info *transmission_info,
            struct mbuf *m) {
    transmission_info->info.mbuf = m;
    return incr_transmission_info(v, transmission_info);
}

// TODO: finish this
int mlx5_refill_rxqueue(struct mlx5_rxq *v, 
                            size_t rx_cnt, 
                            struct registered_mempool *rx_mempool) {
    unsigned int i;
    uint32_t index;
    struct mlx5_wqe_data_seg *seg;

    struct mlx5dv_rwq *wq = &v->rx_wq_dv;

    NETPERF_ASSERT(wraps_lte(rx_cnt + v->wq_head, v->consumer_idx + wq->wqe_cnt), "Wraparound assertion failed");
    
    for (i = 0; i < rx_cnt; i++) {
        // allocate data mbuf
        struct mbuf *metadata_mbuf = allocate_data_and_metadata_mbuf(rx_mempool);
        struct ibv_mr *mr = rx_mempool->mr;

        if (!metadata_mbuf) {
            NETPERF_WARN("Not able to allocate rx mbuf to refill pool");
            return -ENOMEM;
        }

        index = POW2MOD(v->wq_head, wq->wqe_cnt);
        seg = wq->buf + (index << v->rx_wq_log_stride);
        seg->lkey = htobe32(mr->lkey);
        seg->byte_count = htobe32(metadata_mbuf->data_buf_len);
        seg->addr = htobe64((unsigned long)metadata_mbuf->buf_addr);
        v->buffers[index] = metadata_mbuf;
        v->wq_head++;
    }

    udma_to_device_barrier();
    wq->dbrec[0] = htobe32(v->wq_head & 0xffff); 
    return 0;
}

