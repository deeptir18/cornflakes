#pragma once

#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include <base/byteorder.h>
#include <base/mempool.h>
#include <string.h>

#define PORT_NUM 1

#define MAX_INLINE_DATA 256
#define RQ_NUM_DESC			1024
#define SQ_NUM_DESC			128

#define SQ_CLEAN_THRESH			1
#define SQ_CLEAN_MAX			SQ_CLEAN_THRESH
#define MAX_TX_MEMPOOLS_PER_THREAD 64 /* Maximum number of 'extra mempools' a thread can have. */
#define POW2MOD(num, size) ((num & (size - 1)))
#define custom_mlx5_current_segment(v) (POW2MOD(v->sq_head, v->tx_qp_dv.sq.wqe_cnt))

#define custom_mlx5_work_requests_size(v)(v->tx_qp_dv.sq.wqe_cnt * v->tx_qp_dv.sq.stride)
#define custom_mlx5_work_requests_start(v)((char *)v->tx_qp_dv.sq.buf)
#define custom_mlx5_work_requests_end(v)((char *)(custom_mlx5_work_requests_start(v) + custom_mlx5_work_requests_size(v)))
#define custom_mlx5_get_work_request(v, idx)((char *)(v->tx_qp_dv.sq.buf + (idx << v->tx_sq_log_stride)))

#define custom_mlx5_completion_buffer_start(v)((char *)(v->pending_transmissions))
#define custom_mlx5_completion_buffer_end(v)(custom_mlx5_completion_buffer_start(v) + custom_mlx5_work_requests_size(v))
#define custom_mlx5_get_completion_segment(v, idx)((struct custom_mlx5_transmission_info *)(custom_mlx5_completion_buffer_start(v) + v->tx_qp_dv.sq.stride * idx))

#define custom_mlx5_incr_ring_buffer(ptr, start, end, type, ptr_type) \
    ((((char *)ptr + sizeof(type)) == end) ? ((ptr_type)start) : ((ptr_type)((char *)ptr + (sizeof(type)))) )

/*
 * Direct hardware queue support
 */

struct __attribute__((__packed__)) custom_mlx5_transmission_info {
    union {
        struct __attribute__((__packed__)) custom_mlx5_transmission_metadata {
            uint32_t num_wqes; // number of descriptors used by this transmission
            uint32_t num_mbufs; // number of mbufs to decrease the reference count on
        } metadata;
        struct custom_mlx5_mbuf *mbuf;
    } info;
};


#define custom_mlx5_incr_dpseg(v, dpseg) (custom_mlx5_incr_ring_buffer(dpseg,  custom_mlx5_work_requests_start(v), custom_mlx5_work_requests_end(v), struct mlx5_wqe_data_seg, struct mlx5_wqe_data_seg *))

#define custom_mlx5_incr_transmission_info(v, transmission) (custom_mlx5_incr_ring_buffer(transmission, custom_mlx5_completion_buffer_start(v), custom_mlx5_completion_buffer_end(v), struct custom_mlx5_transmission_info, struct custom_mlx5_transmission_info *))

const char *custom_mlx5_err_to_str(int no);

struct custom_mlx5_hardware_q {
	void		*descriptor_table;
	uint32_t	*consumer_idx;
	uint32_t	*shadow_tail;
	uint32_t	descriptor_log_size;
	uint32_t	nr_descriptors;
	uint32_t	parity_byte_offset;
	uint32_t	parity_bit_mask;
};

struct custom_mlx5_direct_txq {};

struct custom_mlx5_rxq {
    /* handle for runtime */
	struct custom_mlx5_hardware_q rxq;

	uint32_t consumer_idx;

	struct mlx5dv_cq rx_cq_dv;
	struct mlx5dv_rwq rx_wq_dv;
	uint32_t wq_head;
	uint32_t rx_cq_log_stride;
	uint32_t rx_wq_log_stride;

	void **buffers; // array of posted buffers


	struct ibv_cq_ex *rx_cq;
	struct ibv_wq *rx_wq;
	struct ibv_rwq_ind_table *rwq_ind_table;
	struct ibv_qp *qp;

    size_t rx_hw_drop;
} __aligned(CACHE_LINE_SIZE);

struct custom_mlx5_txq {
    /* handle for runtime */
	struct custom_mlx5_direct_txq txq;

	/* direct verbs qp */
	//struct custom_mlx5_mbuf **buffers; // pending DMA
    void **buffers;
    void *pending_transmissions;

	struct mlx5dv_qp tx_qp_dv;
	uint32_t sq_head;
	uint32_t tx_sq_log_stride;

	/* direct verbs cq */
	struct mlx5dv_cq tx_cq_dv;
	uint32_t cq_head;
    uint32_t true_cq_head;
	uint32_t tx_cq_log_stride;

	struct ibv_cq_ex *tx_cq;
	struct ibv_qp *tx_qp;
};

/* A registered memory pool. 
 * TODO: is it right for each mempool to have a unique registered / MR region.
 * Or can different `mempools` share the same backing registered region? */
struct registered_mempool {
    struct custom_mlx5_mempool data_mempool;
    struct custom_mlx5_mempool metadata_mempool;
    struct ibv_mr *mr; /* If this is null, this means the mempool isn't registered. */
    struct registered_mempool *next; /* Next allocated registered mempool in the list. */
};

void check_wqe_cnt(struct custom_mlx5_txq *v, size_t original_cnt);

void custom_mlx5_clear_registered_mempool(struct registered_mempool *mempool);

struct custom_mlx5_global_context {
    struct ibv_context *ibv_context; /* IBV Context */
    struct ibv_pd *pd; /* pd variable */
    size_t num_threads; /* Number of total threads */
    void *thread_contexts; /* Per thread contexts */
    struct eth_addr *our_eth;
    struct ibv_rwq_ind_table *rwq_ind_table;
    struct ibv_qp *qp;
};

/* Per core information:
 * receive queue
 * send queue
 * rx metadata pool / data pool
 * */
struct custom_mlx5_per_thread_context {
    size_t thread_id;
    struct custom_mlx5_global_context *global_context; /* Pointer back to the global context. */
    struct custom_mlx5_rxq rxq; /* Rxq for receiving packets. */
    struct custom_mlx5_txq txq; /* Txq for sending packets. */
    struct registered_mempool *rx_mempool; /* Receive mempool associated with the rxq. */
    struct custom_mlx5_mempool external_data_pool; /* Memory pool used for attaching external data.*/
};

/* Given index into threads array, get per thread context. */
struct custom_mlx5_per_thread_context *custom_mlx5_get_per_thread_context(struct custom_mlx5_global_context *context, size_t idx);

/* Clears state in per thread context. */
void custom_mlx5_clear_per_thread_context(struct custom_mlx5_global_context *context, size_t idx);

static inline unsigned int custom_mlx5_nr_inflight_tx(struct custom_mlx5_txq *v)
{
	return v->sq_head - v->true_cq_head;
}

/*
 * cqe_status - retrieves status of completion queue element
 * @cqe: pointer to element
 * @cqe_cnt: total number of elements
 * @idx: index as stored in head pointer
 *
 * returns CQE status enum (MLX5_CQE_INVALID is -1)
 */
static inline uint8_t custom_mlx5_cqe_status(struct mlx5_cqe64 *cqe, uint32_t cqe_cnt, uint32_t head)
{
	uint16_t parity = head & cqe_cnt;
	uint8_t op_own = ACCESS_ONCE(cqe->op_own);
	uint8_t op_owner = op_own & MLX5_CQE_OWNER_MASK;
	uint8_t op_code = (op_own & 0xf0) >> 4;

	return ((op_owner == !parity) * MLX5_CQE_INVALID) | op_code;
}

static inline int custom_mlx5_mlx5_csum_ok(struct mlx5_cqe64 *cqe)
{
	return ((cqe->hds_ip_ext & (MLX5_CQE_L4_OK | MLX5_CQE_L3_OK)) ==
		 (MLX5_CQE_L4_OK | MLX5_CQE_L3_OK)) &
		(((cqe->l4_hdr_type_etc >> 2) & 0x3) == MLX5_CQE_L3_HDR_TYPE_IPV4);
}

static inline int custom_mlx5_get_cqe_opcode(struct mlx5_cqe64 *cqe)
{
	return (cqe->op_own & 0xf0) >> 4;
}

static inline int custom_mlx5_get_cqe_format(struct mlx5_cqe64 *cqe)
{
	return (cqe->op_own & 0xc) >> 2;
}

static inline int custom_mlx5_get_error_syndrome(struct mlx5_cqe64 *cqe) {
    return ((struct mlx5_err_cqe *)cqe)->syndrome;
}

static inline uint32_t custom_mlx5_get_rss_result(struct mlx5_cqe64 *cqe)
{
	return ntoh32(*((uint32_t *)cqe + 3));
}
