#include <base/debug.h>
#include <base/mbuf.h>
#include <base/mempool.h>
#include <base/pci.h>
#include <base/rte_memcpy.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>
#include <mlx5/mlx5.h>
#include <mlx5/mlx5_init.h>
#include <net/ethernet.h>
#include <util/udma_barrier.h>
#include <util/mmio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**********************************************************************/
// STATIC STATE: symmetric RSS key
static uint8_t sym_rss_key[] = {
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
    0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A, 0x6D, 0x5A,
};
/**********************************************************************/

size_t custom_mlx5_get_registered_mempool_size() {
    return sizeof(struct registered_mempool);
}

size_t custom_mlx5_get_global_context_size() {
    return sizeof(struct custom_mlx5_global_context);
}

size_t custom_mlx5_get_per_thread_context_size(size_t num_threads) {
    return sizeof(struct custom_mlx5_per_thread_context) * num_threads; 
}

void *custom_mlx5_get_raw_threads_ptr(struct custom_mlx5_global_context *global_context) {
    return global_context->thread_contexts;
}
 
void custom_mlx5_alloc_global_context(size_t num_threads, unsigned char *global_context_ptr, unsigned char *per_thread_info) {
    struct custom_mlx5_global_context *context = (struct custom_mlx5_global_context *)global_context_ptr;
    context->num_threads = num_threads;
    context->thread_contexts = per_thread_info;
    for (size_t i = 0; i < num_threads; i++) {
        custom_mlx5_clear_per_thread_context(context, i);
        struct custom_mlx5_per_thread_context *t_context = custom_mlx5_get_per_thread_context(context, i);
        t_context->thread_id = i;
        t_context->global_context = context;
    }
}

void custom_mlx5_set_rx_mempool_ptr(struct custom_mlx5_global_context *global_context, 
                                        size_t thread_id, 
                                        struct registered_mempool *rx_mempool_ptr) {
    NETPERF_DEBUG("Setting rx ptr: %p", rx_mempool_ptr);
    struct custom_mlx5_per_thread_context *t_context = custom_mlx5_get_per_thread_context(global_context, thread_id);
    t_context->rx_mempool = rx_mempool_ptr;
    custom_mlx5_clear_registered_mempool(t_context->rx_mempool);
}

const char *custom_mlx5_err_to_str(int no) {
    return strerror(no);
}

struct custom_mlx5_per_thread_context *custom_mlx5_get_per_thread_context(struct custom_mlx5_global_context *context, size_t idx) {
    NETPERF_ASSERT(idx < context->num_threads, "Accessing thread greater than total allocated threads.");
    return (struct custom_mlx5_per_thread_context *)((char *)context->thread_contexts + idx * custom_mlx5_get_per_thread_context_size(1));
}

void custom_mlx5_clear_registered_mempool(struct registered_mempool *mempool) {
    mempool->mr = NULL;
    custom_mlx5_clear_mempool(&mempool->data_mempool);
}


void custom_mlx5_clear_per_thread_context(struct custom_mlx5_global_context *context, size_t idx) {
    struct custom_mlx5_per_thread_context *per_thread_context = custom_mlx5_get_per_thread_context(context, idx);
    per_thread_context->global_context = NULL;
    memset(&per_thread_context->rxq, 0, sizeof(struct custom_mlx5_rxq));
    memset(&per_thread_context->txq, 0, sizeof(struct custom_mlx5_txq));
}

int custom_mlx5_allocate_mempool(struct registered_mempool *mempool,
                    size_t item_len,
                    size_t num_items,
                    size_t data_pgsize,
                    uint32_t use_atomic_ops) {
    int ret = 0;
    size_t total_data_len = item_len * num_items;
    if (total_data_len % data_pgsize != 0) {
        NETPERF_ERROR("Invalid params to create mempool: (%lu x %lu) not aligned to pgsize %lu", item_len, num_items, data_pgsize);           
        return -EINVAL;
    }

    // create the data mempool
    ret = custom_mlx5_mempool_create(&mempool->data_mempool, total_data_len, data_pgsize, item_len, use_atomic_ops);
    if (ret != 0) {
        NETPERF_ERROR("Data Mempool create failed: %s", strerror(-ret));
        return ret;
    }

    return 0;
}

int register_memory_pool_from_thread(struct custom_mlx5_per_thread_context *thread_context, 
        struct registered_mempool *mempool, 
        int flags) {
    if (custom_mlx5_is_registered(&(mempool->data_mempool))) {
        return 0;
    } else {
        void *buf = (&(mempool->data_mempool))->buf;
        size_t len = (&(mempool->data_mempool))->len;
        mempool->mr = ibv_reg_mr(thread_context->global_context->pd, buf, len, flags);
        if (!mempool->mr) {
            NETPERF_ERROR("Failed to do memory reg for region %p len %lu: %s", buf, len, strerror(errno));
            return -errno;
        }
        custom_mlx5_register_mempool(&mempool->data_mempool, mempool->mr->lkey);
        return 0;
    }

}

int custom_mlx5_register_memory_pool(struct custom_mlx5_global_context *context,
                            struct registered_mempool *mempool,
                            int flags) {
    void *buf = (&(mempool->data_mempool))->buf;
    size_t len = (&(mempool->data_mempool))->len;
    mempool->mr = ibv_reg_mr(context->pd, buf, len, flags);
    if (!mempool->mr) {
        NETPERF_ERROR("Failed to do memory reg for region %p len %lu: %s", buf, len, strerror(errno));
        return -errno;
    }
    NETPERF_DEBUG("Registering mempool %p; with lkey %u", mempool, mempool->mr->lkey);
    custom_mlx5_register_mempool(&mempool->data_mempool, mempool->mr->lkey);
    return 0;
}

int custom_mlx5_create_and_register_mempool(struct custom_mlx5_global_context *context, 
                                    struct registered_mempool *mempool,
                                    size_t item_len,
                                    size_t num_items,
                                    size_t data_pgsize,
                                    int registry_flags,
                                    uint32_t use_atomic_ops) {
    int ret = 0;

    // allocate data portions of mempool
    ret = custom_mlx5_allocate_mempool(mempool, item_len, num_items, data_pgsize, use_atomic_ops);
    if (ret != 0) {
        NETPERF_ERROR("Mempool creation failed: %s", strerror(-ret));
        return ret;
    }

    // register the data memory pool
    ret = custom_mlx5_register_memory_pool(context, mempool, registry_flags);
    if (ret != 0) {
        NETPERF_ERROR("Mempool registration failed: %s", strerror(-ret));
        // free the mempool just created
        custom_mlx5_mempool_destroy(&mempool->data_mempool);
        return ret;
    }

    NETPERF_DEBUG("Successfully created and registered memory pool %p", mempool);
    return 0;
}

int custom_mlx5_deregister_memory_pool(struct registered_mempool *mempool) {
    struct custom_mlx5_mempool *data_mempool = &mempool->data_mempool;
    if (!custom_mlx5_is_registered(&(mempool->data_mempool))) {
        return 0;
    }
    struct ibv_mr *mr = mempool->mr;
    int ret = ibv_dereg_mr(mr);
    if (ret != 0) {
        NETPERF_ERROR("Failed to dereg memory region with lkey %d: %s", mempool->mr->lkey, strerror(errno));            
        return -errno;
    }
    mr = NULL;
    custom_mlx5_deregister_mempool(data_mempool);
    return 0;
}

int custom_mlx5_deregister_and_free_registered_mempool(struct registered_mempool *mempool) {
    int ret = 0;
    // unregister mempool if necessary 
    if (custom_mlx5_is_registered(&mempool->data_mempool)) {
        ret = custom_mlx5_deregister_memory_pool(mempool);
        if (ret != 0) {
            NETPERF_ERROR("Failed to dereg memory region with lkey %d: %s", (&(mempool->data_mempool))->lkey, strerror(-errno));
            return errno;
        }
    }
    // unallocate mempool
    if (custom_mlx5_is_allocated(&mempool->data_mempool)) {
        custom_mlx5_mempool_destroy(&mempool->data_mempool);
    }

    return 0;
}

int custom_mlx5_init_rx_mempools(struct custom_mlx5_global_context *context, 
                        size_t item_len, 
                        size_t num_items, 
                        size_t data_pgsize, 
                        int registry_flags) {
    int ret = 0;
    for (int i = 0; i < context->num_threads; i++) {
        struct custom_mlx5_per_thread_context *thread_context = custom_mlx5_get_per_thread_context(context, i);
        struct registered_mempool *mempool = thread_context->rx_mempool;
        NETPERF_DEBUG("Initializing rx mempool at %p", thread_context->rx_mempool);
        ret = custom_mlx5_create_and_register_mempool(context, mempool, item_len, num_items, data_pgsize, registry_flags, false);
        if (ret != 0) {
            NETPERF_ERROR("Creation of rx mempool for thread %d failed: %s", i, strerror(-ret));
            RETURN_ON_ERR(free_rx_mempools(context, i), "Cleanup of rx mempools failed");
        }
    }
    return 0;
}

int custom_mlx5_free_rx_mempools(struct custom_mlx5_global_context *context, size_t max_idx) {
    int ret = 0;
    for (size_t i = 0; i < max_idx; i++) {
        struct custom_mlx5_per_thread_context *per_thread_context = custom_mlx5_get_per_thread_context(context, i);
        ret = custom_mlx5_deregister_and_free_registered_mempool(per_thread_context->rx_mempool);
        if (ret != 0) {
            NETPERF_ERROR("Error freeing rx mempool for thread %lu", i);
        }
    }
    return 0;
}

int custom_mlx5_alloc_tx_pool(struct registered_mempool *mempool,
                                                size_t item_len,
                                                size_t num_items,
                                                size_t data_pgsize,
                                                uint32_t use_atomic_ops) {

    int ret = 0;
    custom_mlx5_clear_mempool(&(mempool->data_mempool));
    if (mempool == NULL) {
        NETPERF_WARN("no memory to malloc registered mempool node");
        return -ENOMEM;
    }

    ret = custom_mlx5_allocate_mempool(mempool, item_len, num_items, data_pgsize, use_atomic_ops);
    if (ret != 0) {
        NETPERF_WARN("Failed to allocate mempool: %s", strerror(-ret));
        return ret;
    }
    
    return 0;
}

int custom_mlx5_alloc_and_register_tx_pool(struct custom_mlx5_per_thread_context *per_thread_context,
                                                        struct registered_mempool *mempool,
                                                        size_t item_len,
                                                        size_t num_items,
                                                        size_t data_pgsize,
                                                        int registry_flags,
                                                        uint32_t use_atomic_ops) {
    int ret = custom_mlx5_alloc_tx_pool(mempool, item_len, num_items, data_pgsize, use_atomic_ops);
    if (ret != 0) {
        NETPERF_WARN("Could not allocate registered mempool: %s", strerror(-ret));
        return 0;
    }

    // register this newly created mempool
    ret = custom_mlx5_register_memory_pool(per_thread_context->global_context, mempool, registry_flags);
    if (ret != 0) {
        NETPERF_WARN("Failed to register tx memory pool: %s", strerror(-ret));
        return ret;
    }
    return 0;
}

uint16_t custom_mlx5_refcnt_read(struct registered_mempool *mempool,
        size_t refcnt_index) {
    struct custom_mlx5_mempool *data_mempool = &mempool->data_mempool;
    return __atomic_load_n(&data_mempool->ref_counts[refcnt_index], __ATOMIC_RELAXED);
}

int custom_mlx5_refcnt_update_or_free(struct registered_mempool *mempool, 
        void *buf, 
        size_t refcnt_index, 
        int8_t change) {
    //NETPERF_INFO("Calling refcnt change with %d for mempool %p, mempool, data_mempool %p, mempool buf %p, buf %p, refcnt_index %lu; old: %u, new: %u", change, mempool, &mempool->data_mempool, mempool->data_mempool.buf, buf, refcnt_index, mempool->data_mempool.ref_counts[refcnt_index], mempool->data_mempool.ref_counts[refcnt_index] + change);
    struct custom_mlx5_mempool *data_mempool = &mempool->data_mempool;
    if (data_mempool->use_atomic_ops) {
        // must use atomics to update this as potentially accessing from
        // multiple threads
        NETPERF_DEBUG("Original refcnt: %u, change: %d", custom_mlx5_refcnt_read(mempool, refcnt_index), change);
        if (change > 0) {
            __atomic_add_fetch(&data_mempool->ref_counts[refcnt_index], (uint16_t)change, __ATOMIC_ACQ_REL);
        NETPERF_DEBUG("Resulting refcnt: %u", custom_mlx5_refcnt_read(mempool, refcnt_index));
        } else {
            uint16_t refcnt = __atomic_sub_fetch(&data_mempool->ref_counts[refcnt_index], (uint16_t)(change * -1), __ATOMIC_ACQ_REL);
        NETPERF_DEBUG("Resulting refcnt: %u", custom_mlx5_refcnt_read(mempool, refcnt_index));
            if (refcnt == 0) {
                custom_mlx5_mempool_free(data_mempool, buf);
            }
        }
    } else {
        data_mempool->ref_counts[refcnt_index] += change;
        if (data_mempool->ref_counts[refcnt_index] == 0) {
            custom_mlx5_mempool_free(data_mempool, buf);
        }
    }
    return 0;
}

int custom_mlx5_teardown(struct custom_mlx5_per_thread_context *per_thread_context) {
    // free the buffers allocated in the txq and rxq
    free((&per_thread_context->txq)->buffers);
    free((&per_thread_context->txq)->pending_transmissions);
    free((&per_thread_context->rxq)->rx_buffers);
    return 0;
}

/* 
 * pci_str_to_addr - converts is string to a PCI address
 * @str: the input string
 * @addr: a pointer to the output address
 *
 * String format is DDDD:BB:SS.f, where D = domain (hex), B = bus (hex),
 * S = slot (hex), and f = function number (decimal).
 *
 * Returns 0 if successful, otherwise failure.
 */
int custom_mlx5_pci_str_to_addr(const char *str, struct custom_mlx5_pci_addr *addr)
{
	int ret;

	ret = sscanf(str, "%04hx:%02hhx:%02hhx.%hhd",
		     &addr->domain, &addr->bus,
		     &addr->slot, &addr->func);

	if (ret != 4)
		return -EINVAL;
	return 0;
}

int
custom_mlx5_ibv_device_to_pci_addr(const struct ibv_device *device,
                                   struct custom_mlx5_pci_addr *pci_addr)
{
    FILE *file;
    char line[32];
    char path[strlen(device->ibdev_path) + strlen("/device/uevent") + 1];
    snprintf(path, sizeof(path), "%s/device/uevent", device->ibdev_path);
    NETPERF_INFO("Path to open: %s", path);

    file = fopen(path, "rb");
    if (!file)
        return -errno;

    while (fgets(line, sizeof(line), file) == line) {
        size_t len = strlen(line);
        int ret;

        /* Truncate long lines. */
        if (len == (sizeof(line) - 1)) {
            while (line[(len - 1)] != '\n') {
                ret = fgetc(file);
                if (ret == EOF)
                    break;
                line[(len - 1)] = ret;
            }
        }
        /* Extract information. */
        if (sscanf(line,
            "PCI_SLOT_NAME="
                "%04hx:%02hhx:%02hhx.%hhd\n",
                &pci_addr->domain,
                &pci_addr->bus,
                &pci_addr->slot,
                &pci_addr->func) == 4) {
            NETPERF_INFO("breaking Domain: %u, bus: %u", pci_addr->domain, pci_addr->bus);
            break;
        }
    }
    fclose(file);
    return 0;
}

int custom_mlx5_init_ibv_context(struct custom_mlx5_global_context *global_context,
                            struct custom_mlx5_pci_addr *nic_pci_addr) {
    NETPERF_INFO("Domain: %u, bus: %u, slot: %u, func: %u\n", nic_pci_addr->domain, nic_pci_addr->bus, nic_pci_addr->slot, nic_pci_addr->func);
    int i = 0;
    int ret = 0;
    
    struct ibv_device **dev_list;
    struct mlx5dv_context_attr attr = {0};

    struct custom_mlx5_pci_addr pci_addr;
        
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        NETPERF_ERROR("Failed to get IB devices list");
        return -1;
    }

    for (i = 0; dev_list[i]; i++) {
        if (strncmp(ibv_get_device_name(dev_list[i]), "mlx5", 4)) {
            continue;
        }

        if (custom_mlx5_ibv_device_to_pci_addr(dev_list[i], &pci_addr)) {
            NETPERF_WARN("failed to read pci addr for %s, skipping",
                            ibv_get_device_name(dev_list[i]));
            continue;
        }

        if (memcmp(&pci_addr, nic_pci_addr, sizeof(pci_addr)) == 0)
            break;
    }

    if (!dev_list[i]) {
        NETPERF_ERROR("mlx5_init: IB device not found");
        return -1;
    }

    attr.flags = 0;
    global_context->ibv_context = mlx5dv_open_device(dev_list[i], &attr);
    if (!(global_context->ibv_context)) {
        NETPERF_ERROR("mlx5_init: Couldn't get context for %s (errno %d)",
        ibv_get_device_name(dev_list[i]), errno);
        return -1;
    }

    /*ret = mlx5dv_set_context_attr(context,
                MLX5DV_CTX_ATTR_BUF_ALLOCATORS, &dv_allocators);
    if (ret) {
        NETPERF_ERROR("mlx5_init: error setting memory allocator");
        return -1;
    }*/

    ibv_free_device_list(dev_list);

    global_context->pd = ibv_alloc_pd(global_context->ibv_context);
    if (!(global_context->pd)) {
        NETPERF_ERROR("mlx5_init: Couldn't allocate PD");
        return -1;
    }

    return ret;
}

int custom_mlx5_qs_init_flows(struct custom_mlx5_global_context *global_context,
                        struct eth_addr *our_eth) {
    // this needs to be a power of 4
    // TODO: figure out exactly what this controls
    size_t SIZE = 4;
    struct ibv_wq *ind_tbl[SIZE];
    for (size_t i = 0; i < SIZE; i++) {
        struct custom_mlx5_per_thread_context *t_context = custom_mlx5_get_per_thread_context(global_context, i % global_context->num_threads);
        struct custom_mlx5_rxq *rxq = &(t_context->rxq);
        ind_tbl[i] = rxq->rx_wq;
    }
	
    struct ibv_rwq_ind_table_init_attr rwq_attr = {0};
	rwq_attr.ind_tbl = ind_tbl;
    rwq_attr.log_ind_tbl_size = __builtin_ctz(SIZE);
    rwq_attr.comp_mask = 0;
	global_context->rwq_ind_table = ibv_create_rwq_ind_table(global_context->ibv_context, &rwq_attr);
	if (!global_context->rwq_ind_table) {
        NETPERF_WARN("Failed to create rx indirection table");
		return -errno;
    }

	struct ibv_rx_hash_conf rss_cnf = {
		.rx_hash_function = IBV_RX_HASH_FUNC_TOEPLITZ,
		.rx_hash_key_len = ARRAY_SIZE(sym_rss_key),
		.rx_hash_key = sym_rss_key,
		.rx_hash_fields_mask = IBV_RX_HASH_SRC_IPV4 | IBV_RX_HASH_DST_IPV4 | IBV_RX_HASH_SRC_PORT_UDP | IBV_RX_HASH_DST_PORT_UDP,
	};

	struct ibv_qp_init_attr_ex qp_ex_attr = {
		.qp_type = IBV_QPT_RAW_PACKET,
		.comp_mask = IBV_QP_INIT_ATTR_RX_HASH | IBV_QP_INIT_ATTR_IND_TABLE | IBV_QP_INIT_ATTR_PD,
		.pd = global_context->pd,
		.rwq_ind_tbl = global_context->rwq_ind_table,
		.rx_hash_conf = rss_cnf,
	};

	global_context->qp = ibv_create_qp_ex(global_context->ibv_context, &qp_ex_attr);
	if (!global_context->qp) {
        NETPERF_WARN("Failed to create rx qp");
		return -errno;
    }

    /* *Register sterring rules to intercept packets to our mac address and
     * place packet in ring pointed by v->qp */
    struct raw_eth_flow_attr {
        struct ibv_flow_attr attr;
        struct ibv_flow_spec_eth spec_eth;
    } __attribute__((packed)) flow_attr = {
        .attr = {
            .comp_mask = 0,
            .type = IBV_FLOW_ATTR_NORMAL,
            .size = sizeof(flow_attr),
            .priority = 0,
            .num_of_specs = 1,
            .port = PORT_NUM, // what port is this? dpdk port?
            .flags = 0,
        },
        .spec_eth = {
            .type = IBV_FLOW_SPEC_ETH,
            .size = sizeof(struct ibv_flow_spec_eth),
            .val = {
                //.src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                .ether_type = 0,
                .vlan_tag = 0,
            },
            .mask = {
                .dst_mac = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
                .src_mac = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
                .ether_type = 0,
                .vlan_tag = 0,
            }
        }
    };
    // TODO: does turning this off prevent throughput past 80 Gbps?
    // E.g., having the NIC check that flows have the source mac address
    custom_mlx5_rte_memcpy(&flow_attr.spec_eth.val.dst_mac, our_eth, 6);
    struct ibv_flow *eth_flow = ibv_create_flow(global_context->qp, &flow_attr.attr);
    if (!eth_flow) {
        NETPERF_ERROR("Not able to create eth_flow: %s", strerror(errno));
        return -errno;
    }

    return 0;
    
}

int custom_mlx5_init_rxq(struct custom_mlx5_per_thread_context *thread_context) {
    int i, ret;
    struct custom_mlx5_global_context *global_context = thread_context->global_context;
    struct custom_mlx5_rxq *v = &(thread_context->rxq);

	/* Create a CQ */
	struct ibv_cq_init_attr_ex cq_attr = {
		.cqe = RQ_NUM_DESC,
		.channel = NULL,
		.comp_vector = 0,
		.wc_flags = IBV_WC_EX_WITH_BYTE_LEN,
		.comp_mask = IBV_CQ_INIT_ATTR_MASK_FLAGS,
		.flags = IBV_CREATE_CQ_ATTR_SINGLE_THREADED,
	};
	struct mlx5dv_cq_init_attr dv_cq_attr = {
		.comp_mask = 0,
	};
	v->rx_cq = mlx5dv_create_cq(global_context->ibv_context, &cq_attr, &dv_cq_attr);
	if (!v->rx_cq) {
        NETPERF_WARN("Failed to create rx cq");
        return -errno;
    }

	/* Create the work queue for RX */
	struct ibv_wq_init_attr wq_init_attr = {
		.wq_type = IBV_WQT_RQ,
		.max_wr = RQ_NUM_DESC,
		.max_sge = 1,
		.pd = global_context->pd,
		.cq = ibv_cq_ex_to_cq(v->rx_cq),
		.comp_mask = 0,
		.create_flags = 0,
	};
	struct mlx5dv_wq_init_attr dv_wq_attr = {
		.comp_mask = 0,
	};
	v->rx_wq = mlx5dv_create_wq(global_context->ibv_context, &wq_init_attr, &dv_wq_attr);
	if (!v->rx_wq) {
        NETPERF_ERROR("Failed to create rx work queue");
        return -errno;
    }
    	
    if (wq_init_attr.max_wr != RQ_NUM_DESC) {
		NETPERF_WARN("Ring size is larger than anticipated");
    }

	/* Set the WQ state to ready */
	struct ibv_wq_attr wq_attr = {0};
	wq_attr.attr_mask = IBV_WQ_ATTR_STATE;
	wq_attr.wq_state = IBV_WQS_RDY;
	ret = ibv_modify_wq(v->rx_wq, &wq_attr);
	if (ret) {
        NETPERF_WARN("Could not modify wq with wq_attr while setting up rx queue")
		return -ret;
    }

	/* expose direct verbs objects */
	struct mlx5dv_obj obj = {
		.cq = {
			.in = ibv_cq_ex_to_cq(v->rx_cq),
			.out = &v->rx_cq_dv,
		},
		.rwq = {
			.in = v->rx_wq,
			.out = &v->rx_wq_dv,
		},
	};
	ret = mlx5dv_init_obj(&obj, MLX5DV_OBJ_CQ | MLX5DV_OBJ_RWQ);
	if (ret) {
        NETPERF_WARN("Failed to init rx mlx5dv_obj");
		return -ret;
    }

	NETPERF_PANIC_ON_TRUE(!is_power_of_two(v->rx_wq_dv.stride), "Stride not power of two; stride: %d", v->rx_wq_dv.stride);
	NETPERF_PANIC_ON_TRUE(!is_power_of_two(v->rx_cq_dv.cqe_size), "CQE size not power of two");
	v->rx_wq_log_stride = __builtin_ctz(v->rx_wq_dv.stride);
	v->rx_cq_log_stride = __builtin_ctz(v->rx_cq_dv.cqe_size);

	/* allocate list of posted buffers */
	v->rx_buffers = aligned_alloc(CACHE_LINE_SIZE, v->rx_wq_dv.wqe_cnt * v->rx_wq_dv.stride);
	if (!v->rx_buffers) {
        NETPERF_WARN("Failed to alloc rx posted buffers");
		return -ENOMEM;
    }
	
    v->rxq.consumer_idx = &v->consumer_idx;
    NETPERF_DEBUG("Consumer idx: %u", *(v->rxq.consumer_idx));
	v->rxq.descriptor_table = v->rx_cq_dv.buf;
	v->rxq.nr_descriptors = v->rx_cq_dv.cqe_cnt;
	v->rxq.descriptor_log_size = __builtin_ctz(sizeof(struct mlx5_cqe64));
	v->rxq.parity_byte_offset = offsetof(struct mlx5_cqe64, op_own);
	v->rxq.parity_bit_mask = MLX5_CQE_OWNER_MASK;

	/* set byte_count and lkey for all descriptors once */
	struct mlx5dv_rwq *wq = &v->rx_wq_dv;
    struct registered_mempool *mempool = thread_context->rx_mempool;
	for (i = 0; i < wq->wqe_cnt; i++) {
		struct mlx5_wqe_data_seg *seg = wq->buf + i * wq->stride;

		/* fill queue with buffers */
        void *data = custom_mlx5_mempool_alloc(&mempool->data_mempool);
        if (data == NULL) {
            NETPERF_WARN("Could not allocate data mbuf to refill rx queue");
            return -ENOMEM;
        }
        int index = custom_mlx5_mempool_find_index(&mempool->data_mempool, data);
        if (index == -1 ) {
            NETPERF_WARN("Calculated index out of bounds for data %p mempool buf: %p, mempool len:   %lu", data, (&mempool->data_mempool)->buf, (&mempool->data_mempool)->len);
            custom_mlx5_mempool_free(&mempool->data_mempool, data);
            return -EINVAL;
        }
        struct ibv_mr *mr = mempool->mr;
		seg->byte_count =  htobe32(mempool->data_mempool.item_len);
		seg->lkey = htobe32(mr->lkey);
		seg->addr = htobe64((unsigned long)(data));

        struct custom_mlx5_rx_buffer *rx_ring_entry = custom_mlx5_get_rx_buffers_segment(v, i);
        rx_ring_entry->buf_addr = data;
        rx_ring_entry->mempool = mempool;
		v->wq_head++;
	}
    NETPERF_DEBUG("After init, allocated in rx mempool is: %lu, capacity: %lu", mempool->data_mempool.allocated, mempool->data_mempool.capacity);

	/* set ownership of cqes to "hardware" */
	struct mlx5dv_cq *cq = &v->rx_cq_dv;
	for (i = 0; i < cq->cqe_cnt; i++) {
		struct mlx5_cqe64 *cqe = cq->buf + i * cq->cqe_size;
		mlx5dv_set_cqe_owner(cqe, 1);
	}

	udma_to_device_barrier();
	wq->dbrec[0] = htobe32(v->wq_head & 0xffff);

    return 0;
}

int custom_mlx5_init_txq(struct custom_mlx5_per_thread_context *thread_context) {
    int ret = 0;
    struct custom_mlx5_global_context *global_context = thread_context->global_context;
    struct custom_mlx5_txq *v = &thread_context->txq;

	/* Create a CQ */
	struct ibv_cq_init_attr_ex cq_attr = {
		.cqe = SQ_NUM_DESC,
		.channel = NULL,
		.comp_vector = 0,
		.wc_flags = 0,
		.comp_mask = IBV_CQ_INIT_ATTR_MASK_FLAGS,
		.flags = IBV_CREATE_CQ_ATTR_SINGLE_THREADED,
	};
	struct mlx5dv_cq_init_attr dv_cq_attr = {
		.comp_mask = 0,
	};
	v->tx_cq = mlx5dv_create_cq(global_context->ibv_context, &cq_attr, &dv_cq_attr);
	if (!v->tx_cq) {
        NETPERF_WARN("Could not create tx cq: %s", strerror(errno));
		return -errno;
    }


	/* Create a 1-sided queue pair for sending packets */
    // TODO: understand the relationship between max_send_sge and how much it's
    // possible to actually scatter-gather
	struct ibv_qp_init_attr_ex qp_init_attr = {
		.send_cq = ibv_cq_ex_to_cq(v->tx_cq),
		.recv_cq = ibv_cq_ex_to_cq(v->tx_cq),
		.cap = {
			.max_send_wr = SQ_NUM_DESC,
			.max_recv_wr = 0,
			.max_send_sge = 1, // TODO: does TX scatter-gather still work if this is 1?
			.max_inline_data = MAX_INLINE_DATA,
		},
		.qp_type = IBV_QPT_RAW_PACKET,
		.sq_sig_all = 1,
		.pd = global_context->pd,
		.comp_mask = IBV_QP_INIT_ATTR_PD
	};
	struct mlx5dv_qp_init_attr dv_qp_attr = {
		.comp_mask = 0,
	};
	v->tx_qp = mlx5dv_create_qp(global_context->ibv_context, &qp_init_attr, &dv_qp_attr);
	if (!v->tx_qp) {
        NETPERF_WARN("Could not create tx qp: %s", strerror(errno));
		return -errno;
    }

	/* Turn on TX QP in 3 steps */
    // TODO: why are these three steps required
	struct ibv_qp_attr qp_attr;
	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state = IBV_QPS_INIT;
	qp_attr.port_num = 1;
	ret = ibv_modify_qp(v->tx_qp, &qp_attr, IBV_QP_STATE | IBV_QP_PORT);
	if (ret) {
        NETPERF_WARN("Could not modify tx qp for IBV_QPS_INIT (1st step)");
		return -ret;
    }

	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state = IBV_QPS_RTR;
	ret = ibv_modify_qp(v->tx_qp, &qp_attr, IBV_QP_STATE);
	if (ret) {
        NETPERF_WARN("Could not modify tx_qp for IBV_QPS_RTR (2nd step)");
		return -ret;
    }

	memset(&qp_attr, 0, sizeof(qp_attr));
	qp_attr.qp_state = IBV_QPS_RTS;
	ret = ibv_modify_qp(v->tx_qp, &qp_attr, IBV_QP_STATE);
	if (ret) {
        NETPERF_WARN("Could not modify tx_qp for IBV_QPS_RTS (3rd step)");
		return -ret;
    }

	struct mlx5dv_obj obj = {
		.cq = {
			.in = ibv_cq_ex_to_cq(v->tx_cq),
			.out = &v->tx_cq_dv,
		},
		.qp = {
			.in = v->tx_qp,
			.out = &v->tx_qp_dv,
		},
	};
	ret = mlx5dv_init_obj(&obj, MLX5DV_OBJ_CQ | MLX5DV_OBJ_QP);
	if (ret) {
        NETPERF_WARN("Could not init mlx5dv_obj");
		return -ret;
    }

	NETPERF_PANIC_ON_TRUE(!is_power_of_two(v->tx_cq_dv.cqe_size), "tx cqe_size not power of two");
	NETPERF_PANIC_ON_TRUE(!is_power_of_two(v->tx_qp_dv.sq.stride), "tx stride size not power of two");
	v->tx_sq_log_stride = __builtin_ctz(v->tx_qp_dv.sq.stride);
	v->tx_cq_log_stride = __builtin_ctz(v->tx_cq_dv.cqe_size);

    NETPERF_WARN("Wqe cnt of tx qp: %u, cqe cnt: %u", v->tx_qp_dv.sq.wqe_cnt, v->tx_cq_dv.cqe_cnt);

	/* allocate list of posted buffers */
    v->buffers = aligned_alloc(CACHE_LINE_SIZE, v->tx_qp_dv.sq.wqe_cnt * sizeof(*v->buffers));
    if (!v->buffers) {
        NETPERF_WARN("Could not alloc tx wqe buffers");
        return -ENOMEM;
    }
    memset(v->buffers, 0, v->tx_qp_dv.sq.wqe_cnt * sizeof(*v->buffers));

    // completion information ring buffer. should be same size as the ring
    // buffer.
    v->pending_transmissions = aligned_alloc(CACHE_LINE_SIZE, v->tx_qp_dv.sq.wqe_cnt * v->tx_qp_dv.sq.stride);

    if (!v->pending_transmissions) {
        NETPERF_WARN("Could not alloc completion info array");
        return -ENOMEM;
    }
    memset(v->pending_transmissions, 0, v->tx_qp_dv.sq.wqe_cnt * v->tx_qp_dv.sq.stride);
    NETPERF_DEBUG("wqe cnt: %u, in flight: %u", v->tx_qp_dv.sq.wqe_cnt, custom_mlx5_nr_inflight_tx(v));

    return 0;
}
