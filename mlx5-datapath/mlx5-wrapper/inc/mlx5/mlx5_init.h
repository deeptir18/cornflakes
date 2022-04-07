/*
 * mlx5_init.h - initialization functions for datapath init and teardown.
 * */

#pragma once

#include <base/pci.h>
#include <base/debug.h>
#include <base/mempool.h>
#include <mlx5/mlx5.h>
#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>

/* Allocates global context. */
struct custom_mlx5_global_context *custom_mlx5_alloc_global_context(size_t num_threads);

/* Frees global context, including per thread context array. */
void custom_mlx5_free_global_context(struct custom_mlx5_global_context *context);

/* Allocates data and metadata for an mbuf from this registered mempool object. */
struct custom_mlx5_mbuf *custom_mlx5_allocate_data_and_metadata_mbuf(struct registered_mempool *mempool);

/* Allocate the data and metadata portions of the given memory pool. */
int custom_mlx5_allocate_mempool(struct registered_mempool *mempool,
                        size_t item_len,
                        size_t num_items,
                        size_t data_pgsize,
                        size_t metadata_pgsize);

/* Registers the mempool from the thread, doing nothing if the memory pool is
 * already not registered. */
int custom_mlx5_register_memory_pool_from_thread(struct custom_mlx5_per_thread_context *thread_context,
                                        struct registered_mempool *mempool,
                                        int flags);

/* Initializes the external data mempool */
/* Memory registration for a specific region of memory. */
int custom_mlx5_register_memory_pool(struct custom_mlx5_global_context *context,
                         struct registered_mempool *mempool,
                            int flags);

/* Create a data mempool, corresponding metadata mempool, and register the data
 * mempool and store information in registered_mempool object.*/
int custom_mlx5_reate_and_register_mempool(struct custom_mlx5_global_context *context, 
                                    struct registered_mempool *mempool,
                                    size_t item_len,
                                    size_t num_items,
                                    size_t data_pgsize,
                                    size_t metadata_pgsize,
                                    int registry_flags);

/* Unregisters the region backing this memory pool. */
int custom_mlx5_deregister_memory_pool(struct registered_mempool *mempool);


/* Unregisters region backing a memory pool, if necessary, and frees memory pool.*/
int custom_mlx5_deregister_and_free_registered_mempool(struct registered_mempool *mempool);

/* Initializes the rx mempools in each per thread context with the given params. */
int custom_mlx5_init_rx_mempools(struct custom_mlx5_global_context *context,
                        size_t item_len,
                        size_t num_items,
                        size_t data_pgsize,
                        size_t metadata_pgsize,
                        int registry_flags);

/* Tears down rx mempool state until a certain thread id.*/
int custom_mlx5_free_rx_mempools(struct custom_mlx5_global_context *context, size_t max_idx);

/* Initializes external metadata mempool per thread with given params. */
int custom_mlx5_init_external_mempools(struct custom_mlx5_global_context *context, 
                            size_t num_pages, 
                            size_t pgsize);

/* Just allocate a new tx pool. */
struct registered_mempool *custom_mlx5_alloc_tx_pool(struct custom_mlx5_per_thread_context *per_thread_context,
                                            size_t item_len,
                                            size_t num_items,
                                            size_t data_pgsize,
                                            size_t metadata_pgsize);

/* Allocate and register a new tx mempool. */
struct registered_mempool *custom_mlx5_alloc_and_register_tx_pool(struct custom_mlx5_per_thread_context *per_thread_context,
                                                        size_t item_len, 
                                                        size_t num_items, 
                                                        size_t data_pgsize,
                                                        size_t metadata_pgsize,
                                                        int registry_flags);

/* Deallocate a tx mempool, unregistering and freeing the backing memory if
 * necessary. */
int custom_mlx5_deallocate_tx_pool(struct custom_mlx5_per_thread_context *per_thread_context, struct registered_mempool *tx_mempool);

/* Tearsdown state in the mlx5 per thread context. 
 * Includes:
 *  Freeing rx mempool
 *  Freeing metadata mempool (if allocated)
 *  Freeing and deregistering any allocated tx mempools. */
int custom_mlx5_teardown(struct custom_mlx5_per_thread_context *per_thread_context);

/* Helper function borrowed from DPDK. */
int custom_mlx5_ibv_device_to_pci_addr(const struct ibv_device *device, struct custom_mlx5_pci_addr *pci_addr);

/* Initializes ibv context within global context. */
int custom_mlx5_init_ibv_context(struct custom_mlx5_global_context *global_context,
                        struct custom_mlx5_pci_addr *nic_pci_addr);

/* Queue steering initialization for rxqs within the global context. 
 * Requires rxqs for each thread to be initialized / allocated. */
int custom_mlx5_qs_init_flows(struct custom_mlx5_global_context *global_context, struct eth_addr *our_eth);

/* Individual rxq initialization. */
int custom_mlx5_init_rxq(struct custom_mlx5_per_thread_context *thread_context);

/* Individual txq initialization. */
int custom_mlx5_init_txq(struct custom_mlx5_per_thread_context *thread_context);

